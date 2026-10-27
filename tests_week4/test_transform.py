"""Run using spark-submit inside the week4 service; no Kafka or DB mutation."""
import json
from pyspark.sql import SparkSession, types as T
from scripts.week4_fixture import records
from src.process_edits import transform


def main():
    spark = (SparkSession.builder.master('local[2]').appName('week4-transform-tests')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.ansi.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        schema = T.StructType([
            T.StructField('message_key', T.StringType()),
            T.StructField('raw_value', T.StringType()),
            T.StructField('source_topic', T.StringType()),
            T.StructField('source_partition', T.IntegerType()),
            T.StructField('source_offset', T.LongType()),
        ])
        items = records()
        no_zone = json.loads(json.dumps(items[0]))
        no_zone['event_time'] = '2026-10-25T19:04:24'
        wrong_type = json.loads(json.dumps(items[0]))
        wrong_type['payload']['bot'] = 'not-a-boolean'
        negative = json.loads(json.dumps(items[0]))
        negative['payload']['length']['new'] = -1
        items.extend([no_zone, wrong_type, negative])
        raw = [( 'kowiki:테스트 문서', item if isinstance(item, str) else json.dumps(item), 'test', 0, i)
               for i, item in enumerate(items)]
        result = transform(spark.createDataFrame(raw, schema)).orderBy('source_offset').collect()
        assert len(result) == 10
        assert sum(r.error_reason is None for r in result) == 4
        assert result[0].event_time.isoformat() == '2026-10-25T10:04:24'
        assert result[0].bytes_delta == 30
        assert result[2].bytes_delta is None
        assert result[3].bytes_delta == -20
        assert [result[i].error_reason for i in range(4, 10)] == [
            'invalid_json_or_types', 'invalid_event_time', 'unsupported_schema_version',
            'invalid_event_time', 'invalid_json_or_types', 'negative_length',
        ]
        wrong_key = spark.createDataFrame([('bad-key', json.dumps(items[0]), 'test', 0, 100)], schema)
        assert transform(wrong_key).first().error_reason == 'key_mismatch'
        print('PASS: 11 records checked for schema, UTC, nulls, signed delta, invalid types, version and key')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
