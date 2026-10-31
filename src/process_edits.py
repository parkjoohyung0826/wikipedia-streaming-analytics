"""Week 4: Kafka -> Spark validation -> idempotent PostgreSQL -> Parquet snapshot."""
import argparse
import os
from pathlib import Path

import psycopg2
from pyspark.sql import SparkSession, functions as F, types as T


ENVELOPE_SCHEMA = T.StructType([
    T.StructField('event_id', T.StringType()),
    T.StructField('event_type', T.StringType()),
    T.StructField('schema_version', T.IntegerType()),
    T.StructField('event_time', T.StringType()),
    T.StructField('ingest_time', T.StringType()),
    T.StructField('source', T.StringType()),
    T.StructField('payload', T.StructType([
        T.StructField('wiki', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('bot', T.BooleanType()),
        T.StructField('type', T.StringType()),
        T.StructField('server_name', T.StringType()),
        T.StructField('length', T.StructType([
            T.StructField('old', T.LongType()),
            T.StructField('new', T.LongType()),
        ])),
    ])),
    T.StructField('_corrupt_record', T.StringType()),
])
EDIT_COLUMNS = [
    'event_id', 'event_type', 'schema_version', 'event_time', 'ingest_time',
    'source', 'wiki', 'title', 'bot', 'old_length', 'new_length', 'bytes_delta',
    'source_topic', 'source_partition', 'source_offset',
]


def connect():
    # libpq reads PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE from environment.
    conn = psycopg2.connect(connect_timeout=15)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()
    return conn


def initialize_database():
    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(Path('sql/week4.sql').read_text(encoding='utf-8'))
                cur.execute(Path('sql/storage.sql').read_text(encoding='utf-8'))
    finally:
        conn.close()


def database_reader(spark):
    host, port = os.environ.get('PGHOST', 'postgres'), os.environ.get('PGPORT', '5432')
    db = os.environ.get('PGDATABASE', 'wiki')
    return (spark.read.format('jdbc').option('url', f'jdbc:postgresql://{host}:{port}/{db}')
            .option('driver', 'org.postgresql.Driver').option('user', os.environ['PGUSER'])
            .option('password', os.environ['PGPASSWORD']))


def transform(raw):
    """Preserve every source record, with either typed columns or a rejection reason."""
    parsed = raw.withColumn('j', F.from_json('raw_value', ENVELOPE_SCHEMA))
    data = parsed.select(
        '*', F.col('j.event_id').alias('event_id'),
        F.col('j.event_type').alias('event_type'),
        F.col('j.schema_version').alias('schema_version'),
        F.to_timestamp('j.event_time').alias('event_time'),
        F.to_timestamp('j.ingest_time').alias('ingest_time'),
        F.col('j.source').alias('source'),
        F.col('j.payload.wiki').alias('wiki'),
        F.col('j.payload.title').alias('title'),
        F.col('j.payload.bot').alias('bot'),
        F.col('j.payload.length.old').alias('old_length'),
        F.col('j.payload.length.new').alias('new_length'),
    ).withColumn('bytes_delta', F.col('new_length') - F.col('old_length'))
    def missing(name):
        return F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)
    # Require an explicit offset or Z; do not silently assign a timezone.
    zoned = r'(?i)(Z|[+-][0-9]{2}:[0-9]{2})$'
    reason = (
        F.when(F.col('j').isNull() | F.col('j._corrupt_record').isNotNull(), 'invalid_json_or_types')
        .when(missing('event_id'), 'missing_event_id')
        .when(F.col('schema_version').isNull() | (F.col('schema_version') != 1), 'unsupported_schema_version')
        .when(F.col('event_type').isNull() | (F.col('event_type') != 'wikipedia.edit'), 'invalid_event_type')
        .when(F.col('event_time').isNull() | ~F.coalesce(F.col('j.event_time').rlike(zoned), F.lit(False)), 'invalid_event_time')
        .when(F.col('ingest_time').isNull() | ~F.coalesce(F.col('j.ingest_time').rlike(zoned), F.lit(False)), 'invalid_ingest_time')
        .when(missing('source') | missing('wiki') | missing('title') | F.col('bot').isNull(), 'missing_payload_fields')
        .when(~F.coalesce((F.col('j.payload.type') == 'edit') & F.col('j.payload.server_name').endswith('.wikipedia.org'), F.lit(False)), 'outside_wikipedia_edits')
        .when((F.col('old_length') < 0) | (F.col('new_length') < 0), 'negative_length')
        .when(F.col('message_key').isNull() | (F.col('message_key') != F.concat_ws(':', 'wiki', 'title')), 'key_mismatch')
    )
    return data.withColumn('error_reason', reason).drop('j')


def write_batch(raw, batch_id, fail_after_commit=False, partitions=4, fail_after_stage=False):
    from src.db_sink import write_audit
    return write_audit(raw, batch_id, fail_after_commit, partitions, fail_after_stage)


def export_snapshot(spark, output, fail_before_publish=False):
    from src.snapshot_store import export_snapshot as export
    return export(spark, output, fail_before_publish)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topic', default='wikipedia-edits')
    parser.add_argument('--bootstrap-servers', default='kafka:29092')
    parser.add_argument('--checkpoint', default='artifacts/week4/checkpoints/main')
    parser.add_argument('--output', default='artifacts/week4/parquet')
    parser.add_argument('--jdbc-partitions', type=int, default=4)
    parser.add_argument('--max-offsets', type=int, default=1000)
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--export-only', action='store_true')
    parser.add_argument('--fail-after-commit', action='store_true', help='Recovery test only')
    args = parser.parse_args()
    if not 1 <= args.jdbc_partitions <= 32:
        parser.error("jdbc-partitions must be between 1 and 32")
    if args.max_offsets < 1:
        parser.error('--max-offsets must be positive')
    os.environ['TZ'] = 'UTC'
    import time
    if hasattr(time, 'tzset'):
        time.tzset()
    spark = (SparkSession.builder.appName('wikipedia-week4')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.ansi.enabled', 'false')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        initialize_database()
        if not args.export_only:
            raw = (spark.readStream.format('kafka')
                   .option('kafka.bootstrap.servers', args.bootstrap_servers)
                   .option('subscribe', args.topic).option('startingOffsets', 'earliest')
                   .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', args.max_offsets)
                   .load().selectExpr('CAST(key AS STRING) AS message_key', 'CAST(value AS STRING) AS raw_value',
                                      'topic AS source_topic', 'partition AS source_partition', 'offset AS source_offset'))
            writer = (raw.writeStream.option('checkpointLocation', args.checkpoint)
                      .foreachBatch(lambda batch, number: write_batch(batch, number, args.fail_after_commit, args.jdbc_partitions)))
            writer = writer.trigger(processingTime='10 seconds') if args.continuous else writer.trigger(availableNow=True)
            writer.start().awaitTermination()
        export_snapshot(spark, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
