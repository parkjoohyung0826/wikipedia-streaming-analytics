"""Week 4: Kafka -> Spark validation -> idempotent PostgreSQL -> Parquet snapshot."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid

import psycopg2
from psycopg2.extras import execute_values
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
    finally:
        conn.close()


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


def write_batch(raw, batch_id, fail_after_commit=False):
    """One database transaction per micro-batch. Replayed IDs keep first committed row."""
    rows = transform(raw).cache()
    conn = connect()
    counts = {'input': 0, 'valid': 0, 'invalid': 0, 'inserted': 0, 'rejected_inserted': 0}
    try:
        with conn:
            with conn.cursor() as cur:
                # Input is bounded by maxOffsetsPerTrigger; stream rows through the driver.
                good, bad = [], []
                def flush():
                    if good:
                        execute_values(cur,
                            'INSERT INTO week4.edits (' + ','.join(EDIT_COLUMNS) +
                            ') VALUES %s ON CONFLICT (event_id) DO NOTHING', good, page_size=len(good))
                        counts['inserted'] += cur.rowcount
                        good.clear()
                    if bad:
                        execute_values(cur,
                            'INSERT INTO week4.rejected (source_id,source_topic,source_partition,source_offset,raw_value,error_reason) '
                            'VALUES %s ON CONFLICT (source_id) DO NOTHING', bad, page_size=len(bad))
                        counts['rejected_inserted'] += cur.rowcount
                        bad.clear()
                for row in rows.toLocalIterator():
                    counts['input'] += 1
                    if row.error_reason is None:
                        counts['valid'] += 1
                        values = [row[c] for c in EDIT_COLUMNS]
                        # Spark timestamps are naive UTC in a UTC session; psycopg needs explicit UTC.
                        for i in (3, 4):
                            values[i] = values[i].replace(tzinfo=timezone.utc)
                        good.append(tuple(values))
                    else:
                        counts['invalid'] += 1
                        source_id = json.dumps([row.source_topic, row.source_partition, row.source_offset])
                        bad.append((source_id, row.source_topic, row.source_partition, row.source_offset,
                                    row.raw_value, row.error_reason))
                    if len(good) + len(bad) >= 500:
                        flush()
                flush()
        counts['duplicate_or_existing'] = counts['valid'] - counts['inserted']
        print(json.dumps({'batch_id': batch_id, **counts}), flush=True)
        if fail_after_commit and counts['input']:
            raise RuntimeError('Injected failure AFTER database commit, BEFORE Spark checkpoint completion')
    finally:
        rows.unpersist()
        conn.close()


def export_snapshot(spark, output):
    """Immutable snapshot, then atomic manifest replacement on the mounted local filesystem.

    Run with a single writer. PostgreSQL is canonical; failed exports can be retried.
    """
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ') + '-' + uuid.uuid4().hex[:8]
    root = Path(output).resolve()
    target = root / 'snapshots' / stamp
    target.mkdir(parents=True)
    host, port = os.environ.get('PGHOST', 'postgres'), os.environ.get('PGPORT', '5432')
    db = os.environ.get('PGDATABASE', 'wiki')
    reader = (spark.read.format('jdbc').option('url', f'jdbc:postgresql://{host}:{port}/{db}')
              .option('driver', 'org.postgresql.Driver').option('user', os.environ['PGUSER'])
              .option('password', os.environ['PGPASSWORD']))
    edits = reader.option('dbtable', 'week4.edits').load().cache()
    try:
        count = edits.count()
        # Both outputs derive from the same cached snapshot, not separate live DB reads.
        hourly = (edits.groupBy(F.date_trunc('hour', 'event_time').alias('hour_utc'), 'wiki')
                  .agg(F.count('*').alias('edit_count'),
                       F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
                       F.count('bytes_delta').alias('size_known_count'),
                       F.sum('bytes_delta').alias('net_bytes_delta')))
        dated = edits.withColumn('event_date', F.to_date('event_time'))
        export = dated.write.mode('error')
        if count:
            export = export.partitionBy('event_date')
        export.parquet(str(target / 'edits'))
        hourly.write.mode('error').parquet(str(target / 'hourly_wiki'))
        manifest = {'snapshot': str(target), 'edits': count, 'timezone': 'UTC', 'created_at': stamp}
        temporary = root / ('CURRENT-' + uuid.uuid4().hex + '.json')
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        os.replace(temporary, root / 'CURRENT.json')
        print(json.dumps({'export': manifest}), flush=True)
        return manifest
    finally:
        edits.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--topic', default='wikipedia-edits')
    parser.add_argument('--bootstrap-servers', default='kafka:29092')
    parser.add_argument('--checkpoint', default='artifacts/week4/checkpoints/main')
    parser.add_argument('--output', default='artifacts/week4/parquet')
    parser.add_argument('--max-offsets', type=int, default=1000)
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--export-only', action='store_true')
    parser.add_argument('--fail-after-commit', action='store_true', help='Recovery test only')
    args = parser.parse_args()
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
                      .foreachBatch(lambda batch, number: write_batch(batch, number, args.fail_after_commit)))
            writer = writer.trigger(processingTime='10 seconds') if args.continuous else writer.trigger(availableNow=True)
            writer.start().awaitTermination()
        export_snapshot(spark, args.output)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
