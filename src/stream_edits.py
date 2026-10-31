"""Stateful streaming v2; legacy process_edits remains the complete audit path."""
import argparse
from datetime import timezone
import json
import os
from pathlib import Path
import time

from pyspark.sql import functions as F, types as T, SparkSession
from psycopg2.extras import execute_values
from src.process_edits import transform, EDIT_COLUMNS, connect, initialize_database, write_batch

CLEAN_SCHEMA = T.StructType([
    T.StructField(name, T.TimestampType() if name in ('event_time', 'ingest_time') else
                  T.BooleanType() if name == 'bot' else
                  T.IntegerType() if name in ('schema_version', 'source_partition') else
                  T.LongType() if name in ('old_length', 'new_length', 'bytes_delta', 'source_offset') else
                  T.StringType()) for name in EDIT_COLUMNS
] + [T.StructField('event_date', T.DateType())])


def clean_events(raw, watermark):
    # ② Decode and validate, then ③ deduplicate before calculating statistics.
    valid = transform(raw).filter('error_reason IS NULL').select(*EDIT_COLUMNS)
    return (valid.withWatermark('event_time', watermark)
            .dropDuplicatesWithinWatermark(['event_id'])
            .withColumn('event_date', F.to_date('event_time')))


def aggregate_events(clean, watermark, window):
    # ④ This is a streaming event-time window, not a query over the whole database.
    return (clean.withWatermark('event_time', watermark)
            .groupBy(F.window('event_time', window), 'wiki')
            .agg(F.count('*').alias('edit_count'),
                 F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
                 F.count('bytes_delta').alias('size_known_count'),
                 F.sum('bytes_delta').alias('net_bytes_delta'))
            .select(F.col('window.start').alias('window_start'),
                    F.col('window.end').alias('window_end'), 'wiki',
                    'edit_count', 'bot_edit_count', 'size_known_count', 'net_bytes_delta'))


def initialize_stream_database():
    initialize_database()
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(Path('sql/stream_v2.sql').read_text())
    finally:
        conn.close()


def upsert_aggregates(batch, batch_id, pipeline_id, fail_after_commit=False):
    # ⑤ Replace the current count; adding it again would double-count retries.
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute('INSERT INTO week4.stream_batches VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING batch_id',
                        (pipeline_id, batch_id))
            already_committed = cur.fetchone() is None
            if already_committed:
                # Stateful foreachBatch output must still be completely consumed.
                for _ in batch.toLocalIterator():
                    pass
                return
            pending = []
            def flush():
                if pending:
                    execute_values(cur, '''INSERT INTO week4.windowed_wiki
                        (pipeline_id,window_start,window_end,wiki,edit_count,bot_edit_count,size_known_count,net_bytes_delta)
                        VALUES %s ON CONFLICT (pipeline_id,window_start,wiki)
                        DO UPDATE SET window_end=EXCLUDED.window_end,edit_count=EXCLUDED.edit_count,
                        bot_edit_count=EXCLUDED.bot_edit_count,size_known_count=EXCLUDED.size_known_count,
                        net_bytes_delta=EXCLUDED.net_bytes_delta,updated_at=now()''', pending)
                    pending.clear()
            for row in batch.toLocalIterator():
                pending.append((pipeline_id, row.window_start.replace(tzinfo=timezone.utc),
                                row.window_end.replace(tzinfo=timezone.utc), row.wiki,
                                row.edit_count, row.bot_edit_count, row.size_known_count, row.net_bytes_delta))
                if len(pending) >= 500:
                    flush()
            flush()
        if fail_after_commit:
            raise RuntimeError('Injected streaming failure AFTER aggregate commit')
    finally:
        conn.close()


def bind_pipeline(root, pipeline_id, contract):
    """Reject incompatible settings and accidental reuse after checkpoint deletion."""
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    contract = dict(contract, root=str(root), version=2,
                    database=os.environ.get('PGDATABASE', 'wiki'),
                    database_host=os.environ.get('PGHOST', 'postgres'))
    path = root / 'contract.json'
    if path.exists() and json.loads(path.read_text()) != contract:
        raise ValueError('Streaming settings changed: use a NEW root AND pipeline-id.')
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute('SELECT contract FROM week4.stream_pipelines WHERE pipeline_id=%s', (pipeline_id,))
            existing = cur.fetchone()
            if existing:
                if existing[0] != contract or not path.exists():
                    raise ValueError('Pipeline binding differs or local files are missing; use a new pipeline-id/root.')
                cur.execute('SELECT 1 FROM week4.stream_batches WHERE pipeline_id=%s LIMIT 1', (pipeline_id,))
                if cur.fetchone() and not (root / 'checkpoints/aggregate/offsets').exists():
                    raise ValueError('Aggregate checkpoint missing; do not reuse existing aggregate rows.')
            else:
                cur.execute('INSERT INTO week4.stream_pipelines VALUES (%s,%s::jsonb)',
                            (pipeline_id, json.dumps(contract)))
    finally:
        conn.close()
    path.write_text(json.dumps(contract, indent=2) + '\n')
    if (root / 'clean/_spark_metadata').exists() and not (root / 'checkpoints/clean/offsets').exists():
        raise ValueError('Clean checkpoint missing while Parquet exists; restore both together.')
    return root


def trigger(writer, continuous):
    return writer.trigger(processingTime='10 seconds') if continuous else writer.trigger(availableNow=True)


def start_clean(raw, root, watermark, continuous=False):
    # File sink commit metadata prevents duplicate files when a batch is retried.
    writer = (clean_events(raw, watermark).writeStream.format('parquet').outputMode('append')
              .partitionBy('event_date').option('path', str(root / 'clean'))
              .option('checkpointLocation', str(root / 'checkpoints/clean')))
    return trigger(writer, continuous).start()


def start_aggregate(spark, root, watermark, window, pipeline_id, continuous=False, fail=False):
    (root / 'clean').mkdir(exist_ok=True)
    # A separate query avoids chaining two stateful operators in update mode.
    clean = spark.readStream.schema(CLEAN_SCHEMA).parquet(str(root / 'clean'))
    agg = aggregate_events(clean, watermark, window)
    writer = (agg.writeStream.outputMode('update')
              .option('checkpointLocation', str(root / 'checkpoints/aggregate'))
              .foreachBatch(lambda df, n: upsert_aggregates(df, n, pipeline_id, fail)))
    return trigger(writer, continuous).start()


def run(spark, args):
    initialize_stream_database()
    root = bind_pipeline(args.root, args.pipeline_id,
                         dict(topic=args.topic, bootstrap=args.bootstrap_servers,
                              watermark=args.watermark, window=args.window))
    # ① Preserve Kafka coordinates alongside the message body.
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', args.bootstrap_servers)
           .option('subscribe', args.topic).option('startingOffsets', 'earliest')
           .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', args.max_offsets)
           .load().selectExpr('CAST(key AS STRING) AS message_key', 'CAST(value AS STRING) AS raw_value',
                              'topic AS source_topic', 'partition AS source_partition', 'offset AS source_offset'))
    queries = []
    try:
        # Audit keeps valid late records and invalid originals even if clean stream drops them.
        audit = trigger(raw.writeStream.option('checkpointLocation', str(root / 'checkpoints/audit'))
                        .foreachBatch(write_batch), args.continuous).start()
        queries.append(audit)
        if not args.continuous:
            audit.awaitTermination()
        clean = start_clean(raw, root, args.watermark, args.continuous)
        queries.append(clean)
        if not args.continuous:
            clean.awaitTermination()
        agg = start_aggregate(spark, root, args.watermark, args.window, args.pipeline_id, args.continuous)
        queries.append(agg)
        if args.continuous:
            while True:
                for query in queries:
                    if not query.isActive:
                        if query.exception():
                            raise query.exception()
                        raise RuntimeError('A streaming query stopped unexpectedly')
                time.sleep(1)
        else:
            agg.awaitTermination()
        report = {'pipeline_id': args.pipeline_id, 'root': str(root),
                  'queries': [q.recentProgress for q in queries]}
        (root / 'progress.json').write_text(json.dumps(report, indent=2) + '\n')
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--topic', default='wikipedia-edits')
    p.add_argument('--bootstrap-servers', default='kafka:29092')
    p.add_argument('--root', type=Path, default=Path('artifacts/week4/stream_v2/main'))
    p.add_argument('--pipeline-id', default='main-v2')
    p.add_argument('--watermark', default='10 minutes')
    p.add_argument('--window', default='1 minute')
    p.add_argument('--max-offsets', type=int, default=1000)
    p.add_argument('--continuous', action='store_true')
    args = p.parse_args()
    if args.max_offsets < 1:
        p.error('--max-offsets must be positive')
    spark = (SparkSession.builder.appName('wikipedia-stream-v2')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
