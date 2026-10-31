"""Bounded executor JDBC staging, followed by an atomic server-side merge.

No event rows are pulled into Python on the driver. Only small SQL summaries return.
"""
import json
import os
import uuid
from pyspark.sql import functions as F, types as T


def jdbc_options():
    return dict(url='jdbc:postgresql://{}:{}/{}'.format(
        os.environ.get('PGHOST','postgres'), os.environ.get('PGPORT','5432'),
        os.environ.get('PGDATABASE','wiki')), driver='org.postgresql.Driver',
        user=os.environ['PGUSER'], password=os.environ['PGPASSWORD'])


def stage(frame, table, load_id, partitions=4):
    if not 1 <= partitions <= 32:
        raise ValueError('JDBC write partitions must be between 1 and 32')
    # repartition runs on executors; numPartitions caps concurrent database connections.
    data = (frame.repartition(partitions).withColumn('stage_partition', F.spark_partition_id())
            .withColumn('load_id', F.lit(load_id)))
    (data.write.format('jdbc').options(**jdbc_options()).option('dbtable', table)
     .option('numPartitions', partitions).option('batchsize', 1000)
     .option('isolationLevel', 'READ_COMMITTED').mode('append').save())


def cleanup(table, load_id):
    from src.process_edits import connect
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            # table is an internal constant, never user input.
            cur.execute(f'DELETE FROM {table} WHERE load_id=%s', (load_id,))
    finally:
        conn.close()


def write_audit(raw, batch_id, fail_after_commit=False, partitions=4, fail_after_stage=False):
    from src.process_edits import connect, transform, EDIT_COLUMNS
    load_id = uuid.uuid4().hex
    table = 'week4.audit_stage'
    identity = F.udf(lambda topic, part, offset: json.dumps([topic, part, offset]), T.StringType())
    frame = (transform(raw).select(*EDIT_COLUMNS, 'raw_value', 'error_reason')
             .withColumn('source_id', identity('source_topic','source_partition','source_offset')))
    try:
        stage(frame, table, load_id, partitions)
        if fail_after_stage:
            raise RuntimeError('Injected failure AFTER staging BEFORE merge')
        conn = connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute('SELECT count(DISTINCT source_id),count(DISTINCT source_id) FILTER (WHERE error_reason IS NULL),'
                            'count(DISTINCT stage_partition) FROM week4.audit_stage WHERE load_id=%s', (load_id,))
                count, valid, writers = cur.fetchone()
                columns = ','.join(EDIT_COLUMNS)
                # Task retries may duplicate staging records. Select one row for each PK.
                cur.execute(f'''INSERT INTO week4.edits ({columns})
                    SELECT {columns} FROM (
                      SELECT DISTINCT ON (event_id) * FROM week4.audit_stage
                      WHERE load_id=%s AND error_reason IS NULL
                      ORDER BY event_id,source_topic,source_partition,source_offset
                    ) selected ON CONFLICT (event_id) DO NOTHING''', (load_id,))
                inserted = cur.rowcount
                cur.execute('''INSERT INTO week4.rejected
                    (source_id,source_topic,source_partition,source_offset,raw_value,error_reason)
                    SELECT DISTINCT ON (source_id)
                    source_id,source_topic,source_partition,source_offset,raw_value,error_reason
                    FROM week4.audit_stage WHERE load_id=%s AND error_reason IS NOT NULL
                    ORDER BY source_id ON CONFLICT (source_id) DO NOTHING''', (load_id,))
                rejected = cur.rowcount
                cur.execute('DELETE FROM week4.audit_stage WHERE load_id=%s', (load_id,))
            result = dict(batch_id=batch_id, input=count, valid=valid, invalid=count-valid,
                          inserted=inserted, rejected_inserted=rejected,
                          duplicate_or_existing=valid-inserted, staging_partitions=writers)
            print(json.dumps(result), flush=True)
            if fail_after_commit and count:
                raise RuntimeError('Injected failure AFTER database commit, BEFORE Spark checkpoint completion')
            return result
        finally:
            conn.close()
    except Exception:
        # A killed process may leave staging rows; UUID isolation keeps them out of merges.
        try:
            cleanup(table, load_id)
        except Exception:
            print('Deferred staging cleanup for load_id='+load_id, flush=True)
        raise


def write_aggregates(batch, batch_id, pipeline_id, fail_after_commit=False, partitions=4):
    from src.process_edits import connect
    load_id = uuid.uuid4().hex
    table = 'week4.aggregate_stage'
    try:
        # JDBC action consumes every stateful output partition, including on replay.
        stage(batch, table, load_id, partitions)
        conn = connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute('INSERT INTO week4.stream_batches VALUES (%s,%s) '
                            'ON CONFLICT DO NOTHING RETURNING batch_id', (pipeline_id,batch_id))
                new_batch = cur.fetchone() is not None
                if new_batch:
                    cur.execute('''INSERT INTO week4.windowed_wiki
                        (pipeline_id,window_start,window_end,wiki,edit_count,bot_edit_count,size_known_count,net_bytes_delta)
                        SELECT %s,window_start,window_end,wiki,edit_count,bot_edit_count,size_known_count,net_bytes_delta
                        FROM (SELECT DISTINCT ON (window_start,wiki) * FROM week4.aggregate_stage
                              WHERE load_id=%s ORDER BY window_start,wiki) staged
                        ON CONFLICT (pipeline_id,window_start,wiki) DO UPDATE SET
                        window_end=EXCLUDED.window_end,edit_count=EXCLUDED.edit_count,
                        bot_edit_count=EXCLUDED.bot_edit_count,size_known_count=EXCLUDED.size_known_count,
                        net_bytes_delta=EXCLUDED.net_bytes_delta,updated_at=now()''', (pipeline_id,load_id))
                cur.execute('DELETE FROM week4.aggregate_stage WHERE load_id=%s', (load_id,))
            if fail_after_commit and new_batch:
                raise RuntimeError('Injected streaming failure AFTER aggregate commit')
        finally:
            conn.close()
    except Exception:
        try:
            cleanup(table, load_id)
        except Exception:
            print('Deferred staging cleanup for load_id='+load_id, flush=True)
        raise
