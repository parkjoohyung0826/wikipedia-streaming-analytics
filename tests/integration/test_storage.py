"""Integration tests for executor staging and copy-on-write daily snapshots."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid
from pyspark.sql import SparkSession, types as T
from src.process_edits import initialize_database, write_batch, export_snapshot, connect, database_reader
from src.snapshot_store import read_snapshot
from src import db_sink

spark=(SparkSession.builder.appName('storage-feedback-tests')
       .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','4').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
root=Path('artifacts/week4/tests/storage-'+uuid.uuid4().hex[:10])
schema=T.StructType([T.StructField(k,t) for k,t in [
    ('message_key',T.StringType()),('raw_value',T.StringType()),('source_topic',T.StringType()),
    ('source_partition',T.IntegerType()),('source_offset',T.LongType())]])

def event(n,day='2026-10-25'):
    message=dict(event_id='storage-'+str(n),event_type='wikipedia.edit',schema_version=1,
                 event_time=day+'T12:00:00Z',ingest_time='2026-10-31T00:00:00Z',source='storage-test',
                 payload=dict(wiki='enwiki',title='Test',bot=False,type='edit',server_name='en.wikipedia.org',
                              length=dict(old=10,new=20)))
    return ('enwiki:Test',json.dumps(message),'storage-test',n%3,n)

def sql(query,params=()):
    conn=connect()
    try:
        with conn,conn.cursor() as cur:
            cur.execute(query,params)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()

def assert_rows(manifest):
    actual=read_snapshot(spark,manifest).drop('event_date')
    expected=database_reader(spark).option('dbtable','week4.edits').load()
    assert actual.exceptAll(expected).count()==0
    assert expected.exceptAll(actual).count()==0
    hours=read_snapshot(spark,manifest,'hourly_wiki')
    assert int(hours.selectExpr('sum(edit_count)').first()[0] or 0)==manifest['edits']

try:
    initialize_database()
    rows=[event(n,'2026-10-25' if n<1200 else '2026-10-26') for n in range(2400)]
    rows.append(('enwiki:Test','{broken','storage-test',0,9999))
    raw=spark.createDataFrame(rows,schema)
    try:
        write_batch(raw,0,fail_after_stage=True)
        raise AssertionError('staging failure missing')
    except RuntimeError as exc:
        assert 'BEFORE merge' in str(exc)
    assert sql('SELECT count(*) FROM week4.edits')[0][0]==0
    assert sql('SELECT count(*) FROM week4.rejected')[0][0]==0
    # Simulate task retry after a JDBC partition has already committed.
    original_stage=db_sink.stage
    def duplicate_stage(*args,**kwargs):
        original_stage(*args,**kwargs)
        original_stage(*args,**kwargs)
    db_sink.stage=duplicate_stage
    result=write_batch(raw,1,partitions=4)
    db_sink.stage=original_stage
    assert result['staging_partitions']>1,result
    assert sql('SELECT count(*) FROM week4.edits')[0][0]==2400
    assert sql('SELECT count(*) FROM week4.rejected')[0][0]==1
    assert sql('SELECT count(*) FROM week4.audit_stage')[0][0]==0
    before=sql('SELECT * FROM week4.edits ORDER BY event_id')
    write_batch(raw,2)
    assert sql('SELECT * FROM week4.edits ORDER BY event_id')==before
    initial=export_snapshot(spark,root)
    assert initial['edits']==2400 and len(initial['partitions'])==2
    assert_rows(initial)
    content=(root/'CURRENT.json').read_bytes()
    files=set(root.rglob('*.parquet'))
    assert export_snapshot(spark,root)==initial
    assert (root/'CURRENT.json').read_bytes()==content and set(root.rglob('*.parquet'))==files
    # Commit succeeds, process crashes, then same batch is retried.
    addition=spark.createDataFrame([event(2400)],schema)
    try:
        write_batch(addition,3,fail_after_commit=True)
        raise AssertionError('commit failure missing')
    except RuntimeError as exc:
        assert 'AFTER database commit' in str(exc)
    write_batch(addition,3)
    second=export_snapshot(spark,root)
    assert second['edits']==2401 and second['rewritten_dates']==['2026-10-25']
    assert second['partitions']['2026-10-26']==initial['partitions']['2026-10-26']
    unchanged_paths=list(Path(initial['partitions']['2026-10-26']['edits_path']).glob('*.parquet'))
    checksums={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in unchanged_paths}
    # Failure after new date files are complete must not publish them.
    write_batch(spark.createDataFrame([event(2401,'2026-10-26')],schema),4)
    content=(root/'CURRENT.json').read_bytes()
    try:
        export_snapshot(spark,root,fail_before_publish=True)
        raise AssertionError('publication failure missing')
    except RuntimeError as exc:
        assert 'BEFORE manifest' in str(exc)
    assert (root/'CURRENT.json').read_bytes()==content
    assert read_snapshot(spark,json.loads(content)).count()==2401
    recovered=export_snapshot(spark,root)
    assert recovered['edits']==2402 and recovered['rewritten_dates']==['2026-10-26']
    assert recovered['partitions']['2026-10-25']==second['partitions']['2026-10-25']
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==digest for p,digest in checksums.items())
    assert_rows(recovered)
    # Updates/deletes, including deleting an entire date, invalidate only affected partitions.
    sql("UPDATE week4.edits SET title='Updated' WHERE event_id='storage-0'")
    updated=export_snapshot(spark,root)
    assert updated['rewritten_dates']==['2026-10-25']
    assert_rows(updated)
    sql("DELETE FROM week4.edits WHERE event_time >= '2026-10-26T00:00:00Z'")
    deleted=export_snapshot(spark,root)
    assert deleted['rewritten_dates']==['2026-10-26'] and len(deleted['partitions'])==1
    assert_rows(deleted)
    # Share one PostgreSQL snapshot across JDBC reads, even if ingestion changes the DB mid-export.
    from src import snapshot_store
    original_hourly=snapshot_store.hourly_frame
    injected=[False]
    def insert_during_export(frame):
        if not injected[0]:
            injected[0]=True
            sql("INSERT INTO week4.edits (event_id,event_type,schema_version,event_time,ingest_time,source,wiki,title,bot,source_topic,source_partition,source_offset) "
                "VALUES ('concurrent','wikipedia.edit',1,'2026-10-27T12:00:00Z','2026-10-31T00:00:00Z','test','enwiki','Concurrent',false,'test',0,1)")
        return original_hourly(frame)
    snapshot_store.hourly_frame=insert_during_export
    concurrent_root=root/'concurrent'
    consistent=export_snapshot(spark,concurrent_root)
    snapshot_store.hourly_frame=original_hourly
    assert consistent['edits']==1201 and '2026-10-27' not in consistent['partitions']
    caught_up=export_snapshot(spark,concurrent_root)
    assert caught_up['edits']==1202 and caught_up['rewritten_dates']==['2026-10-27']
    assert_rows(caught_up)
    # Empty database remains readable using persisted schemas.
    sql('DELETE FROM week4.edits')
    empty=export_snapshot(spark,concurrent_root)
    assert empty['edits']==0 and not empty['partitions']
    assert_rows(empty)
    report=dict(verified=True,checked_at=datetime.now(timezone.utc).isoformat(),
                input_records=2401,staging_partitions=result['staging_partitions'],
                records_after_recovery=2402,
                checks=['staging failure leaves canonical tables untouched','duplicate staging retry idempotence',
                        'reject preservation','post-commit retry','no-change export writes no files',
                        'only changed date rewritten','unchanged partition paths and hashes retained',
                        'failure before publish retains CURRENT','full DB/Parquet equality',
                        'update and date deletion tracked','concurrent ingestion snapshot consistency','empty dataset readable'],root=str(root))
    (root/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    Path('evidence/week4/storage_feedback_verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print('PASS: distributed storage and incremental snapshots')
finally:
    spark.stop()
