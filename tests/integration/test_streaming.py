"""Real Spark state/checkpoint/file-sink and PostgreSQL upsert tests, isolated by pipeline ID."""
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

from pyspark.sql import SparkSession, types as T
from src.stream_edits import (initialize_stream_database, bind_pipeline, start_clean,
                              start_aggregate, connect)

spark = (SparkSession.builder.appName('test-stream-v2')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
name = 'test-' + uuid.uuid4().hex[:12]
root = Path('artifacts/week4/tests') / name
reports = []

def event(identity, timestamp, old=10, new=20, bot=False):
    payload = {'wiki':'enwiki','title':'Test','bot':bot,'type':'edit',
               'server_name':'en.wikipedia.org','length':{'old':old,'new':new}}
    envelope = dict(event_id=identity,event_type='wikipedia.edit',schema_version=1,
                    event_time='2026-10-25T'+timestamp+'Z',ingest_time='2026-10-31T00:00:00Z',
                    source='test',payload=payload)
    return dict(message_key='enwiki:Test',raw_value=json.dumps(envelope),
                source_topic=name,source_partition=0,source_offset=0)

schema = T.StructType([T.StructField(k, typ) for k,typ in [
    ('message_key',T.StringType()),('raw_value',T.StringType()),('source_topic',T.StringType()),
    ('source_partition',T.IntegerType()),('source_offset',T.LongType())]])

def database_rows():
    conn=connect()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT window_start,edit_count,bot_edit_count,size_known_count,net_bytes_delta,updated_at '
                        'FROM week4.windowed_wiki WHERE pipeline_id=%s ORDER BY window_start', (name,))
            return cur.fetchall()
    finally:
        conn.close()

def aggregate(fail=False):
    q=start_aggregate(spark,root,'2 minutes','1 minute',name,fail=fail)
    try:
        q.awaitTermination()
    finally:
        reports.append({'stage':'aggregate','progress':q.recentProgress})

def wave(index, records):
    path=root/'input'/f'{index:03}.json'
    path.write_text(''.join(json.dumps(x)+'\n' for x in records))
    raw=spark.readStream.schema(schema).option('maxFilesPerTrigger',1).json(str(root/'input'))
    q=start_clean(raw,root,'2 minutes')
    q.awaitTermination()
    reports.append({'stage':'clean','wave':index,'progress':q.recentProgress})

try:
    initialize_stream_database()
    root=bind_pipeline(root,name,dict(test=True,watermark='2 minutes',window='1 minute'))
    (root/'input').mkdir()
    a=event('a','00:00:10')
    wave(1,[a,a,event('b','00:00:20',None,None,True)])
    try:
        aggregate(fail=True)
        raise AssertionError('Expected injected commit failure')
    except Exception as exc:
        assert 'Injected streaming failure AFTER aggregate commit' in str(exc),str(exc)
    before=database_rows()
    assert len(before)==1 and before[0][1:5]==(2,1,1,10),before
    aggregate()
    assert database_rows()==before,'Recovery must preserve all columns including updated_at'
    aggregate()
    assert database_rows()==before,'Empty restart changed aggregates'
    # New event in the same window must UPDATE 2 -> 3, while replaying a is ignored.
    wave(2,[a,event('c','00:00:30',20,15)])
    aggregate()
    rows=database_rows()
    assert rows[0][1:5]==(3,1,2,5),rows
    wave(3,[event('d','00:10:00')])
    aggregate()
    # Max event time is 00:10:00, watermark 00:08:00. Old unique event is dropped.
    wave(4,[event('too-late','00:00:40'),a,event('e','00:09:00')])
    aggregate()
    rows=database_rows()
    assert [x[1] for x in rows]==[3,1,1],rows
    parquet=spark.read.parquet(str(root/'clean'))
    ids={r.event_id for r in parquet.select('event_id').collect()}
    assert ids=={'a','b','c','d','e'},ids
    assert parquet.count()==5
    assert {str(r.event_date) for r in parquet.select('event_date').distinct().collect()}=={'2026-10-25'}
    dropped=sum(op.get('numRowsDroppedByWatermark',0) for report in reports
                for progress in report['progress'] for op in progress.get('stateOperators',[]))
    assert dropped>=1, 'No evidence of watermark drops'
    try:
        bind_pipeline(root,name,dict(test=True,watermark='5 minutes',window='1 minute'))
        raise AssertionError('Changed settings were accepted')
    except ValueError:
        pass
    # Continuous mode writes files and updates SQL before either query stops.
    raw=spark.readStream.schema(schema).json(str(root/'input'))
    cq=start_clean(raw,root,'2 minutes',continuous=True)
    aq=start_aggregate(spark,root,'2 minutes','1 minute',name,continuous=True)
    try:
        (root/'input'/'005.json').write_text(json.dumps(event('f','00:10:20'))+'\n')
        cq.processAllAvailable()
        aq.processAllAvailable()
        assert cq.isActive and aq.isActive
        assert [x[1] for x in database_rows()]==[3,1,2]
        assert spark.read.parquet(str(root/'clean')).count()==6
    finally:
        cq.stop()
        aq.stop()
    aggregate()
    rows=database_rows()
    assert [x[1] for x in rows]==[3,1,2]
    result=dict(verified=True,pipeline_id=name,checked_at=datetime.now(timezone.utc).isoformat(),
                checks=['same/cross-batch duplicate removal','event-time window update 2 to 3',
                        'commit failure recovery','empty checkpoint restart','late within watermark accepted',
                        'late outside watermark dropped','null and negative delta','Parquet date partition',
                        'incompatible settings rejected','continuous writes before shutdown'],
                parquet_rows=6,aggregate_counts=[x[1] for x in rows],watermark_dropped=dropped,
                progress=reports)
    (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n')
    dest=Path('evidence/week4/stream_v2_verification.json')
    dest.write_text(json.dumps(result,indent=2)+'\n')
    print('PASS: streaming v2; report='+str(dest))
finally:
    for q in spark.streams.active:
        q.stop()
    spark.stop()
