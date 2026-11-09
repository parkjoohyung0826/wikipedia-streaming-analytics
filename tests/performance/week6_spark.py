"""Spark-side measurement and a deterministic pre-commit crash boundary."""
import json
import os
import time
from pathlib import Path
from src.spark_s3 import Store, session, stream, daily

root=Path(os.environ['WIKI_TEST_ROOT'])
mode=os.environ['WIKI_TEST_MODE']
store=Store()
spark=session()
try:
    if mode=='crash':
        original=store.put
        def pause(key,value,immutable=False):
            if key.startswith('stream/commits/'):
                # Actual S3 output exists, but it is not committed or reader-visible.
                (root/'crash_ready.json').write_text(json.dumps({'key':key,'value':value}))
                time.sleep(300)
            return original(key,value,immutable)
        store.put=pause
        stream(spark,store,True)
    elif mode=='sink-outage':
        from botocore.exceptions import ClientError
        original=store.put
        def unavailable(key,value,immutable=False):
            if key.startswith('stream/commits/'):
                (root/'sink-fault.json').write_text(json.dumps({'key':key,'path':value['path'],'at':time.time()}))
                time.sleep(60)
                raise ClientError({'Error':{'Code':'ServiceUnavailable','Message':'week6 injected outage'}},'PutObject')
            return original(key,value,immutable)
        store.put=unavailable
        try:stream(spark,store,True)
        except Exception as exc:
            from src.spark_s3 import retryable
            if retryable(exc):raise SystemExit(75) from exc
            raise
    elif mode=='daily-airflow':
        from botocore.exceptions import ClientError
        from src.spark_s3 import retryable
        target=os.environ['WIKI_TEST_DATE'];work=os.environ['WIKI_TEST_WORK']
        pointer_key='daily/published/'+target+'/current.json'
        previous=store.get(pointer_key);original=store.put
        marker=root/'airflow-fault-injected.json'
        def fail_once(key,value,immutable=False):
            if key.endswith('/compact_raw.json') and not marker.exists():
                marker.write_text(json.dumps({'key':key,'at':time.time()}))
                assert store.get(pointer_key)==previous
                raise ClientError({'Error':{'Code':'SlowDown','Message':'Airflow retry test'}},'PutObject')
            return original(key,value,immutable)
        store.put=fail_once
        try:
            for action in ('compact_raw','daily_agg','quality_check','publish'):
                daily(spark,store,action,target,work)
        except Exception as exc:
            if retryable(exc):raise SystemExit(75) from exc
            raise
        current=store.get(pointer_key);differences={}
        for stage in ('clean','aggregate'):
            a=spark.read.parquet(previous[stage]['path']);b=spark.read.parquet(current[stage]['path'])
            differences[stage]=a.exceptAll(b).count()+b.exceptAll(a).count()
        assert not any(differences.values()),differences
        (root/'airflow-equality.json').write_text(json.dumps({'differences':differences,
            'rows':current['clean']['rows']},indent=2)+'\n')
    elif mode=='daily-retry':
        from botocore.exceptions import ClientError
        target=os.environ['WIKI_TEST_DATE'];work=os.environ['WIKI_TEST_WORK']
        pointer_key='daily/published/'+target+'/current.json'
        previous=store.get(pointer_key);original=store.put;injected=False
        def fail_manifest(key,value,immutable=False):
            global injected
            if key.endswith('/compact_raw.json') and not injected:
                injected=True
                raise ClientError({'Error':{'Code':'SlowDown','Message':'injected after Parquet write'}},'PutObject')
            return original(key,value,immutable)
        store.put=fail_manifest
        try:
            daily(spark,store,'compact_raw',target,work)
            raise AssertionError('Fault not reached')
        except ClientError as exc:
            from src.spark_s3 import retryable
            assert retryable(exc)
        finally:store.put=original
        assert store.get(pointer_key)==previous
        for action in ('compact_raw','daily_agg','quality_check','publish'):
            daily(spark,store,action,target,work)
        first=store.get(pointer_key)
        for action in ('compact_raw','daily_agg','quality_check','publish'):
            daily(spark,store,action,target,work)
        second=store.get(pointer_key)
        differences={}
        for stage in ('clean','aggregate'):
            a=spark.read.parquet(first[stage]['path']);b=spark.read.parquet(second[stage]['path'])
            differences[stage]=a.exceptAll(b).count()+b.exceptAll(a).count()
        assert not any(differences.values()),differences
        (root/'daily-retry.json').write_text(json.dumps({'fault':'retryable SlowDown after compact_raw write',
            'same_work_id':True,'previous_publication_preserved':True,'differences':differences,
            'rows':second['clean']['rows'],'retry_executor':'test harness, not Airflow scheduler'},indent=2)+'\n')
    elif mode=='daily':
        for action in ('compact_raw','daily_agg','quality_check','publish'):
            daily(spark,store,action,os.environ['WIKI_TEST_DATE'],os.environ['WIKI_TEST_WORK'])
    elif mode=='verify':
        from pyspark.sql import functions as F
        commits=[c for c in store.manifests() if c['rows']]
        records={}
        for path in root.glob('*.metrics.jsonl'):
            for line in path.read_text().splitlines():
                v=json.loads(line);records[v['batch_id']]=v['committed_at']
        metadata=[(c['path'].rsplit('/',1)[-1], records.get(c['batch_id'],c['completed_at'])) for c in commits]
        mapping=spark.createDataFrame(metadata,['attempt','committed_at'])
        data=(spark.read.option('recursiveFileLookup','true').parquet(*[c['path'] for c in commits])
              .withColumn('attempt',F.regexp_extract(F.input_file_name(),r'/attempts/([^/]+)',1))
              .join(F.broadcast(mapping),'attempt')
              .withColumn('latency_s',F.to_timestamp('committed_at').cast('double')-F.col('ingest_time').cast('double'))).cache()
        stats=data.groupBy('source').agg(F.count('*').alias('rows'),F.countDistinct('event_id').alias('unique_ids'),
              F.sum(F.col('error_reason').isNotNull().cast('long')).alias('rejected'),
              F.avg(F.length(F.encode('raw_value','UTF-8'))).alias('mean_payload_bytes'),
              F.expr('percentile_approx(latency_s, array(0.5,0.95),100000)').alias('latency_p50_p95_s'),
              F.min('latency_s').alias('min_latency_s'),F.max('latency_s').alias('max_latency_s'))
        result={'stages':[r.asDict() for r in stats.collect()], 'rows':data.count(),
                'distinct_kafka_offsets':data.select('source_topic','source_partition','source_offset').distinct().count(),
                'commits':len(commits)}
        # Compare every generated ID, not just aggregate counts.
        expected=json.loads((root/'expected.json').read_text())
        expected_frames=[]
        for item in expected:
            expected_frames.append(spark.range(item['count']).select(F.concat(F.lit(item['prefix']),F.col('id')).alias('event_id')))
        ids=expected_frames[0]
        for frame in expected_frames[1:]:ids=ids.union(frame)
        actual=data.filter(F.col('error_reason').isNull()).select('event_id').distinct()
        result['missing_ids']=ids.join(actual,'event_id','left_anti').count()
        result['unexpected_ids']=actual.join(ids,'event_id','left_anti').count()
        pointer=store.get('daily/published/'+os.environ['WIKI_TEST_DATE']+'/current.json')
        if pointer:
            clean=spark.read.parquet(pointer['clean']['path'])
            aggregate=spark.read.parquet(pointer['aggregate']['path'])
            result['published']={'rows':clean.count(),'unique_ids':clean.select('event_id').distinct().count(),
                                 'aggregate_sum':aggregate.agg(F.sum('edit_count')).first()[0]}
        assert result['rows']==result['distinct_kafka_offsets'],result
        assert result['missing_ids']==result['unexpected_ids']==0,result
        (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result))
    else:raise ValueError(mode)
finally:spark.stop()
