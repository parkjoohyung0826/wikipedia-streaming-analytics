"""Kafka -> Spark -> immutable S3 Parquet batches; Airflow date-scoped publication."""
import argparse
import hashlib
import json
import os
import uuid
from datetime import date


def settings():
    with open('config/s3.local.json') as f: return json.load(f)


class Store:
    def __init__(self, config=None, client=None):
        c=config or settings(); self.bucket=c['bucket']; self.prefix=c['prefix'].strip('/')+'/spark-v1'
        if client is None:
            import boto3
            client=boto3.Session(profile_name=c.get('profile'),region_name=c['region']).client('s3')
        self.client=client
    def key(self,key): return self.prefix+'/'+key
    def uri(self,key): return 's3a://'+self.bucket+'/'+self.key(key)
    def get(self,key):
        try: return json.loads(self.client.get_object(Bucket=self.bucket,Key=self.key(key))['Body'].read())
        except Exception as e:
            if getattr(e,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404'): return None
            raise
    def put(self,key,value,immutable=False):
        args=dict(Bucket=self.bucket,Key=self.key(key),Body=json.dumps(value,sort_keys=True).encode(),ServerSideEncryption='AES256',ContentType='application/json')
        if immutable: args['IfNoneMatch']='*'
        self.client.put_object(**args)
    def manifests(self):
        pages=self.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket,Prefix=self.key('stream/commits/'))
        return [json.loads(self.client.get_object(Bucket=self.bucket,Key=o['Key'])['Body'].read()) for p in pages for o in p.get('Contents',[]) if o['Key'].endswith('.json')]


def session():
    from pyspark.sql import SparkSession
    c=settings()
    spark=(SparkSession.builder.appName('wikipedia-s3').config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.shuffle.partitions','2')
        .config('spark.hadoop.fs.s3a.aws.credentials.provider','com.amazonaws.auth.profile.ProfileCredentialsProvider')
        .config('spark.hadoop.fs.s3a.endpoint','s3.'+c['region']+'.amazonaws.com')
        .config('spark.hadoop.fs.s3a.server-side-encryption-algorithm','AES256')
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stream(spark,store,once):
    from pyspark.sql import functions as F
    from src.process_edits import transform
    # Losing checkpoints must not silently reuse old batch IDs and skip new rows.
    commits=store.client.list_objects_v2(Bucket=store.bucket,Prefix=store.key("stream/commits/"),MaxKeys=1)
    offsets=store.client.list_objects_v2(Bucket=store.bucket,Prefix=store.key("stream/checkpoint/offsets/"),MaxKeys=1)
    if commits.get("KeyCount",0) and not offsets.get("KeyCount",0):
        raise ValueError("Checkpoint missing while commits exist; restore both or use a new prefix")
    # Keep this checkpoint and commit namespace together. Never reset one alone.
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:29092')
        .option('subscribe','wikipedia-live').option('startingOffsets','earliest')
        .option('failOnDataLoss','true').option('maxOffsetsPerTrigger',10000).load()
        .selectExpr('CAST(value AS STRING) raw_value','CAST(key AS STRING) message_key',
                    'topic source_topic','partition source_partition','offset source_offset'))
    def save(batch,batch_id):
        key=f'stream/commits/{batch_id:020d}.json'
        if store.get(key): return
        path=store.uri('stream/attempts/'+str(uuid.uuid4()))
        # Preserve rejected rows and raw payload in the same atomic batch boundary.
        data=transform(batch).withColumn('event_date',F.to_date(F.from_utc_timestamp('event_time','Asia/Seoul'))).persist()
        try:
            count=data.count()
            if count: data.write.mode('errorifexists').partitionBy('event_date').parquet(path)
            dates=[str(row.event_date) for row in data.select('event_date').distinct().collect() if row.event_date is not None]
            # A failed write has no commit. Readers only follow committed paths.
            store.put(key,dict(batch_id=batch_id,path=path if count else None,rows=count,dates=dates),immutable=True)
        finally: data.unpersist()
    writer=raw.writeStream.foreachBatch(save).option('checkpointLocation',store.uri('stream/checkpoint'))
    query=(writer.trigger(availableNow=True) if once else writer.trigger(processingTime='30 seconds')).start()
    query.awaitTermination()


def daily(spark,store,action,target,run_id):
    from pyspark.sql import functions as F
    if date.fromisoformat(target).isoformat()!=target: raise ValueError('Invalid date')
    token=hashlib.sha256(run_id.encode()).hexdigest()[:20]
    base='daily/work/'+target+'/'+token+'/'
    def read_stage(stage):
        record=store.get(base+stage+'.json')
        if not record: raise ValueError('Missing prerequisite: '+stage)
        return record
    def save_stage(stage,df):
        path=store.uri(base+stage+'/'+str(uuid.uuid4()))
        df.write.mode('errorifexists').parquet(path)
        store.put(base+stage+'.json',dict(path=path,rows=df.count()))
    if action=='compact_raw':
        # Freeze the committed input list for all retries of this DAG run.
        snapshot=store.get(base+'input.json')
        if snapshot is None:
            snapshot={'batches':[v for v in store.manifests() if 'dates' not in v or target in v['dates']]};store.put(base+'input.json',snapshot,immutable=True)
        paths=[v['path']+'/event_date='+target for v in snapshot['batches'] if v['path']]
        if not paths: raise ValueError('No committed input; refusing empty publication')
        # Read only this date's partitions across independent immutable batch roots.
        df=(spark.read.option('recursiveFileLookup','true').parquet(*paths)
            .withColumn('event_date',F.to_date(F.lit(target))).filter(F.col('error_reason').isNull()))
        from pyspark.sql.window import Window
        w=Window.partitionBy('event_id').orderBy('source_topic','source_partition','source_offset')
        df=df.withColumn('_rank',F.row_number().over(w)).filter('_rank=1').drop('_rank')
        if df.isEmpty(): raise ValueError('No valid input for target date')
        save_stage(action,df)
    elif action=='daily_agg':
        df=spark.read.parquet(read_stage('compact_raw')['path'])
        agg=df.groupBy('event_date','wiki').agg(F.count('*').alias('edit_count'),F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),F.count('bytes_delta').alias('size_known_count'),F.sum('bytes_delta').alias('net_bytes_delta'))
        save_stage(action,agg)
    elif action=='quality_check':
        clean=read_stage('compact_raw');agg=read_stage('daily_agg')
        df=spark.read.parquet(clean['path']);counts=spark.read.parquet(agg['path'])
        if df.count()!=clean['rows'] or df.select('event_id').distinct().count()!=clean['rows']:raise ValueError('Duplicate/count mismatch')
        if counts.agg(F.sum('edit_count')).first()[0]!=clean['rows']:raise ValueError('Aggregate count mismatch')
        store.put(base+'quality_check.json',dict(clean=clean,aggregate=agg,passed=True))
    elif action=='publish':
        checked=read_stage('quality_check')
        if not checked.get('passed'):raise ValueError('Quality check did not pass')
        if checked['clean']!=read_stage('compact_raw') or checked['aggregate']!=read_stage('daily_agg'):raise ValueError('Data changed after validation')
        # One atomic object replacement; never expose half-written Parquet directories.
        store.put('daily/published/'+target+'/current.json',dict(target_date=target,**checked))
    else: raise ValueError('Unknown action')


def retryable(exc):
    """Recognize temporary Python SDK and Java S3A failures without retrying bad data."""
    transient={'SlowDown','RequestTimeout','InternalError','ServiceUnavailable','Throttling'}
    if getattr(exc,'response',{}).get('Error',{}).get('Code') in transient:return True
    if type(exc).__name__ in ('EndpointConnectionError','ConnectionClosedError','ReadTimeoutError','ConnectTimeoutError'):return True
    cause=getattr(exc,'java_exception',None)
    for _ in range(12):
        if cause is None:break
        try:
            name=cause.getClass().getName()
            if name in ('java.net.SocketTimeoutException','java.net.ConnectException','java.net.SocketException'):return True
            if name.endswith(('AmazonS3Exception','AmazonServiceException')):
                if cause.getStatusCode() in (429,500,502,503,504) or cause.getErrorCode() in transient:return True
            cause=cause.getCause()
        except Exception:break
    return False


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['stream','compact_raw','daily_agg','quality_check','publish']);p.add_argument('--once',action='store_true');p.add_argument('--date');p.add_argument('--work',default='manual');a=p.parse_args()
    spark=session()
    try:
        store=Store()
        if a.action=='stream':stream(spark,store,a.once)
        else:daily(spark,store,a.action,a.date,a.work)
    except Exception as exc:
        # Only known transient AWS errors ask Airflow for retry; validation fails fast.
        if retryable(exc):raise SystemExit(75)
        raise
    finally:spark.stop()

if __name__=='__main__':main()
