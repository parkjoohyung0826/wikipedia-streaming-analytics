"""Continuous audit sink with persisted progress, latency, and committed Kafka offsets."""
from contextlib import closing
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import time
from pyspark.sql import SparkSession
from src.process_edits import initialize_database,write_batch,connect

def append(path,value):
    with path.open('a') as f:f.write(json.dumps(value,default=str)+'\n')

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--topic',default='wikipedia-live')
    p.add_argument('--root',default='artifacts/live/stream')
    p.add_argument('--checkpoint',help='Persistent state location; defaults to root/checkpoint')
    p.add_argument('--max-offsets',type=int,default=20000)
    p.add_argument('--trigger',type=int,default=10)
    p.add_argument('--jdbc-partitions',type=int,default=4)
    p.add_argument('--lag-alert',type=int,default=10000)
    p.add_argument('--p95-alert',type=float,default=30)
    a=p.parse_args()
    if min(a.max_offsets,a.trigger,a.jdbc_partitions)<1:p.error('limits must be positive')
    root=Path(a.root);root.mkdir(parents=True,exist_ok=True)
    spark=(SparkSession.builder.appName('wiki-monitored-'+a.topic)
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    initialize_database()
    with closing(connect()) as conn,conn,conn.cursor() as cur:
        cur.execute('CREATE INDEX IF NOT EXISTS edits_ingest_time ON week4.edits (ingest_time)')
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers','kafka:29092')
        .option('subscribe',a.topic).option('startingOffsets','earliest').option('failOnDataLoss','true')
        .option('maxOffsetsPerTrigger',a.max_offsets).load()
        .selectExpr('CAST(key AS STRING) AS message_key','CAST(value AS STRING) AS raw_value',
                    'topic AS source_topic','partition AS source_partition','offset AS source_offset'))
    def batch(frame,number):
        started=time.monotonic()
        result=write_batch(frame,number,partitions=a.jdbc_partitions)
        append(root/'sink.jsonl',dict(at=datetime.now(timezone.utc).isoformat(),seconds=time.monotonic()-started,**result))
    query=raw.writeStream.option('checkpointLocation',a.checkpoint or str(root/'checkpoint')).foreachBatch(batch).trigger(processingTime=f'{a.trigger} seconds').start()
    # Kafka latest offsets come from an independent broker query, not Spark's capped admission offsets.
    props=spark._jvm.java.util.Properties();props.setProperty('bootstrap.servers','kafka:29092')
    props.setProperty('key.deserializer','org.apache.kafka.common.serialization.ByteArrayDeserializer')
    props.setProperty('value.deserializer','org.apache.kafka.common.serialization.ByteArrayDeserializer')
    consumer=spark._jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    parts=spark._jvm.java.util.ArrayList()
    for info in consumer.partitionsFor(a.topic):parts.add(spark._jvm.org.apache.kafka.common.TopicPartition(a.topic,info.partition()))
    seen=None
    try:
        while query.isActive:
            progress=query.lastProgress
            if progress:
                source=progress['sources'][0];ends=source.get('endOffset') or {}
                if isinstance(ends,str):ends=json.loads(ends)
                latest=consumer.endOffsets(parts)
                committed=ends.get(a.topic,{})
                lag=sum(max(0,int(latest.get(part))-int(committed.get(str(part.partition()),0))) for part in parts)
                with closing(connect()) as conn,conn,conn.cursor() as cur:
                    cur.execute("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-ingest_time)), percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-ingest_time)) FROM week4.edits WHERE source_topic=%s AND processed_at>now()-interval '2 minutes'",(a.topic,))
                    p50,p95=cur.fetchone()
                record=dict(at=datetime.now(timezone.utc).isoformat(),batch=progress['batchId'],lag=lag,p50_seconds=p50,p95_seconds=p95,
                            input_rps=progress.get('inputRowsPerSecond'),processed_rps=progress.get('processedRowsPerSecond'),
                            batch_ms=progress.get('durationMs',{}).get('triggerExecution'),end_offsets=committed)
                append(root/'metrics.jsonl',record)
                if seen!=progress['batchId']:
                    append(root/'progress.jsonl',progress);seen=progress['batchId']
                if lag>a.lag_alert or (p95 is not None and p95>a.p95_alert):append(root/'alerts.jsonl',record)
            time.sleep(10)
        query.awaitTermination()
    except Exception as exc:
        append(root/'alerts.jsonl',dict(at=datetime.now(timezone.utc).isoformat(),error=str(exc),action='container restart from checkpoint'))
        raise
    finally:
        consumer.close();query.stop();spark.stop()

if __name__=='__main__':main()
