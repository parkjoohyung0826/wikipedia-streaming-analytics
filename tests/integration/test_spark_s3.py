"""Run in Spark image; exercise daily pipeline using local files, without AWS."""
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from src.spark_s3 import daily


class LocalStore:
    def __init__(self,root):self.root=root;self.objects={};self.inputs=[]
    def uri(self,key):return str(self.root/key)
    def get(self,key):return self.objects.get(key)
    def put(self,key,value,immutable=False):
        if immutable and key in self.objects:raise ValueError('Already exists')
        self.objects[key]=value
    def manifests(self):return list(self.inputs)


def main():
    spark=SparkSession.builder.master('local[2]').appName('s3-daily-contract').getOrCreate()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            store=LocalStore(Path(tmp))
            rows=[('id-1','2026-11-04','kowiki',False,10,'topic',0,1,None),
                  ('id-1','2026-11-04','kowiki',False,10,'topic',0,2,None),
                  ('id-2','2026-11-04','enwiki',True,-2,'topic',1,1,None),
                  ('bad','2026-11-04','enwiki',False,0,'topic',1,2,'invalid'),
                  ('next','2026-11-05','enwiki',False,0,'topic',1,3,None)]
            schema='event_id string,event_date string,wiki string,bot boolean,bytes_delta long,source_topic string,source_partition int,source_offset long,error_reason string'
            path=store.uri('input');spark.createDataFrame(rows,schema).withColumn('event_date',F.to_date('event_date')).write.partitionBy('event_date').parquet(path)
            store.inputs=[{'path':path,'dates':['2026-11-04','2026-11-05']}]
            second=store.uri('input-second')
            spark.read.parquet(path).write.partitionBy('event_date').parquet(second)
            store.inputs.append({'path':second,'dates':['2026-11-04','2026-11-05']})
            for run in ('first','rerun'):
                for action in ('compact_raw','daily_agg','quality_check','publish'):
                    daily(spark,store,action,'2026-11-04',run)
                result=store.get('daily/published/2026-11-04/current.json')
                assert result['clean']['rows']==2
                assert result['aggregate']['rows']==2
                assert spark.read.parquet(result['aggregate']['path']).agg(F.sum('edit_count')).first()[0]==2
            before=store.get('daily/published/2026-11-04/current.json')
            try:daily(spark,store,'publish','2026-11-04','unchecked')
            except ValueError:pass
            else:raise AssertionError('Published without validation')
            assert store.get('daily/published/2026-11-04/current.json')==before
            print('PASS: date isolation, reject exclusion, deduplication, aggregation, replay and publication gate')
    finally:spark.stop()

if __name__=='__main__':main()
