"""Compare the actual sample through Kafka -> clean Parquet -> streaming SQL aggregate."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from src.process_edits import connect, database_reader, EDIT_COLUMNS

p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--pipeline-id',required=True)
p.add_argument('--phase',required=True);args=p.parse_args()
spark=SparkSession.builder.config('spark.sql.session.timeZone','UTC').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
try:
    rows=[json.loads(x) for x in Path('samples/wikipedia_edits.jsonl').read_text().splitlines() if x.strip()]
    expected={x['meta']['id']:x for x in rows}
    clean=spark.read.parquet(args.root+'/clean').cache()
    actual=clean.collect() # Bounded 20-record test fixture, never used in production.
    assert len(actual)==len(expected)==20
    for row in actual:
        src=expected[row.event_id]
        assert row.title==src['title'] and row.wiki==src['wiki'] and row.bot==src['bot']
        assert row.event_time.replace(tzinfo=timezone.utc).timestamp()==src['timestamp']
        assert row.bytes_delta==src['length']['new']-src['length']['old']
    db=database_reader(spark).option('dbtable','week4.edits').load().select(*EDIT_COLUMNS)
    assert db.count()==20
    assert clean.select(*EDIT_COLUMNS).exceptAll(db).count()==0
    assert db.exceptAll(clean.select(*EDIT_COLUMNS)).count()==0
    expected_agg=(clean.groupBy(F.window('event_time','1 minute'),'wiki').agg(
        F.count('*').alias('edit_count'),F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
        F.count('bytes_delta').alias('size_known_count'),F.sum('bytes_delta').alias('net_bytes_delta'))
        .select(F.col('window.start').alias('window_start'),F.col('window.end').alias('window_end'),
                'wiki','edit_count','bot_edit_count','size_known_count','net_bytes_delta'))
    saved=database_reader(spark).option('dbtable','week4.windowed_wiki').load().filter(F.col('pipeline_id')==args.pipeline_id)
    cols=expected_agg.columns
    assert saved.select(*cols).exceptAll(expected_agg).count()==0
    assert expected_agg.exceptAll(saved.select(*cols)).count()==0
    result=dict(verified=True,phase=args.phase,source_records=20,database_records=20,parquet_records=20,
                aggregate_groups=saved.count(),full_row_equality=True,full_aggregate_equality=True,
                checked_at=datetime.now(timezone.utc).isoformat(),root=args.root,pipeline_id=args.pipeline_id)
    dest=Path('evidence/week4/stream_v2_sample_'+args.phase+'.json')
    dest.write_text(json.dumps(result,indent=2)+'\n')
    print('PASS actual Kafka sample '+args.phase)
finally:
    spark.stop()
