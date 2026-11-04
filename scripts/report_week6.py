"""Reconcile every acknowledged input and summarize measurements by production cohort."""
from contextlib import closing
import argparse
import json
from pathlib import Path
from datetime import datetime
import statistics
from src.process_edits import connect

def read_lines(path):
    return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--topic',required=True)
    a=p.parse_args();root=Path(a.root);producer=json.loads((root/'producer.json').read_text())
    metrics=read_lines(root/'stream/metrics.jsonl');progress=read_lines(root/'stream/progress.jsonl')
    sink=read_lines(root/'stream/sink.jsonl');rows=[]
    with closing(connect()) as conn,conn,conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM week4.edits WHERE source_topic=%s',(a.topic,));valid=cur.fetchone()[0]
        cur.execute('SELECT count(*) FROM week4.rejected WHERE source_topic=%s',(a.topic,));rejected=cur.fetchone()[0]
        for stage in producer['stages']:
            start,end=stage['started_at'],stage['finished_at']
            cur.execute('SELECT count(*),percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-ingest_time)),percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at-ingest_time)) FROM week4.edits WHERE source_topic=%s AND ingest_time>=%s AND ingest_time<%s',(a.topic,start,end))
            n,p50,p95=cur.fetchone()
            # Metric samples describe sink completion windows, while latency uses each input stage's cohort.
            select=lambda records:[m for m in records if datetime.fromisoformat(start)<=datetime.fromisoformat(m['at'])<datetime.fromisoformat(end)]
            ms=select(metrics);ss=select(sink)
            rows.append(dict(**stage,saved_valid=n,p50_seconds=p50,p95_seconds=p95,
                sink_rps=sum(m['input'] for m in ss)/stage['elapsed'],
                spark_processed_rps_median=statistics.median([m['processed_rps'] for m in ms if m['processed_rps'] is not None]) if ms else None,
                sink_seconds_median=statistics.median([m['seconds'] for m in ss]) if ss else None,
                batch_ms_median=statistics.median([m['batch_ms'] for m in ms if m['batch_ms'] is not None]) if ms else None,
                lag_start=ms[0]['lag'] if ms else None,lag_end=ms[-1]['lag'] if ms else None,
                lag_max=max([m['lag'] for m in ms],default=None)))
    invalid=sum(s['invalid'] for s in producer['stages'])
    result=dict(topic=a.topic,goal={'input_rps':100,'p95_seconds':30,'lag_alert':10000},
        stages=rows,queued=producer['queued'],delivered=producer['delivered'],saved_valid=valid,saved_rejected=rejected,
        all_acknowledged_inputs_accounted=producer['success'] and valid+rejected==producer['delivered'],
        all_stage_counts_match=all(row['saved_valid']==row['queued']-row['invalid'] for row in rows),
        malformed_exactly_preserved=rejected==invalid,normal_after_malformed_saved=rows[-1]['saved_valid']==rows[-1]['queued'],
        final_lag=metrics[-1]['lag'] if metrics else None,alert_samples=len(read_lines(root/'stream/alerts.jsonl')),
        measurement='Kafka -> same production write_audit JDBC sink. ingest_time is refreshed produced_at. Latency ends at DB insert timestamp, not commit acknowledgement. Snapshot/daily DAG excluded.')
    (root/'report.json').write_text(json.dumps(result,indent=2))
    def fmt(value):return '-' if value is None else f'{value:,.2f}'
    lines=['# 부하 측정 결과','',
           '| 단계 | 입력 건/s | 저장 완료 건/s | 배치 중앙값 ms | 지연 p95 초 | lag 처음 → 마지막 |',
           '|---|---:|---:|---:|---:|---|']
    for row in rows:
        lines.append(f"| {row['name']} | {fmt(row['actual_rps'])} | {fmt(row['sink_rps'])} | {fmt(row['batch_ms_median'])} | {fmt(row['p95_seconds'])} | {row['lag_start']} → {row['lag_end']} |")
    lines.extend(['','지연은 해당 단계에서 생성한 정상 메시지 전체를 처리 완료한 후 계산했다. 처리량은 해당 시간 구간에 저장 완료한 정상·오류 메시지 수 / 경과 시간이다. lag는 10초 간격 표본이며 이전 단계의 잔여 부하도 포함할 수 있다.',''])
    (root/'summary.md').write_text('\n'.join(lines))
    print(json.dumps(result,indent=2))
    if not(result['all_acknowledged_inputs_accounted'] and result['all_stage_counts_match'] and result['malformed_exactly_preserved'] and result['normal_after_malformed_saved'] and result['final_lag']==0):raise SystemExit('Verification failed')

if __name__=='__main__':main()
