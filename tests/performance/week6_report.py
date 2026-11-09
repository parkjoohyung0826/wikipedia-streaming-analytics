"""Summarize evidence without treating incomplete experiments as passed."""
import argparse
import json
import statistics
from datetime import datetime
from pathlib import Path


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def slope(samples):
    if len(samples)<2:return None
    xs=[r['at']-samples[0]['at'] for r in samples]
    ys=[r['lag'] for r in samples]
    mx,my=statistics.mean(xs),statistics.mean(ys)
    denominator=sum((x-mx)**2 for x in xs)
    return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/denominator if denominator else None


def summarize(root):
    load=root/'load'
    producers=lines(load/'producer.jsonl');lag=lines(load/'lag.jsonl')
    # Retries of a committed batch must not double-count telemetry.
    batches={r['batch_id']:r for p in load.glob('*.metrics.jsonl') for r in lines(p)}
    metrics=list(batches.values())
    verification_path=load/'verification.json'
    verified=json.loads(verification_path.read_text()) if verification_path.exists() else None
    by_source={r['source']:r for r in verified['stages']} if verified else {}
    measured_producers=[r for r in producers if r['stage']!='warmup']
    first=min((r['started_at'] for r in measured_producers),default=float('inf'))
    progress=[r for p in load.glob('*.progress.jsonl') for r in lines(p)
              if r.get('numInputRows',0)>0 and datetime.fromisoformat(r['timestamp'].replace('Z','+00:00')).timestamp()>=first]
    results=[]
    for stage in measured_producers:
        start,end=stage['started_at'],stage['finished_at']
        samples=[r for r in lag if start<=r['at']<=end]
        completed=[r for r in metrics if start<=datetime.fromisoformat(r['committed_at']).timestamp()<=end]
        item=dict(stage=stage['stage'],target_per_s=stage['requested_rate'],
            actual_input_per_s=stage['actual_ack_per_s'],input_rows=stage['acknowledged'],
            input_elapsed_s=stage['elapsed_s'],errors=stage['errors'],pending=stage['pending'],
            window_completion_per_s=sum(r['rows'] for r in completed)/(end-start),
            lag_first=samples[0]['lag'] if samples else None,lag_last=samples[-1]['lag'] if samples else None,
            lag_max=max((r['lag'] for r in samples),default=None),lag_slope_per_s=slope(samples),
            verification=by_source.get('synthetic-week6-'+stage['stage']))
        results.append(item)
    durations={}
    for key in sorted({k for r in progress for k in r.get('durationMs',{})}):
        values=[r['durationMs'][key] for r in progress if key in r['durationMs']]
        durations[key]={'mean_ms':statistics.mean(values),'max_ms':max(values),'samples':len(values)}
    measured_batches=[r for r in metrics if datetime.fromisoformat(r['committed_at']).timestamp()>=first]
    components={}
    for key in sorted({k for r in measured_batches for k in r.get('timings_ms',{})}):
        values=[r['timings_ms'][key] for r in measured_batches if key in r.get('timings_ms',{})]
        components[key]={'mean_ms':statistics.mean(values),'total_ms':sum(values),'samples':len(values)}
    run=json.loads((root/'run.json').read_text()) if (root/'run.json').exists() else {'success':False}
    recovery=json.loads((root/'fault/recovery.json').read_text()) if (root/'fault/recovery.json').exists() else None
    return {'run':root.name,'execution':run,'mode':'continuous 30-second trigger; excludes Airflow scheduling delay',
        'latency_definition':'producer ingest_time to S3 batch commit; p50/p95 include backlog and trigger waiting',
        'lag_definition':'Kafka end offsets minus S3 committed offsets; sampled every 5 seconds',
        'measurement_notes':['Warmup excluded from stages and Spark timings.',
            'Each stage drains before the next stage; window throughput is not whole-run throughput.',
            'Short windows and 30-second commits cause lag sawtooth; inspect raw trend and repeated p95.',
            'No end-to-end Airflow or external notification verification is implied.'],
        'stages':results,'spark_progress':durations,'batch_components':components,
        'load_verification':verified,'recovery':recovery,'external_notification':'not configured or tested',
        'extended_faults':json.loads((root/'extended/faults.json').read_text()) if (root/'extended/faults.json').exists() else None,
        'last_sampled_lag':lag[-1]['lag'] if lag else None,
        'verified_unstored_input_rows':sum(r['acknowledged'] for r in producers)-verified['rows'] if verified else None}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('run',type=Path);args=parser.parse_args()
    result=summarize(args.run)
    (args.run/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()
