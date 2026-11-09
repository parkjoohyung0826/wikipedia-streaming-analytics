"""Compare two matched week6 harness runs without claiming maximum capacity."""
import argparse
from datetime import datetime
import json
import statistics
from pathlib import Path
from week6_report import summarize, lines


def details(root):
    summary = summarize(root)
    env = json.loads((root / 'environment.json').read_text())
    assert summary['execution']['success'], 'Failed run cannot be compared'
    verification = summary['load_verification']
    assert verification['missing_ids'] == verification['unexpected_ids'] == 0
    assert verification['rows'] == verification['distinct_kafka_offsets']
    assert summary['verified_unstored_input_rows'] == 0
    assert len(summary['stages']) == 1
    stage = summary['stages'][0]
    producer = [p for p in lines(root/'load/producer.jsonl') if p['stage'] != 'warmup'][0]
    measured = []
    for path in (root/'load').glob('*.metrics.jsonl'):
        measured += [r for r in lines(path) if r['rows'] and
                     datetime.fromisoformat(r['committed_at']).timestamp() >= producer['started_at']]
    # Align progress and callback timings by batch id, including a batch that
    # started just before measurement but committed measured input afterwards.
    batch_ids = {r['batch_id'] for r in measured}
    progress = [r for p in (root/'load').glob('*.progress.jsonl') for r in lines(p)
                if r.get('batchId') in batch_ids and r.get('numInputRows', 0) > 0]
    summary['progress_coverage'] = {'measured_batches': len(batch_ids),
        'recorded_batches': len({r['batchId'] for r in progress}),
        'missing_batch_ids': sorted(batch_ids - {r['batchId'] for r in progress})}
    summary['spark_progress'] = {}
    for key in sorted({k for r in progress for k in r.get('durationMs', {})}):
        values = [r['durationMs'][key] for r in progress if key in r.get('durationMs', {})]
        summary['spark_progress'][key] = dict(mean_ms=statistics.mean(values), max_ms=max(values), samples=len(values))
    # Include the final draining batches: their latency belongs to measured input.
    return dict(run=root.name, environment=env, summary=summary,
                measured_batches=measured,
                mean_batch_rows=sum(r['rows'] for r in measured)/len(measured))


def compare(before, after):
    b, a = details(before), details(after)
    bp, ap = dict(b['environment']['parameters']), dict(a['environment']['parameters'])
    assert bp.pop('max_offsets') == 10000 and ap.pop('max_offsets') == 50000
    assert bp == ap, 'Parameters other than read cap differ'
    for key in ('docker', 'spark_master', 'driver_memory', 'spark_container_memory',
                'kafka_partitions', 'kafka_memory', 'shuffle_partitions', 'spark_image_id'):
        assert b['environment'][key] == a['environment'][key], key
    assert b['summary']['stages'][0]['input_rows'] == a['summary']['stages'][0]['input_rows']
    return {'before': b, 'after': a,
            'controls': 'Only max_offsets changes; fresh topic, checkpoint and prefix per run',
            'limitations': 'One sequential pair, fixed input rate, other host workloads uncontrolled; no maximum throughput claim.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('before', type=Path)
    parser.add_argument('after', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.before, args.after)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    for label in ('before', 'after'):
        s = result[label]['summary']['stages'][0]
        print(label, result[label]['run'], 'p95=', s['verification']['latency_p50_p95_s'][1],
              'window_commit_rows/s=', s['window_completion_per_s'])
