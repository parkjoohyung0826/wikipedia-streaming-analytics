"""Week 7 option B: one-setting comparison using completed experiment evidence."""
import argparse
import json
import statistics
from pathlib import Path
from week6_report import summarize


def build(baseline,tuned):
    before,after=summarize(baseline),summarize(tuned)
    for result in (before,after):
        if not result['execution']['success']:raise ValueError('Only completed successful runs may be compared')
    b=[s for s in before['stages'] if s['target_per_s']==600]
    a=[s for s in after['stages'] if s['target_per_s']==600]
    if len(b)!=2 or len(a)!=2:raise ValueError('Expected two 600 events/s measurements per condition')
    for collection in (b,a):
        for item in collection:
            if item['verification'] is None:raise ValueError('Missing verification')
    throughput=lambda rows:statistics.mean(r['window_completion_per_s'] for r in rows)
    p95=lambda rows:[r['verification']['latency_p50_p95_s'][1] for r in rows]
    span=lambda values:f'{min(values):.2f}~{max(values):.2f}'
    rows=[]
    for label,items in [('개선 전 10,000',b),('개선 후 50,000',a)]:
        for r in items:
            rows.append(f"| {label} | {r['stage']} | {r['actual_input_per_s']:.2f} | {r['window_completion_per_s']:.2f} | {r['verification']['latency_p50_p95_s'][1]:.2f} | {r['lag_max']:,} | {r['lag_slope_per_s']:.2f} |")
    return f'''# 7주차 README — B. 병목 개선

**`maxOffsetsPerTrigger`를 10,000→50,000으로 바꿔, 같은 600건/s 입력의 120초 구간 내 commit 처리률이 평균 {throughput(b):.1f}→{throughput(a):.1f}건/s, 지연 p95가 {span(p95(b))}→{span(p95(a))}초로 바뀌었습니다.** 처리률은 입력 구간의 완료량이며 최대 지속 처리량을 뜻하지 않습니다.

## 선택과 변경

7회차 **B안**을 진행했습니다. 6회차에서 확인한 30초 trigger와 배치당 읽기 상한의 조합을 개선 대상으로 삼았습니다. API·추론 서버는 추가하지 않았습니다.

```python
.option('maxOffsetsPerTrigger', int(os.getenv('WIKI_MAX_OFFSETS', '10000')))
```

실험에서 변경한 값은 `WIKI_MAX_OFFSETS=50000` 하나입니다. Spark 코어, driver 메모리, shuffle partitions, Kafka 파티션, trigger 간격, S3 저장 로직과 계측 코드는 동일합니다. 성능 개선 외에 하네스의 대기 오류와 계측을 정리한 작업은 두 비교 조건에 공통으로 적용했습니다.

## 같은 조건의 재측정

| 항목 | 두 조건 공통 |
|---|---|
| 입력 | Wikipedia 스키마 합성 메시지, 목표 600건/s |
| 준비·측정 | 120초 워밍업 제외, 120초 측정 × 2회 |
| 시작 상태 | 이전 구간의 백로그를 소진한 뒤 시작 |
| Spark | 3.5.8, local[2], driver 1GiB, 메모리 상한 2GiB, shuffle 2 |
| Kafka | 3.8.0, 단일 격리 브로커, 파티션 3 |
| trigger | 30초 |
| 저장 | 실제 S3 Parquet + commit + checkpoint |
| 지연 | ingest_time→S3 commit, 입력 큐와 trigger 대기 포함 |

기본 run은 100→300→600건/s 전체를 두 번, 변경 run은 비교 대상인 600건/s를 두 번 실행합니다. 부하 이력은 완전히 동일하지 않으며, 각 조건의 워밍업과 단계별 drain으로 영향을 줄였습니다. 노트북의 다른 Kubernetes 워크로드도 유지했으므로 완전히 통제된 실험은 아닙니다.

| 조건 | 반복 | 실제 입력 건/s | 구간 내 commit 건/s | p95 초 | 최대 lag 건 | lag 기울기 건/s |
|---|---|---:|---:|---:|---:|---:|
{chr(10).join(rows)}

동일한 개선 후 행을 [6주차 결과 표](week6.md)에도 반영했습니다. 각 반복의 p95 범위를 표시했으며 두 p95의 평균을 전체 데이터 p95라고 부르지 않습니다.

## 원인 해석

배치가 30초 이내에 완료될 때 기본 읽기 상한은 대략 `10,000 / 30 = 333건/s`입니다. 입력 600건/s를 따라가지 못하면 lag가 증가합니다. `addBatch` 안에서 S3 Parquet 쓰기가 가장 큰 비중이어도, 실제 배치 시간이 30초보다 짧다면 입력 상한 때문에 대기하는 시간이 처리량을 제한할 수 있습니다. 6주차의 시간 분해와 lag 추이, 이번 단일 변수 비교를 함께 근거로 판단했습니다.

50,000은 읽기 상한을 높인 값이며 실제로 매번 50,000건을 강제로 만드는 설정은 아닙니다. 데이터가 600건/s로 들어오면 30초당 약 18,000건을 처리할 여지를 줍니다. 메모리 사용량과 저장 시간이 커질 수 있으므로 무제한으로 늘리지 않았습니다.

## 정합성과 적용 범위

두 부하 실험의 ID 누락·예상 밖 ID는 0이며, 저장 행 수와 distinct Kafka offset 수가 같습니다. 오류·중복 입력의 게시층 검증과 장애 복구 결과는 [6주차 README](week6.md)에 기록했습니다.

이번 결과는 연속 스트리밍 실험입니다. 운영은 Airflow가 10분마다 `availableNow`를 실행하므로 운영 전체 지연이 같은 비율로 줄었다고 주장하지 않습니다. **운영 기본값은 10,000을 유지**하며 실험 또는 별도 검증 시 환경변수로 50,000을 선택합니다. 두 번째 병목 개선은 보너스 항목으로 남겼습니다.

## 재현과 증빙

```bash
python tests/performance/week6_run.py
python tests/performance/week6_run.py --rates 600 --max-offsets 50000 --skip-faults
python tests/performance/week7_report.py \\
  --baseline artifacts/week6/{baseline.name} \\
  --tuned artifacts/week6/{tuned.name} --output docs/submissions/week7.md
```

의존성·실행 안내: [performance README](../../tests/performance/README.md).

- 개선 전: `evidence/week6/{baseline.name}/`
- 개선 후: `evidence/week6/{tuned.name}/`
- 원본 ACK·lag·Spark progress·검증 JSON을 보관했습니다. 기존 개선 전 수치는 덮어쓰지 않습니다.
'''


def main():
    p=argparse.ArgumentParser();p.add_argument('--baseline',type=Path,required=True);p.add_argument('--tuned',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.write_text(build(a.baseline,a.tuned))

if __name__=='__main__':main()
