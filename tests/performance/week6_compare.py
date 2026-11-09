"""Create a Korean readout from three successful, inspectable experiment runs."""
import argparse
import json
from pathlib import Path
from week6_report import summarize


def read(path):return json.loads(path.read_text())
def number(value):return '미측정' if value is None else f'{value:,.2f}'


def build(baseline,tuned,extended):
    before,after=summarize(baseline),summarize(tuned)
    extra=read(extended/'extended/faults.json')
    for directory in (baseline,tuned,extended):
        if not read(directory/'run.json')['success']:raise ValueError(f'Incomplete run: {directory}')
    env=read(baseline/'environment.json')
    rows=[]
    for name,result in [('기본 10,000',before),('변경 50,000',after)]:
        for s in result['stages']:
            v=s['verification']
            if v is None:raise ValueError('Missing stage verification')
            rows.append(f"| {name} | {s['stage']} | {number(s['actual_input_per_s'])} | {number(s['window_completion_per_s'])} | {number(v['latency_p50_p95_s'][1])} | {s['lag_max']:,} | {number(s['lag_slope_per_s'])} |")
    b600=[s['verification']['latency_p50_p95_s'][1] for s in before['stages'] if s['target_per_s']==600]
    a600=[s['verification']['latency_p50_p95_s'][1] for s in after['stages'] if s['target_per_s']==600]
    span=lambda values:f'{min(values):.1f}~{max(values):.1f}'
    fault=before['recovery'];kafka=extra['kafka'];sink=extra['sink'];bad=extra['invalid_storm']
    components='\n'.join(f"| {k} | {v['mean_ms']/1000:.3f} | {v['samples']} |" for k,v in before['batch_components'].items())
    progress='\n'.join(f"| {k} | {v['mean_ms']/1000:.3f} | {v['max_ms']/1000:.3f} |" for k,v in before['spark_progress'].items())
    text=f'''# 6회차 결과 — 부하, 병목, 장애·복구

2026-11-07 Wikipedia 이벤트 스키마를 따르는 **합성 데이터**를 실제 로컬 Docker와 AWS S3에서 처리했습니다. **600건/초 입력에서 `maxOffsetsPerTrigger`만 10,000→50,000으로 변경한 비교 실험의 지연 p95는 {span(b600)}초→{span(a600)}초입니다.** 각 값은 같은 조건으로 두 번 실행한 구간의 범위이며, 합쳐서 계산한 p95가 아닙니다.

## 환경과 범위

- Docker {env['docker']['ServerVersion']}, CPU {env['docker']['NCPU']}개 할당, 메모리 {env['docker']['MemTotal']/1024**3:.2f}GiB.
- Spark 3.5.8, `local[2]`, driver 1GiB, 컨테이너 상한 2GiB, shuffle partitions 2.
- Kafka 3.8.0, 격리 단일 브로커·3개 파티션, 컨테이너 상한 1GiB.
- 합성 데이터는 짧고 반복되는 제목·payload를 사용합니다. 실제 Wikimedia 메시지 크기·분포에서 같은 처리량이 나온다는 의미는 아닙니다. 각 구간의 평균 메시지 바이트도 verification에 기록했습니다.
- 실제 S3의 실행별 `validation` prefix, 실제 Parquet·commit·checkpoint.
- 120초 워밍업 제외, 각 부하 구간 120초, 각 조건 2회. 구간 사이 lag를 소진합니다.
- 기존 Kubernetes 컨테이너도 실행 중이었습니다. 다른 워크로드는 중지하지 않았고 환경 파일에 이름을 기록했습니다. 장기 운영 보장이나 엄밀히 통제된 하드웨어 벤치마크는 아닙니다.
- 측정은 **30초 trigger 연속 스트리밍**입니다. 운영 Airflow의 10분 간격 `availableNow` 실행을 그대로 측정한 결과가 아닙니다.

## 부하 측정

| 설정 | 반복/목표 | 실제 ACK 건/s | 입력 구간 내 commit 건/s | S3 commit 지연 p95 초 | 최대 lag 건 | lag 회귀 기울기 건/s |
|---|---|---:|---:|---:|---:|---:|
{chr(10).join(rows)}

지연은 Producer `ingest_time`부터 S3 commit까지이며 trigger 대기와 백로그를 포함합니다. lag는 Kafka end offset에서 S3 commit offset을 뺀 값으로 5초마다 측정했습니다. commit 처리률은 **입력 중인 120초 창**의 수치이며, 마지막 배치가 다음 창에서 완료될 수 있어 목표 입력률보다 작게 나올 수 있습니다. lag는 30초마다 떨어지는 톱니 모양이므로 끝값 하나만으로 판정하지 않습니다.

기본 부하의 총 {before['load_verification']['rows']:,}건, 변경 실험의 총 {after['load_verification']['rows']:,}건에는 각각 워밍업 12,000건이 포함됩니다. 두 실험 모두 예상 정상 ID 누락·예상 밖 ID 0, 저장 행 수와 고유 Kafka offset 수가 일치했습니다.

## 병목과 한 변수 비교

기본 설정은 배치가 30초 안에 끝나더라도 `10,000 / 30 ≈ 333건/s`의 입력 상한을 만듭니다. 파티션별 할당 반올림으로 개별 배치 행 수는 약간 달라질 수 있습니다. 600건/s 구간에서 배치가 약 10,000건에 머무르는지, lag가 증가하는지, `triggerExecution`이 30초보다 짧은지를 원본 progress와 함께 확인합니다.

배치 내부 실측 평균(워밍업 제외):

| 구간 | 평균 초 | 배치 수 |
|---|---:|---:|
{components}

Spark progress의 실측 시간:

| 항목 | 평균 초 | 최대 초 |
|---|---:|---:|
{progress}

`parquet_write_ms`는 S3 출력 단계 전체의 벽시계 시간입니다. 순수 네트워크 전송 시간만 분리한 수치가 아닙니다. 가장 오래 걸리는 단계와 전체 처리량을 제한하는 설정은 구분해야 합니다.

비교 실험은 `WIKI_MAX_OFFSETS=50000`만 바꾸고 Spark 코어·메모리·shuffle·trigger 간격·600건/s 입력·측정 길이를 유지했습니다. **600건/s까지만 개선 후 검증했으며 최대 처리량을 확정하지 않았습니다.** 운영 기본값은 10,000을 유지하고 환경변수로 선택할 수 있게 했습니다. 운영 10분 주기에서도 같은 지연 개선이 난다고 단정하지 않습니다.

## 장애 검증

| 시나리오 | 결과 | 복구 시간/근거 |
|---|---|---|
| Spark SIGKILL | 원시 {fault['rows']:,}건 = 고유 Kafka offset, 정상 게시 1,100건 = 고유 event_id, 기존 게시 유지 | 재시작→stream 완료 {fault['recovery_s']:.2f}초 |
| Kafka 실제 60초 중단 | 전송 ACK 전량, ID 누락 0, offset 중복 0 | 브로커 복구→lag 0 {kafka['recovery_to_zero_lag_s']:.2f}초 |
| 불량 메시지 50% | 정상 {bad['valid_rows']:,} + 오류 {bad['rejected_rows']:,} = 입력 {bad['input_rows']:,} | JSON 파손 500 + 스키마 오류 500, 정상 ID 누락 0 |
| S3 commit 오류 주입 | transient exit 75, 재시도 후 ID 누락·offset 중복 0, 기존 게시 유지 | 재시작→stream 완료 {sink['recovery_s']:.2f}초 |
| 일일 작업 중간 실패 | 같은 work ID 재시도, 정상 행·집계 양방향 차이 0 | {extra['batch_retry']['rows']:,}개 정상 행 |
| Airflow 재시도 | 실제 TaskInstance exit 75→0, DAG success, 결과 차이 0 | 격리 SQLite + `DAG.test()`, 운영 scheduler는 대상 아님 |

SIGKILL 입력은 정상 고유 1,100건 + 중복 입력 100건 + 오류 20건(JSON 파손 10건·스키마 오류 10건) = 1,220건입니다. 원시층은 중복 입력도 보관하며, 최종 정상 게시층에서 중복 0을 확인합니다. 불량 행은 원시 Parquet의 `error_reason`과 `raw_value`에 보관되어 DLQ 역할을 합니다. 별도 Kafka DLQ 토픽을 만들지는 않았습니다.

S3 시나리오는 실제 AWS 장애가 아니라, 실제 Parquet 작성 후 commit 호출을 60초 지연하고 `ServiceUnavailable`을 발생시키는 오류 주입입니다. 실제 저장소가 S3이므로 강의의 DB 중단 시나리오를 여기에 대응했습니다. Kafka 시나리오에서는 격리 브로커를 실제로 pause/unpause했습니다.

## 수정 내용과 남은 범위

- 실행별 격리 브로커·토픽·S3 경로, 반복 부하·워밍업 제외·ID 단위 검증·시간 분해·재현 명령을 추가했습니다.
- 기존 Producer 하네스의 음수 `poll()` 대기 경쟁 조건을 단조 시계와 한 번 계산한 양수 대기로 수정했습니다. 이 버그로 중단한 초기 실행은 측정에서 제외했습니다.
- offset 메타데이터는 캐시된 변환 결과에서 계산해 불필요한 원본 재계산을 줄였습니다.
- Spark foreachBatch가 감싼 원인 예외를 보존해 S3 일시 오류의 재시도 분류가 유지되도록 보완하고 회귀 테스트를 추가했습니다.
- 외부 알림 수신 채널은 연결하지 않았습니다. 운영 지연·lag·불량률·입력 중단·최종 실패의 기준과 중복 억제 방침은 실행 안내에 정리했습니다.
- 장시간 soak test, 실제 AWS 서비스 중단, 운영 Airflow scheduler 전체의 장애복구, 외부 웹훅 수신은 이번 검증 범위 밖입니다.

## 재현과 증빙

실행 명령과 측정 정의: `tests/performance/README.md`.

- 기본 + SIGKILL: `{baseline.name}`
- 50,000 비교: `{tuned.name}`
- 추가 장애 + Airflow: `{extended.name}`

각 실행의 원본은 `artifacts/week6/<run-id>/`에 있습니다. 제출용 증빙은 `evidence/week6/`에 보관합니다. 인증 설정과 config 파일은 증빙에서 제외합니다. 성능 보고서는 `week6_report.py`, 이 문서는 `week6_compare.py`로 다시 생성할 수 있습니다.
'''
    return text


def main():
    p=argparse.ArgumentParser();p.add_argument('--baseline',type=Path,required=True);p.add_argument('--tuned',type=Path,required=True);p.add_argument('--extended',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(build(a.baseline,a.tuned,a.extended))

if __name__=='__main__':main()
