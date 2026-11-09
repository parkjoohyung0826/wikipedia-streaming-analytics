# 6회차: 실제 Kafka → Spark → S3 부하·장애 검증

> 현재 검증 완료: 기본 부하 2회 및 SIGKILL 복구. 50,000 비교는 전후 동일한 워밍업 60초·측정 120초·각 1회 검증 완료, `--extended-only`의 소량 장애 검증을 완료했습니다. Airflow 사용자 설정 보완 후 해당 단계만 재실행했고, 정확한 Kafka 복구 시간은 미완료입니다. 실제 Airflow TaskInstance 실패·재시도 소진·자동 콜백·Discord 수신을 격리된 DAG.test에서 확인했습니다. 최종 상태는 docs/submissions/week6.md·week7.md를 확인하세요.

Wikipedia 스키마의 합성 메시지로 측정합니다. 실제 Wikimedia의 메시지 크기·분포와 다르므로 운영 처리량으로 일반화하지 않습니다. 운영 브로커·토픽·체크포인트를 수정하지 않습니다. 매 실행마다 별도 Kafka 컨테이너·네트워크와 `validation/<UTC run id>` S3 prefix를 사용합니다. 테스트 컨테이너와 네트워크만 종료 시 제거하고 S3 결과·로컬 증빙은 보존합니다. 실제 AWS 요청 비용이 발생합니다.

## 준비 및 실행

프로젝트 루트에서 실행합니다. Docker Desktop, 기존 Spark/Airflow/Kafka 이미지, `config/s3.local.json`, 기존 AWS 프로필이 필요합니다. 기본 Docker 소켓은 macOS Desktop 경로이며 `DOCKER_HOST`로 재지정할 수 있습니다. 호스트 포트 19092가 비어 있어야 합니다.

```bash
python3 -m venv .venv
.venv/bin/pip install -r tests/performance/requirements.txt
.venv/bin/python -m unittest discover -s tests/unit -v

# 120초 워밍업, 100/300/600건/s × 120초 × 2회 + SIGKILL 복구
.venv/bin/python tests/performance/week6_run.py

# 한 변수(maxOffsetsPerTrigger)만 변경한 비교. 기본과 같은 600건/s 두 번
.venv/bin/python tests/performance/week6_run.py \
  --rates 600 --max-offsets 50000 --skip-faults

# SIGKILL만 별도 재실행할 때
.venv/bin/python tests/performance/week6_run.py --crash-only

# Kafka 실제 60초 중단, 불량 50%, S3 오류 주입, 작업 및 Airflow 재시도
.venv/bin/python tests/performance/week6_run.py --extended-only

# 실행 로그가 출력한 run id를 지정
.venv/bin/python tests/performance/week6_report.py artifacts/week6/<run-id>
```

동시에 여러 실험을 실행하지 마세요. 기본 실험은 약 20분 이상 걸릴 수 있으며 과부하 시 drain 시간이 더 필요합니다. 다른 작업의 컨테이너는 자동으로 중지하지 않습니다. 실행 당시 다른 컨테이너 목록을 환경 증빙에 남깁니다.

## 측정 정의

- 실제 입력률: Kafka ACK 수 / Producer 시작부터 최종 flush 완료까지의 시간. 목표값과 별도로 기록합니다.
- p50/p95: 합성 이벤트의 `ingest_time`부터 **S3 commit 완료**까지. 30초 trigger 대기·백로그를 포함합니다. Airflow의 운영 10분 대기와 일일 게시 지연은 포함하지 않습니다.
- lag: 파티션별 Kafka end offset - S3에 commit된 end offset. Kafka consumer group offset을 사용하지 않습니다. 5초 표본입니다.
- 처리률: 입력 구간 내 완료된 행 수 / 해당 구간 길이. 30초 단위 commit으로 계단형 수치이며 전체 처리 능력과 동일하지 않습니다.
- 병목 시간: Spark progress의 `durationMs`, callback 내부의 transform/count·Parquet write·metadata·commit 시간을 함께 보관합니다.
- 워밍업은 결과 표와 Spark 시간 집계에서 제외합니다. 각 단계가 모두 처리된 뒤 다음 단계를 시작해 이전 단계 백로그가 섞이지 않게 합니다.
- 최대 처리량은 관측한 범위에서만 말합니다. 짧은 측정과 노트북의 동시 작업 영향을 받으며 단일 p95나 lag 끝값만으로 안정성을 단정하지 않습니다.

## 장애와 합격 기준

| 시나리오 | 실제로 가하는 장애 | 검증 |
|---|---|---|
| Spark 강제 종료 | S3 Parquet 작성 후 commit 직전에 SIGKILL | 이전 게시 유지, 재시작 후 Kafka offset 유일성·입력 ID 완전성·정상 게시 1,100건 |
| Kafka 60초 중단 | 테스트 브로커 pause/unpause | idempotent Producer ACK 전량, 전송 오류 0, 저장 lag 0까지 복구 시간 |
| 불량 급증 | 정상 1,000 + JSON 파손 500 + 스키마 오류 500 | 저장 정상+오류=입력, 정상 이벤트 ID 누락 0 |
| S3 60초 오류 | 실제 Parquet 후 commit 호출에서 ServiceUnavailable 주입 | exit 75, 기존 게시 유지, 재시작 후 유실·offset 중복 0 |
| 일일 작업 실패 | Parquet 작성 뒤 manifest 저장에 SlowDown 주입 | 같은 work ID 재시도, clean/aggregate `exceptAll` 양방향 차이 0 |
| Airflow 재시도 | 실제 Airflow TaskInstance 1차 실패·2차 성공 | exit 75→0, 결과 차이 0. 격리 SQLite와 `DAG.test()` 사용 |

S3 실험은 실제 AWS 서비스를 중단하지 않는 **오류 주입**입니다. 저장소가 S3이므로 강의의 DB 중단을 이 시나리오로 대응합니다. Airflow 실험은 실제 TaskInstance 재시도지만 상시 운영 scheduler와 운영 DAG 전체를 장애 시험한 것은 아닙니다.

오류 행은 정상 행과 같은 commit 경계의 Parquet에 `raw_value`, `error_reason`을 보관합니다. 별도 Kafka DLQ 토픽은 없으며 `error_reason IS NOT NULL`이 DLQ 역할을 합니다. 정상 일일 결과는 이 행을 제외하고 event_id로 중복 제거합니다. 원시 저장층은 중복 *입력*을 보존하고, 정상 게시층의 중복이 0인지 구분합니다.

## 결과와 실패 처리

`artifacts/week6/<run-id>/`:

- `environment.json`, `run.json`: 환경·설정·전체 성공 여부
- `load/producer.jsonl`, `lag.jsonl`, `*.metrics.jsonl`, `*.progress.jsonl`: 원본 계측
- `load/verification.json`: ID 누락·예상 밖 ID·Kafka offset 중복·지연 분포
- `fault/recovery.json`: SIGKILL 검증
- `extended/faults.json`, `airflow-retry.json`: 추가 장애 결과
- `summary.json`: 워밍업을 제외한 집계

실패·중단한 run은 성공 결과에 합치지 않습니다. 부분 JSON append는 실시간 lag 읽기에서만 건너뛰고 최종 보고서는 불완전 파일을 오류로 취급합니다. 동일 batch telemetry를 중복 합산하지 않습니다. 인증 값과 `config.json`은 Git에 추가하지 마세요.

## 운영 알림 기준

Airflow 최종 실패 알림은 `src/alerts.py`에 구현했고 두 DAG의 실패 콜백에 연결했습니다. `.env.local`의 `DISCORD_WEBHOOK_URL`로 수신 채널을 지정합니다. 로컬 단위 테스트, 콜백 직접 호출, 실제 Airflow DAG.test 작업 실패에 따른 자동 콜백의 Discord 수신을 확인했습니다. 아래 지표 감시 기준은 운영 적용안으로, 배치 최종 실패 외에는 아직 자동 감시에 연결하지 않았습니다.

| 신호 | 기준 | 복구/중복 억제 |
|---|---|---|
| 스트리밍 실험 지연 | p95 > 120초가 5분 지속 | 임계치 아래로 돌아온 뒤 재무장 |
| 운영 수집→S3 지연 | 10분 실행 간격을 감안해 p95 > 15분이 10분 지속 | 작업 소요 시간에 맞춰 조정 |
| lag | 10분 동안 지속 증가 | 일시적인 trigger 계단은 제외 |
| 수집 중단 | 입력 0건 10분 | 소스 자체 무변경과 구분 |
| 불량 | 5분 입력 중 오류 > 1%, 입력 100건 이상 | 정상 비율로 복귀한 뒤 재무장 |
| 배치 실패 | 재시도 소진 후 최종 실패만 | 같은 DAG/run/task는 한 번 알림 |

2026-11-08 Discord 수신 화면과 전송 기록은 `docs/submissions/week6.md`에 보관했습니다. 웹훅·인증정보를 저장소에 넣지 않습니다.

Airflow 실행 방식 참고: [공식 DAG.test 안내](https://airflow.apache.org/docs/apache-airflow/2.10.3/core-concepts/debug.html). 실제 실행은 프로젝트 이미지의 Airflow 2.10.5 소스에서도 재시도 상태 처리를 확인했습니다.

7주차 B안의 동일 조건 비교·사전 가설·시간 분해·S3 상세 증빙은 [7주차 제출 문서](../../docs/submissions/week7.md)에 정리했습니다. `week7_compare.py`는 두 실행의 주요 조건과 정합성을 확인하며, Spark progress 누락 배치는 coverage에 명시합니다.
