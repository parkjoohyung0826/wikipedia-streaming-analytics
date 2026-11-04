# 기존 컨테이너 기반 수집과 일일 처리

> 2026-11-04부터 기본 운영은 [컨테이너 없는 S3 배치](s3_batch.md)로 전환했다. 이 문서는 기존 과제 재현용이며 현재 상시 운영하지 않는다. 과거 원본·checkpoint는 [S3 백업](../../evidence/s3_archives/README.md)으로 이전했으므로 해당 상태로 재현하려면 필요한 파일을 먼저 복원한다.

## 운영 흐름

```text
Wikimedia SSE — 계속 연결
  → live-producer → Kafka wikipedia-live
  → live-spark → PostgreSQL week4.edits / week4.rejected
  → 매일 한국시간 03:00 Airflow wikipedia_daily
  → 전날 정상 데이터의 Parquet 정리·일일 집계·검사·게시
```

실시간 적재와 일일 집계를 분리했다. 운영 Spark는 5초마다 최대 20,000건을 읽는다. Kafka 원본 토픽 `wikipedia-edits`와 새 운영 토픽 `wikipedia-live`는 별도다. 부하 테스트는 또 다른 `week6-*` 토픽과 별도 DB를 사용한다.

2026-11-02 저녁부터 새 데이터를 수집하기 시작했다. 그 이전에 수집하지 않은 날짜가 자동으로 채워지는 것은 아니다. 첫날은 수집 시작 이후의 일부 시간만 포함한다. 03시 이후 늦게 도착한 전날 데이터는 해당 날짜로 재실행해야 집계에 반영된다.

## 시작·조회·정지

Docker Desktop을 실행한 상태에서 프로젝트 폴더에서 실행한다.

```bash
python3 scripts/operations.py start --legacy-streaming
python3 scripts/operations.py status
python3 scripts/operations.py stop
```

`start`는 수집기·Spark·Kafka·DB·Airflow를 시작하고 DAG의 일시 정지를 해제한다. `stop`은 수집기·Spark·Airflow를 정지하며 DB와 Kafka의 저장 데이터는 유지한다. 다른 프로젝트의 컨테이너는 제어하지 않는다.

**Mac과 Docker가 계속 실행 중이어야 한다.** Mac 종료·절전 중에는 수집·스케줄 실행을 할 수 없다. `restart: unless-stopped`는 Docker 재시작 후 서비스 복구를 돕지만, Docker Desktop 자체를 시작하거나 Mac을 깨우는 기능은 아니다. 24시간 운영이 필요하면 같은 구성을 항상 켜진 서버로 옮겨야 한다.

[Airflow](http://localhost:18080) 로컬 계정은 `airflow` / `airflow`다. 서비스 포트는 localhost에만 노출한다.

## 장애와 재시작

| 상황 | 동작 |
|---|---|
| SSE 연결 종료 | 1초부터 최대 60초까지 대기 후 재연결 |
| 수집기 재시작 | 저장한 Last-Event-ID부터 다시 요청 |
| Kafka 전송 실패 | 확인되지 않은 메시지 뒤로 수집 커서를 진행시키지 않음 |
| Spark·DB 일시 장애 | Spark 프로세스 실패 시 컨테이너 재시작, 기존 checkpoint에서 재처리 |
| 같은 이벤트 재수신 | event_id 기본키로 중복 저장 방지 |
| 잘못된 원천 메시지 | 수집기 rejected_source.jsonl에 보존 |
| Kafka 메시지 검증 실패 | DB week4.rejected에 보존 |
| Airflow 품질 검사 실패 | 게시 차단, 기존 게시 결과 유지 |

외부 SSE의 보존 기간을 넘는 장기 중단이나 서버 측 누락까지 복구를 보장하지 않는다. 커서가 거부되면 알림 로그를 확인하고 복구 범위를 판단해야 한다. Kafka 보존 기간은 7일이다. `failOnDataLoss=true`이므로 checkpoint보다 오래된 Kafka 메시지가 삭제되면 조용히 건너뛰지 않고 실패한다.

## 어디를 확인하나?

| 위치 | 내용 |
|---|---|
| `artifacts/live/cursor.json` | 마지막 수신·전송 시각, 누적 확인 전송 건수, 재연결 커서 |
| `artifacts/live/alerts.jsonl` | SSE·Kafka 연결 및 전송 오류 |
| `artifacts/live/stream/metrics.jsonl` | Kafka lag, 배치 시간, 처리 속도, 최근 저장 지연 p50·p95 |
| `artifacts/live/stream/alerts.jsonl` | lag 10,000건 또는 최근 지연 p95 30초 초과, 스트림 오류 |
| `artifacts/live/stream/checkpoint/` | Spark 복구 위치 |
| `artifacts/week5/alerts.jsonl` | Airflow 최종 실패 |
| `data/live/YYYY-MM-DD/events.jsonl` | 선택한 Wikipedia 편집의 원본 보관, UTC 수신일 기준 |

알림은 로컬 기록이다. 담당자가 로그와 화면을 확인해야 하며, 외부 푸시 알림은 설정하지 않았다. 지연 값은 DB 삽입 시각 기준이며 트랜잭션 커밋 완료까지의 시간은 아니다.

컨테이너 로그는 20MB × 3개로 제한한다. **원본 JSONL·메트릭·Parquet 버전·DB는 자동 삭제하지 않는다.** 장기 운영 시 디스크 사용량을 확인하고 보관 정책을 정해야 한다. 실습에서는 부하 테스트 결과 DB도 검증을 위해 보존한다.

## 과거 날짜 재처리

```bash
python3 scripts/week5.py trigger --date 2026-11-02
```

현재 날짜를 지정하면 그 시점까지 저장된 부분 집계다. 다음 날 정기 실행 시 같은 날짜에 새 데이터가 추가됐으면 다시 갱신한다. `catchup=False`이므로 미생성된 과거 실행은 필요한 날짜를 직접 지정한다.

## 자료

- [Wikimedia EventStreams 공식 문서](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service)
- [Spark Kafka 연동 공식 문서](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html)
- [Airflow 일일 집계](../submissions/week5.md)


## 노트북에서 잠시 쉬기

발열과 메모리 사용을 줄이려면 프로젝트의 운영 컨테이너 7개를 모두 정지한다.

```bash
python3 scripts/operations.py sleep
```

DB·Kafka 볼륨과 수집 커서·Spark checkpoint는 삭제하지 않는다. 정지 중에는 새 수집과 새벽 3시 자동 집계가 실행되지 않는다. 다른 프로젝트 컨테이너는 건드리지 않는다.

다시 사용할 때:

```bash
python3 scripts/operations.py start --legacy-streaming
```

재시작 시 저장된 커서와 checkpoint로 복구를 시도한다. 장기 중단이 외부 SSE나 Kafka 보존 기간을 넘으면 누락 없이 복구한다고 보장할 수 없다. catchup=False이므로 빠진 일일 집계는 필요한 target_date로 별도 실행한다.
