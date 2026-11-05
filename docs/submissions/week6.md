# 6주차 제출 — 파이프라인 동작과 검증 결과

데이터 출처는 Wikimedia 실시간 변경 API다. 최초 10분 수집은 **25,860건·36.80MB**, Wikipedia 편집은 **3,846건**이었다. 현재 구성은 이와 별개의 **2026-11-02 편집 12,947건 → 날짜·위키별 집계 121개**로 검증했다. 날짜별 집계가 하루 전체 수집을 뜻하지는 않는다.

## 실행 구조와 환경

```text
Producer → Kafka → Spark availableNow → S3 Parquet·완료 commit
                         ↑
              Airflow가 10분마다 실행
```

상시 컨테이너는 Kafka·Producer·Airflow scheduler·Airflow DB 4개다. Spark는 작업할 때만 추가되고 종료 후 제거된다. 웹 화면은 필요할 때만 켠다.

| 항목 | 확인한 값 |
|---|---|
| 노트북 | Apple M4, CPU 10코어, 메모리 16GiB |
| Docker 할당 | CPU 10개, 메모리 약 7.65GiB |
| 버전 | Docker 29.2.0, Kafka 3.8.0, Spark 3.5.8, Airflow 2.10.5 |
| Spark 설정 | `local[2]`, driver memory 1GB, shuffle partitions 2 |
| Kafka 설정 | 토픽 `wikipedia-live`, 파티션 3개, 단일 브로커 |
| 입력 제한 | `maxOffsetsPerTrigger=10000` |
| 실행·저장 | Airflow 공유 pool 1개 슬롯, S3 서울 리전 |

환경 값은 2026-11-05 확인한 설정이다.

## 1. 데이터 처리 과정과 결과

- 실제 Kafka 데이터 12,947건을 Spark로 처리해 S3에 저장했다.
- 새 입력이 없는 상태에서 같은 체크포인트로 재시작했을 때 완료 배치가 2개로 유지됐다.
- 같은 날짜를 다시 처리한 정상 행과 집계 전체가 동일함을 Spark로 비교했다.
- 최소 구성에서 수집 DAG와 일일 DAG의 6개 Task가 모두 성공했다.

| 검증 항목 | 결과 |
|---|---|
| 단위 테스트 | 현재 최소 구성에서 13개 통과 |
| Spark 변환 테스트 | 통과 |
| Spark 일일 처리 테스트 | 통과 |
| 대상 날짜 정상 데이터 | 12,947행 |
| 날짜·위키별 집계 | 121행 |
| 동일 날짜 재실행 비교 | 정상 데이터·집계 전체 값 동일 |
| 새 입력 없이 체크포인트 재시작 | 추가 완료 배치 0개 |
| Airflow 실행 | 수집 Task 및 일일 DAG 6개 Task 성공 |

121행은 해당 날짜에 데이터가 있던 위키별 집계 수다. 위키는 `enwiki`, `kowiki` 같은 프로젝트 식별자이며 나라 수가 아니다. 같은 입력으로 재실행해도 데이터 내용은 같지만, 저장 경로와 게시 시각은 달라질 수 있다.

[실행·테스트 기록](../../evidence/spark_s3/minimal_runtime.json) · [재실행 비교 기록](../../evidence/spark_s3/verification.json)

## 2. 오류 처리와 결과 보호

| 상황 | 대응 |
|---|---|
| Kafka 전송 미확인 | Producer 커서를 진행하지 않고 재연결·재시도 |
| 잘못된 Kafka 메시지 | 원문·`error_reason` 보존, 정상 집계에서 제외 |
| 알려진 일시 S3·Docker 오류 | Airflow 재시도·지수 백오프 |
| 부분 Parquet 쓰기 | 완료 commit 없는 경로를 읽지 않음 |
| 품질 검사 실패 | 새 결과 게시 차단, 기존 `current.json` 유지 |
| 같은 날짜 재실행 | `event_id` 중복 제거 후 날짜별 게시 참조 교체 |

날짜 결정·품질 검사는 재시도하지 않는다. 최종 실패는 Airflow 상태와 `artifacts/airflow/alerts.jsonl`에 기록한다. **외부 알림 전달은 구현하지 않았다.**

## 3. 작업별 실행 시간

`minimal_daily_20261105T1234` 실행 기록에서 시작·종료 시각 차이를 계산했다. 처리 날짜는 2026-11-02다.

| Task | 소요 시간 |
|---|---:|
| `resolve_target_date` | 0.22초 |
| `drain_kafka` | 93.51초 |
| `compact_raw` | 108.06초 |
| `daily_agg` | 47.84초 |
| `quality_check` | 34.66초 |
| `publish` | 3.59초 |
| Task 시간 합계 | 287.88초 |

각 Task 시간에는 Spark 컨테이너 준비와 실행 비용도 포함된다. 전체 DAG 경과 시간에는 공유 pool 대기도 추가된다. 한 번의 실행이며 준비 구간을 제외한 반복 성능 측정이 아니다. 특히 `drain_kafka`는 당시 Kafka의 잔여 입력을 처리하므로 일일 집계 12,947건과 동일 입력이라고 가정하지 않는다.

이 실행에서는 데이터 정리·중복 제거를 하는 `compact_raw`가 108.06초로 가장 오래 걸렸다. 이 기록만으로 처리량 한계나 지연 원인이 S3·셔플·CPU 중 무엇인지는 판단할 수 없다.

[Task 원본 기록](../../evidence/spark_s3/minimal_runtime.json) · [동일 날짜 결과 비교](../../evidence/spark_s3/verification.json)

## 아직 확인하지 않은 범위

위 결과는 정상 처리와 정상 재실행 검증이다. **입력 속도를 단계적으로 높이는 부하 테스트와 처리 중 Spark를 강제로 종료하는 장애·복구 테스트는 아직 실행하지 않았다.** 따라서 최대 처리량, 메시지 지연 p95, Kafka 미처리량(lag) 추이, 강제 장애 후 유실·중복·복구 시간은 확인되지 않았다. 외부 알림 연결과 수신 검증도 남아 있다.

[현재 실행 방법](../../README.md) · [Spark 코드](../../src/spark_s3.py) · [5주차 Airflow 문서](week5.md)
