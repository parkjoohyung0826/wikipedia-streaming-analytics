# 6주차 제출 — Kafka·Spark·Airflow·S3 파이프라인 운영

## 1. 데이터 출처와 규모

데이터 출처는 [Wikimedia 실시간 변경 API](https://stream.wikimedia.org/v2/stream/recentchange)다. 처음 **10분 동안 수집한 변경 기록은 25,860건·36.80MB**이며, 그중 Wikipedia 편집 **3,846건**을 이전 주차 과제에 사용했다.

현재 구성은 기존 Kafka에 저장된 **2026-11-02 편집 12,947건**으로 검증했다. Spark가 처리한 데이터를 S3에 저장하고, Airflow에서 **날짜·위키별 집계 121개**를 게시했다. 121개는 나라 수가 아니라 해당 날짜 데이터에 포함된 위키별 집계 행 수다.

처음의 10분 수집과 이번 12,947건 검증은 서로 다른 입력이다. 날짜별로 집계했다고 해서 그날의 모든 편집을 수집했다는 의미는 아니다.

## 2. 현재 구성과 각 도구의 역할

```text
Wikimedia API → Producer → Kafka
                            ↓ Airflow가 10분마다 실행
                          Spark → S3 저장 → 종료
                            ↓ 매일 03:00 KST
                    중복 제거 → 집계 → 검사 → 게시
```

| 구성 | 하는 일 | 실행 방식 |
|---|---|---|
| Producer | Wikipedia 편집을 Kafka에 보내고 전송 성공 확인 | 상시 |
| Kafka | Spark가 읽기 전 메시지 보관 | 상시 |
| Spark | 스키마 검증·중복 제거·집계·S3 저장 | 작업할 때만 실행 |
| Airflow scheduler | 실행 시간·순서·재시도 관리 | 상시 |
| Airflow DB | DAG와 Task의 실행 상태 보관 | 상시 |
| S3 | 원문을 포함한 처리 결과·오류 사유·체크포인트·집계 보관 | AWS 저장소 |

**Kafka·Spark·Airflow를 모두 사용하며, 편집 데이터 저장소를 S3로 변경했다.** Airflow DB는 편집 데이터가 아닌 운영 상태를 저장하므로 유지한다.

## 3. 컨테이너와 부하 줄이기

기존 상시 6개에서 **상시 4개**로 줄였다. Kafka, Producer, Airflow scheduler, Airflow DB만 계속 실행한다.

- Spark는 10분마다 `availableNow`로 시작 시점까지의 Kafka 메시지를 처리한 뒤 종료한다. 실행 중 새로 들어온 메시지는 다음 작업에서 읽는다.
- Spark 작업 중에는 컨테이너 1개가 추가된다. 두 DAG가 `spark_jobs` pool의 1개 슬롯을 공유해 Spark를 동시에 실행하지 않는다.
- Airflow 웹 화면은 필요할 때만 켠다. 웹 화면까지 켜면 최대 6개가 된다.
- Spark는 `local[2]`, driver memory 1GB, Kafka heap은 512MB로 설정했다. 이는 전체 실제 메모리 사용량과는 다르다.
- Compose는 `docker-compose.yml` 하나로 통합했다.

S3 반영은 즉시 처리가 아니라 **10분 주기 + 대기·처리 시간**만큼 늦어질 수 있다. 노트북과 Docker가 꺼져 있으면 수집·처리도 중단된다.

## 4. 안전한 저장과 재실행

| 상황 | 처리 방식 |
|---|---|
| Kafka 전송 실패 | 전송 성공을 확인하기 전에는 Producer의 재연결 커서를 진행하지 않는다. |
| 잘못된 메시지 | 원문과 `error_reason`을 Parquet에 보존하고 정상 집계에서 제외한다. |
| Spark 중단·재시작 | S3 체크포인트의 Kafka offset에서 이어서 처리한다. |
| Parquet 쓰기 실패 | 성공한 배치의 commit에 기록된 경로만 읽어 불완전한 결과를 제외한다. |
| 같은 이벤트 재입력 | 일일 전처리에서 `event_id` 기준으로 중복을 제거한다. |
| 같은 날짜 재실행 | 검사 후 날짜별 `current.json` 참조를 교체해 결과를 중복해서 더하지 않는다. |
| 일시적인 통신 오류 | 알려진 일시 오류는 최대 3회 추가 재시도하며 지수 백오프를 적용한다. |

일일 DAG는 **날짜 선택 → Kafka 잔여 처리 → 전처리 → 집계 → 품질 검사 → 게시** 순서다. 처리 날짜는 `data_interval_end`를 한국시간으로 바꾼 날짜의 전날이며, 수동 `target_date`를 우선한다.

품질 검사는 정상 행 수, 이벤트 ID 중복, 집계의 편집 수 합계를 확인한다. 통과한 결과만 게시한다. `catchup=False`이므로 생성되지 않은 과거 실행은 날짜를 지정해 처리한다.

## 5. 실제 검증 결과

| 확인 항목 | 결과 |
|---|---|
| Kafka → Spark → S3 | 기존 입력 12,947건 저장 |
| 위키별 집계 | 121개 게시 |
| 같은 체크포인트로 재시작 | 새 입력이 없을 때 완료 배치 수 2개 유지 |
| 같은 날짜 재실행 | Spark `exceptAll` 양방향 비교로 정상 행·집계 전체 값이 동일함을 확인 |
| 최소 구성의 수집 | 실제 Airflow 수집 작업 성공, 작업 후 Spark 컨테이너 제거 |
| 최소 구성의 일일 처리 | 6개 Task 모두 성공 |
| 현재 코드 테스트 | 단위 테스트 13개, Spark 전처리·일일 처리 통합 테스트 통과 |

최소 구성 검증은 **2026-11-05**에 수행했다. 새 실시간 편집이 추가되므로 현재 전체 저장 건수는 검증 시점보다 늘어날 수 있다.

[최소 구성 실행 기록](../../evidence/spark_s3/minimal_runtime.json) · [데이터 재실행 대조](../../evidence/spark_s3/verification.json) · [검증 설명](../../evidence/spark_s3/README.md)

### 부하 테스트와 구분할 점

위 결과는 **실제 연동·데이터 정합성·재실행 검증**이다. 현재 Kafka → Spark → S3 경로에서는 단계별 입력량 증가 실험, 처리량 한계, p95 지연, 강제 장애 후 복구를 다시 측정하지 않았다. 컨테이너 수 감소만으로 CPU·메모리·처리 속도의 개선율을 판단할 수는 없다.

이전 Python-only S3 도구로 측정한 803,000건 처리와 지연 수치는 현재 구성의 부하 성능으로 사용하지 않는다. **6주차 과제의 부하·장애 실험까지 완료하려면 현재 경로에서 추가 측정이 필요하다.** 이 문서에는 실제 완료한 작업과 검증만 기록했다.

## 실행과 확인

```bash
export WIKI_AWS_HOST="$HOME/.aws"
export WIKI_DOCKER_SOCKET=/var/run/docker.sock

python3 scripts/operations.py start
python3 scripts/operations.py ingest
python3 scripts/operations.py trigger --date 2026-11-02
python3 scripts/operations.py ui       # 웹 화면 켜기
python3 scripts/operations.py ui-stop  # 웹 화면만 끄기
python3 scripts/operations.py sleep    # 프로젝트 서비스 정지
```

- [단일 Compose 설정](../../docker-compose.yml)
- [10분 주기 DAG](../../dags/wikipedia_ingest.py) · [일일 DAG](../../dags/wikipedia_daily.py)
- [Spark 전처리·S3 저장](../../src/spark_s3.py)
- [상세 운영 안내](../guides/kafka_spark_s3.md)
