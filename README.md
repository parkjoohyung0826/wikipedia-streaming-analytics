# Wikipedia 실시간 데이터 파이프라인

**Kafka · Spark · Airflow를 유지하고 데이터 저장소를 S3로 사용합니다.**

```text
Wikimedia API → Producer → Kafka → Spark 전처리 → S3 Parquet
                                          ↓
Airflow 매일 03:00 KST → Spark 날짜별 중복 제거·집계·검증 → S3 게시
```

Kafka는 메시지 전달과 임시 보관, Spark는 실제 데이터 처리, Airflow는 실행 순서·스케줄·재시도를 담당합니다. S3는 원문을 포함한 처리 결과, 오류 사유, 체크포인트와 날짜별 집계를 보관합니다.

## 실행

Docker, 기존 AWS CLI 인증, `config/s3.local.json` 설정이 필요합니다. 먼저 [권한 연결 설정](docs/guides/kafka_spark_s3.md)을 확인하세요. 실제 Kafka → Spark → S3 저장과 Airflow 5개 작업을 검증했습니다.

```bash
python3 scripts/operations.py start
python3 scripts/operations.py status
python3 scripts/operations.py trigger --date 2026-11-04
python3 scripts/operations.py sleep
```

`compose.s3.yml`을 사용하는 실행 경로입니다. 기존 PostgreSQL 데이터 저장용 서비스는 띄우지 않지만 **Airflow 메타데이터 DB는 유지**합니다. Kafka 메시지 볼륨도 유지합니다. Spark를 로컬 2개 스레드로 실행하고 Airflow 동시 작업은 1개로 제한합니다. 노트북과 Docker가 꺼져 있으면 새 데이터 수집·예약 처리는 실행되지 않습니다.

실측 결과: 기존 Kafka 편집 **12,947건**을 S3로 저장하고, Airflow에서 **위키별 집계 121개**를 게시했습니다. [검증 기록](evidence/spark_s3/README.md)

## 코드와 결과

- `src/live_producer.py`: API → Kafka, 전송 확인 후 커서 갱신
- `src/spark_s3.py`: Kafka → Spark → S3, 날짜별 집계·품질 검사·게시
- `dags/wikipedia_daily.py`: 기존 5개 작업과 03:00 KST 스케줄
- `src/airflow_support.py`: Spark 작업 컨테이너 실행과 실패 처리
- `tests/`: 단위·통합 검증
- [S3 저장 방식](docs/guides/kafka_spark_s3.md)

`python3 scripts/s3.py run`은 Kafka·Spark·Airflow를 거치지 않는 별도 실험 도구입니다. 과제의 기본 실행 경로가 아닙니다. 해당 도구로 얻은 10분 42,188건 수집과 803,000건 부하 실험 결과를 새 파이프라인 검증 결과로 해석하면 안 됩니다.
