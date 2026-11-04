# Kafka · Spark · Airflow + S3

## 역할

| 구성 | 담당 |
|---|---|
| Producer | SSE 편집 이벤트를 Kafka에 전달하고 확인된 커서 보관 |
| Kafka | 메시지 임시 보관·순서·재처리 입력 |
| Spark Streaming | 기존 스키마 검증, 정상·오류 행과 원문을 S3 Parquet로 저장 |
| Airflow | 날짜 선택 → compact_raw → daily_agg → quality_check → publish |
| S3 | 처리 결과·오류·체크포인트·검증된 일일 결과 |
| Airflow PostgreSQL | 스케줄·실행 상태만 보관; 편집 데이터 저장소가 아님 |

## 저장 구조

`config/s3.local.json`의 prefix 아래 `spark-v1/`을 사용하므로 이전 Python 실험 객체와 섞이지 않습니다.

```text
spark-v1/
├── stream/checkpoint/       # Kafka offset과 Spark 실행 상태
├── stream/attempts/<UUID>/  # event_date 파티션별 원문·정상 필드·error_reason 포함 Parquet
├── stream/commits/<batch>.json  # 성공한 배치 경로만 공개
├── daily/work/<날짜>/<실행>/   # 입력 목록·전처리·집계·품질 검사
└── daily/published/<날짜>/current.json  # 검증 완료 경로
```

반쯤 작성된 Parquet는 읽지 않습니다. Spark 쓰기가 성공한 뒤 commit 객체를 만들고, 일일 처리는 commit에 기록된 경로만 읽습니다. 실패한 시도의 객체는 증거로 남아 저장 비용이 생길 수 있습니다. 체크포인트와 commits는 함께 보존해야 하며, 같은 체크포인트로 스트림을 두 개 실행하면 안 됩니다.

스트림에는 Kafka 원문(raw_value)과 topic·partition·offset, 검증 필드·오류 사유가 남습니다. 중복 event_id는 일일 compact_raw에서 Spark로 제거합니다. 전체 행을 Python 드라이버로 가져오지 않습니다. 일일 입력 목록은 해당 Airflow 실행의 첫 단계에서 고정하며, 같은 실행을 재시도할 때 유지합니다. 늦게 도착한 데이터는 해당 날짜를 새 실행으로 재처리해야 포함됩니다.

집계는 한국시간 event_date와 wiki별 edit_count, bot_edit_count, size_known_count, net_bytes_delta입니다. quality_check가 중복·건수·집계 합계를 검사한 뒤 publish가 날짜별 current.json 한 객체를 갱신합니다. 같은 날짜 재실행은 결과를 덧붙이지 않고 참조를 교체합니다. 입력이 없으면 기존 게시 결과를 덮어쓰지 않고 실패합니다.

## 스케줄·재시도

기존 DAG와 같이 03:00 KST, catchup=False입니다. data_interval_end의 한국 날짜에서 하루를 뺀 날짜를 처리하고 target_date를 우선합니다. 날짜·품질 오류는 바로 실패하며, 작업은 기본 최대 3회 추가 재시도와 지수 백오프를 사용합니다. S3 SlowDown·RequestTimeout·InternalError·ServiceUnavailable은 재시도 대상으로 분류합니다. 외부 메시지 발송은 설정하지 않습니다.

## 운영 조건

Kafka 데이터 볼륨, Producer 재연결 커서, Airflow 운영 DB는 로컬에 필요합니다. 데이터 저장을 S3로 바꿔도 Kafka나 Airflow 내부 상태까지 S3로 대체할 수는 없습니다. 과제용 Kafka 단일 브로커는 장애 이중화 구성이 아닙니다.

AWS 인증은 코드·이미지에 넣지 않습니다. 이번 환경에서는 사용자의 허용을 받아 AWS 폴더 읽기 전용 연결과 Airflow의 Docker 소켓 연결을 사용합니다. `.dockerignore`로 로컬 인증 설정과 환경 파일을 빌드 컨텍스트에서 제외했습니다.

기존 Python-only 부하 실험은 별도 경로의 결과입니다. 이 구조의 실제 검증은 [검증 기록](../../evidence/spark_s3/README.md)에 별도로 남깁니다.

## 실행 전 연결 설정

권한 연결을 허용한 환경에서만 다음 경로를 설정합니다. AWS 폴더는 Spark에 읽기 전용으로 전달되며 Docker 소켓은 Airflow가 Spark 작업 컨테이너를 생성할 때 사용합니다.

```bash
export WIKI_AWS_HOST="$HOME/.aws"
export WIKI_DOCKER_SOCKET=/var/run/docker.sock
python3 scripts/operations.py start
```

Docker 소켓 위치가 다른 환경에서는 실제 위치로 변경합니다. 설정하지 않으면 실행 도구가 중단됩니다.

## 이번 수정의 검증 상태

- 단위 테스트 20개 통과, Python 구문과 Compose 구성 검증 통과.
- 실제 Docker 이미지 빌드·Kafka → Spark → S3 저장 및 Airflow 5개 작업 성공을 확인했습니다.
- Spark 통합 검증 코드: `tests/integration/test_spark_s3.py`. 날짜 격리, 중복 제거, 오류 제외, 집계, 재실행, 품질 검사 전 게시 차단을 확인합니다. Spark 컨테이너에서 실행하여 통과했습니다.

S3 경로는 원자적 디렉토리 rename을 전제로 하지 않고, 고유 시도 경로를 쓴 뒤 완료 참조를 게시합니다. 참고: [Hadoop S3 커밋 설명](https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/committers.html).
