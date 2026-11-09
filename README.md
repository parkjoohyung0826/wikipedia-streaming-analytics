# Wikipedia → Kafka → Spark → S3

**Kafka·Spark·Airflow를 사용하면서 상시 컨테이너는 4개만 실행합니다.**

```text
Wikimedia → 수집기 → Kafka
                     ↓ Airflow가 10분마다 실행
                   Spark → S3 저장 후 종료
                     ↓ 매일 03:00 KST
              날짜별 집계·품질 검사 → S3 게시
```

| 구성 | 실행 방식 |
|---|---|
| Kafka | 상시: 메시지 임시 보관 |
| Producer | 상시: Wikimedia 편집 수집 |
| Airflow scheduler | 상시: 10분 주기 처리와 일일 집계 예약 |
| Airflow DB | 상시: 실행 상태 보관 |
| Spark | 작업할 때만 생성, 완료·실패 후 제거 |
| Airflow 웹 화면 | 필요할 때만 실행 |

Spark 처리 중에는 일시적으로 5개, 웹 화면까지 켜면 최대 6개가 됩니다. 두 DAG는 공유 pool을 사용하므로 Spark 작업은 한 번에 하나만 실행합니다. S3 반영은 10분 주기와 작업 시간만큼 지연될 수 있습니다. 노트북과 Docker가 꺼지면 수집·처리도 중단됩니다.

## 실행

Python 3, Docker, AWS 인증과 `config/s3.local.json`이 필요합니다. 기존에 승인한 인증 폴더·Docker 소켓 연결을 명시합니다.

```bash
export WIKI_AWS_HOST="$HOME/.aws"
export WIKI_DOCKER_SOCKET=/var/run/docker.sock

python3 scripts/operations.py start       # 상시 4개 시작
python3 scripts/operations.py status      # 상태 확인
python3 scripts/operations.py ingest      # 10분을 기다리지 않고 즉시 처리
python3 scripts/operations.py trigger --date 2026-11-02
python3 scripts/operations.py ui          # 웹 화면 켜기: localhost:18080
python3 scripts/operations.py ui-stop     # 웹 화면 끄기
python3 scripts/operations.py sleep       # 프로젝트 서비스 정지
```

Compose는 **docker-compose.yml 하나**만 사용합니다. 환경 설정은 Git에서 제외되는 `.env.local`입니다. 이전 `.env.week5`는 보관만 하며 실행에 사용하지 않습니다. Kafka·Airflow의 기존 데이터 볼륨은 그대로 사용합니다.

## 폴더

| 경로 | 내용 |
|---|---|
| `src/` | Producer, Spark 전처리·S3 저장, Airflow 실행 도우미 |
| `dags/` | 10분 주기 처리·일일 집계 DAG |
| `scripts/operations.py` | 시작·중지·수동 실행 명령 |
| `docker/` | 현재 이미지 빌드 파일 |
| `tests/` | 현재 코드의 단위·Spark 통합 테스트 |
| `samples/` | 테스트 입력 |
| `docs/` | 현재 운영 안내 |
| `evidence/spark_s3/` | 현재 방식의 핵심 검증 결과만 보관 |
| `backups/` | 정리 전 로컬 ZIP, Git 제외 |
| `artifacts/` | 임시 실행 로그·수집 커서, Git 제외 |

이전 주차 코드·문서, PostgreSQL 데이터 저장 코드, Python-only S3 실험과 상세 증빙은 정리했습니다. 삭제 전 로컬 ZIP을 검증했으며, S3 객체와 Docker 데이터 볼륨은 삭제하지 않았습니다.

[저장·재시도 방식](docs/guides/kafka_spark_s3.md) · [검증 기록](evidence/spark_s3/README.md)

## 주차별 제출 문서

[4주차](docs/submissions/week4.md) · [5주차](docs/submissions/week5.md) · [6주차](docs/submissions/week6.md) · [7주차 B안](docs/submissions/week7.md)

주차별 처리 과정과 실행 결과는 위 문서에서 확인할 수 있습니다.
