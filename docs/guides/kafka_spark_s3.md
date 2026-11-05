# 최소 컨테이너 운영

## 실행 순서

`wikipedia_ingest`는 10분마다 Spark를 실행합니다. Spark Structured Streaming의 availableNow로 시작 시점까지의 Kafka 메시지를 처리하고 종료합니다. 처리 중 새로 들어온 메시지는 다음 실행에서 읽습니다. offset과 처리 상태는 S3 체크포인트로 이어받습니다.

`wikipedia_daily`는 매일 03:00 KST에 실행합니다.

```text
날짜 선택 → Kafka 잔여 처리 → 중복 제거 → 위키별 집계 → 품질 검사 → 게시
```

두 DAG의 Spark 작업은 `spark_jobs` pool 1개 슬롯을 공유합니다. 같은 체크포인트를 동시에 사용하지 않으며, 일일 집계 전에 잔여 Kafka 메시지를 먼저 저장합니다. Spark는 작업마다 생성되고 finally에서 제거됩니다. 웹 화면은 예약 실행에 필요하지 않아 기본적으로 꺼 둡니다.

## 처리 날짜와 재시도

일일 처리는 data_interval_end를 한국시간으로 바꾼 날짜의 전날입니다. 수동 `target_date`가 있으면 우선합니다. catchup=False이므로 생성되지 않은 과거 실행은 날짜를 지정해 처리해야 합니다.

일시적인 전송·실행 오류는 최대 3회 재시도하고 지수 백오프를 적용합니다. 날짜·품질 오류는 즉시 실패합니다. 실패 기록은 `artifacts/airflow/alerts.jsonl`과 Airflow 로그에 남습니다.

## S3 저장

설정 prefix 아래 `spark-v1/`에 저장합니다.

```text
stream/checkpoint/                 Kafka offset·Spark 상태
stream/attempts/<UUID>/event_date=…/  원문·검증 필드·오류 사유 Parquet
stream/commits/<batch>.json         완료된 배치 경로
daily/work/<날짜>/<실행>/            입력 목록·전처리·집계·검사
daily/published/<날짜>/current.json  검증된 최종 결과 경로
```

완료 표시가 있는 배치만 읽으므로 실패한 Parquet 파일은 최종 결과에 섞이지 않습니다. 체크포인트와 commits는 함께 보존해야 합니다. 일일 처리는 대상 날짜의 파티션만 읽고 event_id 중복을 제거합니다. Spark가 분산 처리하며 전체 데이터를 Python 드라이버로 가져오지 않습니다.

입력 목록은 DAG 실행마다 고정합니다. 같은 실행을 재시도할 때는 동일 입력을 사용합니다. 늦게 도착한 데이터는 해당 날짜를 새로 실행해 반영합니다. 품질 검사는 중복·건수·집계 합계를 확인하며, 성공한 뒤 current.json을 갱신합니다. 입력이 없으면 이전 결과를 덮어쓰지 않습니다.

## 리소스와 보관

상시 4개 컨테이너, Spark 실행 중 1개 추가입니다. Spark는 local[2]와 driver memory 1g, Kafka heap은 512MB로 설정했습니다. 컨테이너 로그는 파일당 10MB, 2개로 제한합니다. 컨테이너 수 감소가 고정적인 메모리 감소량을 보장하지는 않습니다.

Kafka는 임시 메시지, Airflow DB는 운영 상태를 보관하므로 로컬 볼륨이 필요합니다. Producer의 재연결 커서도 로컬에 유지합니다. 분석 데이터와 Spark 체크포인트는 S3입니다. 이전 환경 파일은 보관만 하고 현재 `.env.local`을 사용합니다.

AWS 폴더는 읽기 전용으로 Spark에 연결하며, Airflow는 승인된 Docker 소켓으로 Spark 컨테이너를 생성합니다. 인증 정보는 이미지·Git에 포함하지 않습니다. 이전 S3 데이터와 백업 ZIP은 이번 정리에서 삭제하지 않았습니다.
