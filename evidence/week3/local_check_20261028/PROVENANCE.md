# 다른 작업 폴더에서 수행한 로컬 Kafka 검증 기록

원래 실행 위치: `/Users/parkjoohyung/Documents/메타코드 데이터엔지니어링 부트캠프/3week_homework`

원본 결과 위치: `artifacts/environment_check_20261028T112005Z/`

2026-10-28 로컬 실행에서는 해당 폴더의 Producer와 샘플로 Kafka 20건을 전송하고 Python Consumer로 20건을 읽어 대조했다. Spark·PostgreSQL·Parquet 실행은 포함하지 않았다. 본 디렉토리에는 그 기록을 출처와 함께 복사했으며 원본 JSON의 경로나 시각은 바꾸지 않았다.

- `producer_stdout.json`: 성공 20건, 실패 0건
- `consumer_messages.txt`: Python Consumer가 받은 key와 JSON
- `verification.json`: ID·key·원문·전송 위치 대조 결과
- `restoration.json`: 당시 허용받아 정지했던 Airflow 컨테이너 17개의 실행 복원 상태

이 증거는 현재 wikipedia-streaming-analytics 코드의 로컬 Spark 검증 성공을 뜻하지 않는다. 현재 저장소의 Spark 검증 근거는 `../week4/ci_verification.json`이다.
