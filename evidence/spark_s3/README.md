# 최소 컨테이너 구성 검증 (현재)

- 상시 4개: Kafka, Producer, Airflow scheduler, Airflow DB.
- Spark는 10분 주기로 실행하고 완료 후 컨테이너를 제거합니다. 공유 pool 1개 슬롯으로 동시 실행을 제한합니다.
- 웹 화면은 기본 중지, 필요할 때만 실행합니다.
- 현재 단위 테스트 13개와 Spark 전처리·날짜별 처리 통합 테스트 통과.
- 실제 수동 수집 작업과 일일 처리 6개 작업 모두 성공했습니다.
- [실행 상태 기록](minimal_runtime.json)

이전 주차 자료는 로컬 `backups/before-minimal-20261105T122545.zip`에 보관한 뒤 정리했습니다. 백업은 Git에 포함하지 않습니다. 아래는 같은 S3 데이터 처리 로직의 이전 검증 기록입니다.

# Kafka · Spark · Airflow + S3 검증

2026-11-05 실제 Docker와 AWS S3에서 확인했습니다.

- 기존 Kafka 데이터 **12,947건**을 Spark로 처리하고 날짜 파티션 Parquet와 체크포인트를 S3에 저장했습니다.
- 같은 체크포인트로 availableNow를 다시 실행했을 때 완료 배치는 **2개 그대로**였습니다. 이후 실시간 수집이 추가되므로 현재 배치 수는 늘어납니다.
- Airflow `wikipedia_daily`의 5개 작업이 모두 성공했습니다. 처리 날짜는 **2026-11-02**입니다.
- 정상 데이터 **12,947건**, 날짜·위키별 집계 **121개**를 게시했습니다.
- 같은 날짜를 다른 DAG 실행으로 다시 처리하고 Spark `exceptAll`을 양방향으로 비교했습니다. **정상 행 전체와 집계 전체가 동일**했습니다.
- 새 실시간 Wikimedia 데이터도 Producer → Kafka → Spark → S3로 저장되는 것을 확인했습니다.
- 단위 테스트 **20개**, 로컬 Spark 날짜·중복·오류 제외·게시 조건 통합 테스트가 통과했습니다.

[전체 대조 결과](verification.json) · [첫 게시](first_publish.json) · [재게시](second_publish.json) · [재실행 Task 상태](replay_tasks.json)

이 결과는 기능·재실행 검증입니다. 이전 Python-only 실험의 803,000건 처리량 결과를 이 구조의 부하 테스트 결과로 사용하지 않습니다.

검증 중 macOS UID의 Spark Ivy 경로 오류와 여러 배치 파티션의 경로 충돌을 수정했습니다. 초기 실패한 Airflow 실행 이력은 남겨 두었습니다. 이 검증의 작은 요약만 유지하며, 과거 주차 실행 로그는 정리했습니다. 현재 실행 로그는 `artifacts/airflow/`에 저장합니다.
