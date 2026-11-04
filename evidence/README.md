# 현재 기본 파이프라인

- [Kafka · Spark · Airflow + S3 실제 검증](spark_s3/README.md)

아래는 이전 Python-only S3 실험과 백업 기록입니다.

# 검증 결과

- [10분 실제 수집](s3/20261104T125959_017ae9.json): 원본 42,188건, 정상 편집 3,541건
- [S3 저장 검증](s3/README.md)
- [6회차 부하·장애 실험](week6_s3/README.md)
- [기존 원본의 S3 백업](s3_archives/README.md)
- [이번 정리 목록](cleanup.json) · [로컬 백업 복원](../docs/legacy_restore.md)

이전 방식의 상세 로그와 S3 예비 실험은 로컬 백업으로 옮겼습니다. 현재 결과 요약과 주요 검증 자료만 유지합니다.
