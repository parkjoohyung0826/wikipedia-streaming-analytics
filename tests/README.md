# 테스트

로컬 Python 단위 테스트에는 `pip install -r requirements.txt`가 필요합니다. Kafka Producer 테스트가 confluent-kafka를 사용합니다.

```bash
python3 -m unittest discover -s tests/unit -v
```

Spark·S3용 저장 계약 테스트는 `tests/unit/test_spark_s3_store.py`입니다. AWS 없이 불변 commit, 게시 참조 교체, 권한 오류 처리를 확인합니다.

Spark 통합 검증은 이미지 빌드 후 `spark-submit tests/integration/test_spark_s3.py`로 실행합니다. 로컬 임시 파일로 날짜별 중복 제거·집계·게시 절차를 검사하며 실제 AWS 연결은 별도 검증해야 합니다.
