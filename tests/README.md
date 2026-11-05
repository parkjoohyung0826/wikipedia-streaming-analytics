# 현재 코드 테스트

```bash
pip install -r requirements.txt
python3 -m unittest discover -s tests/unit -v
```

단위 테스트는 Producer 확인·재연결 커서·S3 게시 계약을 검증합니다. Spark 통합 테스트는 `tests/integration/test_transform.py`, `test_spark_s3.py`이며 Spark 이미지에서 실행합니다. 실제 S3 재실행 대조는 `test_spark_s3_remote.py`입니다.
