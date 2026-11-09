# 현재 코드 테스트

```bash
pip install -r requirements.txt
python3 -m unittest discover -s tests/unit -v
```

단위 테스트는 Producer 확인·재연결 커서·S3 게시 계약을 검증합니다. Spark 통합 테스트는 `tests/integration/test_transform.py`, `test_spark_s3.py`이며 Spark 이미지에서 실행합니다. 실제 S3 재실행 대조는 `test_spark_s3_remote.py`입니다.

## 6회차 부하·장애 테스트

[실행·측정 정의·합격 기준](performance/README.md)을 참고하세요. 성능 하네스 단위 테스트까지 실행하려면 `pip install -r tests/performance/requirements.txt`가 필요합니다.
