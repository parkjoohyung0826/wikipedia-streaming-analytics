# 테스트 안내

프로젝트 루트에서 실행한다.

## 단위 테스트 — 7개

Kafka·Spark를 실행하지 않고 수집 파서와 Producer의 처리를 검사한다. requirements.txt의 Python 의존성이 필요하다.

```bash
python3 -m unittest discover -s tests/unit -v
```

## Spark 변환 테스트

```bash
docker compose -f docker-compose.yml -f compose.week4.yml run --rm -T spark tests/integration/test_transform.py
```

이미지가 없다면 먼저 `python3 scripts/week4.py setup`으로 준비한다.

## 전체 장애 복구·재전송 검증

```bash
python3 scripts/week4.py verify
```

`fixtures/week4_fixture.py`는 합성 테스트 데이터를 만드는 도구다. 실제 수집 데이터와 구분한다.

## 스트리밍 v2

`python3 scripts/week4.py verify-stream`은 별도 DB·토픽에서 실행한다. `integration/test_streaming.py`는 실제 Spark 상태·지연·중복·commit 후 복구·연속 저장을, `integration/check_stream_sample.py`는 실제 Kafka 샘플 20건의 DB·Parquet·집계 일치와 재전송을 검증한다.
