# Spark 실시간 처리 v2

## 목적과 흐름

[제출 문서](../submissions/week4.md)의 1·3·5절에 단계별 설명, 스키마, 실행 명령을 정리했다.

- `src/stream_edits.py`: 운영 실행 코드. `src/process_edits.py`의 검증 및 원본 DB 저장 함수를 재사용한다.
- audit: Kafka → 전체 정상/오류 DB. 늦은 정상 기록도 보존한다.
- clean: Kafka → 스키마 검증 → 10분 watermark → event_id 중복 제거 → 날짜별 Parquet append.
- aggregate: Parquet 스트림 → 10분 watermark → event_time 1분·wiki별 집계 → update 모드 → DB upsert.

[공식 Spark 3.5.8 문서](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html)의 제약에 따라 update 모드에서 두 상태 연산을 연결하지 않고 파일을 사이에 둔 두 쿼리로 분리했다.

## 실행과 복구

```bash
python3 scripts/week4.py setup
python3 scripts/week4.py send --input samples/wikipedia_edits.jsonl --rate 10
python3 scripts/week4.py run
python3 scripts/week4.py run --continuous
python3 scripts/week4.py verify-stream
```

기본 root는 `artifacts/week4/stream_v2/main`, pipeline ID는 `main-v2`다. 각각 audit/clean/aggregate checkpoint를 사용한다. 기존 `artifacts/week4/checkpoints/main`과 snapshot 폴더를 덮어쓰지 않는다. 유한 실행은 세 쿼리를 차례로 완료하며 연속 실행은 동시에 실행한다. 어느 쿼리든 실패하면 나머지도 중지한다.

정상·오류·집계 적재는 executor JDBC staging 후 DB SQL 병합으로 처리한다. 드라이버로 모든 행을 모으지 않는다. [저장 개선 상세](storage_feedback.md).

집계 값은 누적 합계 자체를 저장한다. `cnt = cnt + 새값`이 아니라 `cnt = 새값`으로 교체한다. `(pipeline_id,batch_id)` 커밋 기록을 집계와 한 transaction에 저장하므로 저장 직후 실패해도 같은 batch가 통계를 다시 변경하지 않는다. 빈 배치도 완전히 소비해 Spark 상태 처리를 완료한다.

DB와 파일은 하나의 transaction이 아니므로 순간적으로 진행 정도가 다를 수 있다. 재시작하면 각각의 checkpoint부터 따라잡는다. 파일 출력은 Spark 파일 sink의 `_spark_metadata`를 함께 보존한다. 같은 root의 동시 writer는 지원하지 않는다. 파일·DB·checkpoint를 함께 백업한다.

설정이 바뀌거나 일부 checkpoint가 없어진 상태의 위험한 재사용은 검사하여 거절한다. 새 처리 정책은 새 root와 pipeline ID로 시작한다. 중복 제거 상태는 영구 ID 저장소가 아니라 watermark 범위 안에서 관리한다. 동일 ID의 event_time을 바꿔 오래 뒤 재전송하는 입력은 계약 밖이다.

## 시간과 출력 해석

10분 watermark는 wall clock이나 전송 시각이 아니라 최대 event_time 기준이다. 과거 날짜 입력이라는 이유만으로 버리지 않는다. 그러나 재생 파일 내부에서 순서가 크게 뒤섞인 기록은 제외될 수 있다. future timestamp 이상치나 실제 지연 분포에 대한 운영 정책은 추후 보완 대상이다.

원본 DB는 모든 유효 기록을, clean 파일과 집계는 허용 지연 범위 내 기록을 대상으로 한다. 두 건수의 차이가 곧 유실은 아니다. `progress.json`의 `stateOperators.numRowsDroppedByWatermark`로 제외 수를 확인한다. 이 파일은 유한 실행 완료 시 저장되며 연속 실행 도중의 진행은 Spark UI·로그에서 확인한다.

원본 DB의 `processed_at`은 PostgreSQL transaction 시작 시각이다. clean 파일은 DB를 거치지 않아 이 열이 없고, 15개 이벤트 열에 `event_date`만 추가된다. 기존 DB snapshot과 스키마가 다르므로 경로를 섞어 읽지 않는다.

## 테스트

`verify-stream`은 별도 DB·토픽을 생성한다. 합성 파일 소스 테스트로 중복·지연 도착·집계 갱신·commit 후 실패 복구·재시작을 확인하고 실제 Wikimedia 20건은 Kafka부터 끝까지 최초 처리/재전송한다. 실패한 단계가 있으면 명령이 실패로 종료된다. DB·토픽은 분석을 위해 자동 삭제하지 않는다.

보고서:
- [상태·지연·복구 검증](../../evidence/week4/stream_v2_verification.json)
- [실제 샘플 최초 처리](../../evidence/week4/stream_v2_sample_initial.json)
- [실제 샘플 재전송](../../evidence/week4/stream_v2_sample_replay.json)

기존 회귀 검증은 `python3 scripts/week4.py verify`와 `demo`로 실행한다.
