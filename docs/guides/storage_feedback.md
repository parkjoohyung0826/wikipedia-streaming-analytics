# week4 피드백 반영: 분산 DB 적재와 날짜별 Parquet 갱신

## 1. 드라이버 병목 제거

기존에는 `foreachBatch` 안에서 `toLocalIterator()`로 모든 행을 Python 드라이버에 가져와 INSERT했다. `ON CONFLICT`는 이미 사용했지만, 데이터를 한곳에 모으는 병목은 남아 있었다.

현재는 다음 순서다.

```text
Spark executor의 파티션 1 ─┐
Spark executor의 파티션 2 ─┼─ JDBC batch insert → staging 테이블
Spark executor의 파티션 3 ─┤                       ↓ 모두 성공
Spark executor의 파티션 4 ─┘                PostgreSQL에서 일괄 병합
                                    정상·오류 기록 / 집계 / 배치 커밋 기록
```

- 코드: [db_sink.py](../../src/db_sink.py), [storage.sql](../../sql/storage.sql).
- 드라이버는 적재 완료 후 SQL 병합 명령과 작은 건수 요약만 처리한다. 이벤트 행을 `collect()`나 `toLocalIterator()`로 모으지 않는다.
- JDBC 쓰기 파티션은 기본 4개, 요청당 전송 묶음은 1,000행이다. `--jdbc-partitions`는 1~32로 제한하며 실제 동시 실행 수는 executor 코어 수에도 제한된다. 현재 로컬 실행은 `local[2]`다.
- 정상·오류 staging은 `load_id`별로 격리한다. 최종 병합은 정상·오류 INSERT와 staging 삭제를 같은 transaction에서 수행한다.
- JDBC 작업이 재시도돼 staging에 같은 행이 두 번 들어와도 DB에서 PK별로 한 행을 고른다. 정상 기록은 `ON CONFLICT DO NOTHING`, 집계는 `ON CONFLICT DO UPDATE`로 **현재 합계를 교체**한다.
- 집계와 `(pipeline_id,batch_id)` 완료 기록은 함께 커밋한다. commit 직후 실패해도 동일 배치가 집계와 `updated_at`을 다시 바꾸지 않는다.
- Spark 상태 연산의 출력은 JDBC action으로 끝까지 소비한다. 재시도한 배치도 동일하다.

`foreachBatch` 자체는 문제의 원인이 아니다. 배치 안에서 행을 드라이버로 가져오는 동작을 제거한 것이다. DB 서버의 SQL 병합·인덱스·디스크 성능은 여전히 처리량 한계가 될 수 있다. 더 많은 JDBC 연결이 무조건 빠른 것은 아니며, 부하 테스트 후 조정해야 한다.

## 2. 변경된 날짜만 snapshot 갱신

실시간 `clean/` Parquet은 이미 append 방식이다. 이번 개선은 `snapshot` 명령의 **DB 전체 재출력**에 적용했다.

```text
변경 전 CURRENT: 10/25 → 파일 A, 10/26 → 파일 B
10/25에 새 편집 추가
새 CURRENT:      10/25 → 새 파일 A2, 10/26 → 기존 파일 B
```

- 코드: [snapshot_store.py](../../src/snapshot_store.py).
- PostgreSQL statement trigger가 실제 INSERT·UPDATE·DELETE에 영향을 받은 UTC 날짜의 revision을 올린다. 중복으로 무시된 INSERT는 날짜를 변경하지 않는다.
- 첫 설치에서는 기존 날짜를 한 번 등록한다. 최초 새 포맷 export는 전체 날짜를 한 번 작성하며, 그 이후에는 revision이 바뀐 날짜만 다시 읽고 쓴다. 변경이 없으면 파일도 CURRENT도 쓰지 않는다.
- 변경 날짜는 `event_time` 범위 조건으로 DB에서 읽는다. 시간 인덱스를 활용할 수 있으며, 날짜 밖의 이벤트를 Spark로 가져오지 않는다.
- 변경 날짜의 편집과 시간별 집계를 새 불변 경로에 작성한다. 모든 파일 쓰기가 끝나면 날짜별 경로 목록을 담은 `CURRENT.json`을 원자적으로 교체한다.
- 수정·삭제로 비게 된 날짜는 새 manifest의 파티션 목록에서 제외한다. 과거 파일은 자동 삭제하지 않는다.
- DB revision과 JDBC 데이터는 동일한 PostgreSQL repeatable-read snapshot을 사용한다. export 중 들어온 변경은 다음 export에서 처리한다.
- 같은 출력 root의 export는 파일 잠금으로 중복 실행을 막는다. snapshot reader는 `read_snapshot(spark, manifest)`를 사용한다. `versions/` 전체를 직접 읽으면 과거·미완성 버전이 섞일 수 있다.

파일 예시:

```text
artifacts/week4/parquet/
  CURRENT.json                  # version=2, 날짜별 완료 경로
  versions/<실행ID>/
    event_date=2026-10-25/
      edits/                    # 그 날짜의 정상 기록
      hourly_wiki/              # 그 날짜의 시간·위키 집계
```

기존 `version` 없는 snapshot manifest도 `read_snapshot`에서 읽을 수 있다. 과거 `snapshots/` 파일은 삭제하지 않는다. 새 포맷에서는 단일 `snapshot` 경로 대신 `partitions`의 경로를 사용한다. 실행 환경 안의 절대 경로를 기록하므로 컨테이너에서는 `/workspace/...`로 보인다.

## 3. 실행과 검증

```bash
# 실시간 경로: 분산 staging 적재
python3 scripts/week4.py run --jdbc-partitions 4
# DB에서 변경된 날짜만 snapshot 갱신
python3 scripts/week4.py snapshot --export-only
# 별도 테스트 DB에서 분산 적재·증분 snapshot 검증
python3 scripts/week4.py verify-storage
# 기존 Kafka 장애 복구 및 스트리밍 회귀 검증
python3 scripts/week4.py verify
python3 scripts/week4.py verify-stream
```

[저장 개선 검증 결과](../../evidence/week4/storage_feedback_verification.json)는 합성 데이터 검증이며 기존 수집 3,846건 검증과 구분한다. staging 중 실패, JDBC 재시도 중복, reject 보관, commit 후 재시도, 날짜별 파일 재사용, 공개 전 실패, 동시 DB 변경, 빈 데이터 결과를 검사한다.

실제 기존 DB **3,846건**도 새 snapshot 형식으로 export해 원본·DB·Parquet의 전체 행과 집계가 일치함을 확인했다. [실행 결과와 이전 manifest 백업](../../evidence/week4/storage_full_check/README.md). 이는 전체 데이터의 Kafka 재전송 테스트와는 구분한다. 현재 실데이터는 하루치여서 해당 날짜에 변경이 있으면 그 날짜 전체를 다시 쓰며, 여러 날짜 간 재사용은 합성 두 날짜 테스트로 확인했다.

## 4. 운영 범위

- 속도 향상 배수나 최대 처리량은 측정하지 않았다. 이번 검증은 driver 행 수집 제거와 저장 결과·복구의 정확성이다.
- staging 작업 도중 프로세스가 강제 종료되면 UUID별 임시 행이 남을 수 있다. 최종 병합에는 다른 load_id의 행이 섞이지 않는다. 실행 중인 작업이 없음을 확인한 후 오래된 staging을 정리해야 한다.
- 과거/미공개 Parquet 버전도 자동 정리하지 않는다. 현재 manifest가 재사용하는 날짜 파일까지 지우지 않도록 별도의 보존 정책이 필요하다.
- JDBC staging은 SQL `append`다. staging에 `overwrite`를 사용하지 않아 다른 작업의 임시 데이터를 지우지 않는다.
- 날짜 추적 trigger를 우회한 `TRUNCATE`, trigger 비활성화, 테이블 교체는 지원하지 않는다. 이런 관리 작업 후에는 날짜 추적 메타데이터와 export를 함께 재구축해야 한다.
- 로컬 Docker 및 공유된 로컬 파일시스템 기준이다. 클러스터에서는 executor도 접근 가능한 DB와 파일 저장소가 필요하며, S3의 manifest 공개/잠금은 별도 구현이 필요하다.

참고: [Spark JDBC 옵션](https://spark.apache.org/docs/3.5.8/sql-data-sources-jdbc.html), [PostgreSQL statement trigger](https://www.postgresql.org/docs/16/sql-createtrigger.html).
