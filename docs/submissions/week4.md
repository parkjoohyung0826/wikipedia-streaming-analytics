# 4회차 제출 — Wikipedia 편집 데이터 전처리와 저장

`3회차_강의자료.pdf` 11~12쪽에서 요구한 다섯 항목을 현재 코드에 맞춰 정리했다.

데이터 출처는 [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)다. 2026-10-25 한국시간 19:04:23~19:14:23에 받은 전체 변경 기록은 **25,860건·36.80MB**, 그중 Wikipedia 편집은 **3,846건**이다. 이전 CI에서는 실제 편집 20건 샘플을, 이번 로컬 Spark 실행에서는 **3,846건 전체**를 처리했다.

**로컬 검증 완료:** 원본 편집 3,846건 → DB 3,846행 → Parquet 3,846행, 오류 0건. 같은 입력을 다시 보내도 모든 DB 열과 행 수가 유지됐다. [전체 결과·CSV·동일 이벤트 입출력 예시](../../evidence/week4/full_run_20261028T221020Z/README.md). 기존 Linux CI의 합성 오류·장애 복구 결과는 별도 근거로 보존한다.

## 1. 처리 흐름 — 3·4·5단계 코드 보완

새 실행 코드: [src/stream_edits.py](../../src/stream_edits.py). 기존 원본 DB와 snapshot 결과는 보존했다.

```text
Kafka → JSON 해석·검증
  ├─ 전체 정상·오류 기록 → 기존 PostgreSQL edits / rejected (감사·재처리용)
  └─ 정상 기록 → watermark + event_id 중복 제거
       → 날짜별 Parquet에 계속 추가
       → 별도 Spark 쿼리가 Parquet을 스트리밍으로 읽음
       → event_time 기준 1분·위키별 집계
       → PostgreSQL windowed_wiki에 변경된 통계 갱신
```

| 단계 | 현재 구현 |
|---|---|
| ① 읽기 | `readStream`으로 Kafka를 읽고 토픽·파티션·offset 보존 |
| ② 풀기 | 기존 `transform`의 명시적 JSON 스키마, UTC 변환, 필수값 검증 재사용 |
| ③ 다듬기 | `withWatermark('event_time', '10 minutes')`와 `dropDuplicatesWithinWatermark(['event_id'])` |
| ④ 모으기 | `window(event_time, '1 minute')`와 wiki별 편집 수·봇 수·길이 제공 수·크기 변화량 집계 |
| ⑤ 쌓기 | Parquet `append`, 집계 `update` + PostgreSQL `ON CONFLICT DO UPDATE`, 쿼리별 checkpoint |

중복 제거와 집계는 Spark 3.5의 상태 연산 제약 때문에 별도 쿼리로 연결한다. Parquet은 두 쿼리 사이의 보관소이기도 하다. 전체 원본 보존·중복 제거·집계의 checkpoint는 각각 `audit`, `clean`, `aggregate`로 분리한다.

10분은 우선 적용한 지연 허용값이다. 현재 시계가 아니라 **관측한 최대 편집 발생 시각**을 기준으로 계산한다. 허용 범위를 넘긴 이벤트는 실시간 결과에서 제외될 수 있지만 원본 DB에는 보존한다. 따라서 원본 DB와 실시간 Parquet의 건수는 늦은 데이터가 있으면 다를 수 있다. 같은 event_id는 동일한 편집 시각을 유지한다는 계약을 전제로 한다.

`wiki:문서명`은 Kafka key이고 `event_id`는 편집 식별자다. 제목이 같은 서로 다른 편집을 합치지 않는다.

## 2. 입력·출력 예시

아래 실제 예시는 기존 전체 실행에서 저장한 원본 DB 행이다. 새 코드도 같은 원본 DB 스키마를 유지한다. 새 실시간 Parquet·집계 출력과의 차이는 3절에 따로 설명한다.

**원본 → Producer가 만든 Kafka 메시지 → Spark 처리 후 DB 행**의 세 단계로 보면 된다. Spark가 직접 읽는 것은 원본 파일이 아니라 Kafka 메시지다.

### ① 수집 원본: 편집 내용

실제 수집한 편집 한 건에서 설명에 필요한 필드만 발췌했다. `meta.id`는 편집 식별자이고, `timestamp`는 초 단위 Unix 시각이다.

```json
{
  "meta": {
    "id": "0a3b0c9a-b4a9-4706-86a6-a074d4d35ee6"
  },
  "type": "edit",
  "server_name": "en.wikipedia.org",
  "wiki": "enwiki",
  "title": "1991–92 Aston Villa F.C. season",
  "timestamp": 1792922664,
  "bot": false,
  "length": {
    "old": 24636,
    "new": 27290
  }
}
```

### ② Kafka 메시지: Spark의 입력

Producer는 원본 전체를 `payload`에 넣고, 바깥에 식별자·발생 시각·전송 시각 등을 붙인다. 아래는 **Producer 코드와 저장된 동일 이벤트를 바탕으로 재구성한 설명용 메시지**이며, 당시 Kafka 메시지를 직접 캡처한 것은 아니다. `payload`는 위와 같은 필드만 발췌했고 실제 전송에서는 원본 전체가 들어간다.

```json
{
  "event_id": "0a3b0c9a-b4a9-4706-86a6-a074d4d35ee6",
  "event_type": "wikipedia.edit",
  "event_time": "2026-10-25T10:04:24+00:00",
  "ingest_time": "2026-10-28T12:12:52.624975+00:00",
  "schema_version": 1,
  "source": "wikimedia-recentchange-replay",
  "payload": {
    "meta": {
      "id": "0a3b0c9a-b4a9-4706-86a6-a074d4d35ee6"
    },
    "type": "edit",
    "server_name": "en.wikipedia.org",
    "wiki": "enwiki",
    "title": "1991–92 Aston Villa F.C. season",
    "timestamp": 1792922664,
    "bot": false,
    "length": {
      "old": 24636,
      "new": 27290
    }
  }
}
```

Kafka key는 `enwiki:1991–92 Aston Villa F.C. season`이다. key는 메시지 본문과 별도로 전달하며, 같은 문서의 편집을 같은 파티션으로 보내는 기준이다. 개별 편집을 구분하는 `event_id`와 역할이 다르다.

### ③ DB 저장 결과: 전체 16개 필드

아래는 [저장된 입출력 증거](../../evidence/week4/full_run_20261028T221020Z/examples/input_output_examples.json)의 실제 DB 행이다. **필드를 생략하지 않았으며, 3절의 정상 테이블 스키마와 정확히 대응한다.** JSON은 조회 결과를 보여주는 표현이며 DB에는 각 열의 자료형으로 저장된다.

```json
{
  "event_id": "0a3b0c9a-b4a9-4706-86a6-a074d4d35ee6",
  "event_type": "wikipedia.edit",
  "schema_version": 1,
  "event_time": "2026-10-25T10:04:24+00:00",
  "ingest_time": "2026-10-28T12:12:52.624975+00:00",
  "source": "wikimedia-recentchange-replay",
  "wiki": "enwiki",
  "title": "1991–92 Aston Villa F.C. season",
  "bot": false,
  "old_length": 24636,
  "new_length": 27290,
  "bytes_delta": 2654,
  "source_topic": "wikipedia-edits",
  "source_partition": 0,
  "source_offset": 2,
  "processed_at": "2026-10-28T12:13:01.589736+00:00"
}
```

이 이벤트는 앞선 샘플 실행에서 먼저 저장됐다. 전체 재전송 때 같은 ID를 다시 넣지 않았기 때문에 최초 `ingest_time`, Kafka 위치, `processed_at`이 유지돼 있다. 과거 편집을 재생했으므로 편집 발생일과 전송·저장일이 다르다.

### 필드는 어디서 만들어지는가?

| 원본 또는 출처 | 최종 필드 | 처리 담당과 방식 |
|---|---|---|
| `meta.id` | `event_id` | Producer가 사용. 없으면 `wiki:edit:원본 id`로 구성 |
| `timestamp` | `event_time` | Producer가 UTC 시각 문자열로 변환 → Spark가 timestamp로 해석 |
| 원본 `wiki`, `title`, `bot` | `wiki`, `title`, `bot` | Spark가 `payload`에서 꺼냄. 값은 유지 |
| 원본 `length.old`, `length.new` | `old_length`, `new_length` | Spark가 중첩된 값을 각각 독립된 열로 꺼냄 |
| 두 길이의 차이 | `bytes_delta` | Spark가 `27290 - 24636 = 2654` 계산 |
| Producer의 고정값 | `event_type`, `schema_version`, `source` | 메시지 종류·형식 버전·출처를 붙이고 Spark가 검증 |
| Producer의 전송 직전 시각 | `ingest_time` | Producer가 생성 → Spark가 timestamp로 해석 |
| 우리가 운영하는 Kafka의 메시지 위치 | `source_topic`, `source_partition`, `source_offset` | Spark가 Kafka에서 읽은 메타데이터를 보존 |
| DB 저장 transaction의 시작 시각 | `processed_at` | PostgreSQL의 `DEFAULT now()`로 자동 생성 |

`source_topic/partition/offset`은 원본 `meta` 안의 Wikimedia 측 위치가 아니라 **이 프로젝트 Kafka의 위치**다. 원본의 `user`, `comment`, `revision` 등은 현재 정상 테이블의 분석 대상 열로 선택하지 않아 저장하지 않는다. 수집 원본에는 남아 있다.

- **잘못된 입력:** JSON·자료형·필수값·시간대 등을 검사하고, 문제가 있으면 정상 테이블 대신 오류 테이블에 원문과 이유를 남긴다.
- **길이 누락:** `old_length` 또는 `new_length`가 없으면 해당 값과 계산할 수 없는 `bytes_delta`는 `null`이다. 모르는 길이를 0으로 바꾸지 않는다.
- **삭제 편집:** 길이 자체는 음수일 수 없지만, 문서가 짧아진 경우 `bytes_delta`는 음수여도 정상이다.
- **중복:** 같은 `event_id`가 다시 오면 최초 저장 값을 유지한다. 제목이 같다는 이유로 다른 편집을 제거하지 않는다.

## 3. 최종 스키마

정의 파일: [sql/week4.sql](../../sql/week4.sql). **스키마는 저장할 열의 이름·자료형을 정한 설계도이고, 위 출력 예시는 그 설계도에 들어간 실제 값 한 행이다.** 정상 편집 한 건을 한 행으로 저장한다. 이전 문서는 출력 필드를 일부만 보여줘 스키마와 달라 보였지만, 현재 예시는 전체 16개 필드를 보여준다.

| 정상 테이블 `week4.edits` 필드 | PostgreSQL 자료형 | 의미 |
|---|---|---|
| event_id | TEXT, 기본 키 | 이벤트 식별자·중복 판단 기준 |
| event_type | TEXT | wikipedia.edit |
| schema_version | INTEGER | 허용 버전 1 |
| event_time | TIMESTAMPTZ | 실제 편집 발생 시각 |
| ingest_time | TIMESTAMPTZ | Producer 전송 직전 시각 |
| source | TEXT | 데이터 출처 |
| wiki / title | TEXT | 위키 이름 / 문서명 |
| bot | BOOLEAN | 봇 편집 여부 |
| old_length / new_length | BIGINT, null 허용 | 편집 전후 문서 크기 |
| bytes_delta | BIGINT, null 허용 | 편집 후 크기 − 편집 전 크기 |
| source_topic | TEXT | 최초 저장 메시지의 Kafka 토픽 |
| source_partition / source_offset | INTEGER / BIGINT | 최초 저장 메시지의 파티션 / 위치 |
| processed_at | TIMESTAMPTZ | 최초 DB 저장 시각 |

아래 오류 테이블과 집계 뷰는 정상 편집 테이블과 용도가 다르므로 필드도 다르다.

| 출력 대상 | 한 행의 의미 | 정상 편집 출력과의 관계 |
|---|---|---|
| `week4.edits` | 정상 편집 한 건 | 위의 16개 필드 예시 |
| `week4.rejected` | 처리할 수 없는 Kafka 기록 한 건 | 정상 행 대신 원문·오류 이유 저장 |
| `week4.hourly_wiki` | 같은 UTC 시간대·위키의 통계 | 정상 편집 여러 건을 모아 집계 |
| 기존 snapshot Parquet `edits` | 정상 편집 한 건 | DB의 16개 필드 + 날짜 분할용 `event_date` |

예시의 Parquet `event_date`는 전송일이 아닌 편집 발생일 `2026-10-25`다.

길이와 변화량 외의 정상 테이블 필드는 null을 허용하지 않는다. 기존 snapshot Parquet에는 이 필드들과 UTC 날짜를 나타내는 `event_date`가 추가된다.

오류 테이블 **`week4.rejected`**는 `source_id`(TEXT 기본 키), `source_topic`(TEXT), `source_partition`(INTEGER), `source_offset`(BIGINT), `raw_value`(TEXT), `error_reason`(TEXT), `rejected_at`(TIMESTAMPTZ)를 저장한다. source_id는 `[토픽, 파티션, offset]` 조합이다.

집계 뷰 **`week4.hourly_wiki`**는 `hour_utc`(TIMESTAMP, UTC 의미)와 `wiki`(TEXT)를 기준으로 `edit_count`, `bot_edit_count`, `size_known_count`(각 BIGINT), `net_bytes_delta`(NUMERIC)를 제공한다. 마지막 값은 Parquet에서는 Spark long 합계인 BIGINT로 저장된다. 변화량이 모두 null인 그룹의 합계도 null이다.

### 새 실시간 출력의 스키마

- **실시간 Parquet `clean/`:** 정상 DB의 16개 필드에서 DB가 생성하는 `processed_at`을 제외한 15개 필드와 `event_date`를 저장한다. DB를 거치지 않고 Spark에서 직접 쓰기 때문이다. 총 16개 필드이며 기존 snapshot과 혼용하지 않는다.
- **`week4.windowed_wiki`:** `pipeline_id`(TEXT), `window_start`·`window_end`(TIMESTAMPTZ), `wiki`(TEXT), `edit_count`·`bot_edit_count`·`size_known_count`·`net_bytes_delta`(BIGINT), `updated_at`(TIMESTAMPTZ). 변화량 합계는 모두 미상일 때 null이다.
- **집계 PK:** `(pipeline_id, window_start, wiki)`. 같은 파이프라인의 같은 시간대·위키 통계를 새 값으로 교체한다. 기존 값에 더하지 않는다.
- **`week4.stream_batches`:** 파이프라인·batch ID를 기록해 DB commit 후 재실행된 배치를 다시 갱신하지 않는다. 집계 갱신과 같은 transaction에서 기록한다.

정의 파일: [sql/stream_v2.sql](../../sql/stream_v2.sql).

## 4. 저장 포맷과 선택 이유

| 저장 대상 | 방식과 이유 |
|---|---|
| 기존 `edits` / `rejected` | 전체 정상·오류 기록 보존. 늦은 데이터 재처리 및 원본 대조용 |
| 새 `clean/` Parquet | Spark에서 중복 제거한 이벤트를 날짜별로 append. 집계 재시작 때 다시 읽을 수 있음 |
| 새 `windowed_wiki` | 1분·위키별 실시간 집계를 PK 기준으로 갱신. SQL로 바로 조회 |
| 기존 snapshot | DB 전체를 내보낸 과거 결과 및 별도 배치 export. 새 스트리밍 출력과 분리 |

Parquet의 `_spark_metadata`와 checkpoint가 파일 커밋을 관리한다. 이 폴더들을 함께 보존해야 하며, 파일을 임의로 옮기거나 직접 덧붙이지 않는다. 실시간 집계는 Spark 상태와 DB의 배치 기록을 함께 사용한다. DB, 출력, checkpoint 중 일부만 삭제하고 재사용하면 안 된다.

## 5. 실행 방법·검증·한계

프로젝트 폴더에서 실행한다. Docker Desktop과 Python 3.9 이상이 필요하다.

```bash
python3 scripts/week4.py setup
# 작은 실제 입력을 Kafka로 전송
python3 scripts/week4.py send --input samples/wikipedia_edits.jsonl --rate 10
# 현재 입력 처리: 원본 보존 → 중복 제거 Parquet → 스트리밍 집계
python3 scripts/week4.py run
# 계속 수신: 세 쿼리가 함께 실행되고 Parquet·집계가 계속 갱신됨
python3 scripts/week4.py run --continuous
# 별도 테스트 DB·토픽에서 지연/중복/복구와 실제 샘플 재전송 검증
python3 scripts/week4.py verify-stream
# 기존 snapshot 기능은 별도 명령으로 유지
python3 scripts/week4.py snapshot --export-only
```

기본 `run`은 `availableNow`로 끝나며, 연속 실행은 `--continuous`를 붙인다. 기존 `demo`, `verify`, `check`는 과거 snapshot 경로의 검증 명령이다.

| 결과 | 기본 위치 |
|---|---|
| 실시간 정상 Parquet | `artifacts/week4/stream_v2/main/clean/event_date=.../` |
| 세 쿼리의 checkpoint | `artifacts/week4/stream_v2/main/checkpoints/{audit,clean,aggregate}/` |
| 유한 실행의 Spark 진행 기록 | `artifacts/week4/stream_v2/main/progress.json` |
| 실시간 집계 | PostgreSQL `week4.windowed_wiki`, `pipeline_id='main-v2'` |
| 새 검증 보고서 | `evidence/week4/stream_v2_verification.json`, `stream_v2_sample_initial.json`, `stream_v2_sample_replay.json` |

```sql
SELECT window_start, wiki, edit_count, bot_edit_count, net_bytes_delta
FROM week4.windowed_wiki
WHERE pipeline_id = 'main-v2'
ORDER BY window_start, wiki;
```

**재시작:** 같은 설정·root·pipeline ID를 사용한다. window나 watermark를 바꾸려면 새로운 `--root`와 `--pipeline-id`를 함께 지정한다. 동일 파이프라인은 한 프로세스만 실행한다. 과거 Kafka 데이터가 보관 기간으로 삭제된 경우 수집 JSONL을 다시 전송하고 새 파이프라인으로 재처리한다.

**검증 범위:** 새 보고서는 합성 지연·중복·장애 사례와 실제 수집 샘플 20건을 구분한다. 앞부분의 3,846건 결과는 기존 snapshot 경로의 검증이며 새 실시간 경로의 전체 검증으로 해석하지 않는다. 10분 watermark는 실측 지연 분포에 맞춰 추후 조정할 초기값이다. 대규모 성능·다중 writer·자동 파일 병합은 포함하지 않았다. 데이터가 더 들어오지 않으면 watermark도 진전하지 않으며, update 모드는 마지막 미확정 창의 현재 통계도 표시한다.

[새 코드 상세 안내](../guides/stream_v2.md) · [기존 snapshot 안내](../guides/week4_pipeline.md)
