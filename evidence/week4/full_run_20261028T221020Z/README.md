# 로컬 Spark 전처리 실행 결과

실행 프로젝트: `/Users/parkjoohyung/Documents/wikipedia-streaming-analytics`  
Docker 프로젝트: `wikipedia-streaming-analytics`  
최종 검증 시각(한국시간): **2026-10-29T07:13:24.203125+09:00**

## 결과

| 항목 | 결과 |
|---|---:|
| 수집 원본 | 25,860건 |
| Wikipedia 편집 선택·전송·처리 | 3,846건 |
| 기존 DB 행 | 20건 |
| 첫 전체 실행에서 신규 저장 / 기존 ID 건너뜀 | 3,826건 / 20건 |
| 최종 PostgreSQL / Parquet | 각각 3,846건 |
| 오류 기록 | 0건 |
| UTC 시간·위키별 집계 그룹 | 82개 |
| 집계 편집 수 합계 | 3,846건 |
| 전체 입력 재전송 후 신규 저장 | 0건 |
| 재전송 후 최종 행 수 | 3,846건 유지 |

원본의 ID·발생 시각·위키·문서명·봇 여부·편집 크기를 DB와 대조했다. DB와 Parquet의 전체 행 및 집계도 일치했다. 전체 입력을 다시 전송한 후에는 `processed_at`을 포함한 DB의 모든 열이 이전과 동일함을 확인했다. 이전 샘플 20건은 최초 저장된 ingest_time·processed_at을 유지한다.

## 실제 DB 조회 화면

정상 3,846행·오류 0행, 정상 편집 최근 10건과 시간·위키별 집계 상위 10개를 실제 PostgreSQL에서 조회한 화면이다. 사진의 집계값은 이 폴더의 전체 출력과 일치한다.

![실제 PostgreSQL 조회 결과](../../../docs/images/week4-db-query.png)

[이미지 원본 보기](../../../docs/images/week4-db-query.png)

## 같은 이벤트의 입력과 출력

처리 전 원본 일부:

```json
{
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

처리 후 DB 행 일부:

```json
{
  "event_id": "0a3b0c9a-b4a9-4706-86a6-a074d4d35ee6",
  "event_time": "2026-10-25T10:04:24+00:00",
  "wiki": "enwiki",
  "title": "1991–92 Aston Villa F.C. season",
  "bot": false,
  "old_length": 24636,
  "new_length": 27290,
  "bytes_delta": 2654
}
```

숫자 timestamp를 UTC 시각으로 바꾸고 중첩된 길이 값을 열로 분리했다. 문서 크기 차이는 `27290 - 24636 = 2654`이다. [입출력 원문 3쌍](examples/input_output_examples.json)은 동일 event_id끼리 연결했다.

## 저장 파일

| 파일 | 내용 |
|---|---|
| [summary.json](checks/summary.json) | 이번 실행의 핵심 결과 |
| [input_wikipedia_edits.jsonl](inputs/input_wikipedia_edits.jsonl) | 선택된 실제 입력 3,846건 |
| [processed_edits.csv](outputs/processed_edits.csv) | 전처리 후 DB 전체 행, 표 형태로 확인 가능 |
| [processed_edits.jsonl](outputs/processed_edits.jsonl) | 전처리 후 DB 전체 행, JSON 형식 |
| [hourly_wiki.csv](outputs/hourly_wiki.csv) | 시간·위키별 집계 82행 |
| [verification.json](checks/verification.json) | 첫 전체 처리의 원본·DB·Parquet 대조 |
| [replay_verification.json](checks/replay_verification.json) | 전체 입력 재전송 후 DB·Parquet 대조 |
| [replay_result.json](checks/replay_result.json) | 재전송 전후 모든 DB 열의 동일성 확인 |
| [parquet_files.json](metadata/parquet_files.json) | 최신 Parquet 파일 경로·크기·SHA-256 |
| [provenance.json](metadata/provenance.json) | 원본 수집 폴더·복사 위치·파일 해시 |

단계별 실행 로그는 [logs/](logs/)의 `producer.log`, `spark.log`, `verification.log` 및 `replay_*.log`에 있다. 원래 수집 파일은 프로젝트의 `data/20261025T100423_674945Z/events.jsonl`에 보관했다.

Parquet 원본은 프로젝트 기준 `artifacts/week4/parquet/snapshots/20261028T221316_433907Z-70e48d2b`에 있다. 기존 snapshot도 보존했으며 최신 결과는 `artifacts/week4/parquet/CURRENT.json`으로 찾는다. `/workspace`는 컨테이너 내부에서 프로젝트 루트를 가리킨다.

## 재현

프로젝트 최상위 폴더에서 Docker를 실행한 뒤:

```bash
python3 scripts/week4.py setup
python3 scripts/week4.py send --input data/20261025T100423_674945Z/events.jsonl --rate 200
python3 scripts/week4.py run
python3 scripts/week4.py check --expected-input data/20261025T100423_674945Z/events.jsonl --expect-edits 3846 --expect-rejected 0 --report evidence/week4/full_verification.json
```

20건 샘플 실행 명령인 `demo`와 전체 입력 실행을 구분한다. 전체 원본은 로컬에 있으며 Git에 포함되지 않을 수 있다.

사용자가 기존 컨테이너를 꺼두겠다고 지시했으므로 기존 Airflow는 다시 시작하지 않았다. 과제용 Kafka·PostgreSQL 데이터는 유지한다. 이번 정상 데이터 실행에는 오류가 없었고, 잘못된 입력·강제 장애 복구의 별도 검증은 기존 CI 기록을 참고한다.
