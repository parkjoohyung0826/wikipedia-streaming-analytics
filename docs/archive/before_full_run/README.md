# Wikipedia Streaming Analytics

Wikimedia 실시간 편집 기록을 수집하고, Kafka → Spark → PostgreSQL·Parquet으로 처리하는 프로젝트다.

**프로젝트 경로:** `/Users/parkjoohyung/Documents/wikipedia-streaming-analytics`

## 문서 안내

| 문서 | 용도 |
|---|---|
| [4회차 제출 README](../../submissions/week4.md) | 과제의 다섯 항목: 흐름·입출력 예시·스키마·저장 포맷·실행 안내 |
| [3회차 README](../week3.md) | Producer와 Kafka 수집 |
| [2회차 설계](../week2.md) | 데이터·전체 구조·기술 선택 |
| [4회차 상세 문서](../../guides/week4_pipeline.md) | 기존 구현·복구·실행 방법의 상세 설명 |
| [수집 측정 결과](../../reports/collection_result.md) | 실제 10분 수집 규모 |

## 데이터와 전체 흐름

[Wikimedia recentchange](https://stream.wikimedia.org/v2/stream/recentchange)를 2026-10-25 한국시간 19:04:23~19:14:23에 수신했다. 전체 25,860건·JSONL 36.80MB 중 Wikipedia 편집 3,846건을 선택했다. 이는 해당 10분에 수신한 데이터 기준이다.

```text
Wikimedia SSE → 원본 JSONL → Producer → Kafka
→ Spark 검증·시각 변환·크기 변화량 계산
→ PostgreSQL 정상·오류 기록 / 시간·위키별 집계
→ Parquet snapshot
```

## 구현과 검증 상태

- 수집·Kafka 전송: 구현돼 있으며, 3회차 전체 3,846건의 전송·수신 검증 기록이 있다.
- Spark·DB·Parquet: `src/process_edits.py`와 `sql/week4.sql`에 구현돼 있다.
- 저장소의 [CI 보고서](../../../evidence/week4/ci_verification.json): 실제 샘플 20건의 DB·Parquet 일치, 합성 입력의 변환·실패 복구·재시작·재전송 검증 결과를 포함한다. Linux Docker CI 결과다.
- 이전 로컬 재확인: **다른 폴더인 3week_homework에서** Kafka 샘플 20건만 검증했다. [기록과 원래 경로](../../../evidence/week3/local_check_20261028/PROVENANCE.md)를 함께 보관한다. 이 프로젝트의 Spark 실행 결과로 간주하지 않는다.
- 이 문서 수정 과정에서는 Docker 작업을 새로 실행하지 않았다. 로컬 Spark 실행 여부와 CI 검증 결과를 구분한다.

## 실행

Docker Desktop을 시작한 뒤 이 프로젝트 폴더에서 실행한다. 호스트 Python은 실행 도구용이며 Spark·Java·패키지는 컨테이너에 포함된다.

```bash
cd /Users/parkjoohyung/Documents/wikipedia-streaming-analytics
python3 scripts/week4.py demo
python3 scripts/week4.py verify
```

`demo`는 실제 샘플 20건 전송·처리·검증, `verify`는 별도 DB·토픽에서 장애 복구를 확인한다. 처음 실행 시 이미지와 의존성 다운로드가 필요하다. `python3 scripts/week4.py stop`은 이 Compose 구성의 서비스를 중지하므로 실행 중인 작업이 있는지 확인한 뒤 사용한다.

## 폴더

```text
├── README.md              # 전체 프로젝트 안내
├── docs/submissions/week4.md        # 4회차 제출 내용
├── src/                   # 수집·Producer·Spark 처리·검증
├── scripts/               # Docker 실행 및 검증 도구
├── sql/                   # DB 테이블·집계 뷰 정의
├── docker/                # 컨테이너 빌드 설정
├── tests/, tests_week4/   # 기존 단위 테스트·Spark 변환 테스트
├── samples/               # 실제 수집 샘플 20건
├── evidence/              # 보관된 검증 근거
├── docs/                  # 기존 회차별 문서
├── docker-compose.yml     # Kafka 구성
└── compose.week4.yml      # Spark·PostgreSQL·Producer 추가 구성
```

이전 3week_homework 폴더에서 작성한 파일 기반 Spark 미구현 계획은 이 프로젝트의 현재 코드와 다르므로 가져오지 않았다. 회차별 상세 문서와 소스코드는 기존 내용을 보존한다.
