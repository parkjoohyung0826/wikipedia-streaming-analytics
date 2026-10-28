# Wikipedia Streaming Analytics

Wikimedia 실시간 편집 기록을 수집하고, Kafka → Spark → PostgreSQL·Parquet으로 처리하는 프로젝트

**프로젝트 경로:** `/Users/parkjoohyung/Documents/wikipedia-streaming-analytics`

## 문서 안내

| 문서 | 용도 |
|---|---|
| [4회차 README](docs/submissions/week4.md) | 과제의 다섯 항목: 흐름·입출력 예시·스키마·저장 포맷·실행 안내 |
| [3회차 README](docs/archive/week3.md) | Producer와 Kafka 수집 |
| [2회차 설계](docs/archive/week2.md) | 데이터·전체 구조·기술 선택 |
| [4회차 상세 문서](docs/guides/week4_pipeline.md) | 기존 구현·복구·실행 방법의 상세 설명 |
| [수집 측정 결과](docs/reports/collection_result.md) | 실제 10분 수집 규모 |

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
- 저장소의 [CI 보고서](evidence/week4/ci_verification.json): 실제 샘플 20건의 DB·Parquet 일치, 합성 입력의 변환·실패 복구·재시작·재전송 검증 결과를 포함한다. Linux Docker CI 결과다.
- **로컬 전체 실행 완료:** Wikipedia 편집 3,846건을 Spark로 처리해 PostgreSQL·Parquet에 저장했고, 전체 재전송 후에도 3,846건과 모든 DB 열이 유지됨을 확인했다. [입출력 예시와 실행 결과](evidence/week4/full_run_20261028T221020Z/README.md)

## 실행

Compose 프로젝트 이름은 `wikipedia-streaming-analytics`다. 이전 `wikipedia-week3`와 다른 컨테이너·볼륨을 사용한다.

Docker Desktop을 시작한 뒤 이 프로젝트 폴더에서 실행한다. 호스트 Python은 실행 도구용이며 Spark·Java·패키지는 컨테이너에 포함된다.

```bash
cd /Users/parkjoohyung/Documents/wikipedia-streaming-analytics
python3 scripts/week4.py demo
python3 scripts/week4.py verify
```

`demo`는 실제 샘플 20건 전송·처리·검증, `verify`는 별도 DB·토픽에서 장애 복구를 확인한다. 처음 실행 시 이미지와 의존성 다운로드가 필요하다. `python3 scripts/week4.py stop`은 이 Compose 구성의 서비스를 중지하므로 실행 중인 작업이 있는지 확인한 뒤 사용한다.

## 폴더

```text
├── README.md                 # 전체 안내: 여기서 시작
├── src/                      # 실제 파이프라인 코드
├── scripts/                  # 실행·검증 명령 도구
├── tests/
│   ├── unit/                 # Kafka·Spark 없이 실행하는 단위 테스트
│   ├── integration/          # Spark 변환 테스트
│   └── fixtures/             # 테스트용 입력 생성기
├── docs/
│   ├── submissions/          # 4회차 제출 README
│   ├── guides/               # 수집·Kafka·Spark 실행 안내
│   ├── reports/              # 수집 규모 보고서
│   └── archive/              # 이전 회차·이전 버전 문서
├── evidence/
│   ├── week3/                # Kafka 검증 기록
│   └── week4/                # Spark 검증과 전체 처리 결과
├── samples/                  # 실행용 작은 샘플
├── data/                     # 원본 수집 데이터 (Git 제외)
├── artifacts/                # checkpoint·Parquet·실행 중간 파일 (Git 제외)
├── docker/                   # 컨테이너 빌드 설정
├── sql/                      # DB 테이블·뷰 정의
├── docker-compose.yml
├── compose.week4.yml
└── requirements.txt
```

- **과제 문서:** [4회차 제출 README](docs/submissions/week4.md)
- **실행 결과:** [결과 폴더 안내](evidence/README.md)
- **실행 방법:** [문서 안내](docs/README.md)
- **테스트:** [테스트 안내](tests/README.md)

`data/`와 `artifacts/`의 경로, DB volume은 유지했다. 기존 checkpoint와 Parquet snapshot을 계속 사용할 수 있다. 보관된 JSON 검증 기록의 경로는 당시 실행 위치를 나타내므로 그대로 보존했다. 최신 파일 배치는 각 결과 폴더의 README에서 확인한다.
