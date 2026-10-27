# 4회차 과제 코드 검토

## 검토 기준

사용자가 제공한 `3회차_강의자료.pdf` 11~12쪽의 4회차 과제를 기준으로 삼았다.
현재 PC에서 추가 실행을 요청하지 않고, 일반 Docker 환경에서 재현할 코드와 문서를 준비한다.

| 강의 요구사항 | 구현·문서 |
|---|---|
| 수집 데이터 읽기 → 전처리 → 저장 | `src/process_edits.py`: Kafka → Spark → PostgreSQL → Parquet |
| 자료형·시각·결측·중복 처리와 이유 | 명시적 StructType, timezone 필수/UTC 변환, 오류 분리, event_id PK |
| 처리 전·후 데이터 예시 | 원본 샘플·메시지, demo 보고서 input_examples와 output_examples |
| 최종 필드명·자료형·의미 | `sql/week4.sql`, `docs/WEEK4.md`의 정상/오류/집계 스키마 |
| 고유 키·중복 기준 | event_id, 최초 커밋 유지; 오류는 topic/partition/offset |
| 실제 테이블·파일 저장 | PostgreSQL 이력·오류 테이블/집계 view, 날짜별 Parquet snapshot |
| 저장 포맷과 이유 | SQL 유일성·transaction, 컬럼 분석용 Parquet, DB 기준 재생성 |
| 동일 입력 재처리 | checkpoint 재시작 및 새 Kafka offset으로 재전송하는 통합 테스트 |
| 실행 방법·입출력 위치·남은 문제 | `docs/WEEK4.md`, `python scripts/week4.py demo/verify/run/check` |

메인 화면에 README를 표시하지 않는 사용자 요청에 따라 루트 README는 생성하지 않는다.
README 업데이트는 기존 `docs/README_DETAILED.md`의 4회차 안내와 상세 보고서로 제공한다.

## 검토 중 수정한 사항

1. 호스트의 confluent-kafka 설치에 의존하던 전송을 Producer 컨테이너로 이동했다.
2. Windows/Linux/macOS가 같은 표준 라이브러리 Python 실행 도구를 사용하도록 구성했다.
3. 임의 UID의 Spark 시작과 절대 Ivy 경로를 설정하고, Maven 의존성은 이미지에 포함했다.
4. DB 연결 실패 시에도 Spark cache를 해제하도록 정리했다.
5. 건수 대조에 더해 DB↔Parquet의 전체 레코드·전체 집계 양방향 비교를 추가했다.
6. 원본 JSONL과 DB의 ID·발생 시각·위키·문서명·봇 여부·길이·변화량 대조를 추가했다.
7. 테스트는 격리 DB/토픽을 사용하며 의도한 장애 지점에 도달했는지도 확인한다.
8. 기존 3회차 테스트도 CI에서 다시 실행하도록 했다.

## 검증의 한계

한 topic/checkpoint/DB에 writer 하나를 사용하는 학습용 구성이다. 분산 고가용성이나 대규모 적재를
보장하지 않는다. 같은 ID의 서로 다른 수정 내용은 최초 커밋 값을 유지한다. DB와 Parquet을
하나의 분산 transaction으로 묶지 않으며, DB 기준 snapshot 재생성으로 복구한다.
이미지 첫 빌드에는 인터넷이 필요하고 실행 시 외부 Maven 다운로드는 하지 않는다.
Windows 네이티브 Docker Desktop 환경은 실행 도구의 호환성을 고려했지만 실제 통합 테스트 환경은
GitHub Actions의 Linux Docker다. 실제 검증한 범위와 아직 확인하지 않은 환경을 구분한다.

## 최종 검증 결과

[GitHub Actions 통합 검증](https://github.com/parkjoohyung0826/wikipedia-streaming-analytics/actions/runs/40466607879)이
성공했다. 테스트한 코드 commit은 `1631540eeda0cedf1b4f1a4b3b5faed30aaaf521`이다.
문서·증거만 바꾼 후속 commit은 동일한 검증 대상 코드를 유지한다.

| 검사 | 실제 결과 |
|---|---|
| Spark 형 변환·오류 분리 | 11개 레코드 검증 통과 |
| DB commit 직후 의도적 실패와 복구 | 정상 3행·오류 3행, 전체 DB/Parquet 일치 |
| checkpoint 유지 후 재시작 | 정상 3행·오류 3행 유지 |
| 같은 입력을 새 Kafka offset으로 재전송 | 정상 3행 유지, 오류 수신 기록 6행 |
| 실제 Wikimedia 수집 샘플 | 원본 20건 대조, DB/Parquet 각 20행, 오류 0건 |
| 전체 레코드·집계 | 양방향 exceptAll 대조 통과 |
| 기존 3회차 수집·Producer 테스트 | 7개 통과 |

정량 결과와 실제 전후 예시는 [ci_verification.json](../evidence/week4/ci_verification.json)에 보관한다.
강의의 코드·문서 요구사항은 위 구현과 증거로 대응했다. 전체 3,846건을 이번 코드로
처리했다고 주장하지 않으며, 수업 피드백 반영과 최종 제출은 별도 사용자 작업이다.
검토 범위에서 확인한 실행 차단 문제를 수정했지만 모든 컴퓨터에서 무결함을 보장하는 표현은 사용하지 않는다.
