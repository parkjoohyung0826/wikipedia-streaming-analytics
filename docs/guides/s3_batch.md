> 별도 Python-only 실험 도구입니다. 과제 기본 구조는 [Kafka·Spark·Airflow + S3](kafka_spark_s3.md)입니다.

# 컨테이너 없는 S3 배치 저장

## 현재 방식

```text
필요할 때 실행
  → Wikimedia API를 지정한 시간만 수집
  → Python으로 Wikipedia 편집 검증·중복 제거·위키별 집계
  → gzip 압축 후 비공개 S3에 업로드
  → 원격 SHA256·파일 크기 확인
  → 완료 manifest 게시 → 종료
```

**상시 컨테이너 0개.** Kafka·Spark·PostgreSQL·Airflow가 필요 없다. S3는 저장소이며 수집이나 전처리를 자동 실행하지 않는다. 이번 기본 경로에는 정기 스케줄을 등록하지 않았다. 노트북이 꺼져 있는 동안에도 이미 저장한 S3 객체는 유지된다.

기존 구현·과제 문서는 [로컬 백업](../legacy_restore.md)으로 옮겼다. 기존 실행 데이터는 S3 아카이브에 있다. S3 경로는 경량 Python 처리이므로 기존 Spark 성능 측정 결과를 그대로 적용할 수 없다.

## 실행

프로젝트 폴더에서 Python 3.9 이상, curl, AWS CLI v2를 사용한다. 별도 Python 패키지 설치는 필요 없다.

```bash
# 한 번만: 현재 CLI 인증 계정에 프로젝트 버킷 생성
python3 scripts/s3.py setup

# 필요할 때: 기본 10분 수집 후 업로드하고 종료
python3 scripts/s3.py run --seconds 600

# 짧은 확인
python3 scripts/s3.py run --seconds 60

# 기존 원본 JSONL을 전처리해 업로드 (원본 파일은 유지)
python3 scripts/s3.py upload --input samples/wikipedia_edits.jsonl

# 저장된 객체 목록
python3 scripts/s3.py status
```

수집 시간은 1~3,600초로 제한한다. 시간 제한으로 실행 자원을 묶어 두지 않도록 했다. 실패하면 `artifacts/s3/<실행 ID>/`의 파일을 유지한다. 성공하면 이번 실행에서 만든 임시 파일만 정리하며, 입력으로 지정한 기존 파일은 지우지 않는다. 임시 파일도 남기려면 `--keep-local`을 사용한다.

## 버킷과 인증

- 리전: 서울 `ap-northeast-2`
- 버킷: `wikipedia-batch-978642868203-1f375cce`
- [AWS 콘솔에서 보기](https://s3.console.aws.amazon.com/s3/buckets/wikipedia-batch-978642868203-1f375cce?region=ap-northeast-2&tab=objects)
- 설정: `config/s3.local.json` (Git 제외), 예시: `config/s3.example.json`
- 인증: 기존 AWS CLI credential chain. 코드나 S3 설정에 access key·secret key를 복사하지 않는다.
- 새 설정 시 프로필 지정: `python3 scripts/s3.py setup --profile PROFILE_NAME --region ap-northeast-2`

현재 인증은 root 주체로 확인됐다. 이후 운영용 IAM 프로필로 바꿀 때는 `config/s3.local.json`의 profile 값을 해당 프로필명으로 바꾸면 된다. 이번 작업에서 별도 IAM 사용자나 키를 만들지는 않았다.

퍼블릭 액세스 전부 차단, ACL 비활성화(BucketOwnerEnforced), SSE-S3 암호화, 버전 관리, HTTP 접근 거부를 적용했다. 같은 입력 재업로드는 새 객체 버전도 만들지 않는다. S3 저장·요청 비용은 발생할 수 있으나 EC2·RDS·상시 컨테이너 등의 추가 실행 자원은 만들지 않았다. 객체 자동 삭제 기간은 설정하지 않았다.

## 저장 구조

```text
s3://버킷/wikipedia/batches/wikipedia-batch-v1-<원본 SHA256>/
├── raw.jsonl.gz          # 전달받은 JSONL 원본 전체
├── edits.jsonl.gz        # 정상 Wikipedia 편집, 배치 내부 중복 제거
├── rejected.jsonl.gz     # 잘못된 메시지와 사유
├── daily_wiki.json       # 이 배치 안의 한국 날짜·위키별 집계
├── summary.json          # 입력·선택·제외·중복·오류 건수
└── manifest.json         # 모든 객체 검증을 통과한 완료 표시
```

**manifest.json이 있는 배치만 완료된 결과로 읽는다.** 일부 파일만 업로드된 경우에는 manifest를 만들지 않는다. 실패 후 같은 입력을 다시 올리면 이미 일치하는 파일을 재사용하고 나머지만 올린다. 같은 경로에 다른 내용이 발견되면 덮어쓰지 않고 실패한다.

정상 편집 필드는 event_id, event_time(UTC), event_date(한국 날짜), wiki, title, bot, old_length, new_length, bytes_delta다. Kafka를 거치지 않으므로 topic·partition·offset과 DB processed_at은 없다. 원래 payload는 raw 파일에 보존한다.

집계는 날짜·위키별 edit_count, bot_edit_count, size_known_count, net_bytes_delta다. 크기 정보가 전부 없으면 net_bytes_delta는 null이다.

## 집계 범위와 중복 주의

10분 수집 결과는 그 10분의 데이터다. `daily_wiki.json`은 배치에 들어 있는 이벤트를 날짜별로 묶은 것이며 하루 전체 수집을 뜻하지 않는다.

동일 파일은 원본 SHA256으로 같은 배치가 되어 중복 업로드되지 않는다. **내용이 다른 배치 사이의 이벤트 중복은 제거하지 않는다.** 수집 범위가 겹치는 여러 배치를 합칠 때는 edits의 event_id로 중복 제거한 뒤 다시 집계해야 한다. 여러 daily_wiki 결과를 무조건 더하면 안 된다.

이전 상시 수집기처럼 연결 종료 지점부터 계속 이어받는 서비스는 아니다. `run`을 다시 실행하면 새로운 관측 구간이 시작된다. 한 번의 수집 구간 안에서 발생하는 연결 중단은 기존 수집기의 재연결 기능을 사용한다.

## 검증

- 기존 10분 원본: 25,860건 → 정상 편집 3,846건 → 집계 82개. 이전 과제와 일치.
- 새 10분 수집: 원본 42,188건 → 정상 편집 3,541건 → 날짜·위키별 집계 88개.
- 새 60초 수집: 원본 4,357건 → 정상 편집 396건 → 집계 49개.
- 11월 2일 상시 수집 원본: 12,948건 → 중복 1건 제외 → 정상 편집 12,947건.
- S3가 반환한 SHA256와 크기를 모든 파일에 대해 확인.
- 같은 10분 원본 재업로드 후 객체 버전 목록이 변하지 않는지 확인.
- 단위 테스트: 입력 분류·한국 날짜·원본 보존·부분 업로드·중복 업로드·체크섬 불일치·빈 결과.

[검증 기록](../../evidence/s3/README.md) · [실행 도구](../../scripts/s3.py) · [처리·업로드 코드](../../src/s3_batch.py)

## 이전 구현

[백업 복원 안내](../legacy_restore.md)를 참고한다.

## 참고

- [S3 조건부 업로드와 체크섬](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object.html)
- [S3 비공개 기본 설정](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)

## 기존 로컬 원본·실행 파일 정리

기존 data와 artifacts의 과거 실행 결과는 S3 아카이브에 백업하고 다운로드 검증 후 정리했다. 이전 코드·과제 문서와 상세 증빙은 로컬 ZIP에 보관했다. [백업 위치와 복원 방법](../../evidence/s3_archives/README.md).
