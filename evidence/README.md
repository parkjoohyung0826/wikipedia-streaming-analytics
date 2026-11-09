# 테스트 증빙 보관 안내

2026-11-08에 6·7주차 상세 증빙 62개를 S3로 옮겼다. 업로드한 압축 파일을 다시 내려받아 SHA-256과 내부 파일 62개의 해시가 원본과 같은지 확인한 뒤 로컬 사본을 정리했다.

## 로컬에 남긴 자료

- `week6/20261107T122932Z/`: 6주차 부하 측정 요약, 정합성 검증, Spark 복구 결과, 실행 조건과 단위 테스트 기록
- `week6/20261108T001726Z/`: 7주차 개선 후 측정 요약, 정합성 검증과 실행 조건
- `week6/20261108T002737Z/`: 장애 시나리오 결과, 저장 정합성, 일별 재처리 및 Airflow 재시도 검증과 실행 조건
- `spark_s3/`: 기존 제출 문서가 참조하는 증빙 전체

장애 실행의 `run.json`은 최초 Airflow 초기화 실패 상태를 보존한다. 이후 재실행 결과는 `extended/completion.json`과 `airflow-retry.json`에서 확인한다.

## S3에 보관한 자료

상세 처리 지표(JSONL), 생산자·lag 기록, 중간 확인 파일, 상세 로그와 실패 실행 기록을 원래 경로 그대로 압축했다. 파일별 경로·크기·SHA-256은 [파일 목록](archive-files.tsv)에 있다. 기존 `artifacts/`는 변경하지 않았다.

- S3 위치: `s3://wikipedia-batch-978642868203-1f375cce/wikipedia/evidence-archives/20261108T005133Z/week6-details-20261108T005133Z.tar.gz`
- 압축 파일 SHA-256: `e74542030e866b3ac047c1e69665c876f9eef093d49456ab861986ca2172ac6e`
- 접근: 기존 AWS 인증을 사용하는 비공개 S3 객체

## 필요할 때 복원

프로젝트 루트에서 실행한다. 별도 폴더에 풀어 현재 증빙과 구분한다.

```bash
aws s3 cp s3://wikipedia-batch-978642868203-1f375cce/wikipedia/evidence-archives/20261108T005133Z/week6-details-20261108T005133Z.tar.gz /tmp/week6-details.tar.gz
shasum -a 256 /tmp/week6-details.tar.gz
# 출력값이 위 SHA-256과 일치하는지 확인
mkdir -p /tmp/wikipedia-evidence-restored
tar -xzf /tmp/week6-details.tar.gz -C /tmp/wikipedia-evidence-restored
```

복원된 파일은 `/tmp/wikipedia-evidence-restored/evidence/week6/`에 있다.
