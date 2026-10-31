# 실제 3,846건의 새 snapshot 저장 검증

기존 PostgreSQL 데이터를 변경하지 않고 날짜별 manifest 형식으로 export했다. 원본 수집 파일의 필드 대조, DB·Parquet 전체 열 비교와 집계 비교가 통과했다. 정상 3,846건, 오류 0건이다. 새 스트리밍 경로에서 전체 Kafka 입력을 재생한 결과가 아니라 **기존 DB의 새 snapshot export 검증**이다.

- [검증 결과](verification.json)
- [기존 manifest 백업](previous_manifest.json)
- [새 날짜별 manifest](current_manifest.json)
- [export 로그](export.log)
- [검증 로그](check.log)

기존 snapshot 파일은 유지했다. 최신 경로는 프로젝트의 `artifacts/week4/parquet/CURRENT.json`에 있다. JSON의 `/workspace`는 Docker 안의 프로젝트 경로다. 최초 변환은 모든 날짜를 한 번 쓰며, 이후 변경된 날짜만 다시 쓴다.
