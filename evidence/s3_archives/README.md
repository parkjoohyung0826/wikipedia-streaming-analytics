# 기존 로컬 산출물 S3 백업

- 백업 위치: `s3://wikipedia-batch-978642868203-1f375cce/archives/legacy-generated/20261104T130356_c8730b.tar.gz`
- 원본 2,295개 파일, 59,999,897바이트
- 압축 아카이브 10,175,043바이트
- 전체 다운로드 SHA256와 아카이브 내부의 모든 파일 SHA256를 대조한 뒤 로컬 삭제 완료
- [파일별 목록과 검증 기록](20261104T130356_c8730b.json)

삭제 대상: data, artifacts/live, artifacts/runs, artifacts/week4, artifacts/week5, artifacts/week6. 새 수집 중인 artifacts/s3와 새 S3 부하 실험은 제외했다. 코드, 문서, samples, evidence, AWS 설정·인증 파일은 삭제하지 않았다. Docker의 DB·Kafka 볼륨은 프로젝트 폴더 밖에 있어 이번 정리 대상이 아니다.

## 복원

먼저 원하는 별도 폴더에서 다운로드하고 체크섬을 확인한다.

```bash
aws s3 cp s3://wikipedia-batch-978642868203-1f375cce/archives/legacy-generated/20261104T130356_c8730b.tar.gz legacy-generated.tar.gz
shasum -a 256 legacy-generated.tar.gz
```

기대 SHA256: `7b399f00d18fa2afa17d5c8e10fec83ce4fee7ac280bf10c3c1a1757fbd3865d`

일치하는지 확인한 뒤 빈 복원 폴더에 해제한다.

```bash
mkdir restored-legacy
tar -xzf legacy-generated.tar.gz -C restored-legacy
```

S3에는 같은 경로에 `.inventory.json`을 붙인 파일별 목록도 보관했다. 자동으로 예전 경로를 덮어쓰지 않는다.
