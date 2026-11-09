# 7주차 동일 조건 비교 증빙

- [사전 실험 계획](experiment-plan.md)
- [비교 결과·환경·배치 시간](comparison.json)
- [제출 보고서](../../docs/submissions/week7.md)

상세 로그와 원본 측정값은 S3에 보관했습니다. 업로드 후 다시 내려받아 SHA-256 일치를 확인했습니다. 설정·인증 파일은 포함하지 않았습니다. 로컬 artifacts 원본도 유지합니다.

```bash
aws s3 cp s3://wikipedia-batch-978642868203-1f375cce/wikipedia/evidence-archives/week7/week7-matched-20261108T122611Z.tar.gz /tmp/week7-details.tar.gz
shasum -a 256 /tmp/week7-details.tar.gz
mkdir -p /tmp/week7-details
tar -xzf /tmp/week7-details.tar.gz -C /tmp/week7-details
```

SHA-256: `281a9c17d31891be45834f683d431db043dcd445b498cb1c375e9397bd66f7cb`
