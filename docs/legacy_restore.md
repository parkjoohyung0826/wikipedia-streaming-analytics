> Kafka·Spark·Airflow 필수 사용 요청에 따라 코드·설정·테스트·이전 문서를 이번 수정에서 복원했습니다. 아래 ZIP은 복원 전 백업 기록이며 상세 증빙은 여전히 백업에 있습니다.

# 이전 구현 복원

현재 기본 실행은 S3 방식입니다. 이전 Kafka·Spark·PostgreSQL·Airflow 코드, Docker 설정, 관련 테스트, 이전 과제 문서와 상세 증빙은 아래 로컬 ZIP에 보관했습니다. 파일을 ZIP에서 다시 읽어 원본과 일치하는지 확인한 뒤 정리했습니다.

- 로컬 백업: `backups/before-s3-cleanup-20261105T074808.zip`
- 파일 수: 168
- SHA256: `3dd377e0a11f20a856e883e3cb99a7d272ddfd2b16483ce083645f9fc948097d`
- [전체 파일 목록](../evidence/cleanup.json)

ZIP은 Git에서 제외되므로 GitHub에 복제한 저장소에는 포함되지 않습니다. 현재 PC의 백업을 별도로 보관해야 복원할 수 있습니다. 이번 ZIP은 S3에 업로드하지 않았습니다. AWS 설정과 `.env.week5`는 ZIP에 포함하지 않았으며 로컬에 유지했습니다.

프로젝트 폴더에서 빈 별도 폴더로 해제합니다.

```bash
shasum -a 256 backups/before-s3-cleanup-20261105T074808.zip
mkdir -p restored-legacy
unzip backups/before-s3-cleanup-20261105T074808.zip -d restored-legacy
```

이전 문서는 작성 당시 경로를 기준으로 합니다. 복원한 디렉토리에서 확인하세요. 이전 실행 원본·체크포인트는 별도의 [기존 S3 백업](../evidence/s3_archives/README.md)에 있습니다. Docker 볼륨과 컨테이너에는 변경을 가하지 않았습니다.
