# 실행 결과 안내

**전체 전처리 결과 3,846건**은 [Spark 전체 실행 보고서](week4/full_run_20261028T221020Z/README.md)에서 확인한다.

| 보고 싶은 내용 | 파일 |
|---|---|
| 처리 후 전체 데이터 | [processed_edits.csv](week4/full_run_20261028T221020Z/outputs/processed_edits.csv) |
| 시간·위키별 집계 | [hourly_wiki.csv](week4/full_run_20261028T221020Z/outputs/hourly_wiki.csv) |
| 같은 이벤트의 처리 전·후 | [input_output_examples.json](week4/full_run_20261028T221020Z/examples/input_output_examples.json) |
| 검증 요약 | [summary.json](week4/full_run_20261028T221020Z/checks/summary.json) |

```text
week3/                         # 이전 Kafka 검증
week4/
├── ci_verification.json        # 기존 CI 검증
├── demo_verification.json      # 샘플 20건 검증
└── full_run_20261028T221020Z/   # 전체 3,846건 처리
    ├── README.md              # 실행 결과 보고서
    ├── inputs/                # 실제 입력
    ├── outputs/               # 전체 CSV·JSONL 및 집계
    ├── examples/              # 입출력 비교
    ├── checks/                # 검증·재전송 결과
    ├── logs/                  # 실행 로그
    └── metadata/              # 출처·파일 해시·이전 결과
```

PostgreSQL이 기준 저장소이며, 이 폴더의 CSV·JSONL은 확인용 내보내기 파일이다. Parquet 원본은 프로젝트의 `artifacts/week4/parquet/`에 있다. 보관된 JSON 내부 경로는 과거 실행 기록으로 유지했으며, 이동 내역은 전체 실행 폴더의 `metadata/layout_changes.json`에 있다.

## 스트리밍 v2 검증

- [상태·중복·지연·복구](week4/stream_v2_verification.json)
- [실제 샘플 20건 최초 처리](week4/stream_v2_sample_initial.json)
- [같은 샘플 재전송](week4/stream_v2_sample_replay.json)

