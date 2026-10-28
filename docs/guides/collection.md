# Wikimedia recentchange 10분 수집

아래 명령은 프로젝트 최상위 폴더에서 실행합니다.

Python 표준 라이브러리와 `curl`로 실시간 SSE 스트림을 수집합니다.

```bash
python3 -m src.collect_recentchange --seconds 600
```

Python 3.9 이상과 curl이 필요합니다. 별도 pip 설치나 API 키는 필요하지 않습니다.
짧게 확인하려면 `--seconds 10`, 저장 위치를 바꾸려면 `--output 다른폴더`를 사용하세요.

## 결과 파일

실행마다 `data/<UTC 실행시각>/`에 별도 저장합니다.

- `raw_001.sse`, `raw_002.sse`, …: 연결별 SSE 응답 본문. 프레임 메타데이터와 종료 시점의 불완전 프레임도 포함합니다. 초기 버전 결과의 `raw.sse`도 분석할 수 있습니다.
- `events.jsonl`: 정상 JSON message 이벤트를 한 줄씩 저장합니다. canary와 중복 meta.id는 제외합니다.
- `events.jsonl.gz`: 동일 JSONL의 gzip 압축본.
- `summary.json`: 수집 시작·종료, 실행 시간, 건수, 바이트 크기, 초당 처리량, 이벤트 유형 및 wiki별 집계.
- `curl.log`: 연결 오류와 정상적인 수집 시간 만료 메시지.

## 측정 기준

- 수집 시간은 클라이언트의 600초 관측 구간이며 연결 준비 시간도 포함됩니다. 스트림 전달 지연 때문에 서버의 편집 발생 시각과 수신 시각은 다릅니다. `event_timestamp_min_utc/max_utc`는 변경 레코드의 `timestamp` 범위이며, 과거 시각의 로그도 포함될 수 있습니다. `stream_meta_dt_min_utc/max_utc`는 스트림 메타데이터 `meta.dt` 범위입니다. 분별 집계 역시 변경 레코드 시각 기준이며 수신 시간별 집계가 아닙니다.
- recentchange는 **전체 Wikimedia 공개 위키의 변경 이벤트**이며 Wikipedia 편집 전용 스트림이 아닙니다.
- `counts.events`: 저장한 전체 고유 이벤트 수.
- `counts.wikipedia_events`: `server_name`이 `.wikipedia.org`로 끝나는 이벤트 수.
- `counts.wikipedia_edit_events`: 위 Wikipedia 이벤트 중 `type == "edit"`. 새 문서 생성(`new`), 로그(`log`), 분류 변경(`categorize`) 등은 이 수치에서 제외합니다.
- `bytes.wikipedia_jsonl`, `bytes.wikipedia_edit_jsonl`: 해당 부분집합이 전체 JSONL에서 차지하는 바이트 합계입니다. 별도 부분집합 파일을 생성하지 않습니다.
- `bytes.raw_sse`는 HTTP 응답 본문 크기입니다. HTTP 헤더·TLS·TCP 트래픽은 포함하지 않으므로 네트워크 전체 전송량은 아닙니다.
- JSONL 크기는 UTF-8 데이터와 줄바꿈을 포함합니다. MB = 1,000,000바이트, MiB = 1,048,576바이트입니다.
- 초당 건수·바이트는 실제 클라이언트 실행 시간으로 나눕니다. 하루 규모로 환산할 경우 단기 표본 기반 추정일 뿐입니다.
- 수집 시간 종료에 따른 curl 종료 코드 `28`은 예상되는 동작입니다. 빈 파일 또는 조기 연결 종료는 실패로 표시하고 가능한 부분 결과를 남깁니다.
- 연결이 끊기면 HTTP/1.1로 자동 재연결하며 마지막 완전한 프레임의 `Last-Event-ID`를 전달해 이어 받습니다. 재전송 중복은 `meta.id`로 제외합니다. 재연결 대기 시간도 600초에 포함합니다. 연결 이력은 `attempts`에 기록합니다. 불완전 수집을 완전한 10분 결과처럼 보고하지 않도록 `collection_window_completed`를 확인합니다. 이 값은 관측 시간 완료 여부이며 서버 이벤트의 무손실 전달을 보장하지는 않습니다.
- 종료 요약이 있는 중단된 실행은 원래 관측 구간이 아직 끝나지 않았을 때 `python3 -m src.collect_recentchange --resume data/<실행폴더>`로 재개할 수 있습니다. 이미 끝난 구간은 새 실행으로 측정하세요.
- 종료 순간의 미완성 SSE 프레임은 원본에 남지만 JSONL 집계에서 제외합니다. 수집 데이터에는 공개된 사용자명·편집 요약 등이 포함됩니다.

## 출처

- 스트림: https://stream.wikimedia.org/v2/stream/recentchange
- SSE 및 canary 설명: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service
- 데이터 범위: https://www.mediawiki.org/wiki/API%3ARecent_changes_stream
