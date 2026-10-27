#!/usr/bin/env bash
# Run from project root in Codespaces: bash scripts/week4.sh demo
set -euo pipefail
cd "$(dirname "$0")/.."
export LAB_UID="$(id -u)" LAB_GID="$(id -g)"
compose=(docker compose -f docker-compose.yml -f compose.week4.yml)
mkdir -p artifacts/week4 evidence/week4
python_bin="${PYTHON_BIN:-python}"
if [[ -x .venv/bin/python ]]; then python_bin=.venv/bin/python; fi

setup() {
  "${compose[@]}" up -d --wait kafka postgres
  "${compose[@]}" build spark
}
topic() {
  "${compose[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --if-not-exists \
    --topic "$1" --partitions 3 --replication-factor 1 \
    --config retention.ms=604800000 --config cleanup.policy=delete
}
run_spark() { "${compose[@]}" run --rm -T spark "$@"; }

case "${1:-help}" in
  demo)
    setup
    topic wikipedia-edits
    "$python_bin" -m src.producer --input samples/wikipedia_edits.jsonl --rate 10
    run_spark src/process_edits.py
    run_spark scripts/check_week4.py --report evidence/week4/demo_verification.json
    ;;
  run)
    run_spark src/process_edits.py "${@:2}"
    ;;
  check)
    run_spark scripts/check_week4.py "${@:2}"
    ;;
  verify)
    setup
    # Isolate all generated test records from the real project database and topic.
    test_id="$(date -u +%Y%m%d%H%M%S)_${RANDOM}"
    test_db="week4_test_${test_id}"
    test_topic="week4-test-${test_id}"
    test_root="artifacts/week4/tests/${test_id}"
    mkdir -p "$test_root"
    "${compose[@]}" exec -T postgres psql -U wiki -d wiki -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${test_db}"
    topic "$test_topic"
    # Unit-level Spark transformations, independent of the streaming sink.
    run_spark tests_week4/test_transform.py 2>&1 | tee "$test_root/transform.log"
    "$python_bin" scripts/week4_fixture.py --topic "$test_topic"
    test_run() { "${compose[@]}" run --rm -T -e "PGDATABASE=$test_db" spark "$@"; }
    args=(src/process_edits.py --topic "$test_topic" --checkpoint "$test_root/checkpoint" --output "$test_root/parquet")
    # Deliberately commit DB writes then fail before Spark can commit its progress.
    if test_run "${args[@]}" --fail-after-commit > "$test_root/injected_failure.log" 2>&1; then
      echo 'ERROR: expected injected failure did not occur' >&2; exit 1
    fi
    grep -q 'Injected failure AFTER database commit' "$test_root/injected_failure.log"
    test_run "${args[@]}" 2>&1 | tee "$test_root/recovery.log"
    test_run scripts/check_week4.py --output "$test_root/parquet" --expect-edits 3 --expect-rejected 3 --fixture --report "$test_root/recovery.json"
    # No new input: same checkpoint must retain identical final results.
    test_run "${args[@]}" 2>&1 | tee "$test_root/restart.log"
    test_run scripts/check_week4.py --output "$test_root/parquet" --expect-edits 3 --expect-rejected 3 --fixture --report "$test_root/restart.json"
    # Resend all seven messages at new Kafka offsets: event IDs must still deduplicate.
    "$python_bin" scripts/week4_fixture.py --topic "$test_topic"
    test_run "${args[@]}" 2>&1 | tee "$test_root/replay.log"
    test_run scripts/check_week4.py --output "$test_root/parquet" --expect-edits 3 --expect-rejected 6 --fixture --report "$test_root/replay.json"
    "$python_bin" - "$test_root" "$test_db" "$test_topic" <<'PY'
import json, pathlib, sys
root, database, topic = sys.argv[1:]
reports = {name: json.loads((pathlib.Path(root) / (name + '.json')).read_text())
           for name in ('recovery', 'restart', 'replay')}
result = {'verified': True, 'fixture': 'synthetic; separate database and topic',
          'database': database, 'topic': topic, 'reports': reports}
pathlib.Path('evidence/week4/recovery_verification.json').write_text(
    json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('PASS: Spark transforms, post-commit failure recovery, checkpoint restart, input replay, DB and Parquet consistency')
print('Evidence: evidence/week4/recovery_verification.json')
PY
    ;;
  *)
    echo 'Usage: bash scripts/week4.sh {demo|verify|run [options]|check [options]}'
    ;;
esac
