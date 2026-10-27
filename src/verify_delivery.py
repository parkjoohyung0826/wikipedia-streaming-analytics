#!/usr/bin/env python3
"""Compare console consumer key<TAB>JSON export with source and ACK log."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from .producer import normalize


def verify(source, consumed, run_directory):
    expected = {}
    for line in source.open(encoding="utf-8"):
        result = normalize(json.loads(line))
        if result:
            key, value = result
            expected[value["event_id"]] = (key, value)
    actual = {}
    duplicates = 0
    value_bytes = 0
    key_bytes = 0
    for line in consumed.open(encoding="utf-8"):
        key, raw = line.rstrip("\n").split("\t", 1)
        value = json.loads(raw)
        event_id = value["event_id"]
        duplicates += event_id in actual
        assert event_id in expected, f"Unexpected event: {event_id}"
        wanted_key, wanted = expected[event_id]
        assert key == wanted_key
        for field in ("event_id", "event_type", "event_time", "schema_version", "source", "payload"):
            assert value[field] == wanted[field], (event_id, field)
        assert value["ingest_time"]
        actual[event_id] = value
        value_bytes += len(raw.encode())
        key_bytes += len(key.encode())
    deliveries = [json.loads(line) for line in (run_directory / "delivery.jsonl").open()]
    assert set(expected) == set(actual), "Missing consumed events"
    assert duplicates == 0, "Duplicate messages in consumed output"
    assert len(deliveries) == len(expected)
    assert all(d["status"] == "delivered" for d in deliveries)
    assert {d["event_id"] for d in deliveries} == set(expected)
    assert len({(d["partition"], d["offset"]) for d in deliveries}) == len(deliveries)
    partitions = Counter(d["partition"] for d in deliveries)
    ranges = {str(p): {"min": min(d["offset"] for d in deliveries if d["partition"] == p),
                       "max": max(d["offset"] for d in deliveries if d["partition"] == p)} for p in partitions}
    return {"verified": True, "expected": len(expected), "consumed": len(actual),
            "duplicates": duplicates, "acknowledged": len(deliveries),
            "partition_counts": dict(sorted(partitions.items())), "offset_ranges": ranges,
            "key_bytes": key_bytes, "value_bytes": value_bytes,
            "console_capture_sha256": hashlib.sha256(consumed.read_bytes()).hexdigest(),
            "checks": ["event_id set", "key", "original payload", "envelope fields", "no duplicate events", "ACK partition/offset uniqueness"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--consumed", required=True, type=Path)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("evidence/verification.json"))
    args = parser.parse_args()
    result = verify(args.input, args.consumed, args.run)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
