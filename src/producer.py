#!/usr/bin/env python3
"""Replay collected Wikimedia JSONL as Wikipedia edit messages in Kafka."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from confluent_kafka import Producer

DEFAULT_INPUT = "data/20261025T100423_674945Z/events.jsonl"


def now():
    return datetime.now(timezone.utc).isoformat()


def normalize(event, ingest_time=None):
    """Return (key, envelope), or None for events outside the topic scope."""
    if event.get("meta", {}).get("domain") == "canary":
        return None
    if not event.get("server_name", "").endswith(".wikipedia.org") or event.get("type") != "edit":
        return None
    for field in ("wiki", "title", "timestamp"):
        if field not in event:
            raise ValueError(f"Missing {field}")
    event_id = event.get("meta", {}).get("id")
    if not event_id:
        if event.get("id") is None:
            raise ValueError("Missing meta.id and id")
        event_id = f'{event["wiki"]}:edit:{event["id"]}'
    return f'{event["wiki"]}:{event["title"]}', {
        "event_id": event_id,
        "event_type": "wikipedia.edit",
        "event_time": datetime.fromtimestamp(event["timestamp"], timezone.utc).isoformat(),
        "ingest_time": ingest_time or now(),
        "schema_version": 1,
        "source": "wikimedia-recentchange-replay",
        "payload": event,
    }


def run(args, factory=Producer):
    directory = args.output / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    directory.mkdir(parents=True)
    counts = Counter(read=0, filtered=0, duplicate_input=0, queued=0, delivered=0, failed=0, invalid=0)
    seen = set()
    started = now()
    start = time.monotonic()
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "wikipedia-week3-replay",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "delivery.timeout.ms": args.delivery_timeout_ms,
        "request.timeout.ms": min(30000, args.delivery_timeout_ms),
    }
    producer = factory(config)
    interrupted = False
    pending = {}
    with (directory / "delivery.jsonl").open("w", encoding="utf-8") as delivery, (directory / "failed.jsonl").open("w", encoding="utf-8") as failed:
        def callback(error, message):
            event_id = json.loads(message.value())["event_id"]
            item = pending.pop(event_id)
            record = {"event_id": event_id, "key": item["key"], "topic": args.topic, "at": now()}
            if error:
                counts["failed"] += 1
                record.update(status="failed", error=str(error))
                failed.write(json.dumps({**item, "error": str(error)}, ensure_ascii=False) + "\n")
                failed.flush()
            else:
                counts["delivered"] += 1
                record.update(status="delivered", partition=message.partition(), offset=message.offset())
            delivery.write(json.dumps(record, ensure_ascii=False) + "\n")

        try:
            with args.input.open(encoding="utf-8") as source:
                for line_number, line in enumerate(source, 1):
                    counts["read"] += 1
                    try:
                        value = normalize(json.loads(line))
                        if value is None:
                            counts["filtered"] += 1
                            continue
                        key, envelope = value
                        event_id = envelope["event_id"]
                        if event_id in seen:
                            counts["duplicate_input"] += 1
                            continue
                        # Limit the rate of selected messages, not source lines.
                        if args.rate:
                            due = start + counts["queued"] / args.rate
                            while time.monotonic() < due:
                                producer.poll(max(0, min(0.1, due - time.monotonic())))
                        envelope["ingest_time"] = now()
                        payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode()
                        pending[event_id] = {"key": key, "value": envelope}
                        queue_deadline = time.monotonic() + args.delivery_timeout_ms / 1000
                        while True:
                            try:
                                producer.produce(args.topic, key=key.encode(), value=payload, on_delivery=callback)
                                break
                            except BufferError:
                                if time.monotonic() >= queue_deadline:
                                    raise TimeoutError("Local producer queue stayed full")
                                producer.poll(0.1)
                        seen.add(event_id)
                        counts["queued"] += 1
                        producer.poll(0)
                        if args.limit and counts["queued"] >= args.limit:
                            break
                    except (ValueError, TypeError, KeyError, OverflowError) as error:
                        counts["invalid"] += 1
                        failed.write(json.dumps({"line": line_number, "error": str(error), "raw": line}, ensure_ascii=False) + "\n")
        except KeyboardInterrupt:
            interrupted = True
        except Exception as error:
            interrupted = True
            failed.write(json.dumps({"fatal_error": str(error)}) + "\n")
        finally:
            remaining = producer.flush(args.delivery_timeout_ms / 1000 + 5)
            for item in pending.values():
                failed.write(json.dumps({**item, "error": "Delivery unknown; verify before replay"}, ensure_ascii=False) + "\n")
    elapsed = time.monotonic() - start
    summary = {"started_at": started, "finished_at": now(), "elapsed_seconds": elapsed,
               "input": str(args.input), "topic": args.topic, "requested_rate": args.rate,
               "counts": dict(counts), "unresolved": len(pending), "flush_remaining": remaining,
               "interrupted": interrupted, "producer_config": config,
               "delivered_per_second": counts["delivered"] / elapsed,
               "output": str(directory)}
    summary["success"] = bool(counts["queued"]) and counts["queued"] == counts["delivered"] and not (counts["failed"] or counts["invalid"] or pending or remaining or interrupted)
    (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["success"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path(DEFAULT_INPUT))
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="wikipedia-edits")
    parser.add_argument("--rate", type=float, default=100, help="Selected messages/second; 0 = unlimited")
    parser.add_argument("--limit", type=int, default=0, help="0 = all matching records")
    parser.add_argument("--delivery-timeout-ms", type=int, default=120000)
    parser.add_argument("--output", type=Path, default=Path("artifacts/runs"))
    args = parser.parse_args()
    if args.rate < 0 or args.limit < 0 or args.delivery_timeout_ms < 1000:
        parser.error("rate/limit must be nonnegative; timeout must be >= 1000")
    if not args.input.is_file():
        parser.error(f"Input not found: {args.input}")
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
