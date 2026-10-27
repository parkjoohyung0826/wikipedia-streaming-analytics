#!/usr/bin/env python3
"""Collect Wikimedia SSE for a fixed wall-clock duration, then measure it.

Requires Python 3.9+ and curl. No third-party Python packages are needed.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import json
from itertools import chain
from pathlib import Path
import shutil
import subprocess
import time

URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def frames(path):
    """Parse complete SSE frames; deliberately ignore a truncated final frame."""
    fields = {}
    data = []
    with path.open(encoding="utf-8", errors="replace") as source:
        for line in source:
            line = line.rstrip("\r\n")
            if not line:
                if data:
                    yield fields.get("event", "message"), "\n".join(data)
                fields, data = {}, []
            elif not line.startswith(":"):
                key, _, value = line.partition(":")
                value = value[1:] if value.startswith(" ") else value
                if key == "data":
                    data.append(value)
                else:
                    fields[key] = value


def analyze(directory, elapsed):
    counts = Counter()
    types = Counter()
    wikis = Counter()
    minute_counts = Counter()
    seen = set()
    timestamps = []
    stream_datetimes = []
    sizes = []
    wikipedia_bytes = 0
    edits_bytes = 0
    with (directory / "events.jsonl").open("wb") as output:
        for kind, payload in chain.from_iterable(frames(p) for p in sorted(directory.glob("raw*.sse"))):
            if kind != "message":
                counts["non_message_frames"] += 1
                continue
            try:
                event = json.loads(payload)
                if not isinstance(event, dict):
                    raise ValueError("Event must be an object")
            except ValueError:
                counts["invalid_json"] += 1
                continue
            meta = event.get("meta", {})
            if meta.get("domain") == "canary":
                counts["canary_excluded"] += 1
                continue
            event_id = meta.get("id")
            if event_id:
                if event_id in seen:
                    counts["duplicates_excluded"] += 1
                    continue
                seen.add(event_id)
            else:
                counts["missing_meta_id"] += 1
            record = (payload + "\n").encode("utf-8")
            if meta.get("dt"):
                stream_datetimes.append(datetime.fromisoformat(meta["dt"].replace("Z", "+00:00")))
            output.write(record)
            sizes.append(len(record))
            counts["events"] += 1
            types[event.get("type", "unknown")] += 1
            wikis[event.get("wiki", "unknown")] += 1
            counts["bot_events"] += int(event.get("bot") is True)
            domain = event.get("server_name", meta.get("domain", ""))
            if domain.endswith(".wikipedia.org"):
                counts["wikipedia_events"] += 1
                wikipedia_bytes += len(record)
                if event.get("type") == "edit":
                    counts["wikipedia_edit_events"] += 1
                    edits_bytes += len(record)
            timestamp = event.get("timestamp")
            if isinstance(timestamp, (int, float)):
                timestamps.append(timestamp)
                minute = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                minute_counts[minute] += 1
    path = directory / "events.jsonl"
    with path.open("rb") as source, gzip.open(directory / "events.jsonl.gz", "wb") as target:
        shutil.copyfileobj(source, target)
    byte_count = path.stat().st_size
    return {
        "counts": dict(counts),
        "bytes": {
            "raw_sse": sum(p.stat().st_size for p in directory.glob("raw*.sse")),
            "jsonl": byte_count,
            "gzip_jsonl": (directory / "events.jsonl.gz").stat().st_size,
            "wikipedia_jsonl": wikipedia_bytes,
            "wikipedia_edit_jsonl": edits_bytes,
        },
        "events_per_second": len(sizes) / elapsed,
        "jsonl_bytes_per_second": byte_count / elapsed,
        "average_jsonl_bytes_per_event": byte_count / len(sizes) if sizes else 0,
        "stream_meta_dt_min_utc": min(stream_datetimes).isoformat() if stream_datetimes else None,
        "stream_meta_dt_max_utc": max(stream_datetimes).isoformat() if stream_datetimes else None,
        "event_timestamp_min_utc": datetime.fromtimestamp(min(timestamps), timezone.utc).isoformat() if timestamps else None,
        "event_timestamp_max_utc": datetime.fromtimestamp(max(timestamps), timezone.utc).isoformat() if timestamps else None,
        "event_types": dict(types.most_common()),
        "wikis": dict(wikis.most_common()),
        "events_by_event_minute_utc": dict(sorted(minute_counts.items())),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=600)
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--resume", type=Path, help="Resume an interrupted run within its original observation window")
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    directory = args.resume or args.output / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    if args.resume:
        previous = json.loads((directory / "summary.json").read_text())
        started = previous["started_at_utc"]
        args.seconds = previous["requested_seconds"]
        prior_elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(started)).total_seconds()
        attempts = previous.get("attempts", [{"curl_exit_code": previous["curl_exit_code"], "elapsed_seconds": previous["elapsed_seconds"]}])
    else:
        directory.mkdir(parents=True, exist_ok=False)
        started = utc_now()
        prior_elapsed = 0
        attempts = []
    if prior_elapsed >= args.seconds:
        parser.error("Original observation window already ended; start a new run")
    start = time.monotonic()
    print(f"Collecting for {args.seconds:g}s into {directory}", flush=True)
    deadline = start + args.seconds - prior_elapsed
    while time.monotonic() < deadline:
        last_id = None
        for path in sorted(directory.glob("raw*.sse")):
            pending_id = None
            with path.open(encoding="utf-8", errors="replace") as stream:
                for line in stream:
                    if line.startswith("id:"):
                        pending_id = line[3:].strip()
                    elif not line.strip() and pending_id:
                        last_id = pending_id
                        pending_id = None
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        segment = directory / f"raw_{len(attempts) + 1:03d}.sse"
        command = [
            "curl", "--no-buffer", "--silent", "--show-error", "--fail",
            "--http1.1", "--connect-timeout", "20", "--max-time", str(remaining),
            "--header", "Accept: text/event-stream",
            "--user-agent", "RecentChangeHomework/1.0 (educational stream measurement)",
            "--output", str(segment), URL,
        ]
        if last_id:
            command.extend(["--header", "Last-Event-ID: " + last_id])
        attempt_start = time.monotonic()
        with (directory / "curl.log").open("a") as log:
            result = subprocess.run(command, stderr=log)
        attempts.append({"curl_exit_code": result.returncode, "elapsed_seconds": time.monotonic() - attempt_start, "segment": segment.name, "resumed_from_last_id": bool(last_id)})
        print(f"Connection ended: {attempts[-1]}", flush=True)
        if time.monotonic() < deadline:
            time.sleep(min(2, max(0, deadline - time.monotonic())))
    elapsed = prior_elapsed + time.monotonic() - start
    finished = utc_now()
    summary = {
        "source": URL,
        "started_at_utc": started,
        "finished_at_utc": finished,
        "requested_seconds": args.seconds,
        "elapsed_seconds": elapsed,
        "curl_exit_code": result.returncode,
        "collection_window_completed": result.returncode == 28 and elapsed >= args.seconds - 0.1 and any(p.stat().st_size for p in directory.glob("raw*.sse")),
        "attempts": attempts,
        "measurement": "Client wall-clock window including connection setup and reconnect gaps; complete received SSE frames only. Last-Event-ID replay and meta.id deduplication. Not an exhaustive event-time census.",
        **analyze(directory, elapsed),
    }
    (directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    if not summary["collection_window_completed"] or not summary["counts"].get("events"):
        raise SystemExit("Collection incomplete; inspect curl.log and summary.json")


if __name__ == "__main__":
    main()
