import argparse
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout
import io

from src.producer import normalize, run


def event():
    return {"meta": {"id": "abc"}, "id": 1, "wiki": "kowiki", "title": "문서",
            "server_name": "ko.wikipedia.org", "type": "edit", "timestamp": 1700000000}


class Message:
    def __init__(self, value): self.data = value
    def value(self): return self.data
    def partition(self): return 0
    def offset(self): return 1


class FakeProducer:
    error = None
    def __init__(self, config): self.queue = []
    def produce(self, topic, key, value, on_delivery): self.queue.append((on_delivery, Message(value)))
    def poll(self, timeout):
        while self.queue:
            callback, message = self.queue.pop()
            callback(self.error, message)
    def flush(self, timeout):
        self.poll(0)
        return 0


class ProducerTests(unittest.TestCase):
    def test_scope_key_and_original_time(self):
        key, value = normalize(event(), "2026-10-25T00:00:00+00:00")
        self.assertEqual(key, "kowiki:문서")
        self.assertEqual(value["event_id"], "abc")
        self.assertEqual(value["event_time"], "2023-11-14T22:13:20+00:00")
        self.assertEqual(value["payload"], event())
        self.assertIsNone(normalize({**event(), "type": "new"}))
        self.assertIsNone(normalize({**event(), "server_name": "commons.wikimedia.org"}))

    def test_missing_identity_is_rejected(self):
        bad = event()
        del bad["meta"]
        del bad["id"]
        with self.assertRaises(ValueError): normalize(bad)

    def execute(self, factory, source):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "input.jsonl").write_text(source)
            args = argparse.Namespace(input=path / "input.jsonl", output=path / "runs", rate=0,
                                      bootstrap_servers="unused", topic="test", limit=0, delivery_timeout_ms=1000)
            with redirect_stdout(io.StringIO()): status = run(args, factory)
            result = next((path / "runs").iterdir())
            return status, json.loads((result / "summary.json").read_text()), (result / "failed.jsonl").read_text()

    def test_duplicate_and_filtered_records(self):
        source = "\n".join(json.dumps(e) for e in [event(), event(), {**event(), "type": "new"}])
        status, summary, errors = self.execute(FakeProducer, source)
        self.assertEqual(status, 0)
        self.assertEqual(summary["counts"]["delivered"], 1)
        self.assertEqual(summary["counts"]["duplicate_input"], 1)
        self.assertEqual(summary["counts"]["filtered"], 1)
        self.assertEqual(errors, "")

    def test_delivery_failure_is_not_success(self):
        class Failed(FakeProducer): error = "broker timeout"
        status, summary, errors = self.execute(Failed, json.dumps(event()))
        self.assertEqual(status, 1)
        self.assertEqual(summary["counts"]["failed"], 1)
        self.assertIn("broker timeout", errors)

    def test_invalid_json_is_recorded(self):
        status, summary, errors = self.execute(FakeProducer, "not json\n" + json.dumps(event()))
        self.assertEqual(status, 1)
        self.assertEqual(summary["counts"]["invalid"], 1)
        self.assertIn("not json", errors)


if __name__ == "__main__": unittest.main()
