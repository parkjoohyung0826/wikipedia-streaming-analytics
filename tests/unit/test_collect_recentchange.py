"""Offline checks: python3 -m unittest discover -s tests/unit -v"""
import json
from pathlib import Path
import tempfile
import unittest

from src.collect_recentchange import analyze, frames


class CollectorTests(unittest.TestCase):
    def test_reconnect_deduplication_and_byte_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event = {"meta": {"id": "a"}, "server_name": "ko.wikipedia.org",
                     "type": "edit", "wiki": "kowiki", "title": "한글"}
            frame = "event: message\r\ndata: " + json.dumps(event, ensure_ascii=False) + "\r\n\r\n"
            (root / "raw_001.sse").write_bytes((frame + 'data: {"partial":').encode())
            (root / "raw_002.sse").write_bytes((frame +
                'data: {"meta":{"domain":"canary"}}\n\n' +
                'data: invalid\n\n' + 'event: error\ndata: {}\n\n').encode())
            result = analyze(root, 600)
            self.assertEqual(result["counts"], {
                "events": 1, "bot_events": 0, "wikipedia_events": 1,
                "wikipedia_edit_events": 1, "duplicates_excluded": 1,
                "canary_excluded": 1, "invalid_json": 1, "non_message_frames": 1,
            })
            stored = (root / "events.jsonl").read_bytes()
            self.assertEqual(json.loads(stored), event)
            self.assertEqual(result["bytes"]["jsonl"], len(stored))
            self.assertEqual(result["bytes"]["wikipedia_edit_jsonl"], len(stored))
            self.assertEqual(result["bytes"]["raw_sse"], sum(p.stat().st_size for p in root.glob("raw*.sse")))

    def test_comments_multiline_and_incomplete_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.sse"
            path.write_text(':heartbeat\n\ndata: {\ndata: "value": 1}\n\ndata: unfinished')
            self.assertEqual(list(frames(path)), [("message", '{\n"value": 1}')])


if __name__ == "__main__":
    unittest.main()
