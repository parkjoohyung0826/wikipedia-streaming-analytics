"""A resume cursor must never pass a message Kafka has not acknowledged."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from src.live_producer import frames,main

class Response:
    def __init__(self,lines):self.lines=lines
    def __enter__(self):return iter(self.lines)
    def __exit__(self,*args):pass

class Producer:
    def __init__(self,error):self.error=error
    def produce(self,*args,**kwargs):self.callback=kwargs['on_delivery']
    def flush(self,*args):self.callback(self.error,None);return 0

class LiveTests(unittest.TestCase):
    def test_only_complete_frames(self):
        self.assertEqual(list(frames([b'id: one\n',b'data: first\n',b'data: second\n',b'\n',b'data: incomplete'])),[('message','one','first\nsecond')])

    def run_once(self,error):
        event=json.loads(Path('samples/wikipedia_edits.jsonl').read_text().splitlines()[0])
        lines=[b'id: durable-cursor\n',('data: '+json.dumps(event)+'\n').encode(),b'\n']
        previous=Path.cwd()
        with tempfile.TemporaryDirectory() as folder:
            try:
                os.chdir(folder)
                with patch('src.live_producer.Producer',return_value=Producer(error)),patch('src.live_producer.urllib.request.urlopen',return_value=Response(lines)),patch('src.live_producer.time.sleep',side_effect=KeyboardInterrupt):
                    with self.assertRaises(KeyboardInterrupt):main()
                cursor=Path('artifacts/live/cursor.json')
                return json.loads(cursor.read_text()) if cursor.exists() else None
            finally:os.chdir(previous)

    def test_advance_only_after_ack(self):
        self.assertEqual(self.run_once(None)['last_event_id'],'durable-cursor')

    def test_failed_delivery_does_not_advance(self):
        self.assertIsNone(self.run_once('delivery failed'))

if __name__=='__main__':unittest.main()
