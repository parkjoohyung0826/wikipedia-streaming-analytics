import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, MagicMock
from src import alerts


class AlertsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'WIKI_ALERT_DIR': self.tmp.name, 'DISCORD_WEBHOOK_URL': ''})
        self.env.start()
        self.context = {'task_instance': SimpleNamespace(dag_id='wiki', task_id='daily', run_id='run1', try_number=4)}

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_no_channel_keeps_one_pending_record(self):
        with patch.object(alerts.request, 'build_opener') as network:
            self.assertEqual(alerts.notify_failure(self.context), 'pending')
            self.assertEqual(alerts.notify_failure(self.context), 'pending')
            network.assert_not_called()
        self.assertEqual(len((Path(self.tmp.name)/'alerts.jsonl').read_text().splitlines()), 1)

    def test_failure_then_replay_then_duplicate(self):
        with patch.object(alerts, 'send', side_effect=TimeoutError('secret-url')):
            with self.assertLogs(level='WARNING') as logs:
                self.assertEqual(alerts.notify_failure(self.context), 'pending')
            self.assertNotIn('secret-url', '\n'.join(logs.output))
        with patch.object(alerts, 'send', return_value=True) as send:
            self.assertEqual(alerts.retry_pending(), {'sent': 1, 'pending': 0})
            self.assertEqual(alerts.notify_failure(self.context), 'duplicate')
            self.assertEqual(send.call_count, 1)

    def test_different_runs_are_separate(self):
        with patch.object(alerts, 'send', return_value=True) as send:
            alerts.notify_failure(self.context)
            self.context['task_instance'].run_id = 'run2'
            alerts.notify_failure(self.context)
            self.assertEqual(send.call_count, 2)

    def test_http_payload_and_timeout(self):
        opener = MagicMock()
        opener.open.return_value.__enter__.return_value.status = 204
        with patch.dict(os.environ, {'DISCORD_WEBHOOK_URL': 'https://example.invalid/secret'}):
            with patch.object(alerts.request, 'build_opener', return_value=opener):
                self.assertEqual(alerts.notify_failure(self.context), 'sent')
        args, kwargs = opener.open.call_args
        payload = json.loads(args[0].data)
        self.assertIn('FINAL TASK FAILURE', payload['content'])
        self.assertEqual(payload['allowed_mentions'], {'parse': []})
        self.assertEqual(kwargs['timeout'], 5)
        self.assertEqual(args[0].get_header('User-agent'), 'WikipediaStreamingAnalytics/1.0')

    def test_storage_error_does_not_escape_callback(self):
        with patch.object(alerts, 'root_path', side_effect=PermissionError):
            self.assertEqual(alerts.notify_failure(self.context), 'error')


if __name__ == '__main__':
    unittest.main()
