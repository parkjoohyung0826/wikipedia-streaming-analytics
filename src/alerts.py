"""Durable final-task-failure alerts. No network calls when no webhook is set."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
from urllib import request, parse


def root_path():
    return Path(os.getenv('WIKI_ALERT_DIR', '/workspace/artifacts/airflow'))


@contextmanager
def outbox():
    root = root_path()
    root.mkdir(parents=True, exist_ok=True)
    # Scheduler workers share this directory; serialize enqueue + send to prevent
    # two concurrent callbacks from delivering the same task failure.
    with (root / 'alerts.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with sqlite3.connect(root / 'alerts.sqlite3') as db:
            db.execute('CREATE TABLE IF NOT EXISTS alerts '
                       '(id TEXT PRIMARY KEY, payload TEXT NOT NULL, sent_at TEXT)')
            yield db


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def send(payload):
    url = os.getenv('DISCORD_WEBHOOK_URL', '').strip()
    if not url:
        return False
    if parse.urlsplit(url).scheme != 'https':
        raise ValueError('HTTPS webhook required')
    fields = ['dag_id', 'task_id', 'run_id', 'try_number', 'at']
    content = '[Wikipedia pipeline] FINAL TASK FAILURE\n' + '\n'.join(
        f'{key}: {str(payload[key])[:300]}' for key in fields)
    body = json.dumps({'content': content, 'allowed_mentions': {'parse': []}}).encode()
    req = request.Request(url, data=body, headers={'Content-Type': 'application/json', 'User-Agent': 'WikipediaStreamingAnalytics/1.0'}, method='POST')
    with request.build_opener(NoRedirect()).open(req, timeout=5) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError('Webhook returned unsuccessful status')
    return True


def deliver(db, ident, payload):
    try:
        if not send(payload):
            return 'pending'
    except Exception as exc:
        # Exception messages can contain the secret webhook URL. Log type only.
        logging.warning('Alert delivery deferred (%s); payload kept in outbox', type(exc).__name__)
        return 'pending'
    db.execute('UPDATE alerts SET sent_at=? WHERE id=?',
               (datetime.now(timezone.utc).isoformat(), ident))
    db.commit()
    return 'sent'


def notify_failure(context):
    """Airflow on_failure_callback: final failure only, never raises into Airflow."""
    try:
        ti = context['task_instance']
        alert = dict(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id,
                     try_number=ti.try_number, at=datetime.now(timezone.utc).isoformat())
        # map_index distinguishes mapped task instances; retry number is excluded.
        key = [ti.dag_id, ti.task_id, ti.run_id, getattr(ti, 'map_index', -1)]
        ident = hashlib.sha256(json.dumps(key).encode()).hexdigest()
        with outbox() as db:
            row = db.execute('SELECT payload, sent_at FROM alerts WHERE id=?', (ident,)).fetchone()
            if row and row[1]:
                return 'duplicate'
            if row:
                alert = json.loads(row[0])
            else:
                payload = json.dumps(alert)
                db.execute('INSERT INTO alerts VALUES (?, ?, NULL)', (ident, payload))
                db.commit()  # Persist before any network I/O.
                with (root_path() / 'alerts.jsonl').open('a') as handle:
                    handle.write(payload + '\n')
                logging.error('[ALERT] Final failure: dag=%s task=%s', ti.dag_id, ti.task_id)
            return deliver(db, ident, alert)
    except Exception as exc:
        logging.error('Alert callback error (%s); inspect alert storage permissions', type(exc).__name__)
        return 'error'


def retry_pending():
    counts = {'sent': 0, 'pending': 0}
    with outbox() as db:
        # Bound manual replay time; run again for the remaining queued records.
        rows = db.execute('SELECT id, payload FROM alerts WHERE sent_at IS NULL LIMIT 20').fetchall()
        for ident, payload in rows:
            counts[deliver(db, ident, json.loads(payload))] += 1
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replay up to 20 pending failure alerts')
    parser.add_argument('action', choices=['retry-pending'])
    parser.parse_args()
    print(json.dumps(retry_pending()))
