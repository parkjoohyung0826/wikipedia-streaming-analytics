"""Short real Airflow failure -> automatic callback -> Discord delivery test.

Run only in an isolated Airflow home/database; sends one real test notification.
"""
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils import db
from src.alerts import notify_failure

root = Path(os.environ['WIKI_ALERT_DIR'])
root.mkdir(parents=True, exist_ok=True)
if not os.environ.get('DISCORD_WEBHOOK_URL'):
    raise RuntimeError('A configured Discord webhook is required')
if (root / 'attempts.jsonl').exists():
    raise RuntimeError('Use a fresh test directory to preserve prior evidence')


def fail_for_test(**context):
    queued = 0
    database = root / 'alerts.sqlite3'
    if database.exists():
        with sqlite3.connect(database) as conn:
            queued = conn.execute('SELECT COUNT(*) FROM alerts').fetchone()[0]
    with (root / 'attempts.jsonl').open('a') as handle:
        handle.write(json.dumps({'try_number': context['ti'].try_number,
                                 'alerts_before_attempt': queued}) + '\n')
    raise AirflowException('Intentional week6 notification test failure; no production data touched')


db.initdb()
with DAG('week6_real_failure_alert',
         start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
         schedule=None, catchup=False,
         default_args={'retries': 1, 'retry_delay': timedelta(seconds=1),
                       'on_failure_callback': notify_failure}) as dag:
    PythonOperator(task_id='intentional_failure', python_callable=fail_for_test)

result = dag.test()
attempts = [json.loads(line) for line in (root / 'attempts.jsonl').read_text().splitlines()]
assert str(result.state) == 'failed', result.state
assert [a['try_number'] for a in attempts] == [1, 2], attempts
assert all(a['alerts_before_attempt'] == 0 for a in attempts), attempts
with sqlite3.connect(root / 'alerts.sqlite3') as conn:
    rows = conn.execute('SELECT payload, sent_at FROM alerts').fetchall()
assert len(rows) == 1 and rows[0][1], 'Expected exactly one delivered final-failure alert'
summary = {'dag_id': dag.dag_id, 'run_id': result.run_id,
           'dag_state': str(result.state), 'attempts': attempts,
           'automatic_failure_callback': True, 'sent_alert_count': len(rows),
           'sent_at': rows[0][1], 'alert': json.loads(rows[0][0]),
           'scope': 'Real Airflow DAG.test TaskInstance failure and retry with automatic production callback; isolated SQLite, not production scheduler.'}
(root / 'result.json').write_text(json.dumps(summary, indent=2) + '\n')
print('PASS: two real task failures, one automatic final-failure Discord delivery')
