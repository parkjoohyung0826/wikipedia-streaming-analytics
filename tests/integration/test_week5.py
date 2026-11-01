"""Run actual Airflow tasks and Spark containers twice, then verify fail-closed publication."""
from datetime import datetime,timedelta,timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch
import psycopg2

sys.path.insert(0,'/workspace')
from src.airflow_support import resolve_date,work_path
spec=importlib.util.spec_from_file_location('week5_dag','/workspace/dags/wikipedia_daily.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
dag=module.dag
out=Path('/workspace/evidence/week5');out.mkdir(parents=True,exist_ok=True)

def state():
    conn=psycopg2.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM week5.daily_wiki WHERE target_date='2026-10-25' ORDER BY wiki")
            daily=cur.fetchall()
            cur.execute("SELECT * FROM week5.publications WHERE target_date='2026-10-25'")
            return daily,cur.fetchall()
    finally:conn.close()

def run(when):
    result=dag.test(execution_date=when,run_conf={'target_date':'2026-10-25'})
    tasks={ti.task_id:ti.state for ti in result.get_task_instances()}
    return result,dict(run_id=result.run_id,state=result.state,tasks=tasks,
                       work=work_path(result.run_id,'2026-10-25'))

assert dag.catchup is False and dag.max_active_runs==1
assert dag.get_task('quality_check').retries==0
assert dag.get_task('compact_raw').retries==3
assert dag.get_task('compact_raw').retry_exponential_backoff
assert dag.get_task('publish').upstream_task_ids >= {'quality_check'}
assert resolve_date('',datetime(2026,11,1,18,tzinfo=timezone.utc))=='2026-11-01'
assert resolve_date('2026-10-25',None)=='2026-10-25'
try:resolve_date('2026-02-30',None);raise AssertionError('invalid date accepted')
except ValueError:pass
now=datetime.now(timezone.utc)-timedelta(minutes=5)
from airflow.exceptions import AirflowException
real_spark=module.run_spark
attempts=[0]
def temporary_failure(action,day,context):
    if action=='compact_raw':
        attempts[0]+=1
        if attempts[0]==1:raise AirflowException('Injected transient connection failure (test only)')
    return real_spark(action,day,context)
compact_task=dag.get_task('compact_raw')
original_delay=compact_task.retry_delay
compact_task.retry_delay=timedelta(seconds=1)
with patch.object(module,'run_spark',temporary_failure):first,a=run(now)
compact_task.retry_delay=original_delay
assert attempts[0]==2,'Transient error was not retried exactly once'

assert first.state=='success',a
before=state()
assert sum(row[2] for row in before[0])==3846,before[0]
second,b=run(now+timedelta(seconds=1))
assert second.state=='success',b
assert state()==before,'Same-day rerun changed stored values or publication'
# Test-only corruption: make the aggregate metadata refer to another day.
original=module.run_spark

def corrupt_after_daily(action,day,context):
    work=original(action,day,context)
    if action=='daily_agg':
        p=Path('/workspace')/work/'daily.json';value=json.loads(p.read_text())
        value['target_date']='1900-01-01';p.write_text(json.dumps(value))
    return work

with patch.object(module,'run_spark',corrupt_after_daily):
    failed,c=run(now+timedelta(seconds=2))
assert failed.state=='failed',c
assert c['tasks']['quality_check']=='failed' and c['tasks']['publish']=='upstream_failed',c
assert state()==before,'Failed quality check altered published output'
alerts=Path('/workspace/artifacts/week5/alerts.jsonl')
assert any(json.loads(x)['run_id']==failed.run_id for x in alerts.read_text().splitlines())
report=dict(verified=True,airflow_version=__import__('airflow').__version__,
            mode='dag.test with real Spark containers; scheduler execution is separate',
            target_date='2026-10-25',rows=3846,groups=len(before[0]),
            same_day_all_columns_unchanged=True,failed_quality_blocks_publish=True,
            transient_exception_retry_verified=True,retry_test_note='Injected AirflowException, test delay 1 second; production 1 minute',
            local_failure_alert_recorded=True,external_webhook_tested=False,runs=[a,b,c])
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
print('PASS Airflow same-day rerun and quality gate; evidence/week5/verification.json')
