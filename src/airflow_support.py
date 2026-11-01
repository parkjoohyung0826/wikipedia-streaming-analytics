"""Lightweight Airflow helpers; Spark and database I/O happen only inside tasks."""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import hashlib
import json
import logging
import os
from pathlib import Path
import urllib.request


def resolve_date(override, interval_end):
    if override:
        value=date.fromisoformat(override)
        if value.isoformat()!=override:raise ValueError('target_date must be YYYY-MM-DD')
        return override
    if interval_end is None:raise ValueError('data_interval_end is required when target_date is empty')
    return (interval_end.astimezone(ZoneInfo('Asia/Seoul')).date()-timedelta(days=1)).isoformat()


def work_path(run_id, target_date):
    token=hashlib.sha256(run_id.encode()).hexdigest()[:20]
    return 'artifacts/week5/runs/'+token+'/'+target_date


def notify_failure(context):
    ti=context['task_instance']
    alert=dict(dag_id=ti.dag_id,task_id=ti.task_id,run_id=ti.run_id,
               try_number=ti.try_number,at=datetime.now(timezone.utc).isoformat())
    # Always keep local evidence; webhooks are opt-in and never embedded in source.
    path=Path('/workspace/artifacts/week5/alerts.jsonl');path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as handle:handle.write(json.dumps(alert)+'\n')
    logging.error('[ALERT] %s',alert)
    webhook=os.environ.get('DISCORD_WEBHOOK_URL')
    if webhook:
        try:
            request=urllib.request.Request(webhook,data=json.dumps({'content':f"Airflow failure: {ti.dag_id}.{ti.task_id} (try {ti.try_number})"}).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(request,timeout=5) as response:response.read()
        except Exception:
            logging.error('Webhook delivery failed; local alert retained')


def run_spark(action,target_date,context):
    import docker
    from airflow.exceptions import AirflowException,AirflowFailException
    client=docker.from_env(timeout=30)
    run_id=context['run_id']
    work=work_path(run_id,target_date)
    environment={k:os.environ[k] for k in ('PGHOST','PGPORT','PGDATABASE','PGUSER','PGPASSWORD')}
    environment.update(PYTHONPATH='/workspace',HOME='/tmp',SPARK_LOCAL_IP='127.0.0.1',TZ='UTC')
    container=None
    try:
        container=client.containers.run(os.environ['WIKI_SPARK_IMAGE'],
            command=['driver','--master','local[2]','--driver-memory','1g','--conf','spark.jars.ivy=/tmp/week5-ivy',
                     'src/daily_jobs.py',action,'--date',target_date,'--work',work],
            entrypoint='/opt/entrypoint.sh',working_dir='/workspace',environment=environment,
            volumes={os.environ['WIKI_WORKSPACE_HOST']:{'bind':'/workspace','mode':'rw'}},
            network=os.environ['WIKI_DOCKER_NETWORK'],user=os.environ['WIKI_JOB_USER'],
            detach=True,labels={'wiki.week5.run':hashlib.sha256(run_id.encode()).hexdigest()[:20]})
        log=Path('/workspace')/work/(action+'.log');log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('wb') as handle:
            for line in container.logs(stream=True,follow=True):
                handle.write(line);handle.flush()
                print(line.decode(errors='replace'),end='',flush=True)
        status=container.wait(timeout=30)['StatusCode']
        if status==75:raise AirflowException('Temporary Spark/DB failure; retry with backoff')
        if status!=0:raise AirflowFailException(f'{action} failed with exit {status}; inspect task log')
        return work
    except docker.errors.APIError as exc:
        if exc.status_code and exc.status_code>=500:raise AirflowException('Docker temporarily unavailable') from exc
        raise AirflowFailException('Docker image/network/permission configuration error') from exc
    except docker.errors.DockerException as exc:
        raise AirflowException('Docker connection temporarily unavailable') from exc
    finally:
        if container is not None:
            try:container.remove(force=True)
            except docker.errors.DockerException:logging.error('Could not remove Spark task container %s',container.id)
        client.close()
