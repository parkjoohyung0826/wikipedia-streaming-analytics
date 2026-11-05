"""Lightweight Airflow helpers; Spark and S3 I/O happen only inside tasks."""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import hashlib
import json
import logging
import os
from pathlib import Path


def resolve_date(override, interval_end):
    if override:
        value=date.fromisoformat(override)
        if value.isoformat()!=override:raise ValueError('target_date must be YYYY-MM-DD')
        return override
    if interval_end is None:raise ValueError('data_interval_end is required when target_date is empty')
    return (interval_end.astimezone(ZoneInfo('Asia/Seoul')).date()-timedelta(days=1)).isoformat()


def work_path(run_id, target_date):
    token=hashlib.sha256(run_id.encode()).hexdigest()[:20]
    return 'artifacts/airflow/runs/'+token+'/'+target_date


def notify_failure(context):
    ti=context['task_instance']
    alert=dict(dag_id=ti.dag_id,task_id=ti.task_id,run_id=ti.run_id,
               try_number=ti.try_number,at=datetime.now(timezone.utc).isoformat())
    # Keep an inspectable local failure record.
    path=Path('/workspace/artifacts/airflow/alerts.jsonl');path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as handle:handle.write(json.dumps(alert)+'\n')
    logging.error('[ALERT] %s',alert)


def run_spark(action,target_date,context):
    import docker
    from airflow.exceptions import AirflowException,AirflowFailException
    client=docker.from_env(timeout=30)
    run_id=context['run_id']
    work=work_path(run_id,target_date)
    environment={}
    volumes={os.environ['WIKI_WORKSPACE_HOST']:{'bind':'/workspace','mode':'rw'}}
    volumes[os.environ['WIKI_AWS_HOST']]={'bind':'/home/spark/.aws','mode':'ro'}
    environment.update(AWS_SHARED_CREDENTIALS_FILE='/home/spark/.aws/credentials',AWS_CONFIG_FILE='/home/spark/.aws/config',AWS_PROFILE=os.getenv('AWS_PROFILE','default'),AWS_CREDENTIAL_PROFILES_FILE='/home/spark/.aws/credentials')
    environment.update(PYTHONPATH='/workspace',HOME='/tmp',SPARK_LOCAL_IP='127.0.0.1',TZ='UTC')
    container=None
    try:
        container=client.containers.run(os.environ['WIKI_SPARK_IMAGE'],
            command=['driver','--master','local[2]','--driver-memory','1g','--conf','spark.jars.ivy=/tmp/spark-ivy',
                     'src/spark_s3.py',action,'--date',target_date,'--work',work]+(['--once'] if action=='stream' else []),
            entrypoint='/opt/entrypoint.sh',working_dir='/workspace',environment=environment,
            volumes=volumes,
            network=os.environ['WIKI_DOCKER_NETWORK'],user=os.environ['WIKI_JOB_USER'],
            detach=True,labels={'wiki.spark.run':hashlib.sha256(run_id.encode()).hexdigest()[:20]})
        log=Path('/workspace')/work/(action+'.log');log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('wb') as handle:
            for line in container.logs(stream=True,follow=True):
                handle.write(line);handle.flush()
                print(line.decode(errors='replace'),end='',flush=True)
        status=container.wait(timeout=30)['StatusCode']
        if status==75:raise AirflowException('Temporary Spark/S3 failure; retry with backoff')
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
