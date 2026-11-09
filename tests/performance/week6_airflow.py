"""Exercise real Airflow TaskInstance retries in an isolated DAG.test/SQLite run."""
import json
import os
from datetime import timedelta
from pathlib import Path
import docker
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException,AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils import db

root=Path(os.environ['WIKI_TEST_ROOT'])

def run(**context):
    client=docker.from_env();container=None
    env={key:os.environ[key] for key in ('WIKI_TEST_ROOT','WIKI_TEST_DATE','WIKI_S3_CONFIG','WIKI_DOCKER_NETWORK')}
    env.update(PYTHONPATH='/workspace',HOME='/tmp',SPARK_LOCAL_IP='127.0.0.1',
        AWS_SHARED_CREDENTIALS_FILE='/home/spark/.aws/credentials',AWS_CONFIG_FILE='/home/spark/.aws/config',
        AWS_CREDENTIAL_PROFILES_FILE='/home/spark/.aws/credentials',AWS_PROFILE=os.environ.get('AWS_PROFILE','default'),
        WIKI_TEST_MODE='daily-airflow',WIKI_TEST_WORK='week6-airflow-retry')
    try:
        container=client.containers.run('wikipedia-streaming-analytics-spark:latest',
            command=['driver','--master','local[2]','--driver-memory','1g','--conf','spark.jars.ivy=/tmp/spark-ivy','tests/performance/week6_spark.py'],
            entrypoint='/opt/entrypoint.sh',working_dir='/workspace',environment=env,
            volumes={os.environ['WIKI_WORKSPACE_HOST']:{'bind':'/workspace','mode':'rw'},
                os.environ['WIKI_AWS_HOST']:{'bind':'/home/spark/.aws','mode':'ro'}},
            user=os.environ['WIKI_JOB_USER'],network=os.environ['WIKI_DOCKER_NETWORK'],mem_limit='2g',detach=True,
            labels={'wiki.week6.test':'airflow-retry'})
        with (root/f'airflow-try-{context["ti"].try_number}.log').open('wb') as handle:
            for line in container.logs(stream=True,follow=True):handle.write(line)
        status=container.wait(timeout=60)['StatusCode']
        with (root/'airflow-attempts.jsonl').open('a') as handle:
            handle.write(json.dumps({'try_number':context['ti'].try_number,'exit_code':status})+'\n')
        if status==75:raise AirflowException('Injected transient S3 error; retry')
        if status:raise AirflowFailException(f'Unexpected Spark exit {status}')
    finally:
        if container is not None:container.remove(force=True)
        client.close()

if __name__=='__main__':
    db.initdb()
    with DAG('week6_isolated_retry',start_date=pendulum.datetime(2026,1,1,tz='UTC'),schedule=None,catchup=False) as dag:
        PythonOperator(task_id='daily_with_transient_failure',python_callable=run,
            retries=1,retry_delay=timedelta(seconds=2))
    result=dag.test()
    attempts=[json.loads(line) for line in (root/'airflow-attempts.jsonl').read_text().splitlines()]
    assert str(result.state)=='success',result.state
    assert [a['exit_code'] for a in attempts]==[75,0],attempts
    (root/'airflow-retry.json').write_text(json.dumps({'state':str(result.state),'attempts':attempts,
        'execution':'Airflow 2.10.5 DAG.test with TaskInstance retries; isolated SQLite, no production scheduler'},indent=2)+'\n')
