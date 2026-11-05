"""Run Spark only when draining Kafka; stop after available offsets are committed."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag,task
from src.airflow_support import run_spark,notify_failure

@dag(dag_id='wikipedia_ingest',schedule='*/10 * * * *',
     start_date=pendulum.datetime(2026,11,5,tz='Asia/Seoul'),catchup=False,
     max_active_runs=1,max_active_tasks=1,
     default_args=dict(pool='spark_jobs',retries=3,retry_delay=timedelta(minutes=1),
                       retry_exponential_backoff=True,max_retry_delay=timedelta(minutes=10),
                       execution_timeout=timedelta(minutes=20),on_failure_callback=notify_failure),
     tags=['wikipedia','s3'])
def wikipedia_ingest():
    @task
    def drain_kafka(**context):
        return run_spark('stream',context['data_interval_end'].date().isoformat(),context)
    drain_kafka()

dag=wikipedia_ingest()
