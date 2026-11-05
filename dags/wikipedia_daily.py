"""Date-scoped Spark/S3 pipeline scheduled by Airflow 2.10.5."""
from datetime import timedelta
import pendulum
from airflow.decorators import dag,task
from airflow.exceptions import AirflowFailException
from src.airflow_support import resolve_date,notify_failure,run_spark


@dag(dag_id='wikipedia_daily',schedule='0 3 * * *',
     start_date=pendulum.datetime(2026,10,26,tz='Asia/Seoul'),
     catchup=False,max_active_runs=1,max_active_tasks=1,
     default_args=dict(pool="spark_jobs",retries=3,retry_delay=timedelta(minutes=1),retry_exponential_backoff=True,
                       max_retry_delay=timedelta(minutes=10),execution_timeout=timedelta(minutes=20),
                       on_failure_callback=notify_failure),
     params={'target_date':''},tags=['wikipedia','s3'],
     doc_md='매일 03:00 KST에 전날 KST 00:00~24:00를 처리합니다. 과거 데이터는 target_date로 지정합니다.')
def wikipedia_daily():
    @task(retries=0)
    def resolve_target_date(**context):
        try:value=resolve_date(context['params'].get('target_date'),context.get('data_interval_end'))
        except (ValueError,TypeError) as exc:raise AirflowFailException(str(exc)) from exc
        print('logical_date=',context.get('logical_date'),'data_interval_start=',context.get('data_interval_start'),
              'data_interval_end=',context.get('data_interval_end'),'target_date=',value)
        return value

    @task
    def drain_kafka(target_date,**context):return run_spark('stream',target_date,context)

    @task
    def compact_raw(target_date,**context):return run_spark('compact_raw',target_date,context)

    @task
    def daily_agg(target_date,**context):return run_spark('daily_agg',target_date,context)

    @task(retries=0)
    def quality_check(target_date,**context):return run_spark('quality_check',target_date,context)

    @task
    def publish(target_date,**context):return run_spark('publish',target_date,context)

    target=resolve_target_date()
    drain_kafka(target) >> compact_raw(target) >> daily_agg(target) >> quality_check(target) >> publish(target)


dag=wikipedia_daily()
