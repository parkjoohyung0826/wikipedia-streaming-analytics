"""Operate Kafka + Spark + Airflow; persist analytical data in S3."""
import argparse
import json
import os
import subprocess
from week5 import ROOT, ensure_env


def call(*args):
    subprocess.run(['docker','compose','--env-file','.env.week5','-f','compose.s3.yml',*args],cwd=ROOT,check=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['start','setup','status','stop','sleep','trigger']);p.add_argument('--date');a=p.parse_args()
    ensure_env()
    if not os.getenv('WIKI_AWS_HOST') or not os.getenv('WIKI_DOCKER_SOCKET'):
        p.error('Set WIKI_AWS_HOST and WIKI_DOCKER_SOCKET after reviewing container access in docs/guides/kafka_spark_s3.md')
    config=json.loads((ROOT/'config/s3.local.json').read_text())
    os.environ['AWS_PROFILE']=config.get('profile') or 'default'
    os.environ['WIKI_WORKSPACE_HOST']=str(ROOT)
    if a.action in ('start','setup'):
        call('build','producer','spark','airflow-scheduler')
        call('up','-d','--wait','kafka','airflow-db')
        call('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--create','--if-not-exists','--topic','wikipedia-live','--partitions','3','--replication-factor','1','--config','retention.ms=86400000')
        call('run','--rm','-T','airflow-init')
        call('up','-d','airflow-scheduler','airflow-webserver')
        if a.action=='start':
            call('up','-d','live-spark','live-producer')
            call('exec','-T','airflow-scheduler','airflow','dags','unpause','wikipedia_daily')
    elif a.action=='status':call('ps')
    elif a.action=='trigger':
        if not a.date:p.error('--date YYYY-MM-DD required')
        call('exec','-T','airflow-scheduler','airflow','dags','trigger','wikipedia_daily','--conf',json.dumps({'target_date':a.date}))
    else:
        call('stop','live-producer')
        call('stop','live-spark','airflow-scheduler','airflow-webserver')
        call('stop','kafka','airflow-db')

if __name__=='__main__':main()
