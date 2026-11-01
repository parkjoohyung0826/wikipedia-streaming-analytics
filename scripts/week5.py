"""Local Airflow runner. Uses only this project's Compose services."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]


def compose(*args):
    return ['docker','compose','--env-file','.env.week5','-f','docker-compose.yml',
            '-f','compose.week4.yml','-f','compose.week5.yml',*args]


def call(*args):
    subprocess.run(compose(*args),cwd=ROOT,check=True)


def setup():
    path=ROOT/'.env.week5'
    if not path.exists():
        path.write_text('WIKI_WORKSPACE_HOST='+json.dumps(str(ROOT),ensure_ascii=False)+'\n'
                        +'LAB_UID='+str(os.getuid() if hasattr(os,'getuid') else 50000)+'\n'
                        +'LAB_GID='+str(os.getgid() if hasattr(os,'getgid') else 0)+'\n'
                        +'WIKI_AIRFLOW_SECRET='+secrets.token_hex(32)+'\n')
        path.chmod(0o600)
    call('build','spark','airflow-scheduler')
    call('up','-d','--wait','postgres','airflow-db')
    call('run','--rm','-T','airflow-init')
    call('up','-d','--wait','airflow-scheduler','airflow-webserver')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['setup','trigger','verify','status','stop'])
    p.add_argument('--date',default='2026-10-25')
    args=p.parse_args()
    if args.action=='setup':setup()
    elif args.action=='trigger':
        call('exec','-T','airflow-scheduler','airflow','dags','unpause','wikipedia_daily')
        call('exec','-T','airflow-scheduler','airflow','dags','trigger','wikipedia_daily',
             '--conf',json.dumps({'target_date':args.date}))
    elif args.action=='verify':
        call('exec','-T','airflow-scheduler','python','/workspace/tests/integration/test_week5.py')
    elif args.action=='status':call('ps')
    elif args.action=='stop':call('stop','airflow-scheduler','airflow-webserver','airflow-db')


if __name__=='__main__':main()
