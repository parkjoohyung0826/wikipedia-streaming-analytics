"""Isolated three-minute step load, malformed-message fault, and measured report."""
import gzip
import shutil
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import secrets
import subprocess
import time
from week5 import ROOT,compose

def call(*args,log=None):
    if log:
        with (ROOT/log).open('w') as f:return subprocess.run(compose('-f','compose.operations.yml',*args),cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
    return subprocess.run(compose('-f','compose.operations.yml',*args),cwd=ROOT,check=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=180);p.add_argument('--rates',default='100,300,1000,3000')
    p.add_argument('--trigger',type=int,default=10);p.add_argument('--max-offsets',type=int,default=20000)
    a=p.parse_args()
    if min(a.trigger,a.max_offsets,a.seconds)<1:p.error('limits must be positive')
    ident=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+secrets.token_hex(3)
    topic='week6-'+ident.lower();db='week6_'+ident.lower();root='evidence/week6/'+ident
    (ROOT/root).mkdir(parents=True);(ROOT/'evidence/week6/latest.txt').write_text(root+'\n')
    call('up','-d','--wait','kafka','postgres')
    call('exec','-T','postgres','psql','-U','wiki','-d','wiki','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db)
    call('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--create','--topic',topic,'--partitions','3','--replication-factor','1','--config','retention.ms=86400000')
    name='wiki-load-'+ident.lower()
    (ROOT/root/'environment.json').write_text(json.dumps(dict(topic=topic,database=db,consumer=name,rates=a.rates,seconds=a.seconds,max_offsets=a.max_offsets,trigger_seconds=a.trigger,jdbc_partitions=4,checkpoint='artifacts/week6/'+ident+'/checkpoint'),indent=2))
    call('run','-d','--no-deps','--name',name,'-e','PGDATABASE='+db,'spark','src/monitored_stream.py','--topic',topic,'--root',root+'/stream','--trigger',str(a.trigger),'--max-offsets',str(a.max_offsets),'--checkpoint','artifacts/week6/'+ident+'/checkpoint')
    try:
        # Warm up query before timing input. It should emit the initial empty batch.
        deadline=time.monotonic()+180
        while not (ROOT/root/'stream/progress.jsonl').exists():
            if time.monotonic()>deadline:raise RuntimeError('Consumer startup timed out; inspect '+name)
            time.sleep(3)
        call('run','--rm','-T','--no-deps','producer','-m','src.load_generator','--topic',topic,'--root',root,'--rates',a.rates,'--seconds',str(a.seconds),log=root+'/producer.log')
        expected=json.loads((ROOT/root/'producer.json').read_text())['delivered']
        deadline=time.monotonic()+1800
        while time.monotonic()<deadline:
            path=ROOT/root/'stream/metrics.jsonl'
            if path.exists():
                metrics=[json.loads(x) for x in path.read_text().splitlines()]
                if metrics and metrics[-1]['lag']==0 and sum(metrics[-1]['end_offsets'].values())==expected:break
            time.sleep(10)
        else:raise RuntimeError('Backlog did not drain in 30 minutes; partial evidence retained')
        call('run','--rm','-T','--no-deps','-e','PGDATABASE='+db,'spark','scripts/report_week6.py','--root',root,'--topic',topic,log=root+'/report.log')
    finally:
        with (ROOT/root/'spark.log').open('w') as f:subprocess.run(['docker','logs',name],stdout=f,stderr=subprocess.STDOUT)
        subprocess.run(['docker','stop','-t','30',name],check=False)
        with (ROOT/root/'spark.log').open('rb') as source,gzip.open(ROOT/root/'spark.log.gz','wb') as target:
            shutil.copyfileobj(source,target)
        (ROOT/root/'spark.log').unlink()
    print('Results: '+root,flush=True)

if __name__=='__main__':main()
