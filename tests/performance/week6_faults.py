"""Additional isolated broker, bad-input, sink and date-task recovery cases."""
import json
import threading
import time


def exercise(h,broker):
    directory,topic,cfg=h.prepare('extended')
    total=h.produce(directory,topic,'seed',100,count=100)
    h.wait(h.spark(directory,topic,once=True))
    h.wait(h.spark(directory,topic,'daily'))
    previous=h.pointer(cfg)
    # Real broker suspension, while an idempotent producer and Spark are active.
    prior=set(directory.glob('*.progress.jsonl'))
    c=h.spark(directory,topic)
    deadline=time.time()+180
    while not set(directory.glob('*.progress.jsonl'))-prior:
        c.reload()
        if c.status!='running':raise RuntimeError(c.logs(tail=20).decode(errors='replace'))
        if time.time()>deadline:raise TimeoutError('Streaming query did not initialize')
        time.sleep(1)
    failure=[];sent=[]
    def send():
        try:sent.append(h.produce(directory,topic,'broker-outage',100,count=3000))
        except BaseException as exc:failure.append(exc)
    broker.pause();paused=time.time()
    worker=threading.Thread(target=send);worker.start()
    try:time.sleep(60)
    finally:broker.unpause()
    restored=time.time();worker.join(240)
    if worker.is_alive():raise TimeoutError('Producer still blocked after broker recovery')
    if failure:raise failure[0]
    total+=sent[0];h.drain(directory,topic,total,c);recovered=time.time()
    c.stop(timeout=30);c.remove();h.active.remove(c)
    h.wait(h.spark(directory,topic,'verify'))
    broker_check=json.loads((directory/'verification.json').read_text())
    assert broker_check['rows']==total
    result={'kafka':{'kind':'real isolated broker pause','outage_s':restored-paused,
        'recovery_to_zero_lag_s':recovered-restored,'input_rows':total,
        'missing_ids':broker_check['missing_ids'],'duplicate_offsets':total-broker_check['distinct_kafka_offsets']}}
    (directory/'faults.json').write_text(json.dumps(result,indent=2)+'\n')
    # A high invalid ratio must not stop valid records. Invalid rows are the DLQ equivalent.
    total+=h.produce(directory,topic,'invalid-storm',500,count=1000,invalid=1000)
    h.wait(h.spark(directory,topic,once=True));h.wait(h.spark(directory,topic,'verify'))
    check=json.loads((directory/'verification.json').read_text())
    rejected=sum(r['rejected'] or 0 for r in check['stages'])
    assert check['rows']==total and rejected==1000
    result['invalid_storm']={'input_rows':2000,'valid_rows':1000,'rejected_rows':1000,
        'stored_total':check['rows'],'missing_ids':check['missing_ids']}
    (directory/'faults.json').write_text(json.dumps(result,indent=2)+'\n')
    # Inject a retryable S3 service error at the commit boundary, after actual Parquet writes.
    total+=h.produce(directory,topic,'sink-outage',100,count=500)
    c=h.spark(directory,topic,'sink-outage')
    status=c.wait(timeout=360)['StatusCode']
    c.remove();h.active.remove(c)
    assert status==75, f'Expected transient exit 75, got {status}'
    assert h.pointer(cfg)==previous
    restored=time.time();h.wait(h.spark(directory,topic,once=True));recovered=time.time()
    h.wait(h.spark(directory,topic,'verify'))
    check=json.loads((directory/'verification.json').read_text())
    assert check['rows']==total
    result['sink']={'kind':'injected S3 ServiceUnavailable at commit; real S3 remains online',
        'injected_seconds':60,'exit_code':status,'recovery_s':recovered-restored,
        'missing_ids':check['missing_ids'],'duplicate_offsets':total-check['distinct_kafka_offsets'],
        'previous_publication_preserved':True}
    (directory/'faults.json').write_text(json.dumps(result,indent=2)+'\n')
    # Same date/work ID retry checks exact row/aggregate equality and publication protection.
    h.wait(h.spark(directory,topic,'daily-retry'))
    result['batch_retry']=json.loads((directory/'daily-retry.json').read_text())
    (directory/'faults.json').write_text(json.dumps(result,indent=2)+'\n')
    import os
    from pathlib import Path
    inside='/workspace/'+str(directory.relative_to(h.ROOT))
    airflow=h.DOCKER.containers.run('wikipedia-streaming-analytics-airflow:2.10.5',
        command=['python','tests/performance/week6_airflow.py'],entrypoint='',working_dir='/workspace',
        user=f'{os.getuid()}:0',mem_limit='1g',detach=True,network=h.NETWORK,
        environment={'USER':'airflow','LOGNAME':'airflow','HOME':'/tmp','PYTHONPATH':'/workspace','AIRFLOW_HOME':'/tmp/week6-airflow',
            'AIRFLOW__CORE__LOAD_EXAMPLES':'false','AIRFLOW__CORE__EXECUTOR':'SequentialExecutor',
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN':'sqlite:////tmp/week6-airflow/airflow.db',
            'WIKI_TEST_ROOT':inside,'WIKI_TEST_DATE':h.DATE,'WIKI_S3_CONFIG':inside+'/config.json',
            'WIKI_DOCKER_NETWORK':h.NETWORK,'WIKI_WORKSPACE_HOST':str(h.ROOT),
            'WIKI_AWS_HOST':str(Path.home()/'.aws'),'WIKI_JOB_USER':f'{os.getuid()}:{os.getgid()}',
            'AWS_PROFILE':h.CONFIG.get('profile') or 'default'},
        volumes={str(h.ROOT):{'bind':'/workspace','mode':'rw'},
            '/var/run/docker.sock':{'bind':'/var/run/docker.sock','mode':'rw'}})
    h.active.append(airflow)
    try:
        status=airflow.wait(timeout=900)['StatusCode']
        (directory/'airflow.log').write_bytes(airflow.logs())
        assert status==0, 'Airflow retry test failed; inspect airflow.log'
    finally:airflow.remove(force=True);h.active.remove(airflow)
    result['airflow_retry']=json.loads((directory/'airflow-retry.json').read_text())
    (directory/'faults.json').write_text(json.dumps(result,indent=2)+'\n')
    h.log('PASS extended fault scenarios')
