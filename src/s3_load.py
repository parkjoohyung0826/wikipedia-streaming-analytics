"""Bounded local replay -> real S3 batch sink; no Kafka, Spark, DB, or public API load."""
from array import array
from datetime import datetime,timezone
import json
from pathlib import Path
import queue
import shutil
import threading
import time
from src.s3_batch import Aws,prepare,publish,dump


def now():return datetime.now(timezone.utc).isoformat()

def percentile(values,q):
    if not values:return None
    values=sorted(values);position=(len(values)-1)*q;low=int(position);high=min(low+1,len(values)-1)
    return values[low]+(values[high]-values[low])*(position-low)


def run(root,config,rates,seconds,chunk_size=20000,flush_seconds=10,inject_failure=True):
    root=Path(root);root.mkdir(parents=True,exist_ok=False)
    evidence=root/'evidence';evidence.mkdir()
    aws=Aws(config['region'],config.get('profile'))
    config=dict(config,prefix='week6-s3/'+root.name)
    if aws.call('sts','get-caller-identity')['Account']!=config['account_id']:raise ValueError('AWS account mismatch')
    templates=[json.loads(line) for line in Path('samples/wikipedia_edits.jsonl').read_text().splitlines()]
    templates=[x for x in templates if x.get('type')=='edit']
    inbox=queue.Queue(maxsize=8);errors=[];stop=threading.Event();done=threading.Event()
    state=dict(produced=0,committed=0,rejected=0,selected=0,batches=0)
    phases=[];latencies={};batches=[];samples=[];failure_injected=[False]
    lock=threading.Lock()
    def append(name,value):
        with lock,(evidence/name).open('a') as f:f.write(json.dumps(value)+'\n')
    class FaultClient:
        def call(self,*args,**kwargs):
            if inject_failure and not failure_injected[0] and args[1]=='put-object' and args[args.index('--key')+1].endswith('/edits.jsonl.gz'):
                failure_injected[0]=True
                raise RuntimeError('Injected temporary upload exception AFTER raw object upload')
            return aws.call(*args,**kwargs)
    client=FaultClient()
    def worker():
        try:
            while True:
                item=inbox.get()
                if item is None:return
                folder,ledger=item
                begin=time.monotonic();summary=prepare(folder/'input.jsonl',folder/'prepared')
                prep=time.monotonic()-begin
                assert summary['counts']['selected']+summary['counts']['rejected']==len(ledger)
                assert summary['counts']['rejected']==sum(x[2] for x in ledger)
                upload_start=time.monotonic()
                for attempt in range(4):
                    try:
                        receipt=publish(client,config,folder/'prepared',summary);break
                    except RuntimeError as exc:
                        transient=any(word in str(exc) for word in ('Injected temporary','SlowDown','InternalError','ServiceUnavailable','RequestTimeout','Could not connect','timed out'))
                        append('alerts.jsonl',dict(at=now(),kind='upload_error',attempt=attempt+1,error=str(exc),retry=transient and attempt<3))
                        if not transient or attempt==3:raise
                        time.sleep(2**attempt)
                finished=time.monotonic()
                for stamp,phase,invalid in ledger:
                    if not invalid:latencies.setdefault(phase,array('d')).append(finished-stamp)
                state['committed']+=len(ledger);state['selected']+=summary['counts']['selected'];state['rejected']+=summary['counts']['rejected'];state['batches']+=1
                record=dict(at=now(),monotonic=finished,rows=len(ledger),selected=summary['counts']['selected'],rejected=summary['counts']['rejected'],
                    prepare_seconds=prep,upload_verify_seconds=finished-upload_start,total_seconds=finished-begin,
                    manifest=receipt['manifest'],batch_id=summary['batch_id'],counts=summary['counts'])
                batches.append(record);append('batches.jsonl',record)
                # Only this verified chunk is removed; on failure the spool stays available.
                shutil.rmtree(folder)
        except BaseException as exc:
            errors.append(str(exc));stop.set()
    def monitor():
        while not done.is_set():
            record=dict(at=now(),monotonic=time.monotonic(),**state,backlog=state['produced']-state['committed'])
            samples.append(record);append('metrics.jsonl',record)
            if record['backlog']>40000:append('alerts.jsonl',dict(**record,kind='backlog_threshold'))
            done.wait(2)
    consumer=threading.Thread(target=worker,daemon=True);monitor_thread=threading.Thread(target=monitor,daemon=True)
    consumer.start();monitor_thread.start()
    sequence=0;chunk_number=0;handle=None;ledger=[];chunk_started=0
    def check():
        if errors:raise RuntimeError(errors[0])
    def send(item):
        while True:
            check()
            try:inbox.put(item,timeout=1);return
            except queue.Full:pass
    def flush():
        nonlocal handle,ledger
        if handle:
            handle.close();send((handle_path.parent,ledger));handle=None;ledger=[]
    plans=[(str(rate),rate,seconds,False) for rate in rates]+[('malformed',1000,10,True),('after_failure',100,10,False)]
    try:
        for name,rate,duration,bad in plans:
            phase=dict(name=name,rate=rate,seconds=duration,started_at=now(),start_monotonic=time.monotonic(),produced=0,invalid=0)
            dump(evidence/'current_phase.json',phase)
            end=phase['start_monotonic']+duration
            while time.monotonic()<end:
                check();due=phase['start_monotonic']+phase['produced']/rate
                delay=due-time.monotonic()
                if delay>0:time.sleep(min(delay,0.01));continue
                if handle is None:
                    folder=root/'spool'/f'{chunk_number:06d}';folder.mkdir(parents=True);chunk_number+=1
                    handle_path=folder/'input.jsonl';handle=handle_path.open('w');chunk_started=time.monotonic()
                stamp=time.monotonic();invalid=bad and sequence%2==0
                event=dict(templates[sequence%len(templates)])
                event['meta']=dict(event.get('meta',{}),id=f'{root.name}:{sequence}')
                event['produced_at']=now()
                handle.write(('{BROKEN JSON' if invalid else json.dumps(event,ensure_ascii=False,separators=(',',':')))+'\n')
                ledger.append((stamp,name,invalid));sequence+=1;phase['produced']+=1;phase['invalid']+=int(invalid);state['produced']+=1
                if len(ledger)>=chunk_size or stamp-chunk_started>=flush_seconds:flush()
            phase.update(finished_at=now(),end_monotonic=time.monotonic())
            phase['actual_rps']=phase['produced']/(phase['end_monotonic']-phase['start_monotonic'])
            phases.append(phase);append('phases.jsonl',phase)
        flush();input_finished=time.monotonic();send(None)
        consumer.join(timeout=1800)
        if consumer.is_alive():raise TimeoutError('S3 backlog did not drain in 30 minutes; spool retained')
        check()
        recovery=time.monotonic()-input_finished
    finally:
        if handle:handle.close()
        done.set();monitor_thread.join(timeout=3)
    assert state['produced']==state['committed']==state['selected']+state['rejected']
    assert state['rejected']==sum(x['invalid'] for x in phases)
    rows=[]
    for phase in phases:
        start,end=phase['start_monotonic'],phase['end_monotonic']
        bs=[b for b in batches if start<=b['monotonic']<end]
        ms=[m for m in samples if start<=m['monotonic']<end]
        ls=latencies.get(phase['name'],array('d'))
        assert len(ls)==phase['produced']-phase['invalid']
        row=dict(**phase,saved_valid=len(ls),saved_invalid=phase['invalid'],p50_seconds=percentile(ls,0.5),p95_seconds=percentile(ls,0.95),
            completed_rps=sum(b['rows'] for b in bs)/(end-start),batch_seconds_median=percentile([b['total_seconds'] for b in bs],0.5),
            prepare_seconds_median=percentile([b['prepare_seconds'] for b in bs],0.5),upload_seconds_median=percentile([b['upload_verify_seconds'] for b in bs],0.5),
            backlog_start=ms[0]['backlog'] if ms else None,backlog_end=ms[-1]['backlog'] if ms else None,
            backlog_max=max([m['backlog'] for m in ms],default=0))
        rows.append(row)
        if row['p95_seconds'] and row['p95_seconds']>30:append('alerts.jsonl',dict(at=now(),kind='phase_p95_threshold',phase=phase['name'],p95=row['p95_seconds']))
    report=dict(verified=True,config=dict(rates=rates,seconds=seconds,chunk_size=chunk_size,flush_seconds=flush_seconds,queue_capacity=8),
        bucket=config['bucket'],s3_prefix=config['prefix'],phases=rows,totals=state,final_backlog=0,recovery_seconds=recovery,
        injected_upload_exception_recovered=failure_injected[0],broken_messages_preserved=True,all_stage_counts_match=True,
        measurement='Latency: locally recorded production -> real S3 manifest upload AND checksum verification completed. Backlog includes open buffer, waiting and uploading records; not Kafka lag.')
    dump(evidence/'report.json',report)
    return evidence
