"""Rate-controlled replay with fresh IDs; never send load traffic to the public API."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import time
from confluent_kafka import Producer
from src.producer import normalize,now

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--topic',required=True);p.add_argument('--root',required=True)
    p.add_argument('--rates',default='100,300,1000,3000');p.add_argument('--seconds',type=int,default=180)
    a=p.parse_args();rates=[int(x) for x in a.rates.split(',')]
    if a.seconds<1 or any(x<1 for x in rates):p.error('rates and seconds must be positive')
    if not a.topic.startswith('week6-'):p.error('load requires an isolated week6- topic')
    root=Path(a.root);root.mkdir(parents=True,exist_ok=True)
    templates=[normalize(json.loads(line)) for line in Path('samples/wikipedia_edits.jsonl').read_text().splitlines()]
    templates=[x for x in templates if x]
    producer=Producer({'bootstrap.servers':'kafka:29092','enable.idempotence':True,'acks':'all',
                       'compression.type':'lz4','linger.ms':5,'delivery.timeout.ms':120000})
    delivered=0;errors=[];sequence=0;stages=[]
    def ack(error,msg):
        nonlocal delivered
        if error:errors.append(str(error))
        else:delivered+=1
    # Broken JSON burst and normal traffic after it prove the error path does not halt the stream.
    plans=[(str(rate),rate,a.seconds,False) for rate in rates]+[('malformed',1000,10,True),('after_failure',100,10,False)]
    for name,rate,seconds,bad in plans:
        stage=dict(name=name,requested_rps=rate,seconds=seconds,started_at=now(),queued=0,invalid=0)
        (root/'current_stage.json').write_text(json.dumps(stage))
        start=time.monotonic();deadline=start+seconds
        while time.monotonic()<deadline:
            due=start+stage['queued']/rate
            if time.monotonic()<due:
                producer.poll(max(0,min(0.01,due-time.monotonic())));continue
            key,template=templates[sequence%len(templates)]
            event=dict(template);event['event_id']=f'{a.topic}:{sequence}'
            event['ingest_time']=now();event['source']='week6-replay'
            invalid=bad and sequence%2==0
            value='{BROKEN JSON' if invalid else json.dumps(event,separators=(',',':'))
            while True:
                try:producer.produce(a.topic,key=key,value=value,on_delivery=ack);break
                except BufferError:producer.poll(0.1)
            sequence+=1;stage['queued']+=1;stage['invalid']+=int(invalid);producer.poll(0)
        stage.update(finished_at=now(),elapsed=time.monotonic()-start)
        stage['actual_rps']=stage['queued']/stage['elapsed'];stages.append(stage)
        (root/'producer.json').write_text(json.dumps(dict(stages=stages,delivered=delivered,errors=errors),indent=2))
        print(json.dumps(stage),flush=True)
    remaining=producer.flush(130)
    result=dict(stages=stages,queued=sequence,delivered=delivered,errors=errors,remaining=remaining,
                success=not errors and remaining==0 and delivered==sequence)
    (root/'producer.json').write_text(json.dumps(result,indent=2))
    if not result['success']:raise SystemExit('Some messages were not acknowledged')

if __name__=='__main__':main()
