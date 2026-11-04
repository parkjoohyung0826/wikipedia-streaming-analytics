"""Continuous SSE -> acknowledged Kafka delivery -> durable resume cursor."""
import json
import os
from pathlib import Path
import time
import urllib.request
from datetime import datetime, timezone
from confluent_kafka import Producer
from src.producer import normalize, now

URL='https://stream.wikimedia.org/v2/stream/recentchange'

def frames(lines):
    fields={};data=[]
    for raw in lines:
        line=raw.decode('utf-8').rstrip('\r\n')
        if not line:
            if data:yield fields.get('event','message'),fields.get('id'),'\n'.join(data)
            fields={};data=[]
        elif not line.startswith(':'):
            key,_,value=line.partition(':');value=value.removeprefix(' ')
            if key=='data':data.append(value)
            else:fields[key]=value

def save(path,value):
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value)+'\n');temp.replace(path)

def main():
    root=Path('artifacts/live');root.mkdir(parents=True,exist_ok=True)
    cursor=root/'cursor.json'
    state=json.loads(cursor.read_text()) if cursor.exists() else {}
    producer=Producer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','kafka:29092'),
        'enable.idempotence':True,'acks':'all','compression.type':'lz4','delivery.timeout.ms':120000})
    topic=os.getenv('KAFKA_TOPIC','wikipedia-live');delay=1
    while True:
        try:
            headers={'Accept':'text/event-stream','User-Agent':'WikipediaStreamingAnalytics/1.0 (educational personal project)'}
            if state.get('last_event_id'):headers['Last-Event-ID']=state['last_event_id']
            with urllib.request.urlopen(urllib.request.Request(URL,headers=headers),timeout=90) as response:
                for kind,ident,payload in frames(response):
                    if kind!='message':continue
                    try:
                        value=normalize(json.loads(payload))
                    except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as exc:
                        with (root/'rejected_source.jsonl').open('a') as f:
                            f.write(json.dumps({'at':now(),'error':str(exc),'raw':payload})+'\n')
                        value=None
                    if value:
                        key,event=value;event['source']='wikimedia-recentchange-live'
                        # Keep selected original messages for audit; duplicates are possible after a crash.
                        if os.getenv('WIKI_STORAGE')!='s3':
                            archive=Path('data/live')/datetime.now(timezone.utc).strftime('%Y-%m-%d')
                            archive.mkdir(parents=True,exist_ok=True)
                            with (archive/'events.jsonl').open('a') as f:f.write(payload+'\n')
                        outcome=[]
                        producer.produce(topic,key=key,value=json.dumps(event),on_delivery=lambda error,msg:outcome.append(error))
                        pending=producer.flush(125)
                        if pending or not outcome or outcome[0]:raise RuntimeError('Kafka delivery not confirmed')
                        state['delivered']=state.get('delivered',0)+1
                        state['last_delivered_at']=now()
                    # Never advance beyond a selected message until Kafka acknowledges it.
                    if ident:state['last_event_id']=ident
                    state['heartbeat_at']=now();save(cursor,state);delay=1
            raise ConnectionError('SSE connection ended')
        except Exception as exc:
            with (root/'alerts.jsonl').open('a') as f:f.write(json.dumps({'at':now(),'error':str(exc),'retry_seconds':delay})+'\n')
            print(f'Reconnecting in {delay}s: {exc}',flush=True)
            time.sleep(delay);delay=min(delay*2,60)

if __name__=='__main__':main()
