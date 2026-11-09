"""Isolated real Kafka/Spark/S3 load and fault recovery experiments.

Run from the repository root. Leaves S3 evidence and removes only test containers.
"""
import argparse
import sys
import json
import os
import threading
import time
from datetime import datetime,timezone
from pathlib import Path
import boto3
import docker
from confluent_kafka import Producer,Consumer,TopicPartition
from confluent_kafka.admin import AdminClient,NewTopic

def initialize():
    global ROOT,ARGS,RUN,NETWORK,BROKER,BOOTSTRAP,OUT,CONFIG,S3,DOCKER,DATE,active
    ROOT=Path(__file__).resolve().parents[2]
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rates',type=int,nargs='+',default=[100,300,600])
    parser.add_argument('--seconds',type=int,default=120)
    parser.add_argument('--warmup',type=int,default=120)
    parser.add_argument('--repetitions',type=int,default=2)
    parser.add_argument('--max-offsets',type=int,default=10000)
    parser.add_argument('--skip-faults',action='store_true')
    parser.add_argument('--extended-only',action='store_true')
    parser.add_argument('--crash-only',action='store_true')
    ARGS=parser.parse_args()
    if min(*ARGS.rates,ARGS.seconds,ARGS.warmup,ARGS.repetitions,ARGS.max_offsets)<=0:
        parser.error('all numeric parameters must be positive')
    RUN=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    NETWORK='week6-'+RUN.lower()
    BROKER=NETWORK+'-kafka'
    BOOTSTRAP='127.0.0.1:19092'
    OUT=ROOT/'artifacts'/'week6'/RUN
    OUT.mkdir(parents=True)
    CONFIG=json.loads((ROOT/'config/s3.local.json').read_text())
    S3=boto3.Session(profile_name=CONFIG.get('profile'),region_name=CONFIG['region']).client('s3')
    DOCKER=docker.DockerClient(base_url=os.getenv('DOCKER_HOST','unix://'+str(Path.home()/'.docker/run/docker.sock')))
    DOCKER.ping()
    DATE=datetime.now(timezone.utc).astimezone(__import__('zoneinfo').ZoneInfo('Asia/Seoul')).date().isoformat()
    active=[]

def log(message):
    print(datetime.now(timezone.utc).isoformat(),message,flush=True)

def prepare(name):
    directory=OUT/name;directory.mkdir()
    topic='week6-'+RUN.lower()+'-'+name
    cfg=dict(CONFIG,prefix=CONFIG['prefix'].rstrip('/')+'/validation/'+RUN+'/'+name)
    (directory/'config.json').write_text(json.dumps(cfg))
    admin=AdminClient({'bootstrap.servers':BOOTSTRAP})
    for future in admin.create_topics([NewTopic(topic,3,1,config={'retention.ms':'86400000'})]).values():future.result(30)
    return directory,topic,cfg

def spark(directory,topic,mode='stream',once=False):
    relative=str(directory.relative_to(ROOT));inside='/workspace/'+relative
    env={'PYTHONPATH':'/workspace','HOME':'/tmp','SPARK_LOCAL_IP':'127.0.0.1',
         'AWS_SHARED_CREDENTIALS_FILE':'/home/spark/.aws/credentials','AWS_CONFIG_FILE':'/home/spark/.aws/config',
         'AWS_CREDENTIAL_PROFILES_FILE':'/home/spark/.aws/credentials','AWS_PROFILE':CONFIG.get('profile') or 'default',
         'WIKI_KAFKA_BOOTSTRAP':BROKER+':29092','WIKI_MAX_OFFSETS':str(ARGS.max_offsets),
         'WIKI_S3_CONFIG':inside+'/config.json','WIKI_KAFKA_TOPIC':topic,
         'WIKI_TEST_ROOT':inside,'WIKI_TEST_MODE':mode,'WIKI_TEST_DATE':DATE,'WIKI_TEST_WORK':RUN+'-'+str(time.time_ns())}
    label=mode+'-'+str(time.time_ns())
    env['WIKI_STREAM_METRICS']=inside+'/'+label+'.metrics.jsonl'
    args=['src/spark_s3.py','stream']+(['--once'] if once else []) if mode=='stream' else ['tests/performance/week6_spark.py']
    c=DOCKER.containers.run('wikipedia-streaming-analytics-spark:latest',
        command=['driver','--master','local[2]','--driver-memory','1g','--conf','spark.jars.ivy=/tmp/spark-ivy']+args,
        entrypoint='/opt/entrypoint.sh',working_dir='/workspace',environment=env,
        mem_limit='2g',volumes={str(ROOT):{'bind':'/workspace','mode':'rw'},str(Path.home()/'.aws'):{'bind':'/home/spark/.aws','mode':'ro'}},
        network=NETWORK,user=f'{os.getuid()}:{os.getgid()}',
        detach=True,labels={'wiki.week6.test':RUN})
    active.append(c)
    def logs():
        with (directory/(label+'.log')).open('wb') as f:
            try:
                for line in c.logs(stream=True,follow=True):f.write(line);f.flush()
            except Exception:pass
    threading.Thread(target=logs,daemon=True).start()
    return c

def wait(c,timeout=1800):
    result=c.wait(timeout=timeout)
    if result['StatusCode']!=0:raise RuntimeError(f'Spark exit {result}: '+c.logs(tail=15).decode(errors='replace'))
    c.remove();active.remove(c)

def committed(directory):
    rows=0;offsets={i:0 for i in range(3)};seen=set()
    for path in directory.glob('*.metrics.jsonl'):
        for line in path.read_text().splitlines():
            try:v=json.loads(line)
            except json.JSONDecodeError:continue  # a concurrent append may be incomplete
            if v['batch_id'] in seen:continue
            seen.add(v['batch_id']);rows+=v['rows']
            for item in v['offsets']:offsets[item['partition']]=max(offsets[item['partition']],item['end_offset'])
    return rows,offsets

def produce(directory,topic,stage,rate,seconds=None,count=None,duplicates=0,invalid=0):
    count=count if count is not None else rate*seconds
    p=Producer({'bootstrap.servers':BOOTSTRAP,'enable.idempotence':True,'acks':'all','compression.type':'lz4','delivery.timeout.ms':180000})
    ack=0;errors=[];started=time.time();clock_started=time.monotonic();prefix=RUN+'-'+stage+'-'
    def callback(error,msg):
        nonlocal ack
        if error:errors.append(str(error))
        else:ack+=1
    total=count+duplicates+invalid
    for n in range(total):
        remaining=clock_started+n/rate-time.monotonic()
        while remaining>0:
            p.poll(min(.02,remaining))
            remaining=clock_started+n/rate-time.monotonic()
        index=n if n<count else (n-count if n<count+duplicates else n)
        now=datetime.now(timezone.utc).isoformat()
        wiki=['enwiki','kowiki','dewiki'][index%3];title='Synthetic '+str(index%100)
        value=dict(event_id=prefix+str(index),event_type='wikipedia.edit',schema_version=1,event_time=now,ingest_time=now,
                   source='synthetic-week6-'+stage,payload=dict(wiki=wiki,title=title,bot=False,type='edit',server_name=wiki[:-4]+'.wikipedia.org',length={'old':100,'new':110}))
        if n>=count+duplicates:value['schema_version']=999
        encoded='{invalid json' if n>=count+duplicates and n%2 else json.dumps(value)
        while True:
            try:p.produce(topic,key=wiki+':'+title,value=encoded,on_delivery=callback);break
            except BufferError:p.poll(.1)
        p.poll(0)
    pending=p.flush(185);finished=time.time();elapsed=time.monotonic()-clock_started
    result=dict(stage=stage,requested_rate=rate,planned_seconds=seconds,unique_valid=count,duplicate_input=duplicates,invalid_input=invalid,
                acknowledged=ack,errors=errors,pending=pending,started_at=started,finished_at=finished,elapsed_s=elapsed,actual_ack_per_s=ack/elapsed)
    with (directory/'producer.jsonl').open('a') as f:f.write(json.dumps(result)+'\n')
    ep=directory/'expected.json';expected=json.loads(ep.read_text()) if ep.exists() else []
    expected.append({'prefix':prefix,'count':count});ep.write_text(json.dumps(expected))
    assert ack==total and not errors and not pending,result
    log(f'{stage}: acknowledged {ack}, {result["actual_ack_per_s"]:.1f}/s')
    return total

def drain(directory,topic,total,c,timeout=1500):
    deadline=time.time()+timeout
    while time.time()<deadline:
        rows,_=committed(directory)
        if rows==total:return
        c.reload()
        if c.status!='running':raise RuntimeError('Spark stopped before drain: '+c.logs(tail=15).decode(errors='replace'))
        time.sleep(5)
    raise TimeoutError(f'Backlog did not drain: {committed(directory)[0]}/{total}')

def pointer(cfg):
    key=cfg['prefix']+'/spark-v1/daily/published/'+DATE+'/current.json'
    return S3.get_object(Bucket=cfg['bucket'],Key=key)['Body'].read().decode()

def main():
    initialize()
    stop_monitor=threading.Event();broker=None;network=None;success=False
    try:
        # A dedicated broker prevents fault injection from interrupting live ingestion.
        network=DOCKER.networks.create(NETWORK)
        broker=DOCKER.containers.run('apache/kafka:3.8.0',name=BROKER,hostname=BROKER,
            detach=True,network=NETWORK,ports={'9092/tcp':('127.0.0.1',19092)},mem_limit='1g',
            environment={'KAFKA_NODE_ID':'1','KAFKA_PROCESS_ROLES':'broker,controller',
              'KAFKA_LISTENERS':'INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093',
              'KAFKA_ADVERTISED_LISTENERS':f'INTERNAL://{BROKER}:29092,EXTERNAL://127.0.0.1:19092',
              'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP':'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
              'KAFKA_INTER_BROKER_LISTENER_NAME':'INTERNAL','KAFKA_CONTROLLER_LISTENER_NAMES':'CONTROLLER',
              'KAFKA_CONTROLLER_QUORUM_VOTERS':f'1@{BROKER}:9093',
              'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':'1','KAFKA_AUTO_CREATE_TOPICS_ENABLE':'false',
              'KAFKA_HEAP_OPTS':'-Xms256m -Xmx512m','CLUSTER_ID':'MkU3OEVBNTcwNTJENDM2Qk'})
        deadline=time.time()+120
        while True:
            try:AdminClient({'bootstrap.servers':BOOTSTRAP}).list_topics(timeout=3);break
            except Exception:
                if time.time()>deadline:raise TimeoutError('Test broker startup timed out')
                time.sleep(2)
        info=DOCKER.info()
        (OUT/'environment.json').write_text(json.dumps({
            'docker':{k:info.get(k) for k in ('ServerVersion','NCPU','MemTotal','OperatingSystem','Architecture')},
            'spark_master':'local[2]','driver_memory':'1g','spark_container_memory':'2g',
            'kafka_partitions':3,'kafka_memory':'1g','shuffle_partitions':2,
            'parameters':vars(ARGS),'storage':'actual AWS S3','production_services':'untouched',
            'other_running_containers':[c.name for c in DOCKER.containers.list() if c.name!=BROKER],
            'spark_image_id':DOCKER.images.get('wikipedia-streaming-analytics-spark:latest').id},indent=2)+'\n')
        # Fail before load generation when credentials/configuration are unavailable.
        S3.head_bucket(Bucket=CONFIG['bucket'])
        log('Run '+RUN+'; isolated broker and S3 validation prefix')
        if not ARGS.extended_only and not ARGS.crash_only:
            directory,topic,cfg=prepare('load')
            consumer=Consumer({'bootstrap.servers':BOOTSTRAP,'group.id':'metrics-'+RUN,'enable.auto.commit':False})
            def monitor():
                with (directory/'lag.jsonl').open('a') as f:
                    while not stop_monitor.is_set():
                        try:
                            ends={i:consumer.get_watermark_offsets(TopicPartition(topic,i),timeout=5,cached=False)[1] for i in range(3)}
                            rows,offsets=committed(directory)
                            f.write(json.dumps({'at':time.time(),'end_offsets':ends,'committed_offsets':offsets,'committed_rows':rows,'lag':sum(ends[i]-offsets[i] for i in ends)})+'\n');f.flush()
                        except Exception as e:log('monitor: '+str(e))
                        stop_monitor.wait(5)
            thread=threading.Thread(target=monitor,daemon=True);thread.start()
            c=spark(directory,topic)
            total=produce(directory,topic,'warmup',100,seconds=ARGS.warmup)
            drain(directory,topic,total,c)
            for repetition in range(1,ARGS.repetitions+1):
                for rate in ARGS.rates:
                    total+=produce(directory,topic,f'r{repetition}-rate{rate}',rate,seconds=ARGS.seconds)
                    drain(directory,topic,total,c)  # each stage starts without previous-stage backlog
            
            drain(directory,topic,total,c)
            stop_monitor.set();thread.join(15);consumer.close()
            c.stop(timeout=30);c.remove();active.remove(c)
            wait(spark(directory,topic,'verify'))
            (directory/'phase.json').write_text(json.dumps({'success':True,'phase':'load'}))
            if ARGS.skip_faults:
                success=True
                raise SystemExit(0)
        if not ARGS.extended_only and not ARGS.skip_faults:
            log('Beginning real SIGKILL recovery')
            directory,topic,cfg=prepare('fault')
            total=produce(directory,topic,'baseline',100,count=100)
            wait(spark(directory,topic,once=True));wait(spark(directory,topic,'daily'))
            before=pointer(cfg)
            total+=produce(directory,topic,'recovery',1000,count=1000,duplicates=100,invalid=20)
            c=spark(directory,topic,'crash')
            deadline=time.time()+300
            while not (directory/'crash_ready.json').exists():
                c.reload()
                if c.status!='running':raise RuntimeError(c.logs(tail=20).decode(errors='replace'))
                if time.time()>deadline:raise TimeoutError('Crash boundary not reached')
                time.sleep(.5)
            ready=json.loads((directory/'crash_ready.json').read_text());killed=time.time()
            c.kill(signal='SIGKILL');status=c.wait(timeout=30)['StatusCode'];c.remove();active.remove(c)
            objects=S3.list_objects_v2(Bucket=cfg['bucket'],Prefix=ready['value']['path'].replace('s3a://'+cfg['bucket']+'/',''))
            assert objects.get('KeyCount',0)>0
            try:S3.head_object(Bucket=cfg['bucket'],Key=cfg['prefix']+'/spark-v1/'+ready['key']);raise AssertionError('Unexpected commit')
            except S3.exceptions.ClientError as e:
                if e.response['Error']['Code'] not in ('404','NoSuchKey'):raise
            assert pointer(cfg)==before
            restarted=time.time();wait(spark(directory,topic,once=True));recovered=time.time()
            wait(spark(directory,topic,'daily'));wait(spark(directory,topic,'verify'))
            v=json.loads((directory/'verification.json').read_text())
            assert v['rows']==1220 and v['published']=={'rows':1100,'unique_ids':1100,'aggregate_sum':1100},v
            recovery=dict(signal='SIGKILL',exit_code=status,killed_at=killed,restarted_at=restarted,stream_completed_at=recovered,
                          recovery_s=recovered-restarted,uncommitted_objects=objects['KeyCount'],old_publication_preserved=True,
                          input_rows=total,valid_unique=1100,duplicate_input=100,rejected_input=20,**v)
            (directory/'recovery.json').write_text(json.dumps(recovery,indent=2)+'\n')
        if ARGS.extended_only:
            from week6_faults import exercise
            exercise(sys.modules[__name__],broker)
        success=True
        log('PASS selected experiments; '+str(OUT))
    finally:
        stop_monitor.set()
        for c in active:
            try:c.remove(force=True)
            except Exception:pass
        if broker is not None:
            try:
                broker.reload()
                (OUT/'broker-state.json').write_text(json.dumps(broker.attrs['State'],indent=2))
                (OUT/'broker.log').write_bytes(broker.logs(tail=200))
            except Exception as exc:log('Broker diagnostics unavailable: '+str(exc))
            try:broker.remove(force=True)
            except Exception as exc:log('Test broker cleanup required: '+str(exc))
        if network is not None:
            try:network.remove()
            except Exception as exc:log('Test network cleanup required: '+str(exc))
        (OUT/'run.json').write_text(json.dumps({'run':RUN,'target_date':DATE,'success':success,
            'parameters':vars(ARGS),'output':str(OUT)},indent=2)+'\n')
        DOCKER.close()


if __name__=="__main__":main()
