"""Airflow's real Spark tasks; per-run immutable candidates are published only after quality checks."""
import argparse
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid
import psycopg2
from pyspark.sql import SparkSession, functions as F
from src.process_edits import initialize_database, connect, export_snapshot
from src.snapshot_store import read_snapshot
from src.db_sink import stage, cleanup

KST = ZoneInfo('Asia/Seoul')


class QualityError(ValueError):
    pass


def bounds(day):
    parsed = date.fromisoformat(day)
    if parsed.isoformat()!=day:
        raise ValueError('target_date must be YYYY-MM-DD')
    start = datetime.combine(parsed,time.min,KST).astimezone(timezone.utc)
    return start, start+timedelta(days=1)


def save(path, value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    os.replace(temp,path)


def load(path):
    return json.loads(Path(path).read_text())


def selected_snapshot(spark, manifest, day):
    start,end=bounds(day)
    if manifest.get('version')==2:
        selected=dict(manifest)
        selected['partitions']={d:p for d,p in manifest['partitions'].items()
                                if start.date().isoformat() <= d <= (end-timedelta(microseconds=1)).date().isoformat()}
    else:
        selected=manifest
    frame=read_snapshot(spark,selected)
    return frame.filter((F.col('event_time')>=F.lit(start)) & (F.col('event_time')<F.lit(end)))


def compact_raw(spark, work, day, source_root):
    # Refresh only changed source dates, then pin an immutable manifest for this DAG run.
    manifest=export_snapshot(spark,source_root)
    frame=selected_snapshot(spark,manifest,day).cache()
    try:
        count=frame.count()
        folder=work/'compact'/uuid.uuid4().hex
        # This lab's 3846 rows fit one output file. Never overwrite streaming sink metadata.
        frame.coalesce(1).write.parquet(str(folder))
        start,end=bounds(day)
        relevant={d:rev for d,rev in manifest.get('revisions',{}).items()
                  if start.date().isoformat()<=d<=(end-timedelta(microseconds=1)).date().isoformat()}
        fingerprint=hashlib.sha256(json.dumps(dict(day=day,source=manifest.get('source_id'),
                       revisions=relevant,policy='kst-daily-v1'),sort_keys=True).encode()).hexdigest()
        record=dict(target_date=day,path=str(folder),row_count=count,
                    fingerprint=fingerprint,source_manifest=manifest,
                    input_files=len(frame.inputFiles()),output_files=len(list(folder.glob('*.parquet'))),
                    start_utc=start.isoformat(),end_utc=end.isoformat())
        save(work/'compact.json',record)
    finally:
        frame.unpersist()


def daily_agg(spark,work,day):
    compact=load(work/'compact.json')
    if compact['target_date']!=day: raise ValueError('Date differs from compact task')
    frame=spark.read.parquet(compact['path'])
    result=(frame.groupBy('wiki').agg(F.count('*').alias('edit_count'),
            F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
            F.count('bytes_delta').alias('size_known_count'),F.sum('bytes_delta').alias('net_bytes_delta'))
            .withColumn('target_date',F.lit(day).cast('date')))
    path=work/'daily'/uuid.uuid4().hex
    result.coalesce(1).write.parquet(str(path))
    save(work/'daily.json',dict(target_date=day,path=str(path),fingerprint=compact['fingerprint']))


def quality_check(spark,work,day):
    compact,daily=load(work/'compact.json'),load(work/'daily.json')
    frame=spark.read.parquet(compact['path']).cache()
    agg=spark.read.parquet(daily['path'])
    try:
        n=frame.count()
        required=['event_id','event_time','ingest_time','wiki','title','bot']
        invalid=None
        for name in required:
            cond=F.col(name).isNull()
            if name in ('event_id','wiki','title'):cond=cond | (F.length(F.trim(F.col(name)))==0)
            invalid=cond if invalid is None else invalid|cond
        nulls=frame.filter(invalid).count()
        dup=n-frame.select('event_id').distinct().count()
        source=selected_snapshot(spark,compact['source_manifest'],day)
        columns=frame.columns
        equal=(not source.select(*columns).exceptAll(frame.select(*columns)).take(1)
               and not frame.select(*columns).exceptAll(source.select(*columns)).take(1))
        expected=(frame.groupBy('wiki').agg(F.count('*').alias('edit_count'),
            F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
            F.count('bytes_delta').alias('size_known_count'),F.sum('bytes_delta').alias('net_bytes_delta'))
            .withColumn('target_date',F.lit(day).cast('date')))
        cols=expected.columns
        aggregate_equal=(not expected.exceptAll(agg.select(*cols)).take(1)
                         and not agg.select(*cols).exceptAll(expected).take(1))
        report=dict(target_date=day,row_count=n,null_rows=nulls,null_ratio=nulls/n if n else 0,
                    duplicate_count=dup,source_rows=source.count(),source_equal=equal,
                    aggregate_equal=aggregate_equal,fingerprint=compact['fingerprint'],
                    compact_path=compact['path'],daily_path=daily['path'])
        report['passed']=(n>0 and nulls==0 and dup==0 and equal and aggregate_equal
                          and compact['fingerprint']==daily['fingerprint'] and daily['target_date']==day)
        save(work/'quality.json',report)
        if not report['passed']: raise QualityError('Quality gate failed: '+json.dumps(report))
    finally:
        frame.unpersist()


def publish(spark,work,day):
    report=load(work/'quality.json');daily=load(work/'daily.json');compact=load(work/'compact.json')
    if (not report['passed'] or report['target_date']!=day
        or report['daily_path']!=daily['path'] or report['compact_path']!=compact['path']
        or report['fingerprint']!=compact['fingerprint'] or daily['fingerprint']!=compact['fingerprint']):
        raise QualityError('No matching passed quality report; publish blocked')
    frame=spark.read.parquet(daily['path'])
    token=uuid.uuid4().hex
    stage(frame,'week5.daily_stage',token,partitions=2)
    conn=connect()
    try:
        with conn,conn.cursor() as cur:
            # Serialize publication for a date, including manual reruns outside Airflow.
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',('week5:'+day,))
            cur.execute('SELECT fingerprint FROM week5.publications WHERE target_date=%s',(day,))
            prior=cur.fetchone()
            if prior and prior[0]==compact['fingerprint']:
                changed=False
            else:
                cur.execute('''DELETE FROM week5.daily_wiki dest WHERE target_date=%s
                    AND NOT EXISTS (SELECT 1 FROM week5.daily_stage s WHERE s.load_id=%s AND s.wiki=dest.wiki)''',(day,token))
                cur.execute('''INSERT INTO week5.daily_wiki
                    (target_date,wiki,edit_count,bot_edit_count,size_known_count,net_bytes_delta)
                    SELECT DISTINCT target_date,wiki,edit_count,bot_edit_count,size_known_count,net_bytes_delta
                    FROM week5.daily_stage WHERE load_id=%s
                    ON CONFLICT (target_date,wiki) DO UPDATE SET edit_count=EXCLUDED.edit_count,
                    bot_edit_count=EXCLUDED.bot_edit_count,size_known_count=EXCLUDED.size_known_count,
                    net_bytes_delta=EXCLUDED.net_bytes_delta,updated_at=now()
                    WHERE (daily_wiki.edit_count,daily_wiki.bot_edit_count,daily_wiki.size_known_count,daily_wiki.net_bytes_delta)
                    IS DISTINCT FROM (EXCLUDED.edit_count,EXCLUDED.bot_edit_count,EXCLUDED.size_known_count,EXCLUDED.net_bytes_delta)''',(token,))
                cur.execute('''INSERT INTO week5.publications VALUES (%s,%s,%s::jsonb,now())
                    ON CONFLICT(target_date) DO UPDATE SET fingerprint=EXCLUDED.fingerprint,
                    manifest=EXCLUDED.manifest,published_at=now()''',(day,compact['fingerprint'],json.dumps(report)))
                changed=True
            cur.execute('DELETE FROM week5.daily_stage WHERE load_id=%s',(token,))
        save(work/'published.json',dict(target_date=day,changed=changed,fingerprint=compact['fingerprint']))
    finally:
        conn.close()
        try:cleanup('week5.daily_stage',token)
        except Exception:print('Deferred staging cleanup for '+token,file=sys.stderr)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['compact_raw','daily_agg','quality_check','publish'])
    p.add_argument('--date',required=True);p.add_argument('--work',type=Path,required=True)
    p.add_argument('--source-root',default='artifacts/week4/parquet')
    args=p.parse_args();bounds(args.date)
    spark=(SparkSession.builder.appName('week5-'+args.action)
           .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        initialize_database()
        conn=connect()
        try:
            with conn,conn.cursor() as cur:cur.execute(Path('sql/week5.sql').read_text())
        finally:conn.close()
        if args.action=='compact_raw':compact_raw(spark,args.work,args.date,args.source_root)
        else:globals()[args.action](spark,args.work,args.date)
    finally:spark.stop()


if __name__=='__main__':
    try:main()
    except (QualityError,ValueError,PermissionError,FileNotFoundError) as exc:
        print(type(exc).__name__+': '+str(exc),file=sys.stderr);sys.exit(2)
    except (psycopg2.OperationalError,psycopg2.InterfaceError) as exc:
        print(type(exc).__name__+': '+str(exc),file=sys.stderr);sys.exit(75)
    except Exception as exc:
        # Spark wraps JDBC errors; retry only recognizable transient connection/availability errors.
        text=str(exc)
        transient=any(x in text for x in ['Connection refused','connection timed out','SQLSTATE: 08','SQLSTATE: 57P01','connection attempt failed'])
        print(type(exc).__name__+': '+text,file=sys.stderr);sys.exit(75 if transient else 2)
