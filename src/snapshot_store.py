"""Copy-on-write daily Parquet snapshots; publish one atomic manifest after all writes."""
from datetime import datetime, timezone, date, timedelta
from functools import reduce
import fcntl
import json
import os
from pathlib import Path
import uuid
from pyspark.sql import functions as F


def hourly_frame(edits):
    return (edits.groupBy(F.date_trunc('hour','event_time').alias('hour_utc'),'wiki')
            .agg(F.count('*').alias('edit_count'),F.sum(F.col('bot').cast('long')).alias('bot_edit_count'),
                 F.count('bytes_delta').alias('size_known_count'),F.sum('bytes_delta').alias('net_bytes_delta')))


def read_snapshot(spark, manifest, kind='edits'):
    """Never glob versions/: only the manifest's completed paths are visible."""
    if manifest.get('version') != 2:
        return spark.read.parquet(str(Path(manifest['snapshot']) / kind))
    paths = [part[kind+'_path'] for part in manifest['partitions'].values()]
    if not paths:
        paths = [manifest['empty'][kind+'_path']]
    frames = [spark.read.parquet(path) for path in paths]
    return reduce(lambda a,b: a.unionByName(b), frames)


def export_snapshot(spark, output, fail_before_publish=False):
    from src.process_edits import connect, database_reader
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    # Lock only publishers for this local output root, never the ingestion transaction.
    with (root / '.export.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        conn = connect()
        try:
            conn.set_session(isolation_level='REPEATABLE READ', readonly=True)
            with conn.cursor() as cur:
                # Executor JDBC readers share the same DB snapshot as date revision metadata.
                cur.execute('SELECT pg_export_snapshot()')
                snapshot_id = cur.fetchone()[0]
                cur.execute("SELECT value FROM week4.storage_meta WHERE key='date_tracking'")
                source_id = cur.fetchone()[0]
                cur.execute('SELECT event_date,revision FROM week4.export_dates ORDER BY event_date')
                revisions = {str(day):version for day,version in cur.fetchall()}
            current = root / 'CURRENT.json'
            prior = json.loads(current.read_text()) if current.exists() else {}
            if prior.get('version') == 2 and prior.get('source_id') != source_id:
                raise ValueError('Snapshot root belongs to another database; use a new output path')
            previous_versions = prior.get('revisions', {}) if prior.get('version') == 2 else {}
            changed = [day for day,version in revisions.items() if previous_versions.get(day) != version]
            if prior.get('version') == 2 and not changed:
                print(json.dumps({'export':'unchanged','rewritten_dates':[]}), flush=True)
                return prior
            stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')+'-'+uuid.uuid4().hex[:8]
            target = root / 'versions' / stamp
            # Snapshot identifier is supplied by PostgreSQL, not user input.
            session = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '"+snapshot_id+"'"
            def read_where(condition):
                return (database_reader(spark).option('sessionInitStatement',session)
                        .option('dbtable','(SELECT * FROM week4.edits WHERE '+condition+') exported').load())
            empty = prior.get('empty') if prior.get('version') == 2 else None
            if empty is None:
                df = read_where('false')
                empty = {kind+'_path':str(target/'empty'/kind) for kind in ('edits','hourly_wiki')}
                df.withColumn('event_date',F.to_date('event_time')).write.parquet(empty['edits_path'])
                hourly_frame(df).write.parquet(empty['hourly_wiki_path'])
            parts = dict(prior.get('partitions', {})) if prior.get('version') == 2 else {}
            for day in changed:
                start = date.fromisoformat(day)
                end = start + timedelta(days=1)
                condition = f"event_time >= TIMESTAMPTZ '{start} 00:00:00+00' AND event_time < TIMESTAMPTZ '{end} 00:00:00+00'"
                frame = read_where(condition).cache()
                try:
                    count = frame.count()
                    if not count:
                        parts.pop(day, None)
                        continue
                    folder = target / ('event_date='+day)
                    entry = dict(edits=count,revision=revisions[day],
                                 edits_path=str(folder/'edits'),hourly_wiki_path=str(folder/'hourly_wiki'))
                    frame.withColumn('event_date',F.to_date('event_time')).write.parquet(entry['edits_path'])
                    hourly_frame(frame).write.parquet(entry['hourly_wiki_path'])
                    parts[day] = entry
                finally:
                    frame.unpersist()
            manifest = dict(version=2,source_id=source_id,created_at=stamp,timezone='UTC',
                            revisions=revisions,partitions=parts,empty=empty,
                            edits=sum(p['edits'] for p in parts.values()),rewritten_dates=changed)
            if fail_before_publish:
                raise RuntimeError('Injected failure BEFORE manifest publication')
            temporary = root / ('CURRENT-'+uuid.uuid4().hex+'.json')
            with temporary.open('w') as handle:
                handle.write(json.dumps(manifest,indent=2)+'\n')
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary,current)
            print(json.dumps({'export':manifest}),flush=True)
            return manifest
        finally:
            conn.rollback()
            conn.close()
