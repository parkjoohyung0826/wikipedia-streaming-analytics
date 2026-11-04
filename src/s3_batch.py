"""Container-free Wikipedia batch preparation and verified, immutable S3 publication."""
import base64
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

POLICY = 'wikipedia-batch-v1'


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n')


class Aws:
    def __init__(self, region='ap-northeast-2', profile=None):
        self.base = ['aws', '--no-cli-pager', '--region', region, '--output', 'json']
        if profile:
            self.base += ['--profile', profile]

    def call(self, *args, missing_ok=False):
        result = subprocess.run(self.base + list(args), capture_output=True, text=True)
        if result.returncode:
            if missing_ok and ('(404)' in result.stderr or '(NoSuchKey)' in result.stderr):
                return None
            raise RuntimeError(result.stderr.strip())
        return json.loads(result.stdout) if result.stdout.strip() else {}


def prepare(source, output):
    """Keep all raw lines, reject invalid edits, and aggregate selected unique edits by KST day."""
    source, output = Path(source), Path(output)
    source_hash = digest(source)
    batch_id = POLICY + '-' + source_hash
    output.mkdir(parents=True, exist_ok=True)
    counts = Counter(input=0, selected=0, filtered=0, duplicates=0, rejected=0)
    seen, groups = set(), {}
    # mtime=0 and an empty filename make gzip payloads reproducible for the same input.
    # Use explicit outer file contexts so every file descriptor is closed.
    with (output/'raw.jsonl.gz').open('wb') as raw_file, (output/'edits.jsonl.gz').open('wb') as edits_file, (output/'rejected.jsonl.gz').open('wb') as reject_file:
        with gzip.GzipFile(filename='', fileobj=raw_file, mode='wb', mtime=0) as raw, gzip.GzipFile(filename='', fileobj=edits_file, mode='wb', mtime=0) as edits, gzip.GzipFile(filename='', fileobj=reject_file, mode='wb', mtime=0) as rejected:
            with source.open('rb') as lines:
                for number, binary in enumerate(lines, 1):
                    raw.write(binary)
                    counts['input'] += 1
                    try:
                        event = json.loads(binary)
                        if not isinstance(event, dict):
                            raise ValueError('event must be an object')
                        meta = event.get('meta') or {}
                        if not isinstance(meta, dict):
                            raise ValueError('meta must be an object')
                        if meta.get('domain') == 'canary' or event.get('type') != 'edit' or not str(event.get('server_name', '')).endswith('.wikipedia.org'):
                            counts['filtered'] += 1
                            continue
                        wiki, title = event.get('wiki'), event.get('title')
                        if not all(isinstance(x, str) and x.strip() for x in (wiki, title)):
                            raise ValueError('wiki/title required')
                        identity = meta.get('id')
                        if not identity:
                            if event.get('id') is None:
                                raise ValueError('event identity required')
                            identity = f"{wiki}:edit:{event['id']}"
                        if not isinstance(identity, str):
                            raise ValueError('event identity must be a string')
                        if not isinstance(event.get('bot'), bool):
                            raise ValueError('bot must be boolean')
                        timestamp = event.get('timestamp')
                        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
                            raise ValueError('timestamp must be numeric')
                        dt = datetime.fromtimestamp(timestamp, timezone.utc)
                        lengths = event.get('length') or {}
                        if not isinstance(lengths, dict):
                            raise ValueError('length must be an object')
                        old, new = lengths.get('old'), lengths.get('new')
                        if any(x is not None and (isinstance(x, bool) or not isinstance(x, int) or x < 0) for x in (old, new)):
                            raise ValueError('length must be a nonnegative integer or null')
                        if identity in seen:
                            counts['duplicates'] += 1
                            continue
                        seen.add(identity)
                        day = dt.astimezone(ZoneInfo('Asia/Seoul')).date().isoformat()
                        delta = new - old if old is not None and new is not None else None
                        row = dict(event_id=identity, event_time=dt.isoformat(), event_date=day,
                                   wiki=wiki, title=title, bot=event['bot'], old_length=old,
                                   new_length=new, bytes_delta=delta)
                        edits.write((json.dumps(row, ensure_ascii=False, sort_keys=True)+'\n').encode())
                        counts['selected'] += 1
                        group = groups.setdefault((day,wiki), dict(target_date=day, wiki=wiki, edit_count=0,
                            bot_edit_count=0, size_known_count=0, net_bytes_delta=None))
                        group['edit_count'] += 1
                        group['bot_edit_count'] += int(event['bot'])
                        if delta is not None:
                            group['size_known_count'] += 1
                            group['net_bytes_delta'] = (group['net_bytes_delta'] or 0) + delta
                    except (ValueError, TypeError, OverflowError, OSError) as exc:
                        counts['rejected'] += 1
                        rejected.write((json.dumps(dict(line=number, reason=str(exc), raw=binary.decode('utf-8',errors='replace')),ensure_ascii=False)+'\n').encode())
    if digest(source) != source_hash:
        raise ValueError('Input changed during preparation; retry from a stable copy')
    if not counts['selected']:
        raise ValueError('No valid Wikipedia edits; not publishing')
    if sum(groups[g]['edit_count'] for g in groups) != counts['selected']:
        raise ValueError('Aggregate count mismatch')
    dump(output/'daily_wiki.json', [groups[g] for g in sorted(groups)])
    summary = dict(policy=POLICY,batch_id=batch_id,source_sha256=source_hash,counts=dict(counts),groups=len(groups),
                   scope='Per-input batch, KST event dates. Deduplication within a batch only; overlapping batches must not be summed blindly.')
    dump(output/'summary.json', summary)
    return summary


def publish(aws, config, directory, summary):
    """Conditional writes + remote SHA256 check. manifest.json is published LAST."""
    directory = Path(directory)
    bucket, owner = config['bucket'], config['account_id']
    prefix = config.get('prefix','wikipedia').strip('/') + '/batches/' + summary['batch_id']
    uploaded = []
    def upload(path, key):
        checksum = base64.b64encode(bytes.fromhex(digest(path))).decode()
        args = ['s3api','head-object','--bucket',bucket,'--key',key,'--expected-bucket-owner',owner,'--checksum-mode','ENABLED']
        remote = aws.call(*args, missing_ok=True)
        if remote is None:
            try:
                aws.call('s3api','put-object','--bucket',bucket,'--key',key,'--body',str(path),
                    '--expected-bucket-owner',owner,'--if-none-match','*','--server-side-encryption','AES256',
                    '--checksum-algorithm','SHA256','--checksum-sha256',checksum,
                    '--content-type','application/gzip' if path.suffix=='.gz' else 'application/json')
            except RuntimeError as exc:
                # Another identical publisher may have won the conditional write.
                if 'PreconditionFailed' not in str(exc) and '(412)' not in str(exc):
                    raise
            remote = aws.call(*args)
        if remote.get('ChecksumSHA256') != checksum or remote.get('ContentLength') != path.stat().st_size:
            raise ValueError('Remote object differs; refusing overwrite: '+key)
        return dict(key=key,bytes=path.stat().st_size,sha256=digest(path))
    for name in ['raw.jsonl.gz','edits.jsonl.gz','rejected.jsonl.gz','daily_wiki.json','summary.json']:
        uploaded.append(upload(directory/name,prefix+'/'+name))
    manifest = dict(version=1, batch_id=summary['batch_id'], objects=uploaded, counts=summary['counts'],groups=summary['groups'])
    dump(directory/'manifest.json',manifest)
    upload(directory/'manifest.json',prefix+'/manifest.json')
    return dict(verified=True,bucket=bucket,manifest='s3://'+bucket+'/'+prefix+'/manifest.json',**manifest)
