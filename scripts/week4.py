"""Portable runner: Docker Compose + host Python 3.9+, no host pip installs.

Usage on Windows, Linux and macOS: python scripts/week4.py demo
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'compose.week4.yml']


def command(arguments, log=None, allow_failure=False):
    print('+ ' + ' '.join(map(str, arguments)), flush=True)
    env = os.environ.copy()
    if hasattr(os, 'getuid'):
        env['LAB_UID'], env['LAB_GID'] = str(os.getuid()), str(os.getgid())
    if log is None:
        code = subprocess.run(arguments, cwd=ROOT, env=env).returncode
    else:
        target = ROOT / log
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('w', encoding='utf-8') as handle:
            process = subprocess.Popen(arguments, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
            for line in process.stdout:
                print(line, end='', flush=True)
                handle.write(line)
            code = process.wait()
    if code and not allow_failure:
        raise subprocess.CalledProcessError(code, arguments)
    return code


def compose(*arguments, **kwargs):
    return command(COMPOSE + list(arguments), **kwargs)


def spark(*arguments, database=None, **kwargs):
    options = ['run', '--rm', '-T']
    if database:
        options += ['-e', 'PGDATABASE=' + database]
    return compose(*options, 'spark', *arguments, **kwargs)


def produce(*arguments):
    return compose('run', '--rm', '-T', 'producer', *arguments)


def setup():
    command(['docker', 'compose', 'version'])
    compose('build', 'producer', 'spark')
    compose('up', '-d', '--wait', 'kafka', 'postgres')


def create_topic(name):
    compose('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
            '--bootstrap-server', 'localhost:29092', '--create', '--if-not-exists',
            '--topic', name, '--partitions', '3', '--replication-factor', '1',
            '--config', 'retention.ms=604800000', '--config', 'cleanup.policy=delete')


def demo():
    setup()
    create_topic('wikipedia-edits')
    produce('-m', 'src.producer', '--bootstrap-servers', 'kafka:29092',
            '--input', 'samples/wikipedia_edits.jsonl', '--rate', '10')
    spark('src/process_edits.py')
    spark('scripts/check_week4.py', '--expected-input', 'samples/wikipedia_edits.jsonl',
          '--report', 'evidence/week4/demo_verification.json')


def verify():
    setup()
    test_id = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S') + '_' + secrets.token_hex(3)
    database, topic = 'week4_test_' + test_id, 'week4-test-' + test_id
    test_root = 'artifacts/week4/tests/' + test_id
    (ROOT / test_root).mkdir(parents=True)
    compose('exec', '-T', 'postgres', 'psql', '-U', 'wiki', '-d', 'wiki',
            '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE ' + database)
    create_topic(topic)
    spark('tests/integration/test_transform.py', log=test_root + '/transform.log')
    send = ['tests/fixtures/week4_fixture.py', '--topic', topic, '--bootstrap-servers', 'kafka:29092']
    produce(*send)
    args = ['src/process_edits.py', '--topic', topic, '--checkpoint', test_root + '/checkpoint',
            '--output', test_root + '/parquet']
    failure_log = test_root + '/injected_failure.log'
    result = spark(*args, '--fail-after-commit', database=database, log=failure_log, allow_failure=True)
    text = (ROOT / failure_log).read_text(encoding='utf-8')
    if result == 0 or 'Injected failure AFTER database commit' not in text:
        raise RuntimeError('Recovery test did not reach the intended failure; inspect ' + failure_log)
    reports = {}
    for name, rejected in [('recovery', 3), ('restart', 3), ('replay', 6)]:
        if name == 'replay':
            produce(*send)
        spark(*args, database=database, log=test_root + '/' + name + '.log')
        report = test_root + '/' + name + '.json'
        spark('scripts/check_week4.py', '--output', test_root + '/parquet', '--expect-edits', '3',
              '--expect-rejected', str(rejected), '--fixture', '--report', report, database=database)
        reports[name] = json.loads((ROOT / report).read_text(encoding='utf-8'))
    evidence = {'verified': True, 'fixture': 'synthetic; separate database and topic',
                'database': database, 'topic': topic, 'reports': reports}
    path = ROOT / 'evidence/week4/recovery_verification.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('PASS: transforms, post-commit recovery, checkpoint restart, replay, DB/Parquet row and aggregate equality')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'demo', 'verify', 'run', 'check', 'send', 'status', 'stop'])
    args, rest = parser.parse_known_args()
    if rest and args.action not in ('run', 'check', 'send'):
        parser.error('Unexpected arguments: ' + ' '.join(rest))
    if args.action == 'setup':
        setup()
    elif args.action == 'demo':
        demo()
    elif args.action == 'verify':
        verify()
    elif args.action == 'run':
        spark('src/process_edits.py', *rest)
    elif args.action == 'check':
        spark('scripts/check_week4.py', *rest)
    elif args.action == 'send':
        create_topic('wikipedia-edits')
        produce('-m', 'src.producer', '--bootstrap-servers', 'kafka:29092', *rest)
    elif args.action == 'status':
        compose('ps')
    elif args.action == 'stop':
        compose('stop')


if __name__ == '__main__':
    try:
        main()
    except FileNotFoundError as error:
        print(f'Required command/file missing: {error}', file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as error:
        print(f'Step failed (exit {error.returncode}); later steps were not run.', file=sys.stderr)
        sys.exit(error.returncode)
