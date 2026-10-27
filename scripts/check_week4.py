"""Read-only database and Parquet checks, plus before/after examples for submission."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from src.process_edits import connect, database_reader, EDIT_COLUMNS


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='artifacts/week4/parquet')
    parser.add_argument('--expect-edits', type=int)
    parser.add_argument('--expect-rejected', type=int)
    parser.add_argument('--fixture', action='store_true')
    parser.add_argument('--expected-input', help='Original Wikimedia JSONL to compare IDs and immutable payload fields')
    parser.add_argument('--report', default='artifacts/week4/verification.json')
    args = parser.parse_args()
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT count(*), count(DISTINCT event_id) FROM week4.edits')
            count, unique = cur.fetchone()
            cur.execute('SELECT count(*) FROM week4.rejected')
            rejected = cur.fetchone()[0]
            cur.execute('SELECT coalesce(sum(edit_count),0) FROM week4.hourly_wiki')
            aggregate_count = int(cur.fetchone()[0])
            cur.execute('SELECT event_id,event_time,ingest_time,wiki,title,bot,bytes_delta FROM week4.edits ORDER BY event_id LIMIT 3')
            example = [dict(zip([c.name for c in cur.description], row)) for row in cur.fetchall()]
            cur.execute('SELECT error_reason,count(*) FROM week4.rejected GROUP BY error_reason ORDER BY error_reason')
            reasons = dict(cur.fetchall())
            expected_count, input_examples = None, []
            if args.expected_input:
                expected = {}
                for line in Path(args.expected_input).read_text(encoding='utf-8').splitlines():
                    event = json.loads(line)
                    if event.get('type') != 'edit' or not event.get('server_name', '').endswith('.wikipedia.org'):
                        continue
                    event_id = event.get('meta', {}).get('id') or f"{event['wiki']}:edit:{event['id']}"
                    length = event.get('length') or {}
                    old, new = length.get('old'), length.get('new')
                    expected[event_id] = (datetime.fromtimestamp(event['timestamp'], timezone.utc),
                                          event['wiki'], event['title'], event['bot'], old, new,
                                          new - old if new is not None and old is not None else None)
                    if len(input_examples) < 3:
                        input_examples.append({'event_id': event_id, 'timestamp': event['timestamp'],
                                               'wiki': event['wiki'], 'title': event['title'],
                                               'bot': event['bot'], 'length': length})
                if not expected:
                    raise RuntimeError('Expected input contains no Wikipedia edits')
                cur.execute('SELECT event_id,event_time,wiki,title,bot,old_length,new_length,bytes_delta '
                            'FROM week4.edits WHERE event_id = ANY(%s)', (list(expected),))
                actual = {row[0]: tuple(row[1:]) for row in cur.fetchall()}
                assert actual == expected, 'Missing or changed events compared with source JSONL'
                expected_count = len(expected)
            if args.fixture:
                assert reasons == {name: rejected // 3 for name in (
                    'invalid_event_time', 'invalid_json_or_types', 'unsupported_schema_version')}, reasons
                cur.execute('SELECT sum(bytes_delta),count(bytes_delta),count(*) FILTER (WHERE bot),min(event_time) FROM week4.edits')
                delta, known, bots, earliest = cur.fetchone()
                assert (delta, known, bots) == (10, 2, 1), (delta, known, bots)
                assert earliest.isoformat() == '2026-10-25T10:04:24+00:00'
        assert count == unique == aggregate_count
        if args.expect_edits is not None:
            assert count == args.expect_edits, (count, args.expect_edits)
        if args.expect_rejected is not None:
            assert rejected == args.expect_rejected, (rejected, args.expect_rejected)
    finally:
        conn.close()
    manifest = json.loads((Path(args.output) / 'CURRENT.json').read_text(encoding='utf-8'))
    spark = (SparkSession.builder.appName('verify-week4')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        path = Path(manifest['snapshot'])
        edits = spark.read.parquet(str(path / 'edits'))
        hourly = spark.read.parquet(str(path / 'hourly_wiki'))
        columns = EDIT_COLUMNS + ['processed_at']
        database_edits = database_reader(spark).option('dbtable', 'week4.edits').load()
        left, right = edits.select(*columns), database_edits.select(*columns)
        assert not left.exceptAll(right).take(1), 'Parquet contains changed or extra rows'
        assert not right.exceptAll(left).take(1), 'Parquet is missing database rows'
        assert not edits.filter(F.col('event_date').isNull() | (F.col('event_date') != F.to_date('event_time'))).take(1), 'Incorrect date partition'
        database_hourly = database_reader(spark).option('dbtable', 'week4.hourly_wiki').load()
        hourly_columns = ['hour_utc', 'wiki', 'edit_count', 'bot_edit_count', 'size_known_count', 'net_bytes_delta']
        db_hours = database_hourly.withColumn('net_bytes_delta', F.col('net_bytes_delta').cast('long')).select(*hourly_columns)
        file_hours = hourly.select(*hourly_columns)
        assert not db_hours.exceptAll(file_hours).take(1), 'Missing or incorrect Parquet aggregates'
        assert not file_hours.exceptAll(db_hours).take(1), 'Extra Parquet aggregate rows'
        parquet_count = edits.count()
        parquet_aggregate = int(hourly.agg(F.sum('edit_count')).first()[0] or 0)
        assert parquet_count == edits.select('event_id').distinct().count() == count == manifest['edits']
        assert parquet_aggregate == count
        result = {'verified': True, 'database_edits': count, 'distinct_ids': unique,
                  'source_events_verified': expected_count, 'full_row_equality': True,
                  'full_aggregate_equality': True,
                  'input_examples': input_examples,
                  'rejected': rejected, 'rejection_reasons': reasons,
                  'aggregate_edit_count': aggregate_count, 'parquet_edits': parquet_count,
                  'parquet_aggregate_edit_count': parquet_aggregate, 'snapshot': manifest,
                  'output_examples': example, 'checked_at': datetime.now().astimezone().isoformat()}
        out = Path(args.report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
