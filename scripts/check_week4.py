"""Read-only database and Parquet checks, plus before/after examples for submission."""
import argparse
from datetime import datetime
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from src.process_edits import connect


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='artifacts/week4/parquet')
    parser.add_argument('--expect-edits', type=int)
    parser.add_argument('--expect-rejected', type=int)
    parser.add_argument('--fixture', action='store_true')
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
            if args.fixture:
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
        parquet_count = edits.count()
        parquet_aggregate = int(hourly.agg(F.sum('edit_count')).first()[0] or 0)
        assert parquet_count == edits.select('event_id').distinct().count() == count == manifest['edits']
        assert parquet_aggregate == count
        result = {'verified': True, 'database_edits': count, 'distinct_ids': unique,
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
