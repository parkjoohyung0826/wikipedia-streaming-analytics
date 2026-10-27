"""Send deliberately labelled synthetic records for the isolated recovery test."""
import argparse
import json


def records():
    base = {
        'event_id': 'week4-test-a', 'event_type': 'wikipedia.edit', 'schema_version': 1,
        'event_time': '2026-10-25T19:04:24+09:00',
        'ingest_time': '2026-10-25T10:05:00Z', 'source': 'synthetic-week4-test',
        'payload': {'wiki': 'kowiki', 'title': '테스트 문서', 'type': 'edit',
                    'server_name': 'ko.wikipedia.org', 'bot': False,
                    'length': {'old': 100, 'new': 130}},
    }
    def copy():
        return json.loads(json.dumps(base))
    b = copy()
    b['event_id'] = 'week4-test-b'
    b['payload']['bot'] = True
    b['payload'].pop('length')
    c = copy()
    c['event_id'] = 'week4-test-c'
    c['payload']['length']['new'] = 80
    missing = copy()
    missing['event_id'] = 'week4-test-missing'
    missing.pop('event_time')
    version = copy()
    version['event_id'] = 'week4-test-version'
    version['schema_version'] = 2
    return [base, base, b, c, '{broken json', missing, version]


def main():
    from confluent_kafka import Producer
    p = argparse.ArgumentParser()
    p.add_argument('--topic', required=True)
    args = p.parse_args()
    producer = Producer({'bootstrap.servers': 'localhost:9092', 'enable.idempotence': True})
    errors = []
    def ack(error, message):
        if error:
            errors.append(str(error))
    for record in records():
        value = record if isinstance(record, str) else json.dumps(record, ensure_ascii=False)
        producer.produce(args.topic, key='kowiki:테스트 문서', value=value.encode(), on_delivery=ack)
        producer.poll(0)
    remaining = producer.flush(30)
    if errors or remaining:
        raise SystemExit(f'Fixture delivery failed: errors={errors}, remaining={remaining}')
    print('Sent 7 synthetic records: 4 valid occurrences / 3 unique events / 3 invalid records')


if __name__ == '__main__':
    main()
