"""Validate Kafka envelopes and retain rejected messages for S3 audit."""
from pyspark.sql import functions as F, types as T

ENVELOPE_SCHEMA = T.StructType([
    T.StructField('event_id', T.StringType()),
    T.StructField('event_type', T.StringType()),
    T.StructField('schema_version', T.IntegerType()),
    T.StructField('event_time', T.StringType()),
    T.StructField('ingest_time', T.StringType()),
    T.StructField('source', T.StringType()),
    T.StructField('payload', T.StructType([
        T.StructField('wiki', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('bot', T.BooleanType()),
        T.StructField('type', T.StringType()),
        T.StructField('server_name', T.StringType()),
        T.StructField('length', T.StructType([
            T.StructField('old', T.LongType()),
            T.StructField('new', T.LongType()),
        ])),
    ])),
    T.StructField('_corrupt_record', T.StringType()),
])
EDIT_COLUMNS = [
    'event_id', 'event_type', 'schema_version', 'event_time', 'ingest_time',
    'source', 'wiki', 'title', 'bot', 'old_length', 'new_length', 'bytes_delta',
    'source_topic', 'source_partition', 'source_offset',
]


def transform(raw):
    """Preserve every source record, with either typed columns or a rejection reason."""
    parsed = raw.withColumn('j', F.from_json('raw_value', ENVELOPE_SCHEMA))
    data = parsed.select(
        '*', F.col('j.event_id').alias('event_id'),
        F.col('j.event_type').alias('event_type'),
        F.col('j.schema_version').alias('schema_version'),
        F.to_timestamp('j.event_time').alias('event_time'),
        F.to_timestamp('j.ingest_time').alias('ingest_time'),
        F.col('j.source').alias('source'),
        F.col('j.payload.wiki').alias('wiki'),
        F.col('j.payload.title').alias('title'),
        F.col('j.payload.bot').alias('bot'),
        F.col('j.payload.length.old').alias('old_length'),
        F.col('j.payload.length.new').alias('new_length'),
    ).withColumn('bytes_delta', F.col('new_length') - F.col('old_length'))
    def missing(name):
        return F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)
    # Require an explicit offset or Z; do not silently assign a timezone.
    zoned = r'(?i)(Z|[+-][0-9]{2}:[0-9]{2})$'
    reason = (
        F.when(F.col('j').isNull() | F.col('j._corrupt_record').isNotNull(), 'invalid_json_or_types')
        .when(missing('event_id'), 'missing_event_id')
        .when(F.col('schema_version').isNull() | (F.col('schema_version') != 1), 'unsupported_schema_version')
        .when(F.col('event_type').isNull() | (F.col('event_type') != 'wikipedia.edit'), 'invalid_event_type')
        .when(F.col('event_time').isNull() | ~F.coalesce(F.col('j.event_time').rlike(zoned), F.lit(False)), 'invalid_event_time')
        .when(F.col('ingest_time').isNull() | ~F.coalesce(F.col('j.ingest_time').rlike(zoned), F.lit(False)), 'invalid_ingest_time')
        .when(missing('source') | missing('wiki') | missing('title') | F.col('bot').isNull(), 'missing_payload_fields')
        .when(~F.coalesce((F.col('j.payload.type') == 'edit') & F.col('j.payload.server_name').endswith('.wikipedia.org'), F.lit(False)), 'outside_wikipedia_edits')
        .when((F.col('old_length') < 0) | (F.col('new_length') < 0), 'negative_length')
        .when(F.col('message_key').isNull() | (F.col('message_key') != F.concat_ws(':', 'wiki', 'title')), 'key_mismatch')
    )
    return data.withColumn('error_reason', reason).drop('j')

