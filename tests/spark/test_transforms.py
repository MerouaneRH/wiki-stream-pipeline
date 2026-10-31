import hashlib
import json
from datetime import UTC, date, datetime

from pyspark.sql import Row

from spark.transforms import (
    BRONZE_COLUMNS,
    SILVER_COLUMNS,
    bronze_from_kafka,
    bronze_from_text,
    parse,
)
from tests.unit.factories import make_event, make_line

SALT = "test-salt"


def bronze_df(spark, lines):
    return bronze_from_text(spark.createDataFrame([(line,) for line in lines], "value string"))


def test_bronze_from_kafka_keeps_raw_value_and_offsets(spark):
    kafka = spark.createDataFrame(
        [
            Row(
                key=b"enwiki",
                value=b'{"a":1}',
                topic="wiki.edits.raw",
                partition=2,
                offset=7,
                timestamp=datetime(2026, 10, 31, tzinfo=UTC),
                timestampType=0,
            )
        ]
    )
    row = bronze_from_kafka(kafka).collect()[0]
    assert bronze_from_kafka(kafka).columns == BRONZE_COLUMNS
    assert (row.value, row.topic, row.partition, row.offset) == ('{"a":1}', "wiki.edits.raw", 2, 7)
    assert row.ingest_ts is not None


def test_bronze_from_text_has_same_columns(spark):
    assert bronze_df(spark, ["x"]).columns == BRONZE_COLUMNS


def test_parse_maps_fields(spark):
    event = make_event(
        **{
            "meta.dt": "2026-10-31T18:33:08.398Z",
            "length.old": 100,
            "length.new": 120,
            "user": "Alice",
            "bot": True,
            "type": "new",
        }
    )
    row = parse(bronze_df(spark, [json.dumps(event)]), SALT).collect()[0]

    assert row._valid is True
    assert row.event_id == event["meta"]["id"]
    assert row.event_ts == datetime(2026, 10, 31, 18, 33, 8, 398000)
    assert row.event_date == date(2026, 10, 31)
    assert (row.wiki, row.type, row.title, row.is_bot) == ("enwiki", "new", "Example page", True)
    assert row.bytes_delta == 20
    assert row.user_hash == hashlib.sha256(f"{SALT}Alice".encode()).hexdigest()


def test_silver_columns_never_contain_raw_user_or_comment():
    for banned in ("user", "comment", "parsedcomment", "value"):
        assert banned not in SILVER_COLUMNS


def test_missing_optional_fields_give_nulls(spark):
    line = make_line(user=None, length=None)
    row = parse(bronze_df(spark, [line]), SALT).collect()[0]
    assert row._valid is True
    assert row.user_hash is None
    assert row.bytes_delta is None


def test_bad_rows_are_marked_invalid_with_reason(spark):
    lines = ["{broken", make_line(**{"meta.dt": "not a date"}), make_line(title=None)]
    rows = parse(bronze_df(spark, lines), SALT).collect()
    assert [r._valid for r in rows] == [False, False, False]
    assert [r._reject_reason for r in rows] == [
        "unparseable",
        "missing_required",
        "missing_required",
    ]
