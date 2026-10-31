from spark.config import Config
from spark.jobs.silver import start_silver
from spark.transforms import bronze_from_text, write_silver_batch
from tests.unit.factories import make_line


def config(tmp_path):
    return Config(
        lakehouse=tmp_path / "lakehouse",
        checkpoints=tmp_path / "checkpoints",
        bootstrap="unused:9092",
        trigger={"availableNow": True},
        salt="test-salt",
    )


def append_bronze(spark, cfg, lines):
    df = bronze_from_text(spark.createDataFrame([(line,) for line in lines], "value string"))
    df.write.format("delta").mode("append").save(cfg.table("bronze"))


def run_silver(spark, cfg):
    query = start_silver(spark, cfg)
    query.awaitTermination()
    assert query.exception() is None


def silver(spark, cfg):
    return spark.read.format("delta").load(cfg.table("silver"))


def rejects(spark, cfg):
    return spark.read.format("delta").load(cfg.table("silver_rejects"))


def test_batch_dedups_on_event_id_and_is_idempotent(spark, tmp_path):
    cfg = config(tmp_path)
    a, b = make_line(), make_line()
    batch = bronze_from_text(spark.createDataFrame([(a,), (a,), (b,), ("{bad",)], "value string"))

    write_silver_batch(batch, 0, cfg=cfg)
    write_silver_batch(batch, 0, cfg=cfg)  # retry of the same micro-batch
    write_silver_batch(batch, 1, cfg=cfg)  # same events in a later batch (producer re-send)

    assert silver(spark, cfg).count() == 2
    assert silver(spark, cfg).select("event_id").distinct().count() == 2
    assert rejects(spark, cfg).count() == 2  # batch 0 once + batch 1 once


def test_stream_bronze_to_silver_and_restart(spark, tmp_path):
    cfg = config(tmp_path)
    first = [make_line(wiki="enwiki") for _ in range(20)]
    append_bronze(spark, cfg, first + ["{bad"])
    run_silver(spark, cfg)
    assert silver(spark, cfg).count() == 20
    assert rejects(spark, cfg).count() == 1

    # restart with no new data: nothing changes
    run_silver(spark, cfg)
    assert silver(spark, cfg).count() == 20

    # new data plus duplicates of old events
    second = [make_line(wiki="frwiki") for _ in range(5)]
    append_bronze(spark, cfg, second + first[:3])
    run_silver(spark, cfg)
    df = silver(spark, cfg)
    assert df.count() == 25
    assert {
        r.wiki: r.n for r in df.groupBy("wiki").count().withColumnRenamed("count", "n").collect()
    } == {"enwiki": 20, "frwiki": 5}


def test_silver_is_partitioned_by_event_date(spark, tmp_path):
    cfg = config(tmp_path)
    append_bronze(spark, cfg, [make_line()])
    run_silver(spark, cfg)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{cfg.table('silver')}`").collect()[0]
    assert detail.partitionColumns == ["event_date"]
