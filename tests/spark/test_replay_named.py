"""The Databricks replay path, run locally: NDJSON files -> bronze -> silver -> gold,
with tables addressed by name (as Unity Catalog tables are) instead of by path."""

import uuid

from spark import storage
from spark.config import Config
from spark.jobs.bronze import start_bronze_files
from spark.jobs.gold import start_gold
from spark.jobs.silver import start_silver
from spark.transforms import ensure_bronze_table, ensure_silver_table, write_silver_batch
from tests.spark.test_gold import edit


def run_all(spark, cfg, source_dir):
    ensure_bronze_table(spark, cfg)
    ensure_silver_table(spark, cfg)
    for start in (
        lambda: [start_bronze_files(spark, cfg, source_dir)],
        lambda: [start_silver(spark, cfg)],
        lambda: start_gold(spark, cfg),
    ):
        for query in start():
            query.awaitTermination()
            assert query.exception() is None


def test_files_to_gold_with_named_tables(spark, tmp_path):
    db = f"replay_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db} LOCATION '{tmp_path / 'warehouse'}'")
    cfg = Config(
        lakehouse=tmp_path / "unused",
        checkpoints=tmp_path / "checkpoints",
        bootstrap="unused:9092",
        trigger={"availableNow": True},
        salt="test-salt",
        namespace=db,
    )
    source = tmp_path / "replay"
    source.mkdir()
    (source / "events-1.ndjson").write_text("\n".join([edit("00:10"), edit("00:20")]) + "\n")
    (source / "events-2.ndjson").write_text("\n".join([edit("01:10"), "{bad"]) + "\n")
    (source / "events-3.part").write_text(edit("02:10") + "\n")  # incomplete: must be skipped

    run_all(spark, cfg, str(source))

    assert spark.table(f"{db}.bronze").count() == 4
    assert spark.table(f"{db}.silver").count() == 3
    assert spark.table(f"{db}.silver_rejects").count() == 1
    gold = {
        r.window_start.strftime("%H:%M"): r.event_count
        for r in spark.table(f"{db}.gold_edits_per_minute").collect()
    }
    assert gold == {"10:00": 2, "10:01": 1}
    assert not (tmp_path / "unused").exists()  # nothing written by path

    # rerun: idempotent
    run_all(spark, cfg, str(source))
    assert storage.read(spark, cfg, "silver").count() == 3

    # rejects append is idempotent by batch id in name mode too
    batch = spark.table(f"{db}.bronze").filter("value = '{bad'")
    write_silver_batch(batch, 99, cfg=cfg)
    write_silver_batch(batch, 99, cfg=cfg)
    assert spark.table(f"{db}.silver_rejects").count() == 2
