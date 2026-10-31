"""Databricks job: replay NDJSON files from a Unity Catalog volume through
bronze (Auto Loader) -> silver -> gold, using the same code as the local pipeline.

Runs on serverless with Trigger.AvailableNow (ProcessingTime is not supported there).
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from spark.config import DEV_SALT, Config
from spark.jobs.bronze import start_bronze_files
from spark.jobs.gold import start_gold
from spark.jobs.silver import start_silver
from spark.progress import summarize
from spark.transforms import ensure_bronze_table, ensure_silver_table

log = logging.getLogger("replay")

TABLES = [
    "bronze",
    "silver",
    "silver_rejects",
    "gold_edits_per_minute",
    "gold_page_edits_per_minute",
]


def get_salt(scope: str, key: str) -> str:
    """User-hash salt from a Databricks secret scope; dev salt if the scope is missing."""
    try:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as exc:  # noqa: BLE001
        reason = (str(exc).splitlines() or [type(exc).__name__])[0][:200]
        log.warning("secret %s/%s not available (%s); using the dev salt", scope, key, reason)
        return DEV_SALT


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="wiki_stream")
    p.add_argument("--secret-scope", default="wiki-stream")
    args = p.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    namespace = f"{args.catalog}.{args.schema}"
    volumes = f"/Volumes/{args.catalog}/{args.schema}"
    cfg = Config(
        lakehouse=Path("/unused"),  # tables are addressed by name
        checkpoints=Path(f"{volumes}/checkpoints"),
        bootstrap="",
        trigger={"availableNow": True},
        salt=get_salt(args.secret_scope, "user-hash-salt"),
        namespace=namespace,
    )

    ensure_bronze_table(spark, cfg)
    ensure_silver_table(spark, cfg)
    layers = [
        lambda: [start_bronze_files(spark, cfg, f"{volumes}/raw/replay", auto_loader=True)],
        lambda: [start_silver(spark, cfg)],
        lambda: start_gold(spark, cfg),
    ]
    for start in layers:
        for query in start():
            query.awaitTermination()
            log.info("done %s", summarize(query))

    for table in TABLES:
        name = cfg.name(table)
        if spark.catalog.tableExists(name):
            log.info("%s rows=%d", name, spark.table(name).count())
    spark.sql(f"DESCRIBE HISTORY {cfg.name('gold_edits_per_minute')}").select(
        "version", "timestamp", "operation", "operationMetrics.numTargetRowsInserted"
    ).show(10, truncate=False)


if __name__ == "__main__":
    main()
