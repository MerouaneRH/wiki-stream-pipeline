"""SparkSession with Delta Lake (and optionally the Kafka connector)."""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Must match the pinned pyspark version (Scala 2.13 build).
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def build_spark(app_name: str, *, kafka: bool = False, master: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # local[4]: benchmarked 20% lower p95 latency than local[2] (bench/experiments/)
        .master(master or os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.ui.showConsoleProgress", "false")
        # managed (named) tables go here; gitignored
        .config(
            "spark.sql.warehouse.dir",
            str(Path(os.getenv("SPARK_WAREHOUSE_DIR", "spark-warehouse")).resolve()),
        )
    )
    extra = [KAFKA_PACKAGE] if kafka else []
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()
