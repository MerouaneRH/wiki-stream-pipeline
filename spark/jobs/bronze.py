"""Bronze jobs (append, exactly-once via the Delta sink checkpoint).

start_bronze:       Kafka topic wiki.edits.raw -> bronze (local, live)
start_bronze_files: NDJSON replay files -> bronze (Databricks replay; Auto Loader there)
"""

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from producer.routing import RAW_TOPIC
from spark import storage
from spark.config import Config
from spark.transforms import bronze_from_kafka, bronze_from_text


def start_bronze(spark: SparkSession, cfg: Config) -> StreamingQuery:
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 20_000)
        # topic retention is 24 h; if the job is down longer, keep going instead of failing
        .option("failOnDataLoss", "false")
        .load()
    )
    return _write_bronze(bronze_from_kafka(source), cfg)


def start_bronze_files(
    spark: SparkSession, cfg: Config, source_dir: str, *, auto_loader: bool = False
) -> StreamingQuery:
    """Only complete `*.ndjson` files are read; the producer's `.part` files are skipped."""
    if auto_loader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("cloudFiles.maxFilesPerTrigger", 20)
        )
    else:
        reader = spark.readStream.format("text").option("maxFilesPerTrigger", 20)
    source = reader.option("pathGlobFilter", "*.ndjson").load(source_dir)
    return _write_bronze(bronze_from_text(source), cfg)


def _write_bronze(bronze, cfg: Config) -> StreamingQuery:
    writer = (
        bronze.writeStream.queryName("bronze")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint("bronze"))
        .trigger(**cfg.trigger)
    )
    return storage.start(writer, cfg, "bronze")
