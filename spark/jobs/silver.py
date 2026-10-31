"""Silver job: bronze Delta stream -> parsed, deduplicated silver (+ silver_rejects)."""

from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from spark import storage
from spark.config import Config
from spark.transforms import write_silver_batch


def start_silver(spark: SparkSession, cfg: Config) -> StreamingQuery:
    source = storage.read_stream(spark.readStream.option("maxFilesPerTrigger", 50), cfg, "bronze")
    return (
        source.writeStream.queryName("silver")
        .foreachBatch(partial(write_silver_batch, cfg=cfg))
        .option("checkpointLocation", cfg.checkpoint("silver"))
        .trigger(**cfg.trigger)
        .start()
    )
