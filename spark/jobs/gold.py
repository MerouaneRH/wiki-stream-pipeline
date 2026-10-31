"""Gold job: silver Delta stream -> per-minute gold tables (update mode + MERGE)."""

from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from spark import storage
from spark.aggregates import (
    EDITS_KEY,
    EDITS_TABLE,
    PAGES_KEY,
    PAGES_TABLE,
    edits_per_minute,
    merge_batch,
    page_edits_per_minute,
)
from spark.config import Config

GOLD = [
    (EDITS_TABLE, EDITS_KEY, edits_per_minute),
    (PAGES_TABLE, PAGES_KEY, page_edits_per_minute),
]


def start_gold(spark: SparkSession, cfg: Config) -> list[StreamingQuery]:
    queries = []
    for table, key, aggregate in GOLD:
        silver = storage.read_stream(spark.readStream, cfg, "silver")
        query = (
            aggregate(silver)
            .writeStream.queryName(table)
            .outputMode("update")
            .foreachBatch(partial(merge_batch, cfg=cfg, table=table, key=key))
            .option("checkpointLocation", cfg.checkpoint(table))
            .trigger(**cfg.trigger)
            .start()
        )
        queries.append(query)
    return queries
