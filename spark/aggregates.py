"""Gold aggregations: 1-minute tumbling windows on event_ts with a 2-minute watermark.

Queries run in update mode and each micro-batch is MERGEd into Delta on the window key.
Rows are visible after every batch and corrected as late events arrive; events older
than the watermark are dropped from gold (they stay in silver).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark import storage
from spark.config import Config

WINDOW = "1 minute"
USER_NAMESPACES = (2, 3)
WATERMARK = "2 minutes"

EDITS_TABLE = "gold_edits_per_minute"
EDITS_KEY = ["window_start", "wiki", "is_bot"]

PAGES_TABLE = "gold_page_edits_per_minute"
PAGES_KEY = ["window_start", "wiki", "title"]


def _windowed(silver: DataFrame) -> DataFrame:
    return silver.withWatermark("event_ts", WATERMARK)


def _flatten_window(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        *[c for c in df.columns if c != "window"],
    )


def edits_per_minute(silver: DataFrame) -> DataFrame:
    delta = F.col("bytes_delta")
    agg = (
        _windowed(silver)
        .groupBy(F.window("event_ts", WINDOW), "wiki", "is_bot")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.count(F.when(F.col("type") == "edit", 1)).alias("edit_count"),
            F.count(F.when(F.col("type") == "new", 1)).alias("new_page_count"),
            F.coalesce(F.sum(F.when(delta > 0, delta)), F.lit(0)).alias("bytes_added"),
            F.coalesce(F.sum(F.when(delta < 0, -delta)), F.lit(0)).alias("bytes_removed"),
            F.approx_count_distinct("user_hash").alias("unique_editors"),
        )
    )
    return _flatten_window(agg)


def page_edits_per_minute(silver: DataFrame) -> DataFrame:
    agg = (
        _windowed(silver)
        .filter(F.col("type").isin("edit", "new"))
        # User (2) and User talk (3) page titles contain usernames: keep them out of gold
        .filter(~F.col("namespace").isin(*USER_NAMESPACES))
        .groupBy(F.window("event_ts", WINDOW), "wiki", "title")
        .agg(F.count(F.lit(1)).alias("edit_count"))
    )
    return _flatten_window(agg)


def merge_batch(batch: DataFrame, _batch_id: int, *, cfg: Config, table: str, key: list[str]):
    """Upsert one update-mode batch. Rows carry full running totals, so MERGE is idempotent."""
    spark = batch.sparkSession
    rows = batch.withColumn("updated_at", F.current_timestamp())
    storage.create_if_not_exists(spark, cfg, table).addColumns(rows.schema).execute()
    oldest = rows.agg(F.min("window_start")).first()[0]
    if oldest is None:
        return  # empty batch
    # Only windows still open can change, so skip scanning older target rows.
    condition = F.col("t.window_start") >= F.lit(oldest)
    for c in key:
        condition = condition & (F.col(f"t.{c}") == F.col(f"s.{c}"))
    (
        storage.delta_table(spark, cfg, table)
        .alias("t")
        .merge(rows.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
