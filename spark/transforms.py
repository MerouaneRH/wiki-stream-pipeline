"""Shared bronze/silver transforms. Used by the local jobs and the Databricks notebooks."""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import is_remote

from spark import storage
from spark.config import Config

BRONZE_COLUMNS = ["value", "topic", "partition", "offset", "kafka_ts", "ingest_ts"]

_OLD_NEW = StructType([StructField("old", LongType()), StructField("new", LongType())])

EVENT_SCHEMA = StructType(
    [
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("dt", StringType()),
                    StructField("domain", StringType()),
                ]
            ),
        ),
        StructField("type", StringType()),
        StructField("namespace", LongType()),
        StructField("title", StringType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("length", _OLD_NEW),
        StructField("revision", _OLD_NEW),
        StructField("wiki", StringType()),
        StructField("server_name", StringType()),
        StructField("log_type", StringType()),
        StructField("log_action", StringType()),
    ]
)

SILVER_COLUMNS = [
    "event_id",
    "event_ts",
    "event_date",
    "wiki",
    "domain",
    "type",
    "namespace",
    "title",
    "is_bot",
    "is_minor",
    "user_hash",
    "length_old",
    "length_new",
    "bytes_delta",
    "rev_old",
    "rev_new",
    "server_name",
    "log_type",
    "log_action",
    "kafka_ts",
    "ingest_ts",
]

REJECT_COLUMNS = ["value", "topic", "partition", "offset", "ingest_ts", "reject_reason"]

_REQUIRED = ["event_id", "event_ts", "domain", "type", "wiki", "title", "is_bot", "namespace"]


def bronze_from_kafka(df: DataFrame) -> DataFrame:
    """Kafka source rows -> bronze. The payload is kept as the raw JSON string."""
    return df.select(
        F.col("value").cast("string").alias("value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )


def bronze_from_text(df: DataFrame) -> DataFrame:
    """NDJSON replay files (one `value` column) -> bronze. Used on Databricks."""
    return df.select(
        F.col("value"),
        F.lit(None).cast("string").alias("topic"),
        F.lit(None).cast("int").alias("partition"),
        F.lit(None).cast("long").alias("offset"),
        F.lit(None).cast("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )


BRONZE_SCHEMA = StructType(
    [
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
    ]
)


def ensure_bronze_table(spark: SparkSession, cfg: Config) -> None:
    """Create an empty bronze table so silver can start before bronze has written."""
    storage.create_if_not_exists(spark, cfg, "bronze").addColumns(BRONZE_SCHEMA).execute()


def _user_hash(user: Column, salt: str) -> Column:
    return F.when(user.isNotNull(), F.sha2(F.concat(F.lit(salt), user), 256))


def parse(bronze: DataFrame, salt: str) -> DataFrame:
    """Bronze -> silver columns plus `_valid` and `_reject_reason`."""
    e = F.col("e")
    parsed = bronze.withColumn("e", F.from_json("value", EVENT_SCHEMA, {"mode": "PERMISSIVE"}))
    event_ts = F.try_to_timestamp(e["meta"]["dt"])
    out = parsed.select(
        "*",
        e["meta"]["id"].alias("event_id"),
        event_ts.alias("event_ts"),
        F.to_date(event_ts).alias("event_date"),
        e["wiki"].alias("wiki"),
        e["meta"]["domain"].alias("domain"),
        e["type"].alias("type"),
        e["namespace"].alias("namespace"),
        e["title"].alias("title"),
        e["bot"].alias("is_bot"),
        e["minor"].alias("is_minor"),
        _user_hash(e["user"], salt).alias("user_hash"),
        e["length"]["old"].alias("length_old"),
        e["length"]["new"].alias("length_new"),
        (e["length"]["new"] - e["length"]["old"]).alias("bytes_delta"),
        e["revision"]["old"].alias("rev_old"),
        e["revision"]["new"].alias("rev_new"),
        e["server_name"].alias("server_name"),
        e["log_type"].alias("log_type"),
        e["log_action"].alias("log_action"),
    )
    complete = F.lit(True)
    for name in _REQUIRED:
        complete = complete & F.col(name).isNotNull() & (F.col(name).cast("string") != "")
    return (
        out.withColumn("_valid", F.coalesce(complete, F.lit(False)))
        .withColumn(
            "_reject_reason",
            F.when(F.col("_valid"), None)
            .when(e.isNull() | F.col("e").getField("meta").isNull(), "unparseable")
            .otherwise("missing_required"),
        )
        .drop("e")
    )


def ensure_silver_table(spark: SparkSession, cfg: Config) -> None:
    """Create an empty silver table so gold can start before silver has written."""
    empty = spark.createDataFrame([], BRONZE_SCHEMA)
    (
        storage.create_if_not_exists(spark, cfg, "silver")
        .addColumns(parse(empty, cfg.salt).select(SILVER_COLUMNS).schema)
        .partitionedBy("event_date")
        .execute()
    )


def _persist(df: DataFrame) -> tuple[DataFrame, bool]:
    """Cache if the runtime allows it. Databricks serverless (Spark Connect) rejects caching."""
    if is_remote():
        return df, False
    try:
        return df.persist(), True
    except Exception:  # noqa: BLE001
        return df, False


def write_silver_batch(batch: DataFrame, batch_id: int, *, cfg: Config) -> None:
    """foreachBatch sink: insert-only MERGE on event_id, rejects appended once per batch.

    Insert-only MERGE dedups across batches and producer re-sends without dropping late
    events, and only adds files, so gold can still stream from silver.
    """
    spark = batch.sparkSession
    batch, cached = _persist(batch)
    try:
        parsed = parse(batch, cfg.salt)
        valid = parsed.filter("_valid").select(SILVER_COLUMNS).dropDuplicates(["event_id"])
        ensure_silver_table(spark, cfg)
        (
            storage.delta_table(spark, cfg, "silver")
            .alias("t")
            .merge(valid.alias("s"), "t.event_date = s.event_date AND t.event_id = s.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        rejects = parsed.filter("NOT _valid").select(
            *REJECT_COLUMNS[:-1], F.col("_reject_reason").alias("reject_reason")
        )
        if not rejects.isEmpty():
            writer = rejects.write.option("txnAppId", "silver_rejects").option(
                "txnVersion", batch_id
            )
            storage.append(writer, cfg, "silver_rejects")
    finally:
        if cached:
            batch.unpersist()
