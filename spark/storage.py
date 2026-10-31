"""Address Delta tables by path (local) or by catalog name (Databricks Unity Catalog).

Config.namespace = None   -> paths under Config.lakehouse
Config.namespace = "c.s"  -> managed tables c.s.<name>
"""

from delta.tables import DeltaTable, DeltaTableBuilder
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery

from spark.config import Config


def create_if_not_exists(spark: SparkSession, cfg: Config, name: str) -> DeltaTableBuilder:
    builder = DeltaTable.createIfNotExists(spark)
    if cfg.namespace:
        return builder.tableName(cfg.name(name))
    return builder.location(cfg.table(name))


def delta_table(spark: SparkSession, cfg: Config, name: str) -> DeltaTable:
    if cfg.namespace:
        return DeltaTable.forName(spark, cfg.name(name))
    return DeltaTable.forPath(spark, cfg.table(name))


def read_stream(reader: DataStreamReader, cfg: Config, name: str) -> DataFrame:
    reader = reader.format("delta")
    return reader.table(cfg.name(name)) if cfg.namespace else reader.load(cfg.table(name))


def read(spark: SparkSession, cfg: Config, name: str) -> DataFrame:
    if cfg.namespace:
        return spark.read.table(cfg.name(name))
    return spark.read.format("delta").load(cfg.table(name))


def start(writer: DataStreamWriter, cfg: Config, name: str) -> StreamingQuery:
    writer = writer.format("delta")
    return writer.toTable(cfg.name(name)) if cfg.namespace else writer.start(cfg.table(name))


def append(writer: DataFrameWriter, cfg: Config, name: str) -> None:
    writer = writer.format("delta").mode("append")
    if cfg.namespace:
        writer.saveAsTable(cfg.name(name))
    else:
        writer.save(cfg.table(name))
