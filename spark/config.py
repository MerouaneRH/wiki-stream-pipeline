"""Job settings from environment variables. Same code runs locally and on Databricks."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from producer.routing import RAW_TOPIC

log = logging.getLogger(__name__)

DEV_SALT = "local-dev-salt"


def parse_trigger(value: str) -> dict:
    """'available_now' -> {'availableNow': True}; '10 seconds' -> {'processingTime': ...}."""
    if value.strip().lower() == "available_now":
        return {"availableNow": True}
    return {"processingTime": value}


@dataclass(frozen=True)
class Config:
    lakehouse: Path
    checkpoints: Path
    bootstrap: str
    trigger: dict
    salt: str
    # "catalog.schema" to use named tables (Databricks Unity Catalog); None = paths.
    namespace: str | None = None
    # Kafka topic read by bronze; benchmarks point this at a throwaway topic.
    topic: str = RAW_TOPIC

    def table(self, name: str) -> str:
        return str(Path(self.lakehouse) / name)

    def name(self, table: str) -> str:
        return f"{self.namespace}.{table}"

    def checkpoint(self, name: str) -> str:
        return str(Path(self.checkpoints) / name)

    @classmethod
    def from_env(cls) -> "Config":
        salt = os.getenv("USER_HASH_SALT")
        if not salt:
            log.warning("USER_HASH_SALT not set, using the dev salt (fine locally only)")
            salt = DEV_SALT
        # Absolute paths: Delta's table builder reads a relative location as a table name.
        return cls(
            lakehouse=Path(os.getenv("LAKEHOUSE_DIR", "lakehouse")).resolve(),
            checkpoints=Path(os.getenv("CHECKPOINT_DIR", "checkpoints")).resolve(),
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            trigger=parse_trigger(os.getenv("TRIGGER", "10 seconds")),
            salt=salt,
            namespace=os.getenv("TABLE_NAMESPACE") or None,
            topic=os.getenv("KAFKA_TOPIC", RAW_TOPIC),
        )
