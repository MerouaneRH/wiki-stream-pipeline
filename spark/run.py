"""Run streaming jobs locally: python -m spark.run bronze silver gold"""

import argparse
import logging
import os
import signal
import threading
import time

from pyspark.sql.streaming import StreamingQuery

from spark.config import Config
from spark.jobs.bronze import start_bronze
from spark.jobs.gold import start_gold
from spark.jobs.silver import start_silver
from spark.progress import JsonlProgressListener, summarize
from spark.session import build_spark
from spark.transforms import ensure_bronze_table, ensure_silver_table

# Pipeline order: each layer reads the one before it.
JOBS = {"bronze": start_bronze, "silver": start_silver, "gold": start_gold}

log = logging.getLogger("spark.run")


def _as_list(result: StreamingQuery | list[StreamingQuery]) -> list[StreamingQuery]:
    return result if isinstance(result, list) else [result]


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("jobs", nargs="+", choices=list(JOBS))
    args = p.parse_args(argv)

    cfg = Config.from_env()
    spark = build_spark("wiki-stream", kafka="bronze" in args.jobs)
    spark.sparkContext.setLogLevel("WARN")
    if progress_log := os.getenv("PROGRESS_LOG"):
        spark.streams.addListener(JsonlProgressListener(progress_log))
    ensure_bronze_table(spark, cfg)
    ensure_silver_table(spark, cfg)
    jobs = sorted(args.jobs, key=list(JOBS).index)
    if cfg.trigger.get("availableNow"):
        # One-shot: each layer must finish before the next one reads it.
        for name in jobs:
            for query in _as_list(JOBS[name](spark, cfg)):
                query.awaitTermination()
                log.info("done %s", summarize(query))
    else:
        for name in jobs:
            JOBS[name](spark, cfg)
        _await_until_signal(spark)


def _await_until_signal(spark) -> None:
    """Block until a query fails or SIGINT/SIGTERM arrives, then stop queries cleanly."""
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        while not stop.is_set():
            if spark.streams.awaitAnyTermination(timeout=2):
                break  # a query ended on its own (error is raised by awaitAnyTermination)
    finally:
        for query in spark.streams.active:
            _wait_for_idle(query)
            log.info("stopping %s", summarize(query))
            query.stop()
        spark.stop()


def _wait_for_idle(query: StreamingQuery, timeout_s: float = 60) -> None:
    """Let the running micro-batch finish; stopping mid-batch interrupts foreachBatch."""
    deadline = time.monotonic() + timeout_s
    while query.isActive and query.status["isTriggerActive"] and time.monotonic() < deadline:
        time.sleep(0.2)


if __name__ == "__main__":
    main()
