"""Benchmark the local pipeline. Needs `make up` and recorded files in data/replay/.

    uv run python -m bench.run_bench --rates 50 500 --duration 180

Each scenario uses its own Kafka topics, lakehouse and checkpoints (under data/bench/),
so live data is never touched. Results: bench/results.json (first run also baseline.json).
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.compute as pc
from confluent_kafka.admin import AdminClient, NewTopic
from deltalake import DeltaTable

from bench.metrics import (
    batch_stats,
    compare,
    make_probe,
    percentiles,
    probe_latencies,
    restamp,
)
from producer.main import KafkaSink, run
from producer.replay import iter_file
from producer.routing import DLQ_TOPIC, RAW_TOPIC

WORK = Path("data/bench")
RESULTS = Path("bench/results.json")
BASELINE = Path("bench/baseline.json")
QUERIES = {"bronze", "silver", "gold_edits_per_minute", "gold_page_edits_per_minute"}
TABLES = ["bronze", "silver", "gold_edits_per_minute", "gold_page_edits_per_minute"]


def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def git_info() -> dict:
    def git(*args: str) -> str:
        # fixed git subcommands only, no external input
        cmd = ["git", *args]
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout  # noqa: S603

    diff = git("diff", "HEAD")
    untracked = git("ls-files", "--others", "--exclude-standard")
    dirty = bool(diff or untracked)
    return {
        "commit": git("rev-parse", "HEAD").strip(),
        "dirty": dirty,
        # identifies uncommitted changes when dirty
        "worktree_sha256": hashlib.sha256((diff + untracked).encode()).hexdigest()
        if dirty
        else None,
    }


class Topics:
    def __init__(self, bootstrap: str, run_id: str):
        self.admin = AdminClient({"bootstrap.servers": bootstrap})
        self.map = {RAW_TOPIC: f"bench.{run_id}.raw", DLQ_TOPIC: f"bench.{run_id}.dlq"}

    def __enter__(self) -> dict:
        new = [NewTopic(self.map[RAW_TOPIC], 3, 1), NewTopic(self.map[DLQ_TOPIC], 1, 1)]
        for f in self.admin.create_topics(new).values():
            f.result(10)
        return self.map

    def __exit__(self, *_):
        for f in self.admin.delete_topics(list(self.map.values())).values():
            f.result(10)


def producer_max_throughput(bootstrap: str, lines: list[str]) -> dict:
    with Topics(bootstrap, uuid.uuid4().hex[:8]) as topic_map:
        started = time.perf_counter()
        counts = run(
            ((line, None) for line in lines), KafkaSink(bootstrap, topic_map), checkpoint_every=1000
        )
        seconds = time.perf_counter() - started
    return {
        "events": counts.raw + counts.dlq,
        "seconds": round(seconds, 3),
        "events_per_s": round((counts.raw + counts.dlq) / seconds),
    }


class GoldPoller(threading.Thread):
    """Records (time, probe events visible in gold) every interval."""

    def __init__(self, table: Path, wiki: str, interval: float = 0.5):
        super().__init__(daemon=True)
        self.table, self.wiki, self.interval = table, wiki, interval
        self.polls: list[tuple[float, int]] = []
        self.errors = 0
        self.stop_event = threading.Event()

    def total(self) -> int:
        if not (self.table / "_delta_log").exists():
            return 0
        data = (
            DeltaTable(str(self.table))
            .to_pyarrow_dataset()
            .to_table(filter=pc.field("wiki") == self.wiki, columns=["event_count"])
        )
        return int(pc.sum(data["event_count"]).as_py() or 0)

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                self.polls.append((time.time(), self.total()))
            except Exception:  # noqa: BLE001  (table mid-commit; next poll will catch up)
                self.errors += 1


def paced_source(lines, rate: float, duration: float, probe_wiki: str, sent: list[float]):
    """Yield restamped events at `rate`/s plus one probe per second for `duration` s."""
    start = time.time()
    next_probe = start
    i = 0
    while (now := time.time()) < start + duration:
        if now >= next_probe:
            sent.append(now)
            yield make_probe(lines[0], wiki=probe_wiki, seq=len(sent), now=datetime.now(UTC)), None
            next_probe += 1
        due = int((now - start) * rate)
        while i < due:
            yield restamp(lines[i % len(lines)], datetime.now(UTC)), None
            i += 1
        time.sleep(0.005)


def wait_for(predicate, timeout: float, step: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return False


def read_progress(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def run_scenario(name: str, rate: float, duration: float, bootstrap: str, lines: list[str]) -> dict:
    run_id = uuid.uuid4().hex[:8]
    work = WORK / f"{name}-{run_id}"
    progress = work / "progress.jsonl"
    work.mkdir(parents=True)
    probe_wiki = f"bench{run_id}"
    with Topics(bootstrap, run_id) as topic_map:
        env = os.environ | {
            "LAKEHOUSE_DIR": str(work / "lakehouse"),
            "CHECKPOINT_DIR": str(work / "checkpoints"),
            "SPARK_WAREHOUSE_DIR": str(work / "warehouse"),
            "KAFKA_TOPIC": topic_map[RAW_TOPIC],
            "KAFKA_BOOTSTRAP": bootstrap,
            "PROGRESS_LOG": str(progress),
            "SPARK_LOCAL_IP": "127.0.0.1",
        }
        log(f"[{name}] starting Spark")
        with (work / "spark.log").open("w") as spark_log:
            spark = subprocess.Popen(
                [sys.executable, "-m", "spark.run", "bronze", "silver", "gold"],
                env=env,
                stdout=spark_log,
                stderr=subprocess.STDOUT,
            )
        try:
            started = wait_for(lambda: QUERIES <= {p["name"] for p in read_progress(progress)}, 180)
            if not started:
                raise RuntimeError(f"Spark did not start, see {work / 'spark.log'}")
            poller = GoldPoller(work / "lakehouse" / "gold_edits_per_minute", probe_wiki)
            poller.start()
            log(f"[{name}] producing {rate}/s for {duration}s")
            sent: list[float] = []
            counts = run(
                paced_source(lines, rate, duration, probe_wiki, sent),
                KafkaSink(bootstrap, topic_map),
                checkpoint_every=max(1, int(rate)),
            )
            log(f"[{name}] sent {counts.raw} events ({len(sent)} probes), draining")
            drained = wait_for(lambda: poller.polls and poller.polls[-1][1] >= len(sent), 120, 0.5)
            poller.stop_event.set()
            poller.join()
        finally:
            spark.send_signal(signal.SIGTERM)
            try:
                spark.wait(90)
            except subprocess.TimeoutExpired:
                spark.kill()
    latencies, missing = probe_latencies(sent, poller.polls)
    return {
        "name": name,
        "rate_per_s": rate,
        "duration_s": duration,
        "events_sent": counts.raw,
        "dlq": counts.dlq,
        "drained": drained,
        "probe_latency_s": percentiles(latencies) | {"missing": missing},
        "poll_errors": poller.errors,
        "micro_batches": batch_stats(read_progress(progress)),
        "_lakehouse": str(work / "lakehouse"),
    }


def optimize_stats(lakehouses: dict[str, str]) -> dict:
    """Delta file counts before/after OPTIMIZE, per scenario and table."""
    from spark.session import build_spark

    spark = build_spark("bench-optimize")
    spark.sparkContext.setLogLevel("ERROR")
    out = {}
    for name, lakehouse in lakehouses.items():
        out[name] = {}
        for table in TABLES:
            path = str(Path(lakehouse).resolve() / table)

            def files(p=path) -> int:
                return spark.sql(f"DESCRIBE DETAIL delta.`{p}`").first().numFiles

            before = files()
            started = time.perf_counter()
            spark.sql(f"OPTIMIZE delta.`{path}`").collect()
            out[name][table] = {
                "files_before": before,
                "files_after": files(),
                "optimize_s": round(time.perf_counter() - started, 2),
            }
    spark.stop()
    return out


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--rates", type=float, nargs="+", default=[50, 500])
    p.add_argument("--duration", type=float, default=180)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092"))
    p.add_argument("--replay-dir", default="data/replay")
    p.add_argument(
        "--out", default=str(RESULTS), help="results file (experiments: bench/experiments/)"
    )
    args = p.parse_args()

    lines = [line for f in sorted(Path(args.replay_dir).glob("*.ndjson")) for line in iter_file(f)]
    if not lines:
        sys.exit(f"no replay files in {args.replay_dir}; run `make produce` first")
    AdminClient({"bootstrap.servers": args.bootstrap}).list_topics(timeout=5)  # fail fast
    shutil.rmtree(WORK, ignore_errors=True)

    results = {
        "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "git": git_info(),
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
        },
        "spark": {
            "master": os.getenv("SPARK_MASTER", "local[4]"),
            "trigger": os.getenv("TRIGGER", "10 seconds"),
            "shuffle_partitions": 4,
            "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        },
    }
    log(f"producer max throughput on {len(lines)} recorded events")
    results["producer_max_throughput"] = producer_max_throughput(args.bootstrap, lines)
    scenarios = [
        run_scenario(f"rate_{int(r)}", r, args.duration, args.bootstrap, lines) for r in args.rates
    ]
    log("OPTIMIZE file counts")
    optimize = optimize_stats({s["name"]: s.pop("_lakehouse") for s in scenarios})
    for s in scenarios:
        s["optimize"] = optimize[s["name"]]
    results["scenarios"] = scenarios

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2) + "\n")
    if not BASELINE.exists():
        shutil.copy(out, BASELINE)
        log(f"wrote {out} and {BASELINE} (first run = baseline)")
    else:
        log(f"wrote {out}")
    for s in scenarios:
        lat = s["probe_latency_s"]
        log(
            f"{s['name']}: p50={lat['p50']}s p95={lat['p95']}s max={lat['max']}s "
            f"missing={lat['missing']}"
        )
    if BASELINE.exists() and out != BASELINE:
        log("vs baseline:")
        for name, metric, old, new, change in compare(json.loads(BASELINE.read_text()), results):
            pct = "n/a" if change is None else f"{change:+}%"
            log(f"  {name:9} {metric:38} {old!s:>9} -> {new!s:>9}  ({pct})")


if __name__ == "__main__":
    main()
