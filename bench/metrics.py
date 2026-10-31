"""Pure helpers for the benchmark: event restamping, probe latency, batch statistics."""

import json
import math
import uuid
from collections.abc import Iterable
from datetime import datetime


def percentiles(values: list[float]) -> dict:
    """Nearest-rank p50/p95, max and mean."""
    if not values:
        return {"n": 0, "p50": None, "p95": None, "max": None, "mean": None}
    ordered = sorted(values)

    def rank(p: float) -> float:
        return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]

    return {
        "n": len(ordered),
        "p50": round(rank(50), 3),
        "p95": round(rank(95), 3),
        "max": round(ordered[-1], 3),
        "mean": round(sum(ordered) / len(ordered), 3),
    }


def probe_latencies(sent: list[float], polls: list[tuple[float, int]]) -> tuple[list[float], int]:
    """Latency of probe i = first poll at or after its send time whose gold total >= i + 1.

    sent: send time of each probe, in order. polls: (time, total probe count seen in gold).
    Returns (latencies of probes that were seen, number never seen).
    """
    polls = sorted(polls)
    latencies: list[float] = []
    missing = 0
    for i, sent_at in enumerate(sent):
        seen = next((t for t, total in polls if t >= sent_at and total >= i + 1), None)
        if seen is None:
            missing += 1
        else:
            latencies.append(round(seen - sent_at, 3))
    return latencies, missing


def batch_stats(progress: Iterable[dict]) -> dict:
    """Micro-batch duration per query (batches that read rows) and late rows dropped."""
    durations: dict[str, list[float]] = {}
    rows: dict[str, int] = {}
    dropped = 0
    for p in progress:
        for op in p.get("stateOperators") or []:
            dropped += op.get("numRowsDroppedByWatermark") or 0
        n = p.get("numInputRows") or 0
        if n <= 0:
            continue
        name = p.get("name") or "unnamed"
        durations.setdefault(name, []).append(p["durationMs"]["triggerExecution"])
        rows[name] = rows.get(name, 0) + n
    return {
        "queries": {
            name: {"batches": len(ms), "rows": rows[name], "ms": percentiles(ms)}
            for name, ms in sorted(durations.items())
        },
        "late_dropped": dropped,
    }


def _iso_ms(now: datetime) -> str:
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def restamp(line: str, now: datetime) -> str:
    """Give a recorded event a fresh event time and id so it is on time and not a dup."""
    event = json.loads(line)
    event["meta"]["dt"] = _iso_ms(now)
    event["meta"]["id"] = str(uuid.uuid4())
    return json.dumps(event)


def make_probe(template: str, *, wiki: str, seq: int, now: datetime) -> str:
    event = json.loads(restamp(template, now))
    event.update({"wiki": wiki, "title": f"probe-{seq}", "bot": False, "type": "edit"})
    return json.dumps(event)
