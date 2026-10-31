import json
from datetime import UTC, datetime

import pytest

from bench.metrics import (
    batch_stats,
    make_probe,
    percentiles,
    probe_latencies,
    restamp,
)
from producer.schema import validate
from tests.unit.factories import make_line

NOW = datetime(2026, 10, 31, 21, 0, 5, 123000, tzinfo=UTC)


def test_percentiles_nearest_rank():
    out = percentiles([float(x) for x in range(1, 101)])
    assert (out["n"], out["p50"], out["p95"], out["max"]) == (100, 50.0, 95.0, 100.0)
    assert out["mean"] == pytest.approx(50.5)


def test_percentiles_empty():
    assert percentiles([]) == {"n": 0, "p50": None, "p95": None, "max": None, "mean": None}


def test_probe_latency_is_first_poll_that_includes_the_probe():
    sent = [10.0, 11.0, 12.0]
    polls = [(10.5, 0), (20.0, 1), (25.0, 2), (31.0, 3)]
    latencies, missing = probe_latencies(sent, polls)
    assert latencies == [10.0, 14.0, 19.0]
    assert missing == 0


def test_probe_never_seen_counts_as_missing():
    latencies, missing = probe_latencies([1.0, 2.0], [(5.0, 1)])
    assert latencies == [4.0]
    assert missing == 1


def test_poll_before_send_is_ignored():
    # total already >= 1 from an earlier probe window glitch must not give negative latency
    latencies, _ = probe_latencies([10.0], [(9.0, 1), (12.0, 1)])
    assert latencies == [2.0]


def test_batch_stats_only_counts_batches_with_rows():
    lines = [
        {"name": "bronze", "numInputRows": 100, "durationMs": {"triggerExecution": 400}},
        {"name": "bronze", "numInputRows": 0, "durationMs": {"triggerExecution": 5}},
        {"name": "bronze", "numInputRows": 50, "durationMs": {"triggerExecution": 600}},
        {
            "name": "gold_edits_per_minute",
            "numInputRows": 10,
            "durationMs": {"triggerExecution": 900},
            "stateOperators": [{"numRowsDroppedByWatermark": 3}],
        },
    ]
    stats = batch_stats(lines)
    assert stats["queries"]["bronze"]["batches"] == 2
    assert stats["queries"]["bronze"]["rows"] == 150
    assert stats["queries"]["bronze"]["ms"]["max"] == 600
    assert stats["late_dropped"] == 3


def test_restamp_sets_fresh_time_and_id_and_stays_valid():
    line = make_line()
    out = json.loads(restamp(line, NOW))
    assert out["meta"]["dt"] == "2026-10-31T21:00:05.123Z"
    assert out["meta"]["id"] != json.loads(line)["meta"]["id"]
    assert validate(out) is None


def test_make_probe():
    out = json.loads(make_probe(make_line(), wiki="benchabc", seq=7, now=NOW))
    assert (out["wiki"], out["title"], out["bot"], out["type"]) == (
        "benchabc",
        "probe-7",
        False,
        "edit",
    )
    assert validate(out) is None
