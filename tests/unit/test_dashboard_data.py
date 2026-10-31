from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from deltalake import write_deltalake

from dashboard import data

NOW = datetime(2026, 10, 31, 10, 10, 30, tzinfo=UTC)


def minute(m: int) -> datetime:
    return datetime(2026, 10, 31, 10, m, tzinfo=UTC)


def edits_frame(rows):
    """rows: (minute, wiki, is_bot, edit_count, new_page_count, event_count, added, removed)"""
    df = pd.DataFrame(
        rows,
        columns=[
            "m",
            "wiki",
            "is_bot",
            "edit_count",
            "new_page_count",
            "event_count",
            "bytes_added",
            "bytes_removed",
        ],
    )
    df["window_start"] = df.pop("m").map(minute)
    df["window_end"] = df["window_start"] + timedelta(minutes=1)
    df["unique_editors"] = 1
    df["updated_at"] = NOW - timedelta(seconds=12)
    return df


EDITS = edits_frame(
    [
        (8, "enwiki", False, 10, 2, 20, 500, 100),
        (8, "enwiki", True, 4, 0, 10, 50, 0),
        (9, "enwiki", False, 6, 0, 8, 300, 50),
        (9, "frwiki", False, 3, 1, 5, 40, 10),
        (9, "dewiki", True, 1, 0, 1, 0, 5),
        (10, "enwiki", False, 2, 0, 2, 10, 0),  # current, still-open minute
    ]
)


def test_per_minute_splits_bot_and_human_and_counts_edits_plus_new_pages():
    out = data.edits_per_minute(EDITS)
    assert list(out.columns) == ["Human", "Bot"]
    assert out.loc[minute(8), "Human"] == 12
    assert out.loc[minute(8), "Bot"] == 4
    assert out.loc[minute(9), "Bot"] == 1


def test_kpis_use_last_complete_minute_and_previous_for_delta():
    k = data.kpis(EDITS, now=NOW)
    assert k.edits_last_minute == 11  # minute 9: 6 + 4 + 1
    assert k.edits_delta == 11 - 16  # minute 8: 12 + 4
    assert k.bot_share == pytest.approx(5 / 29)  # bots 4+1 over all 29 edits
    assert k.active_wikis == 3
    assert k.net_bytes == (500 + 50 + 300 + 40 + 10) - (100 + 50 + 10 + 5)
    assert k.freshness_s == pytest.approx(12)


def test_kpis_on_empty_frame():
    k = data.kpis(EDITS.iloc[0:0], now=NOW)
    assert (k.edits_last_minute, k.active_wikis, k.net_bytes) == (0, 0, 0)
    assert k.edits_delta is None
    assert k.bot_share is None
    assert k.freshness_s is None


def test_top_wikis():
    out = data.top_wikis(EDITS, n=2)
    assert list(out["wiki"]) == ["enwiki", "frwiki"]
    assert list(out["edits"]) == [24, 4]


def test_fill_minutes_adds_missing_minutes_as_nan_so_lines_break():
    per_minute = data.edits_per_minute(EDITS.drop(index=[2, 3, 4]))  # minute 9 missing
    out = data.fill_minutes(per_minute, since=minute(7), until=minute(10))
    assert list(out.index) == [minute(7), minute(8), minute(9), minute(10)]
    assert out.loc[minute(9)].isna().all()
    assert out.loc[minute(7)].isna().all()
    assert out.loc[minute(8), "Human"] == 12


def test_bytes_per_minute():
    out = data.bytes_per_minute(EDITS)
    assert out.loc[minute(9), "Added"] == 340
    assert out.loc[minute(9), "Removed"] == 65


def test_top_pages():
    pages = pd.DataFrame(
        {
            "window_start": [minute(8), minute(9), minute(9), minute(9)],
            "wiki": ["enwiki", "enwiki", "frwiki", "enwiki"],
            "title": ["A", "A", "A", "B"],
            "edit_count": [2, 3, 4, 1],
        }
    )
    out = data.top_pages(pages, n=2)
    assert out.to_dict("records") == [
        {"wiki": "enwiki", "title": "A", "edits": 5},
        {"wiki": "frwiki", "title": "A", "edits": 4},
    ]


def test_load_reads_only_recent_windows(tmp_path):
    write_deltalake(str(tmp_path / "gold_edits_per_minute"), EDITS)
    out = data.load_table(tmp_path, "gold_edits_per_minute", since=minute(9))
    assert set(out["window_start"]) == {minute(9), minute(10)}


def test_load_missing_table_returns_none(tmp_path):
    assert data.load_table(tmp_path, "gold_edits_per_minute", since=minute(0)) is None
