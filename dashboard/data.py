"""Read gold Delta tables with delta-rs (no Spark) and shape them for the dashboard.

Pure pandas functions so they can be unit-tested without Streamlit.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

EDITS_TABLE = "gold_edits_per_minute"
PAGES_TABLE = "gold_page_edits_per_minute"

# Gold lags the clock (benchmark p95 producer->gold ~34 s with the default config).
# A minute counts as complete only once it ended at least this long ago.
SETTLE = timedelta(seconds=60)


def load_table(lakehouse: str | Path, name: str, since: datetime) -> pd.DataFrame | None:
    """Rows with window_start >= since, or None if the table does not exist yet."""
    path = Path(lakehouse) / name
    if not (path / "_delta_log").exists():
        return None
    dataset = DeltaTable(str(path)).to_pyarrow_dataset()
    column_type = dataset.schema.field("window_start").type
    since_scalar = pa.scalar(since, type=column_type)
    return dataset.to_table(filter=pc.field("window_start") >= since_scalar).to_pandas()


def _edits(df: pd.DataFrame) -> pd.Series:
    """'Edits' = page edits + new pages (categorize and log events are not edits)."""
    return df["edit_count"] + df["new_page_count"]


def edits_per_minute(df: pd.DataFrame) -> pd.DataFrame:
    """Index window_start, columns Human and Bot."""
    out = (
        df.assign(edits=_edits(df), who=df["is_bot"].map({True: "Bot", False: "Human"}))
        .pivot_table(index="window_start", columns="who", values="edits", aggfunc="sum")
        .reindex(columns=["Human", "Bot"])
        .fillna(0)
        .astype(int)
    )
    out.columns.name = None
    return out


def complete_minutes(df: pd.DataFrame, now: datetime, settle: timedelta = SETTLE) -> pd.DataFrame:
    """Keep settled minutes only, so charts don't show a misleading dip at the end."""
    return df[df["window_end"] <= now - settle]


def fill_minutes(per_minute: pd.DataFrame, since: datetime, until: datetime) -> pd.DataFrame:
    """Reindex to every minute in [since, until]. Minutes with no gold row become NaN
    (pipeline down or no data), so charts show a gap instead of a straight line."""
    minutes = pd.date_range(since, until, freq="1min", name="window_start")
    return per_minute.astype(float).reindex(minutes)


def bytes_per_minute(df: pd.DataFrame) -> pd.DataFrame:
    """Index window_start, columns Added and Removed."""
    return (
        df.groupby("window_start")[["bytes_added", "bytes_removed"]]
        .sum()
        .rename(columns={"bytes_added": "Added", "bytes_removed": "Removed"})
    )


def top_wikis(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    return (
        df.assign(edits=_edits(df))
        .groupby("wiki", as_index=False)["edits"]
        .sum()
        .sort_values(["edits", "wiki"], ascending=[False, True])
        .head(n)
        .reset_index(drop=True)
    )


def top_pages(pages: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    return (
        pages.groupby(["wiki", "title"], as_index=False)["edit_count"]
        .sum()
        .rename(columns={"edit_count": "edits"})
        .sort_values(["edits", "wiki", "title"], ascending=[False, True, True])
        .head(n)
        .reset_index(drop=True)
    )


@dataclass(frozen=True)
class Kpis:
    edits_last_minute: int
    edits_delta: int | None
    bot_share: float | None
    active_wikis: int
    net_bytes: int
    freshness_s: float | None


def kpis(df: pd.DataFrame, now: datetime, settle: timedelta = SETTLE) -> Kpis:
    """Headline numbers. 'Last minute' is the latest settled window (see SETTLE)."""
    if df.empty:
        return Kpis(0, None, None, 0, 0, None)
    edits = df.assign(edits=_edits(df))
    settled = edits[edits["window_end"] <= now - settle]
    per_minute = settled.groupby("window_start")["edits"].sum()
    last = prev = None
    if not per_minute.empty:
        last_start = per_minute.index.max()
        last = int(per_minute[last_start])
        prev_start = last_start - timedelta(minutes=1)
        prev = int(per_minute[prev_start]) if prev_start in per_minute.index else None
    total = int(edits["edits"].sum())
    bots = int(edits.loc[edits["is_bot"], "edits"].sum())
    return Kpis(
        edits_last_minute=last or 0,
        edits_delta=None if last is None or prev is None else last - prev,
        bot_share=bots / total if total else None,
        active_wikis=int(df["wiki"].nunique()),
        net_bytes=int(df["bytes_added"].sum() - df["bytes_removed"].sum()),
        freshness_s=(now - df["updated_at"].max()).total_seconds(),
    )
