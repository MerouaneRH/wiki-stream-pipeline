from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from dashboard import charts

START = datetime(2026, 10, 31, 10, 0, tzinfo=UTC)


def frame(columns) -> pd.DataFrame:
    index = pd.date_range(START, START + timedelta(minutes=15), freq="1min", name="window_start")
    return pd.DataFrame({c: float("nan") for c in columns}, index=index)


@pytest.mark.parametrize(
    ("build", "columns"),
    [(charts.edits_line, ["Human", "Bot"]), (charts.bytes_bars, ["Added", "Removed"])],
)
def test_empty_minutes_keep_the_full_time_axis(build, columns):
    spec = build(frame(columns)).to_dict()
    # NaN minutes must stay in the domain (and break lines), not be filtered out
    assert spec["mark"]["invalid"] == "break-paths-show-domains"
    x = spec["encoding"]["x"]
    assert x["type"] == "temporal"
    assert x["axis"]["format"] == "%H:%M"
    assert "domain" not in x.get("scale", {})  # domain comes from the data itself
    rows = next(iter(spec["datasets"].values()))
    assert len(rows) == 16 * 2  # every minute x both series, including the NaN ones
