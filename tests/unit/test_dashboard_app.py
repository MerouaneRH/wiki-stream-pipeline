from datetime import UTC, datetime, timedelta

import pandas as pd
from deltalake import write_deltalake
from streamlit.testing.v1 import AppTest

APP = "../../dashboard/streamlit_app.py"


def app(monkeypatch, lakehouse):
    monkeypatch.setenv("LAKEHOUSE_DIR", str(lakehouse))
    return AppTest.from_file(APP, default_timeout=20)


def test_shows_waiting_message_when_gold_is_missing(monkeypatch, tmp_path):
    at = app(monkeypatch, tmp_path).run()
    assert not at.exception
    assert any("No gold data yet" in i.value for i in at.info)


def test_renders_kpis_from_gold(monkeypatch, tmp_path):
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    starts = [now - timedelta(minutes=m) for m in (3, 2, 1)]
    edits = pd.DataFrame(
        {
            "window_start": starts,
            "window_end": [s + timedelta(minutes=1) for s in starts],
            "wiki": ["enwiki"] * 3,
            "is_bot": [False] * 3,
            "event_count": [10, 12, 14],
            "edit_count": [8, 9, 10],
            "new_page_count": [1, 1, 1],
            "bytes_added": [100, 100, 100],
            "bytes_removed": [10, 10, 10],
            "unique_editors": [3, 3, 3],
            "updated_at": [now] * 3,
        }
    )
    pages = pd.DataFrame(
        {
            "window_start": starts,
            "window_end": [s + timedelta(minutes=1) for s in starts],
            "wiki": ["enwiki"] * 3,
            "title": ["Page"] * 3,
            "edit_count": [1, 2, 3],
            "updated_at": [now] * 3,
        }
    )
    write_deltalake(str(tmp_path / "gold_edits_per_minute"), edits)
    write_deltalake(str(tmp_path / "gold_page_edits_per_minute"), pages)

    at = app(monkeypatch, tmp_path).run()
    assert not at.exception
    labels = {m.label: m.value for m in at.metric}
    # settled minute = ended >= 60 s ago: the window starting 2 minutes ago (9 + 1)
    assert labels["Edits in the last full minute"] == "10"
    assert labels["Active wikis"] == "1"
    assert len(at.dataframe) == 1
