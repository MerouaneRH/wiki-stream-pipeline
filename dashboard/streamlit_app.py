"""Live dashboard over the gold Delta tables. Run: make dashboard"""

import os
from datetime import UTC, datetime, timedelta

import streamlit as st

from dashboard import charts, data

LAKEHOUSE = os.getenv("LAKEHOUSE_DIR", "lakehouse")
RANGES = {"15 min": 15, "1 hour": 60, "3 hours": 180}

st.set_page_config(
    page_title="Wikipedia live edits", page_icon=":material/edit_note:", layout="wide"
)


@st.cache_data(ttl=5, max_entries=16, show_spinner=False)
def load(lakehouse: str, table: str, since: datetime):
    return data.load_table(lakehouse, table, since)


with st.container(horizontal=True, vertical_alignment="bottom"):
    st.title("Wikipedia live edits", anchor=False)
    range_label = st.segmented_control(
        "Time range", list(RANGES), default="15 min", key="range", label_visibility="collapsed"
    )
st.caption(
    "Every edit on every Wikipedia, per minute (your local time). Refreshes every 10 seconds."
)


@st.fragment(run_every="10s")
def live(minutes: int) -> None:
    now = datetime.now(UTC)
    until = now.replace(second=0, microsecond=0)
    since = until - timedelta(minutes=minutes)
    edits = load(LAKEHOUSE, data.EDITS_TABLE, since)
    if edits is None or edits.empty:
        st.info(
            "No gold data yet. Start the stack, the producer and Spark: "
            "`make up`, `make produce`, `make spark`.",
            title="Waiting for data",
            icon=":material/hourglass_empty:",
        )
        return
    pages = load(LAKEHOUSE, data.PAGES_TABLE, since)
    k = data.kpis(edits, now=now)

    cols = st.columns(5)
    cols[0].metric(
        "Edits in the last full minute",
        f"{k.edits_last_minute:,}",
        k.edits_delta,
        delta_description="vs. the minute before",
        border=True,
        height="stretch",
    )
    cols[1].metric(
        "Bot share",
        "n/a" if k.bot_share is None else f"{k.bot_share:.0%}",
        border=True,
        height="stretch",
    )
    cols[2].metric("Active wikis", f"{k.active_wikis:,}", border=True, height="stretch")
    cols[3].metric("Net bytes added", f"{k.net_bytes:+,}", border=True, height="stretch")
    cols[4].metric(
        "Pipeline freshness",
        "n/a" if k.freshness_s is None else f"{k.freshness_s:.0f} s",
        help="Time since gold was last updated by Spark.",
        border=True,
        height="stretch",
    )

    left, right = st.columns(2)
    with left.container(border=True):
        st.subheader("Edits per minute", anchor=False)
        per_minute = data.fill_minutes(data.edits_per_minute(edits), since, until)
        st.altair_chart(charts.edits_line(per_minute), alt="Edits per minute, humans vs bots")
    with right.container(border=True):
        st.subheader("Bytes added vs removed", anchor=False)
        st.altair_chart(
            charts.bytes_bars(data.bytes_per_minute(edits)),
            alt="Bytes added (up) and removed (down) per minute",
        )

    left, right = st.columns(2)
    with left.container(border=True):
        st.subheader("Most active wikis", anchor=False)
        st.altair_chart(charts.wikis_bars(data.top_wikis(edits)), alt="Top 10 wikis by edits")
    with right.container(border=True):
        st.subheader("Most edited pages", anchor=False)
        if pages is None or pages.empty:
            st.caption("No page edits in this range yet.")
        else:
            top = data.top_pages(pages)
            st.dataframe(
                top,
                hide_index=True,
                column_config={
                    "wiki": st.column_config.TextColumn("Wiki", width="small"),
                    "title": st.column_config.TextColumn("Page"),
                    "edits": st.column_config.ProgressColumn(
                        "Edits", format="%d", min_value=0, max_value=int(top["edits"].max())
                    ),
                },
                alt="Top 10 most edited pages",
            )


live(RANGES[range_label or "15 min"])
