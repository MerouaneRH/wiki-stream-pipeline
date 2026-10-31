"""Altair charts. Times render in the viewer's local time with a 24-hour axis."""

import altair as alt
import pandas as pd

WHO_COLORS = alt.Scale(domain=["Human", "Bot"], range=["#4c78a8", "#e45756"])
BYTES_COLORS = alt.Scale(domain=["Added", "Removed"], range=["#54a24b", "#f58518"])


# Empty minutes (NaN) break the line but still count toward the x domain, so the axis
# always spans the selected range. Requires Vega-Lite 6 (bundled with Streamlit 1.66).
KEEP_EMPTY = "break-paths-show-domains"
TIME_X = alt.X(
    "window_start:T", axis=alt.Axis(format="%H:%M", title=None, labelAngle=0, labelOverlap=True)
)


def _long(wide: pd.DataFrame, var: str, value: str) -> pd.DataFrame:
    return wide.reset_index().melt(id_vars="window_start", var_name=var, value_name=value)


def edits_line(per_minute: pd.DataFrame) -> alt.Chart:
    """per_minute: index window_start, columns Human/Bot; NaN rows break the line."""
    return (
        alt.Chart(_long(per_minute, "who", "edits"))
        .mark_line(point=True, invalid=KEEP_EMPTY)
        .encode(
            x=TIME_X,
            y=alt.Y("edits:Q", title="Edits"),
            color=alt.Color(
                "who:N", scale=WHO_COLORS, legend=alt.Legend(title=None, orient="bottom")
            ),
            tooltip=[
                alt.Tooltip("window_start:T", format="%H:%M", title="Minute"),
                "who:N",
                "edits:Q",
            ],
        )
    )


def bytes_bars(per_minute: pd.DataFrame) -> alt.Chart:
    """Added above zero, removed below zero."""
    signed = per_minute.assign(Removed=-per_minute["Removed"])
    return (
        alt.Chart(_long(signed, "kind", "bytes"))
        .mark_bar(invalid=KEEP_EMPTY)
        .encode(
            x=TIME_X,
            y=alt.Y("bytes:Q", title="Bytes"),
            color=alt.Color(
                "kind:N", scale=BYTES_COLORS, legend=alt.Legend(title=None, orient="bottom")
            ),
            tooltip=[
                alt.Tooltip("window_start:T", format="%H:%M", title="Minute"),
                "kind:N",
                "bytes:Q",
            ],
        )
    )


def wikis_bars(top: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(top)
        .mark_bar(color="#4c78a8")
        .encode(
            x=alt.X("edits:Q", title="Edits"),
            y=alt.Y("wiki:N", sort="-x", title=None),
            tooltip=["wiki:N", "edits:Q"],
        )
    )
