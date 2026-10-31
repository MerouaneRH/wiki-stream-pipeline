"""Readable one-line summaries of StreamingQueryProgress."""

import threading

from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener


def _field(obj, key):
    """Read a progress field from a dict (classic Spark) or an object (Spark Connect)."""
    value = obj.get(key) if hasattr(obj, "get") else None
    return value if value is not None else getattr(obj, key, None)


def summarize(query: StreamingQuery) -> str:
    """One-line summary of the last micro-batch (StreamingQueryProgress)."""
    p = query.lastProgress or {}
    operators = _field(p, "stateOperators") or []
    dropped = sum(_field(op, "numRowsDroppedByWatermark") or 0 for op in operators)
    duration = _field(p, "durationMs") or {}
    return (
        f"{query.name}: batch={_field(p, 'batchId')} rows={_field(p, 'numInputRows')} "
        f"ms={_field(duration, 'triggerExecution')} dropped_late={dropped}"
    )


class JsonlProgressListener(StreamingQueryListener):
    """Appends every StreamingQueryProgress as one JSON line (used by the benchmark)."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        with self._lock, open(self.path, "a", encoding="utf-8") as f:
            f.write(event.progress.json.replace("\n", " ") + "\n")

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass
