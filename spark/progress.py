"""Readable one-line summaries of StreamingQueryProgress."""

from pyspark.sql.streaming import StreamingQuery


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
