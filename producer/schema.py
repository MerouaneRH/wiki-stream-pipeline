"""Schema v1 for mediawiki.recentchange events. See docs/spec.md section 4."""

import uuid
from datetime import datetime
from typing import Any

SCHEMA_VERSION = "1"

EVENT_TYPES = frozenset({"edit", "new", "log", "categorize", "external"})


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _iso_datetime(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _strict_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


REQUIRED_FIELDS = (
    ("meta.id", _uuid),
    ("meta.dt", _iso_datetime),
    ("meta.domain", _non_empty_str),
    ("type", lambda v: v in EVENT_TYPES),
    ("wiki", _non_empty_str),
    ("title", _non_empty_str),
    ("bot", lambda v: isinstance(v, bool)),
    ("namespace", _strict_int),
)

_MISSING = object()


def _lookup(event: dict, dotted: str) -> Any:
    value: Any = event
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def validate(event: Any) -> str | None:
    """Return None if the event is valid, else a short reason like 'missing:title'."""
    if not isinstance(event, dict):
        return "invalid:not_object"
    if not isinstance(event.get("meta"), dict):
        return "missing:meta"
    for field, check in REQUIRED_FIELDS:
        value = _lookup(event, field)
        if value is _MISSING:
            return f"missing:{field}"
        if not check(value):
            return f"invalid:{field}"
    return None
