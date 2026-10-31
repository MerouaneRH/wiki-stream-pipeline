"""Decide where each raw event goes: the raw topic or the dead-letter topic."""

import json
from dataclasses import dataclass

from producer.schema import SCHEMA_VERSION, validate

RAW_TOPIC = "wiki.edits.raw"
DLQ_TOPIC = "wiki.edits.dlq"


@dataclass(frozen=True)
class Message:
    topic: str
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]


def route(line: str) -> Message:
    """Route one raw JSON line. The payload is passed through unchanged."""
    value = line.encode("utf-8")
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return Message(DLQ_TOPIC, None, value, [("error", b"invalid_json")])
    reason = validate(event)
    if reason is not None:
        return Message(DLQ_TOPIC, None, value, [("error", reason.encode())])
    return Message(
        RAW_TOPIC,
        event["wiki"].encode("utf-8"),
        value,
        [("schema_version", SCHEMA_VERSION.encode()), ("source", b"eventstreams")],
    )
