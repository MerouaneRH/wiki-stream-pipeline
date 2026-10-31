"""Synthetic recentchange events. No real usernames or comments."""

import copy
import json
import uuid

BASE_EVENT = {
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
        "id": "00000000-0000-4000-8000-000000000001",
        "dt": "2026-10-31T18:33:08.398Z",
        "domain": "en.wikipedia.org",
        "stream": "mediawiki.recentchange",
    },
    "id": 1,
    "type": "edit",
    "namespace": 0,
    "title": "Example page",
    "user": "SyntheticUser",
    "bot": False,
    "minor": False,
    "length": {"old": 100, "new": 120},
    "revision": {"old": 10, "new": 11},
    "wiki": "enwiki",
    "server_name": "en.wikipedia.org",
    "comment": "synthetic comment",
}


def make_event(**overrides) -> dict:
    """Return a valid event. Dotted keys like 'meta.dt' set nested fields; None deletes."""
    event = copy.deepcopy(BASE_EVENT)
    event["meta"]["id"] = str(uuid.uuid4())
    for key, value in overrides.items():
        target = event
        parts = key.split(".")
        for part in parts[:-1]:
            target = target[part]
        if value is None:
            target.pop(parts[-1], None)
        else:
            target[parts[-1]] = value
    return event


def make_line(**overrides) -> str:
    return json.dumps(make_event(**overrides))
