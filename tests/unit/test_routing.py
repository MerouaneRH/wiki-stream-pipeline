import json

from producer.routing import DLQ_TOPIC, RAW_TOPIC, route
from tests.unit.factories import make_line


def headers(message):
    return dict(message.headers)


def test_valid_event_goes_to_raw_topic_keyed_by_wiki():
    line = make_line(wiki="frwiki")
    message = route(line)
    assert message.topic == RAW_TOPIC == "wiki.edits.raw"
    assert message.key == b"frwiki"
    assert message.value == line.encode()
    assert headers(message) == {"schema_version": b"1", "source": b"eventstreams"}


def test_raw_payload_is_not_rewritten():
    line = '{"meta": {"id": "00000000-0000-4000-8000-000000000001", "dt": "2026-10-31T18:33:08Z",'
    line += ' "domain": "x"},  "type": "edit", "wiki": "w", "title": "t", "bot": true,'
    line += ' "namespace": 0}'
    assert route(line).value == line.encode()


def test_invalid_json_goes_to_dlq_with_reason():
    message = route("{not json")
    assert message.topic == DLQ_TOPIC == "wiki.edits.dlq"
    assert message.key is None
    assert message.value == b"{not json"
    assert headers(message)["error"] == b"invalid_json"


def test_schema_violation_goes_to_dlq_with_reason():
    line = make_line(title="")
    message = route(line)
    assert message.topic == DLQ_TOPIC
    assert message.value == line.encode()
    assert headers(message)["error"] == b"invalid:title"


def test_non_object_json_goes_to_dlq():
    message = route(json.dumps([1, 2]))
    assert message.topic == DLQ_TOPIC
    assert headers(message)["error"] == b"invalid:not_object"
