import requests

from producer.sse import EventStreamClient, SSEEvent, backoff_delay, parse_sse


def test_parse_single_event():
    lines = ["event: message", 'id: [{"offset":1}]', 'data: {"a": 1}', ""]
    assert list(parse_sse(lines)) == [
        SSEEvent(id='[{"offset":1}]', event="message", data='{"a": 1}')
    ]


def test_parse_ignores_comments_and_joins_multiline_data():
    lines = [":ok", "", "data: line1", "data: line2", "", ":heartbeat"]
    assert list(parse_sse(lines)) == [SSEEvent(id=None, event="message", data="line1\nline2")]


def test_parse_skips_events_without_data():
    assert list(parse_sse(["id: 5", ""])) == []


def test_parse_handles_value_without_space_and_bytes():
    assert list(parse_sse([b"data:x", b""])) == [SSEEvent(id=None, event="message", data="x")]


def test_backoff_is_capped_and_grows():
    assert backoff_delay(0, base=1, cap=60, rand=lambda a, b: b) == 1
    assert backoff_delay(3, base=1, cap=60, rand=lambda a, b: b) == 8
    assert backoff_delay(20, base=1, cap=60, rand=lambda a, b: b) == 60


class FakeResponse:
    def __init__(self, lines, fail_after=False):
        self.lines = lines
        self.fail_after = fail_after
        self.status_code = 200

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=True):
        yield from self.lines
        if self.fail_after:
            raise requests.ConnectionError("server closed connection")

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, headers=None, stream=None, timeout=None):
        self.calls.append(dict(headers or {}))
        if not self.responses:
            raise requests.ConnectionError("no more responses")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def sse_lines(event_id, data):
    return [f"id: {event_id}", f"data: {data}", ""]


def test_client_resumes_with_last_event_id_after_disconnect():
    session = FakeSession(
        [
            FakeResponse(sse_lines("id-1", "a") + sse_lines("id-2", "b"), fail_after=True),
            FakeResponse(sse_lines("id-3", "c")),
        ]
    )
    sleeps = []
    client = EventStreamClient(
        "https://example.org/stream", session=session, user_agent="test/0.1", sleep=sleeps.append
    )
    received = []
    for event in client.events():
        received.append(event.data)
        if len(received) == 3:
            break

    assert received == ["a", "b", "c"]
    assert "Last-Event-ID" not in session.calls[0]
    assert session.calls[1]["Last-Event-ID"] == "id-2"
    assert session.calls[0]["User-Agent"] == "test/0.1"
    assert client.last_event_id == "id-3"
    assert len(sleeps) == 1


def test_client_starts_from_saved_last_event_id():
    session = FakeSession([FakeResponse(sse_lines("id-9", "z"))])
    client = EventStreamClient(
        "https://example.org/stream",
        session=session,
        user_agent="test/0.1",
        last_event_id="id-8",
        sleep=lambda s: None,
    )
    assert next(client.events()).data == "z"
    assert session.calls[0]["Last-Event-ID"] == "id-8"


def test_client_backs_off_on_repeated_connect_errors_then_recovers():
    session = FakeSession(
        [
            requests.ConnectionError("down"),
            requests.ConnectionError("down"),
            FakeResponse(sse_lines("id-1", "ok")),
        ]
    )
    sleeps = []
    client = EventStreamClient(
        "https://example.org/stream",
        session=session,
        user_agent="test/0.1",
        sleep=sleeps.append,
        rand=lambda a, b: b,
    )
    assert next(client.events()).data == "ok"
    assert sleeps == [1, 2]


def test_client_stops_after_max_retries():
    session = FakeSession([requests.ConnectionError("down")] * 5)
    client = EventStreamClient(
        "https://example.org/stream",
        session=session,
        user_agent="test/0.1",
        sleep=lambda s: None,
        max_retries=3,
    )
    try:
        list(client.events())
    except requests.ConnectionError:
        pass
    else:
        raise AssertionError("expected ConnectionError after max retries")
    assert len(session.calls) == 4
