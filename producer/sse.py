"""Minimal Server-Sent Events client with Last-Event-ID resume and backoff."""

import logging
import random
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SSEEvent:
    id: str | None
    event: str
    data: str


def parse_sse(lines: Iterable[str | bytes]) -> Iterator[SSEEvent]:
    """Parse SSE lines into events. A blank line ends an event."""
    event_id: str | None = None
    event_type = "message"
    data: list[str] = []
    for raw in lines:
        line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if line == "":
            if data:
                yield SSEEvent(id=event_id, event=event_type, data="\n".join(data))
            event_id, event_type, data = None, "message", []
            continue
        if line.startswith(":"):
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
        elif field == "event":
            event_type = value


def backoff_delay(
    attempt: int,
    base: float = 1.0,
    cap: float = 60.0,
    rand: Callable[[float, float], float] = random.uniform,
) -> float:
    """Exponential backoff with full jitter."""
    return rand(0, min(cap, base * 2**attempt))


class EventStreamClient:
    """Yields SSE events forever, reconnecting and resuming from the last event id."""

    def __init__(
        self,
        url: str,
        *,
        user_agent: str,
        session: requests.Session | None = None,
        last_event_id: str | None = None,
        max_retries: int | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rand: Callable[[float, float], float] = random.uniform,
    ):
        self.url = url
        self.user_agent = user_agent
        self.session = session or requests.Session()
        self.last_event_id = last_event_id
        self.max_retries = max_retries
        self._sleep = sleep
        self._rand = rand

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "text/event-stream", "User-Agent": self.user_agent}
        if self.last_event_id:
            headers["Last-Event-ID"] = self.last_event_id
        return headers

    def events(self) -> Iterator[SSEEvent]:
        attempt = 0
        while True:
            try:
                response = self.session.get(
                    self.url, headers=self._headers(), stream=True, timeout=(10, 60)
                )
                response.raise_for_status()
                try:
                    for event in parse_sse(response.iter_lines(decode_unicode=True)):
                        attempt = 0
                        if event.id:
                            self.last_event_id = event.id
                        yield event
                finally:
                    response.close()
                log.info("stream ended, reconnecting")
            except requests.RequestException as exc:
                if self.max_retries is not None and attempt >= self.max_retries:
                    raise
                log.warning("stream error (attempt %d): %s", attempt + 1, exc)
            self._sleep(backoff_delay(attempt, rand=self._rand))
            attempt += 1
