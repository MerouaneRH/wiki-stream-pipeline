"""Producer CLI: Wikimedia EventStreams (or an NDJSON file) -> Redpanda.

Delivery is at-least-once: the SSE resume id is saved only after Kafka confirms
delivery (flush). Duplicates after a crash are removed in silver on meta.id.
"""

import argparse
import logging
import os
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from producer.replay import ReplayWriter, iter_file
from producer.routing import RAW_TOPIC, Message, route
from producer.sse import EventStreamClient

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = os.getenv("WIKI_USER_AGENT", "wiki-stream-pipeline/0.1 (learning project)")

log = logging.getLogger("producer")


class Sink(Protocol):
    def produce(self, message: Message) -> None: ...
    def flush(self) -> None: ...


class State(Protocol):
    def save(self, event_id: str) -> None: ...


@dataclass
class Counts:
    raw: int = 0
    dlq: int = 0


class StateFile:
    """Stores the last delivered SSE event id. Writes are atomic."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> str | None:
        if not self.path.exists():
            return None
        return self.path.read_text(encoding="utf-8").strip() or None

    def save(self, event_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(event_id, encoding="utf-8")
        tmp.replace(self.path)


class KafkaSink:
    def __init__(self, bootstrap: str, topic_map: dict[str, str] | None = None):
        """topic_map renames routed topics, e.g. to isolated topics in tests."""
        from confluent_kafka import Producer

        self._topic_map = topic_map or {}
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )
        self.errors = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1
            log.error("delivery failed: %s", err)

    def produce(self, message: Message) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic_map.get(message.topic, message.topic),
                    value=message.value,
                    key=message.key,
                    headers=message.headers,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining or self.errors:
            raise RuntimeError(f"kafka delivery failed: {remaining} pending, {self.errors} errors")


def run(
    source: Iterable[tuple[str, str | None]],
    sink: Sink,
    *,
    replay: ReplayWriter | None = None,
    state: State | None = None,
    checkpoint_every: int = 500,
    max_events: int | None = None,
) -> Counts:
    counts = Counts()
    last_id: str | None = None
    pending = 0

    def checkpoint() -> None:
        sink.flush()
        if state is not None and last_id is not None:
            state.save(last_id)

    for n, (line, event_id) in enumerate(source, start=1):
        message = route(line)
        sink.produce(message)
        if message.topic == RAW_TOPIC:
            counts.raw += 1
            if replay is not None:
                replay.write(line)
        else:
            counts.dlq += 1
        if event_id is not None:
            last_id = event_id
        pending += 1
        if pending >= checkpoint_every:
            checkpoint()
            pending = 0
        if max_events is not None and n >= max_events:
            break
    if pending:
        checkpoint()
    return counts


def live_source(client: EventStreamClient) -> Iterator[tuple[str, str | None]]:
    for event in client.events():
        if event.event == "message":
            yield event.data, event.id


def file_source(path: str, rate: float) -> Iterator[tuple[str, None]]:
    """Replay a file. rate is events per second; 0 means as fast as possible."""
    interval = 1.0 / rate if rate > 0 else 0.0
    next_at = time.monotonic()
    for line in iter_file(path):
        if interval:
            next_at += interval
            delay = next_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        yield line, None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092"))
    p.add_argument("--url", default=STREAM_URL)
    p.add_argument("--from-file", help="replay an NDJSON file instead of the live stream")
    p.add_argument("--rate", type=float, default=0.0, help="events/s for --from-file, 0 = max")
    p.add_argument("--max-events", type=int, help="stop after N events")
    p.add_argument("--replay-dir", help="also write valid events to rotating NDJSON files")
    p.add_argument("--state-file", default="data/producer_state.txt")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    sink = KafkaSink(args.bootstrap)
    replay = ReplayWriter(args.replay_dir) if args.replay_dir else None
    state = None
    if args.from_file:
        source = file_source(args.from_file, args.rate)
    else:
        state = StateFile(args.state_file)
        client = EventStreamClient(args.url, user_agent=USER_AGENT, last_event_id=state.load())
        source = live_source(client)
    started = time.monotonic()
    try:
        counts = run(source, sink, replay=replay, state=state, max_events=args.max_events)
    except KeyboardInterrupt:
        log.info("interrupted")
        counts = None
    finally:
        if replay is not None:
            replay.close()
        sink.flush()
    if counts is not None:
        elapsed = time.monotonic() - started
        log.info("done: raw=%d dlq=%d in %.1fs", counts.raw, counts.dlq, elapsed)


if __name__ == "__main__":
    main()
