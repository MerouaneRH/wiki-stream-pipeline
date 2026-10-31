"""Producer -> Redpanda round trip. Needs `make up`.

Uses its own throwaway topics so test events never reach the real pipeline topics.
"""

import os
import uuid

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from producer.main import KafkaSink, run
from producer.routing import DLQ_TOPIC, RAW_TOPIC
from tests.unit.factories import make_line

pytestmark = pytest.mark.integration

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092")
N_VALID = 200
N_INVALID = 3


@pytest.fixture
def topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redpanda not reachable at {BOOTSTRAP}: {exc}")
    run_id = uuid.uuid4().hex[:8]
    names = {RAW_TOPIC: f"it.{run_id}.raw", DLQ_TOPIC: f"it.{run_id}.dlq"}
    new = [NewTopic(names[RAW_TOPIC], 3, 1), NewTopic(names[DLQ_TOPIC], 1, 1)]
    for future in admin.create_topics(new).values():
        future.result(10)
    yield names
    for future in admin.delete_topics(list(names.values())).values():
        future.result(10)


def consume_all(topic, expected):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"it-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    idle = 0
    while len(messages) < expected and idle < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            raise RuntimeError(msg.error())
        messages.append(msg)
    consumer.close()
    return messages


def test_producer_routes_valid_and_invalid_events(topics):
    wikis = ["enwiki", "frwiki", "dewiki", "jawiki"]
    lines = [make_line(wiki=wikis[i % 4], title=f"page-{i}") for i in range(N_VALID)]
    lines += ["{broken json"] * N_INVALID
    sink = KafkaSink(BOOTSTRAP, topic_map=topics)
    counts = run(((line, None) for line in lines), sink, checkpoint_every=50)
    assert (counts.raw, counts.dlq) == (N_VALID, N_INVALID)

    raw = consume_all(topics[RAW_TOPIC], N_VALID)
    dlq = consume_all(topics[DLQ_TOPIC], N_INVALID)

    assert len(raw) == N_VALID
    assert len(dlq) == N_INVALID
    assert {m.value().decode() for m in raw} == set(lines[:N_VALID])
    assert {m.key().decode() for m in raw} == set(wikis)
    # same key always lands on the same partition
    partition_by_key = {}
    for m in raw:
        assert partition_by_key.setdefault(m.key(), m.partition()) == m.partition()
    assert all(dict(m.headers())["schema_version"] == b"1" for m in raw)
    assert all(dict(m.headers())["error"] == b"invalid_json" for m in dlq)
