from producer.main import StateFile, parse_args, run
from producer.replay import ReplayWriter
from producer.routing import DLQ_TOPIC, RAW_TOPIC
from tests.unit.factories import make_line


class FakeSink:
    def __init__(self, log):
        self.log = log
        self.messages = []

    def produce(self, message):
        self.messages.append(message)

    def flush(self):
        self.log.append(("flush", len(self.messages)))


class RecordingState:
    def __init__(self, log):
        self.log = log

    def save(self, event_id):
        self.log.append(("save", event_id))


def source(n_valid, invalid_at=()):
    for i in range(n_valid):
        line = "{broken" if i in invalid_at else make_line()
        yield line, f"id-{i}"


def test_run_routes_and_counts():
    log = []
    sink = FakeSink(log)
    counts = run(source(5, invalid_at={2}), sink)
    assert (counts.raw, counts.dlq) == (4, 1)
    assert [m.topic for m in sink.messages].count(RAW_TOPIC) == 4
    assert [m.topic for m in sink.messages].count(DLQ_TOPIC) == 1


def test_state_is_saved_only_after_flush():
    log = []
    run(source(5), FakeSink(log), state=RecordingState(log), checkpoint_every=2)
    assert log == [
        ("flush", 2),
        ("save", "id-1"),
        ("flush", 4),
        ("save", "id-3"),
        ("flush", 5),
        ("save", "id-4"),
    ]


def test_max_events_stops_early():
    counts = run(source(100), FakeSink([]), max_events=10)
    assert counts.raw == 10


def test_replay_gets_only_valid_events(tmp_path):
    writer = ReplayWriter(tmp_path)
    run(source(4, invalid_at={0}), FakeSink([]), replay=writer)
    writer.close()
    lines = [line for f in tmp_path.glob("*.ndjson") for line in f.read_text().splitlines()]
    assert len(lines) == 3
    assert "{broken" not in lines


def test_state_file_roundtrip(tmp_path):
    state = StateFile(tmp_path / "sub" / "state.txt")
    assert state.load() is None
    state.save('[{"topic":"x","offset":5}]')
    assert StateFile(tmp_path / "sub" / "state.txt").load() == '[{"topic":"x","offset":5}]'


def test_parse_args_defaults_and_overrides():
    args = parse_args([])
    assert args.bootstrap == "127.0.0.1:19092"
    assert args.from_file is None
    args = parse_args(["--from-file", "x.ndjson", "--rate", "50", "--max-events", "7"])
    assert (args.from_file, args.rate, args.max_events) == ("x.ndjson", 50.0, 7)
