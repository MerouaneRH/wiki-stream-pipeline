from producer.replay import ReplayWriter, iter_file


class FakeClock:
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now


def completed(tmp_path):
    return sorted(p for p in tmp_path.iterdir() if p.suffix == ".ndjson")


def test_rotates_after_max_events_and_only_publishes_complete_files(tmp_path):
    writer = ReplayWriter(tmp_path, max_events=2, max_seconds=300, clock=FakeClock())
    for i in range(5):
        writer.write(f'{{"n": {i}}}')

    files = completed(tmp_path)
    assert len(files) == 2
    assert [len(f.read_text().splitlines()) for f in files] == [2, 2]
    assert len(list(tmp_path.glob("*.part"))) == 1

    writer.close()
    files = completed(tmp_path)
    assert len(files) == 3
    assert not list(tmp_path.glob("*.part"))
    lines = [line for f in files for line in f.read_text().splitlines()]
    assert lines == [f'{{"n": {i}}}' for i in range(5)]


def test_rotates_after_max_seconds(tmp_path):
    clock = FakeClock()
    writer = ReplayWriter(tmp_path, max_events=1000, max_seconds=300, clock=clock)
    writer.write("a")
    clock.now += 301
    writer.write("b")
    writer.close()
    assert [f.read_text() for f in completed(tmp_path)] == ["a\n", "b\n"]


def test_close_without_writes_leaves_no_files(tmp_path):
    ReplayWriter(tmp_path).close()
    assert list(tmp_path.iterdir()) == []


def test_iter_file_skips_blank_lines(tmp_path):
    path = tmp_path / "in.ndjson"
    path.write_text('{"a":1}\n\n{"b":2}\n')
    assert list(iter_file(path)) == ['{"a":1}', '{"b":2}']
