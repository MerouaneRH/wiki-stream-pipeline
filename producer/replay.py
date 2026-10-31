"""Rotating NDJSON replay files, used for tests, benchmarks and the Databricks replay."""

import time
from collections.abc import Callable, Iterator
from pathlib import Path


class ReplayWriter:
    """Writes lines to a `.part` file and renames it to `.ndjson` once complete.

    Readers (Spark, Databricks) only pick up `.ndjson`, so they never see a partial file.
    """

    def __init__(
        self,
        directory: str | Path,
        max_events: int = 10_000,
        max_seconds: float = 300,
        clock: Callable[[], float] = time.time,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.max_events = max_events
        self.max_seconds = max_seconds
        self._clock = clock
        self._seq = 0
        self._file = None
        self._path: Path | None = None
        self._count = 0
        self._opened_at = 0.0

    def _open(self) -> None:
        now = self._clock()
        stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime(now))
        self._path = self.directory / f"events-{stamp}-{self._seq:06d}.part"
        self._seq += 1
        self._file = self._path.open("w", encoding="utf-8")
        self._count = 0
        self._opened_at = now

    def _rotate(self) -> None:
        if self._file is None:
            return
        self._file.close()
        self._path.rename(self._path.with_suffix(".ndjson"))
        self._file = None
        self._path = None

    def write(self, line: str) -> None:
        if self._file is not None and self._clock() - self._opened_at >= self.max_seconds:
            self._rotate()
        if self._file is None:
            self._open()
        self._file.write(line + "\n")
        self._count += 1
        if self._count >= self.max_events:
            self._rotate()

    def close(self) -> None:
        self._rotate()


def iter_file(path: str | Path) -> Iterator[str]:
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line:
                yield line
