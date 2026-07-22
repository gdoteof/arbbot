"""JSONL event persistence — import-light on purpose.

Lives separately from store.py so the recorder process never imports duckdb
(large native extension; the recorder's in-process native surface is kept
minimal after the 2026-07-20 heap-corruption segfaults)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from arbbot.models.core import BookDelta, BookEvent, BookSnapshot, Trade


class JsonlWriter:
    """Append-only, line-buffered event writer. One file per venue per UTC day."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, object] = {}

    def path_for(self, venue: str, utc_day: str) -> Path:
        return self.data_dir / f"{venue}-{utc_day}.jsonl"

    def write(self, event: BookEvent, utc_day: str) -> None:
        key = f"{event.venue.value}-{utc_day}"
        fh = self._handles.get(key)
        if fh is None:
            fh = open(self.path_for(event.venue.value, utc_day), "a", buffering=1)
            self._handles[key] = fh
        fh.write(event.model_dump_json() + "\n")

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()
        self._handles.clear()


def iter_jsonl(path: str | Path) -> Iterator[tuple[int, dict]]:
    """Yield (line_number, parsed) tolerating a truncated final line."""
    with open(path) as f:
        lines = f.readlines()
    for i, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            yield i, json.loads(line)
        except json.JSONDecodeError:
            if i == len(lines):
                return  # crash-truncated final line: benign, skip
            raise  # corruption mid-file is NOT benign


_KIND_MODELS = {"snapshot": BookSnapshot, "delta": BookDelta, "trade": Trade}


def parse_event(raw: dict) -> BookEvent:
    model = _KIND_MODELS[raw["kind"]]
    return model.model_validate(raw)
