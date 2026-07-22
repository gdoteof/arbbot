"""Event persistence: line-buffered JSONL append -> batch DuckDB ingest.

DuckDB is single-writer OLAP: the live WS loops never touch it. They append
JSONL (one file per venue per UTC day); analysis/replay batch-ingests. The
ingest is truncation-tolerant (a crash mid-write leaves a partial final line,
which is skipped) and idempotent (re-ingesting a file cannot duplicate rows).
"""

from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import duckdb

from arbbot.models.core import BookEvent
from arbbot.record.jsonl import JsonlWriter, iter_jsonl, parse_event  # noqa: F401 (re-export)

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    kind TEXT NOT NULL,           -- snapshot | delta | trade
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    ts_local_ns BIGINT NOT NULL,
    ts_venue TEXT,
    payload JSON NOT NULL,        -- full event, source of truth for replay
    source_file TEXT NOT NULL,
    source_line BIGINT NOT NULL,
    PRIMARY KEY (source_file, source_line)
);
CREATE TABLE IF NOT EXISTS ingested_files (
    source_file TEXT PRIMARY KEY,
    lines BIGINT NOT NULL,
    ingested_at TIMESTAMP DEFAULT current_timestamp
);
"""


class EventStore:
    def __init__(self, db_path: str | Path):
        self.conn = duckdb.connect(str(db_path))
        self.conn.execute(SCHEMA)

    def ingest_file(self, path: str | Path) -> int:
        """Idempotent: a file already fully ingested is skipped; a partially
        ingested file (crash during a prior ingest) is resumed via the
        (source_file, source_line) primary key anti-join."""
        path = str(path)
        rows = []
        for line_no, raw in iter_jsonl(path):
            rows.append(
                (
                    raw["kind"],
                    raw["venue"],
                    raw["market_id"],
                    int(raw.get("seq", 0)),
                    int(raw.get("ts_local_ns", 0)),
                    raw.get("ts_venue"),
                    json.dumps(raw),
                    path,
                    line_no,
                )
            )
        if not rows:
            return 0
        self.conn.executemany(
            """INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT (source_file, source_line) DO NOTHING""",
            rows,
        )
        self.conn.execute(
            """INSERT INTO ingested_files (source_file, lines) VALUES (?, ?)
               ON CONFLICT (source_file) DO UPDATE SET
                 lines = excluded.lines, ingested_at = now()""",
            (path, len(rows)),
        )
        return len(rows)

    def events_for_replay(
        self, venues_markets: set[tuple[str, str]] | None = None
    ) -> Iterator[BookEvent]:
        """All events in deterministic replay order: (ts_local_ns, venue, seq)."""
        q = "SELECT payload FROM events"
        params: list = []
        if venues_markets:
            placeholders = " OR ".join("(venue=? AND market_id=?)" for _ in venues_markets)
            q += f" WHERE {placeholders}"
            for v, m in sorted(venues_markets):
                params.extend([v, m])
        q += " ORDER BY ts_local_ns, venue, seq, source_file, source_line"
        cur = self.conn.execute(q, params)
        while batch := cur.fetchmany(10_000):
            for (payload,) in batch:
                yield parse_event(json.loads(payload))

    def close(self) -> None:
        self.conn.close()
