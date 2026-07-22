"""JSONL writer + DuckDB ingest: truncation tolerance, idempotency, replay order."""

import json
from decimal import Decimal

import pytest

from arbbot.models.core import BookDelta, BookSnapshot, Level, Trade, Venue
from arbbot.record.store import EventStore, JsonlWriter, iter_jsonl, parse_event


def make_events():
    return [
        BookSnapshot(
            venue=Venue.KALSHI, market_id="M", bids=[], asks=[], seq=1, ts_local_ns=100
        ),
        BookDelta(
            venue=Venue.KALSHI, market_id="M", side="bid",
            price=Decimal("0.40"), size=Decimal("10"), seq=2, ts_local_ns=200,
        ),
        Trade(
            venue=Venue.POLYMARKET, market_id="T", price=Decimal("0.55"),
            size=Decimal("3"), taker_side="buy", seq=1, ts_local_ns=150,
        ),
    ]


def test_writer_roundtrip_and_parse(tmp_path):
    w = JsonlWriter(tmp_path)
    for e in make_events():
        w.write(e, "2026-07-20")
    w.close()
    k = list(iter_jsonl(tmp_path / "kalshi-2026-07-20.jsonl"))
    p = list(iter_jsonl(tmp_path / "polymarket-2026-07-20.jsonl"))
    assert len(k) == 2 and len(p) == 1
    assert parse_event(k[0][1]) == make_events()[0]
    assert parse_event(p[0][1]) == make_events()[2]


def test_truncated_final_line_tolerated_mid_file_corruption_not(tmp_path):
    good = json.dumps({"kind": "trade", "venue": "kalshi", "market_id": "M"})
    f = tmp_path / "a.jsonl"
    f.write_text(good + "\n" + '{"kind": "tra')  # crash mid-write
    assert len(list(iter_jsonl(f))) == 1
    f2 = tmp_path / "b.jsonl"
    f2.write_text('{"broken\n' + good + "\n")
    with pytest.raises(json.JSONDecodeError):
        list(iter_jsonl(f2))


def test_ingest_idempotent_and_replay_ordered(tmp_path):
    w = JsonlWriter(tmp_path)
    for e in make_events():
        w.write(e, "2026-07-20")
    w.close()
    store = EventStore(tmp_path / "events.duckdb")
    n1 = store.ingest_file(tmp_path / "kalshi-2026-07-20.jsonl")
    n2 = store.ingest_file(tmp_path / "polymarket-2026-07-20.jsonl")
    assert (n1, n2) == (2, 1)
    # re-ingest: no duplicates
    store.ingest_file(tmp_path / "kalshi-2026-07-20.jsonl")
    assert store.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 3
    # replay order is by ts_local_ns across venues
    ts = [e.ts_local_ns for e in store.events_for_replay()]
    assert ts == sorted(ts) == [100, 150, 200]
    # market filter
    only_m = list(store.events_for_replay({("kalshi", "M")}))
    assert len(only_m) == 2
    store.close()


def test_ingest_resumes_after_partial(tmp_path):
    """Appending to a file after ingest and re-ingesting picks up new lines only."""
    w = JsonlWriter(tmp_path)
    events = make_events()
    w.write(events[0], "2026-07-20")
    w.close()
    store = EventStore(tmp_path / "events.duckdb")
    store.ingest_file(tmp_path / "kalshi-2026-07-20.jsonl")
    w2 = JsonlWriter(tmp_path)
    w2.write(events[1], "2026-07-20")
    w2.close()
    store.ingest_file(tmp_path / "kalshi-2026-07-20.jsonl")
    assert store.conn.execute("SELECT count(*) FROM events").fetchone()[0] == 2
    store.close()
