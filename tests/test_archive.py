"""Fixed-schema Parquet archiver: round-trip fidelity, ns ts_venue, merged reads."""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import duckdb

from arbbot.models.core import BookDelta, BookSnapshot, Level, Trade, Venue
from arbbot.record.archive import iter_day, iter_day_merged
from arbbot.record.jsonl import JsonlWriter, parse_event

spec = importlib.util.spec_from_file_location(
    "archive_to_parquet", Path(__file__).parent.parent / "scripts" / "archive_to_parquet.py")
atp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(atp)

DAY = "2026-01-01"
NS_TS = "2026-01-01T12:00:00.123456789Z"


def make_events(venue=Venue.KALSHI):
    return [
        BookSnapshot(
            venue=venue, market_id="M",
            bids=[Level(price=Decimal("0.40"), size=Decimal("10.5"))],
            asks=[Level(price=Decimal("0.42"), size=Decimal("3"))],
            seq=1, ts_local_ns=100, ts_venue=NS_TS,
        ),
        BookDelta(
            venue=venue, market_id="M", side="bid",
            price=Decimal("0.4100"), size=Decimal("7.25"), seq=2, ts_local_ns=200,
        ),
        Trade(
            venue=venue, market_id="M", price=Decimal("0.55"),
            size=Decimal("3"), taker_side="buy", seq=3, ts_local_ns=300,
            ts_venue="1784777866192",
        ),
    ]


def write_day(tmp_path, events):
    raw = tmp_path / "raw"
    w = JsonlWriter(raw)
    for e in events:
        w.write(e, DAY)
    w.close()
    return raw


def archive(tmp_path, src, keep=False):
    out = tmp_path / "parquet"
    out.mkdir(exist_ok=True)
    status, rows = atp.archive_one(duckdb.connect(), src, DAY, out, keep)
    return status, rows, out


def test_fixed_schema_roundtrip_byte_level(tmp_path):
    """JSONL -> archive -> read layer must be byte-identical per event, for
    all 3 event kinds (snapshot/delta/trade)."""
    events = make_events()
    raw = write_day(tmp_path, events)
    src = raw / f"kalshi-{DAY}.jsonl"
    orig = [json.loads(l) for l in src.read_text().splitlines()]
    status, rows, out = archive(tmp_path, src)
    assert status.startswith("ok") and rows == 3
    assert not src.exists()  # verified -> deleted
    back = list(iter_day(raw, "kalshi", DAY))
    assert len(back) == 3
    # canonical byte-level equality against the original JSONL lines
    for o, b in zip(orig, back):
        assert atp.canonical_row(o) == atp.canonical_row(b)
    # and full pydantic event equality (Decimal-exact)
    assert [parse_event(b) for b in back] == events


def test_ts_venue_survives_nanosecond_precision(tmp_path):
    """ts_venue must round-trip VERBATIM at nanosecond precision. The OLD
    archiver (read_json_auto) would FAIL this: it auto-typed ISO-8601 ts_venue
    as TIMESTAMP, silently truncating ...123456789Z to microseconds."""
    events = make_events()
    raw = write_day(tmp_path, events)
    status, _, _ = archive(tmp_path, raw / f"kalshi-{DAY}.jsonl")
    assert status.startswith("ok")
    back = list(iter_day(raw, "kalshi", DAY))
    assert back[0]["ts_venue"] == NS_TS
    assert back[2]["ts_venue"] == "1784777866192"  # epoch-ms strings too
    assert "ts_venue" not in back[1]  # JSON null dropped, like json.loads side


def test_merged_read_across_new_parquet_and_jsonl(tmp_path):
    """iter_day_merged unions a fixed-schema parquet venue with a live-JSONL
    venue, ordered by (ts_local_ns, venue, seq), events parsing identically."""
    k = make_events(Venue.KALSHI)
    p = make_events(Venue.POLYMARKET)
    raw = write_day(tmp_path, k + p)
    status, _, _ = archive(tmp_path, raw / f"kalshi-{DAY}.jsonl")
    assert status.startswith("ok")  # kalshi archived; polymarket stays JSONL
    merged = [parse_event(r) for r in
              iter_day_merged(DAY, ["kalshi", "polymarket"], raw)]
    assert merged == [k[0], p[0], k[1], p[1], k[2], p[2]]
    assert merged[0].ts_venue == NS_TS


def test_merged_read_tolerates_legacy_parquet(tmp_path):
    """A day can mix pre-fix parquet (auto-inferred STRUCT[] levels) with
    fixed-schema parquet; the union must still parse identically."""
    k = make_events(Venue.KALSHI)
    p = make_events(Venue.POLYMARKET)
    raw = write_day(tmp_path, k + p)
    out = tmp_path / "parquet"
    out.mkdir(exist_ok=True)
    # kalshi: legacy archiver behavior (auto inference)
    legacy_src = raw / f"kalshi-{DAY}.jsonl"
    duckdb.connect().execute(
        f"COPY (SELECT * FROM read_json_auto('{legacy_src}', "
        f"format='newline_delimited', union_by_name=true, sample_size=-1)) "
        f"TO '{out / f'kalshi-{DAY}.parquet'}' (FORMAT parquet)")
    legacy_src.unlink()
    # polymarket: fixed-schema archiver
    status, _, _ = archive(tmp_path, raw / f"polymarket-{DAY}.jsonl")
    assert status.startswith("ok")
    merged = [parse_event(r) for r in
              iter_day_merged(DAY, ["kalshi", "polymarket"], raw)]
    assert merged == [k[0], p[0], k[1], p[1], k[2], p[2]]


def test_content_mismatch_keeps_jsonl(tmp_path):
    """A parquet whose content diverges from the JSONL must never trigger
    deletion (digest gate), even when row counts agree."""
    events = make_events()
    raw = write_day(tmp_path, events)
    src = raw / f"kalshi-{DAY}.jsonl"
    out = tmp_path / "parquet"
    out.mkdir(exist_ok=True)
    # pre-plant a corrupt archive with the right row count: archive_one skips
    # re-copying only when src is gone, so simulate via monkeypatched digests
    orig = atp.content_digests
    atp.content_digests = lambda s, q: ("aaaa", "bbbb")
    try:
        status, rows = atp.archive_one(duckdb.connect(), src, DAY, out, False)
    finally:
        atp.content_digests = orig
    assert "CONTENT-MISMATCH" in status and rows == 0
    assert src.exists()
    assert not (out / f"kalshi-{DAY}.parquet").exists()
