"""M1 reader flip: the ledgerdb read path must equal exec/ledger.py (frozen
reference) over the same JSONL — for a DB built by the importer/backfill AND
for one built record-by-record by the live dual-write path. Reuses the
synthetic fixture from test_ledger_import_parity (do not modify that file).
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from import_ledger_to_sqlite import import_ledger
from test_ledger_import_parity import RECORDS

from arbbot.exec import ledgerdb
from arbbot.exec.ledger import apply_corrections, open_baskets, parse_lines


def _write_ledger(tmp_path):
    ledger = tmp_path / "trades.jsonl"
    ledger.write_text("".join(json.dumps(r) + "\n" for r in RECORDS))
    return ledger


def _reference(ledger):
    return open_baskets(parse_lines(ledger.read_text().splitlines()))


def _db_open(db):
    conn = ledgerdb.connect(db, readonly=True)
    try:
        return ledgerdb.open_baskets_db(conn)
    finally:
        conn.close()


def _db_all(db):
    conn = ledgerdb.connect(db, readonly=True)
    try:
        return ledgerdb.all_records_db(conn)
    finally:
        conn.close()


def test_open_baskets_db_parity_import_built(tmp_path):
    """DB built by import_ledger_to_sqlite from the same JSONL."""
    ledger = _write_ledger(tmp_path)
    db = tmp_path / "trading.db"
    import_ledger(ledger, db)
    assert _db_open(db) == _reference(ledger)


def test_open_baskets_db_parity_dual_append_built(tmp_path):
    """DB built record-by-record by the live dual-write path."""
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    for rec in RECORDS:
        ledgerdb.dual_append(rec, source="test-writer", ledger_path=ledger, db_path=db)
    assert _db_open(db) == _reference(ledger)


def test_open_baskets_db_partial_unwind_shapes(tmp_path):
    """The scaled record keeps ledger.py's exact shapes: integral remaining
    qty collapses to int, economics scale by the same float fraction."""
    ledger = _write_ledger(tmp_path)
    db = tmp_path / "trading.db"
    import_ledger(ledger, db)
    by_rel = {r["relationship_id"]: r for r in _db_open(db)}
    a = by_rel["rel-a"]                       # 5 opened, 4 unwound
    assert a["qty"] == 1 and isinstance(a["qty"], int)
    assert a["cost_usd"] == 4.4535 * (1 / 5)  # bit-identical float arithmetic
    b = by_rel["rel-b"]                       # untouched but corrected
    assert b["cost_usd"] == 9.66 and b["qty"] == 10
    assert set(by_rel) == {"rel-a", "rel-b"}  # rel-c closed, rel-void voided


def test_all_records_db_matches_corrected_view_dual_append(tmp_path):
    """Live-path DB reproduces apply_corrections(parse_lines(...)) exactly
    (every surviving record, corrected fields folded, ledger order) — except
    degenerate qty-0 records, which the basket CHECK (qty > 0) rejects at
    mirror time on both paths."""
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    for rec in RECORDS:
        ledgerdb.dual_append(rec, source="test-writer", ledger_path=ledger, db_path=db)
    expected = [r for r in apply_corrections(parse_lines(ledger.read_text().splitlines()))
                if float(r.get("qty") or 0) > 0]
    assert _db_all(db) == expected


def test_all_records_db_import_built_omits_voided_by_design(tmp_path):
    """The importer never creates basket rows for records whose correction
    rewrote status away from 'open' — everything else must round-trip."""
    ledger = _write_ledger(tmp_path)
    db = tmp_path / "trading.db"
    import_ledger(ledger, db)
    expected = [r for r in apply_corrections(parse_lines(ledger.read_text().splitlines()))
                if r.get("status") in ("open", "unwound", "realized")]
    assert _db_all(db) == expected


def test_empty_db_yields_empty_views(tmp_path):
    """Absent/empty DB == today's missing-file behavior: empty lists."""
    conn = ledgerdb.connect(tmp_path / "fresh.db")
    try:
        assert ledgerdb.open_baskets_db(conn) == []
        assert ledgerdb.all_records_db(conn) == []
    finally:
        conn.close()


def test_ownership_claim_and_lookup(tmp_path):
    conn = ledgerdb.connect(tmp_path / "trading.db")
    try:
        ledgerdb.claim_ownership(conn, "aec-mlb-nyy-bos-2026-07-23", "probe-pmus-maker")
        ledgerdb.claim_ownership(conn, "xvus-fedcut-26", "python-trader", note="runner")
        assert ledgerdb.owned_by(conn, "probe-") == ["aec-mlb-nyy-bos-2026-07-23"]
        # re-claim overwrites (last claim wins)
        ledgerdb.claim_ownership(conn, "aec-mlb-nyy-bos-2026-07-23", "probe-toxicity")
        assert ledgerdb.owned_by(conn, "probe-toxicity") == ["aec-mlb-nyy-bos-2026-07-23"]
        assert ledgerdb.owned_by(conn, "rust-") == []
    finally:
        conn.close()


def test_dual_append_heals_a_torn_tail_instead_of_welding(tmp_path):
    """A crash leaves a final line with no newline. Appending onto it fuses two
    records into one line that parses as neither — destroying both. The Rust
    engine's writer heals and its reader refuses to arm on a fused line, and
    arbbot-hedge.timer runs this path every 5 minutes, so this is the writer
    most likely to meet a Rust stump.
    """
    ledger = tmp_path / "trades.jsonl"
    ledger.write_text(
        '{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}\n'
        '{"status":"open","relationship_id":"r2","ts":2.0,"qt'
    )
    rec = {"status": "open", "relationship_id": "r3", "ts": 3.0, "qty": 7}
    ledgerdb.dual_append(rec, source="test", ledger_path=ledger,
                         db_path=tmp_path / "trading.db")

    lines = ledger.read_text().splitlines()
    assert len(lines) == 3, f"the stump must not eat the new record: {lines}"
    assert json.loads(lines[2]) == rec
    # the torn line stays ONE bad line, recoverable, and r1 is untouched
    assert json.loads(lines[0])["relationship_id"] == "r1"


def test_dual_append_is_byte_identical_to_the_old_writer(tmp_path):
    """The heal must not change what a normal append looks like: trades.jsonl is
    SoR and several readers pin its exact bytes.
    """
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    recs = [{"status": "open", "relationship_id": "r1", "ts": 1.0, "qty": 5},
            {"status": "unwound", "relationship_id": "r1", "closes_ts": 1.0, "qty": 5}]
    for r in recs:
        ledgerdb.dual_append(r, source="test", ledger_path=ledger, db_path=db)
    want = "".join(json.dumps(r) + "\n" for r in recs)
    assert ledger.read_bytes() == want.encode()
