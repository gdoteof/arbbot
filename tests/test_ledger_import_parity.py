"""P0 parity gate: the SQLite trading DB must reproduce exec/ledger.py netting.

exec/ledger.py is the frozen reference implementation; these tests import
synthetic JSONL (covering every drifted shape observed in the live file) and
assert v_open_baskets == ledger.open_baskets(), plus the DB-only properties
the JSONL never had (idempotent re-import, correction audit trail, visible
orphan closes).
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from import_ledger_to_sqlite import check_parity, import_ledger

from arbbot.exec import ledgerdb


def _write(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


MAKER_LEG = {"venue": "polymarket_us", "market_id": "tpoyc-x", "side": "no",
             "role": "maker", "qty": 5, "yes_price": "0.31", "cost": "3.45"}
TAKER_LEG = {"venue": "kalshi", "market_id": "KX-X", "side": "yes", "role": "taker",
             "qty": 5, "avg_price": "0.1900", "fees": "0.0535", "order_id": "oid-1"}
UNWIND_LEG = {"venue": "kalshi", "market_id": "KX-X", "side": "yes",
              "action": "sell", "qty": 4, "yes_price": "0.1900"}

RECORDS = [
    # make-take open with both drifted leg shapes
    {"ts": 100.5, "relationship_id": "rel-a", "title": "A", "qty": 5,
     "strategy": "make-take", "legs": [MAKER_LEG, TAKER_LEG], "cost_usd": 4.4535,
     "payoff_usd": 5.0, "profit_usd": 0.5465, "status": "open",
     "hedge_latency_ms": 40.3},
    # take-take open, resolves fields
    {"ts": 200.25, "relationship_id": "rel-b", "title": "B", "qty": 10,
     "strategy": "take-take", "resolves_by": "2027-04-25", "resolves_estimated": True,
     "legs": [], "cost_usd": 9.48, "payoff_usd": 10.0, "profit_usd": 0.52,
     "status": "open"},
    # partial unwind of rel-a (4 of 5)
    {"ts": 300.0, "relationship_id": "rel-a", "strategy": "unwind",
     "status": "unwound", "closes_ts": 100.5, "qty": 4, "legs": [UNWIND_LEG],
     "proceeds_usd": 4.0, "realized_pnl_usd": 0.11},
    # correction rewrites rel-b economics
    {"ts": 400.0, "relationship_id": "rel-b", "status": "correction",
     "corrects_ts": 200.25, "reason": "hedge response lost to 429",
     "fields": {"cost_usd": 9.66, "profit_usd": 0.34}},
    # full unwind of a basket opened and closed same file
    {"ts": 500.0, "relationship_id": "rel-c", "title": "C", "qty": 3,
     "strategy": "sports-take-take", "legs": [], "cost_usd": 2.7,
     "payoff_usd": 3.0, "profit_usd": 0.3, "status": "open"},
    {"ts": 600.0, "relationship_id": "rel-c", "strategy": "settlement",
     "status": "unwound", "closes_ts": 500.0, "qty": 3, "kalshi_result": "yes",
     "proceeds_usd": 3.0, "realized_pnl_usd": 0.3},
    # standalone realized event — no open counterpart, must not affect opens
    {"ts": 700.0, "relationship_id": "sports-mlb-X", "title": "naked hold",
     "qty": 4, "strategy": "naked-settlement", "status": "realized",
     "kalshi_result": "no", "realized_pnl_usd": -2.16, "note": "probe hold"},
    # orphan unwind — closes_ts matches nothing; must import visibly, not vanish
    {"ts": 800.0, "relationship_id": "rel-ghost", "strategy": "unwind",
     "status": "unwound", "closes_ts": 42.0, "qty": 1, "realized_pnl_usd": 0.0},
    # open record voided by correction (live file has qty-0 'superseded' voids);
    # the corrected view has status != open, so it must NOT become a basket
    {"ts": 900.0, "relationship_id": "rel-void", "title": "V", "qty": 0,
     "strategy": "sports-take-take", "legs": [], "cost_usd": 0.0,
     "payoff_usd": 0.0, "profit_usd": 0.0, "status": "open"},
    {"ts": 901.0, "relationship_id": "rel-void", "status": "correction",
     "corrects_ts": 900.0, "reason": "degenerate qty-0 capture record",
     "fields": {"status": "superseded"}},
]


def _setup(tmp_path):
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    _write(ledger, RECORDS)
    stats = import_ledger(ledger, db)
    return ledger, db, stats


def test_parity_gate(tmp_path):
    ledger, db, _ = _setup(tmp_path)
    assert check_parity(ledger, db) == []


def test_import_counts_and_orphans(tmp_path):
    _, _, stats = _setup(tmp_path)
    assert stats["baskets"] == 3
    assert stats["closes"] == 4          # 2 unwinds + 1 realized + 1 orphan
    assert stats["audits"] == 1
    assert stats["orphan_closes"] == [("rel-ghost", 42.0)]


def test_reimport_is_noop(tmp_path):
    ledger, db, _ = _setup(tmp_path)
    stats2 = import_ledger(ledger, db)
    assert stats2["baskets"] == 0 and stats2["closes"] == 0
    assert stats2["skipped"] == 7        # every previously-imported line is a no-op
    assert check_parity(ledger, db) == []


def test_correction_applied_with_audit_trail(tmp_path):
    _, db, _ = _setup(tmp_path)
    conn = ledgerdb.connect(db, readonly=True)
    try:
        b = conn.execute("SELECT * FROM basket WHERE relationship_id='rel-b'").fetchone()
        assert b["cost_usd_u"] == 9_660_000 and b["profit_usd_u"] == 340_000
        a = conn.execute("SELECT * FROM audit_log WHERE pk = ?",
                         (str(b["basket_id"]),)).fetchone()
        assert a["reason"] == "hedge response lost to 429"
        assert json.loads(a["before_json"]) == {"cost_usd": 9.48, "profit_usd": 0.52}
    finally:
        conn.close()


def test_leg_normalization(tmp_path):
    _, db, _ = _setup(tmp_path)
    conn = ledgerdb.connect(db, readonly=True)
    try:
        legs = conn.execute(
            "SELECT l.* FROM leg l JOIN basket b USING (basket_id)"
            " WHERE b.relationship_id='rel-a' ORDER BY leg_index").fetchall()
        maker, taker = legs
        assert maker["price_pu"] == 310_000 and maker["cost_usd_u"] == 3_450_000
        assert maker["fees_usd_u"] == 0 and maker["role"] == "maker"
        assert taker["price_pu"] == 190_000 and taker["fees_usd_u"] == 53_500
        assert taker["venue_order_id"] == "oid-1" and taker["cost_usd_u"] is None
    finally:
        conn.close()


def test_views_net_correctly(tmp_path):
    _, db, _ = _setup(tmp_path)
    conn = ledgerdb.connect(db, readonly=True)
    try:
        opens = {r["relationship_id"]: r for r in
                 conn.execute("SELECT * FROM v_open_baskets").fetchall()}
        assert set(opens) == {"rel-a", "rel-b"}   # rel-c fully closed
        assert opens["rel-a"]["open_qty"] == 1    # 5 - 4
        assert opens["rel-a"]["open_cost_usd_u"] == round(4.4535e6 / 5)
        pnl = {(r["kind"]): r["pnl_usd_u"] for r in
               conn.execute("SELECT * FROM v_realized_pnl").fetchall()}
        assert pnl["naked-settlement"] == -2_160_000
    finally:
        conn.close()


def test_dual_append_then_backfill_is_noop(tmp_path):
    """Live dual-writes and the backfill mint identical content-hash keys."""
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    for rec in RECORDS:
        ledgerdb.dual_append(rec, source="test-writer", ledger_path=ledger, db_path=db)
    stats = import_ledger(ledger, db)
    assert stats["baskets"] == 0 and stats["closes"] == 0 and stats["unhealed"] == []
    assert check_parity(ledger, db) == []


def test_dual_append_correction_updates_with_audit(tmp_path):
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    open_rec = RECORDS[1]                      # rel-b
    corr_rec = RECORDS[3]                      # corrects rel-b economics
    ledgerdb.dual_append(open_rec, source="test-writer", ledger_path=ledger, db_path=db)
    ledgerdb.dual_append(corr_rec, source="test-writer", ledger_path=ledger, db_path=db)
    conn = ledgerdb.connect(db, readonly=True)
    try:
        b = conn.execute("SELECT * FROM basket WHERE relationship_id='rel-b'").fetchone()
        assert b["cost_usd_u"] == 9_660_000 and b["voided"] == 0
        a = conn.execute("SELECT * FROM audit_log").fetchone()
        assert a["actor"] == "test-writer"
        assert a["reason"] == "hedge response lost to 429"
        assert json.loads(a["before_json"])["cost_usd_u"] == 9_480_000
    finally:
        conn.close()


def test_dual_append_void_correction_hides_basket(tmp_path):
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    open_rec = {**RECORDS[0], "relationship_id": "rel-v"}
    void = {"ts": 101.0, "relationship_id": "rel-v", "status": "correction",
            "corrects_ts": open_rec["ts"], "reason": "dup",
            "fields": {"status": "superseded"}}
    ledgerdb.dual_append(open_rec, source="test-writer", ledger_path=ledger, db_path=db)
    ledgerdb.dual_append(void, source="test-writer", ledger_path=ledger, db_path=db)
    conn = ledgerdb.connect(db, readonly=True)
    try:
        assert conn.execute("SELECT voided FROM basket").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM v_open_baskets").fetchone()[0] == 0
    finally:
        conn.close()
    assert check_parity(ledger, db) == []


def test_missed_mirror_heals_on_backfill(tmp_path):
    """Simulate DB-down during live writes: JSONL has records the DB lacks;
    the importer (backfill) heals them, including a missed correction."""
    ledger = tmp_path / "trades.jsonl"
    db = tmp_path / "trading.db"
    # first record mirrored fine, rest JSONL-only
    ledgerdb.dual_append(RECORDS[0], source="test-writer", ledger_path=ledger, db_path=db)
    with open(ledger, "a") as f:
        for rec in RECORDS[1:]:
            f.write(json.dumps(rec) + "\n")
    stats = import_ledger(ledger, db)
    assert stats["baskets"] == 2 and stats["closes"] == 4
    assert stats["unhealed"] == []
    assert check_parity(ledger, db) == []


def test_rogue_update_leaves_audit_trace(tmp_path):
    """Even a direct UPDATE outside ledgerdb leaves a before-image row
    (actor NULL) — the triggers capture, they don't block."""
    import sqlite3
    _, db, _ = _setup(tmp_path)
    raw = sqlite3.connect(db)
    try:
        raw.execute("UPDATE basket SET note='rogue' WHERE basket_id=1")
        raw.commit()
        actor, before = raw.execute(
            "SELECT actor, before_json FROM audit_log WHERE pk='1'"
            " ORDER BY audit_id DESC LIMIT 1").fetchone()
        assert actor is None
        assert json.loads(before)["note"] is None
    finally:
        raw.close()
