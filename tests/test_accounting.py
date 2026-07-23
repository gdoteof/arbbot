"""Accounting-kernel invariants (card 988c18d8): the identity
locked_at_open == realized + remaining_locked + slippage holds for every
basket, closes only CONVERT unrealized -> realized, rollups equal row sums,
and the digest is stable. The real-ledger test runs the fold over
data/exec/trades.jsonl when present."""

import json
import pathlib
from decimal import Decimal

from arbbot.exec.accounting import digest, fold, identity_error, row_floats


def _open(rel="r1", ts=100.0, qty=10, cost="9.0", payoff="10.0", profit="1.0",
          strategy="take-take"):
    return {"ts": ts, "relationship_id": rel, "title": rel, "qty": qty,
            "strategy": strategy, "status": "open", "cost_usd": cost,
            "payoff_usd": payoff, "profit_usd": profit,
            "legs": []}


def _unwind(rel="r1", closes=100.0, ts=200.0, qty=4, realized="0.9",
            proceeds="4.5"):
    return {"ts": ts, "relationship_id": rel, "status": "unwound",
            "strategy": "unwind", "closes_ts": closes, "qty": qty,
            "proceeds_usd": proceeds, "realized_pnl_usd": realized}


def _rows(res):
    return {(r["relationship_id"], r["ts"]): r for r in res["rows"]}


def test_open_only_basket():
    res = fold([_open()])
    row = res["rows"][0]
    assert row["remaining_qty"] == 10
    assert row["remaining_locked"] == Decimal("1.0000")
    assert row["realized"] == 0 and row["slippage"] == 0
    assert identity_error(row) == 0


def test_partial_unwind_beats_lock():
    # 4/10 closed for 0.9 realized vs 0.4 expected -> negative slippage
    res = fold([_open(), _unwind()])
    row = res["rows"][0]
    assert row["remaining_qty"] == 6
    assert row["remaining_locked"] == Decimal("0.6000")
    assert row["realized"] == Decimal("0.9000")
    assert row["slippage"] == Decimal("-0.5000")
    assert identity_error(row) == 0


def test_full_close_converts_everything():
    res = fold([_open(), _unwind(qty=4), _unwind(ts=300.0, qty=6, realized="0.6",
                                                 proceeds="6.6")])
    row = res["rows"][0]
    assert row["remaining_qty"] == 0
    assert row["remaining_locked"] == 0
    assert identity_error(row) == 0


def test_conversion_is_monotonic():
    """Folding successive prefixes of the event stream, remaining_locked
    never increases — closes only convert, never mint."""
    events = [_open(), _unwind(qty=2, realized="0.3", proceeds="2.2"),
              _unwind(ts=300.0, qty=3, realized="0.2", proceeds="3.0"),
              _unwind(ts=400.0, qty=5, realized="0.55", proceeds="5.3")]
    prev = None
    for n in range(1, len(events) + 1):
        row = fold(events[:n])["rows"][0]
        if prev is not None:
            assert row["remaining_locked"] <= prev
        prev = row["remaining_locked"]


def test_correction_folds_before_accounting():
    rec = [_open(profit="1.0"),
           {"ts": 150.0, "relationship_id": "r1", "status": "correction",
            "corrects_ts": 100.0, "fields": {"profit_usd": "2.0"}}]
    row = fold(rec)["rows"][0]
    assert row["locked_at_open"] == Decimal("2.0000")
    assert identity_error(row) == 0


def test_standalone_realized_row():
    rec = [{"ts": 500.0, "relationship_id": "sports-x", "title": "naked",
            "qty": 4, "strategy": "naked-settlement", "status": "realized",
            "realized_pnl_usd": -2.16}]
    row = fold(rec)["rows"][0]
    assert "standalone_realized" in row["flags"]
    assert row["locked_at_open"] == 0
    assert row["realized"] == Decimal("-2.1600")
    assert row["remaining_qty"] == 0
    assert identity_error(row) == 0


def test_over_close_flagged_not_crashed():
    res = fold([_open(qty=5), _unwind(qty=9, realized="1.0")])
    row = res["rows"][0]
    assert "over_closed" in row["flags"]
    assert identity_error(row) == 0


def test_orphan_close_surfaces():
    res = fold([_unwind(rel="ghost", closes=42.0)])
    row = res["rows"][0]
    assert "orphan_close" in row["flags"]
    assert row["realized"] == Decimal("0.9000")
    assert identity_error(row) == 0


def test_marks_attach_only_to_open_remainder():
    marks = {"positions": [
        {"relationship_id": "r1", "ts": 100.0, "mark_pnl_usd": 0.42},
        {"relationship_id": "r2", "ts": 100.0, "mark_pnl_usd": 9.99},
    ]}
    rec = [_open(rel="r1"),
           _open(rel="r2"),
           _unwind(rel="r2", qty=10, realized="1.1", proceeds="10.1")]
    rows = _rows(fold(rec, marks))
    assert rows[("r1", 100.0)]["unrealized_mark"] == Decimal("0.4200")
    assert rows[("r2", 100.0)]["unrealized_mark"] is None  # fully closed


def test_totals_equal_row_sums_and_digest_stable():
    rec = [_open(), _unwind(), _open(rel="r2", ts=110.0)]
    a, b = fold(rec), fold(rec)
    assert a["digest"] == b["digest"] == digest(a["rows"])
    for k in ("locked_at_open", "realized", "remaining_locked", "slippage"):
        assert a["totals"][k] == sum((r[k] for r in a["rows"]), Decimal(0))


def test_row_floats_renders():
    row = row_floats(fold([_open()])["rows"][0])
    assert isinstance(row["locked_at_open"], float)
    assert json.dumps(row)  # JSON-serializable


def test_real_ledger_identity():
    p = pathlib.Path("data/exec/trades.jsonl")
    if not p.exists():
        return
    recs = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    res = fold(recs)
    assert res["rows"], "real ledger folded to zero rows"
    for row in res["rows"]:
        assert identity_error(row) == 0, row["relationship_id"]
    # the fold is deterministic over the same ledger
    assert fold(recs)["digest"] == res["digest"]
