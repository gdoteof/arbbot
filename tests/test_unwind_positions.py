"""unwind_positions.close_basket — exit-fee accounting on the close (card
b83b0449).

The exit path books real money into the ledger the accounting kernel folds, so
the arithmetic is pinned here: proceeds must be NET of both legs' taker fees,
and the "never unwind at a loss" guard must apply those fees BEFORE deciding —
a close that only clears on gross proceeds is a losing close.

Fake gateways + monkeypatched book reads; no venue, no orders.
"""

import importlib.util
import json
import pathlib
import sys
from decimal import Decimal

import pytest

spec = importlib.util.spec_from_file_location(
    "unwind_positions",
    pathlib.Path(__file__).parent.parent / "scripts" / "unwind_positions.py")
up = importlib.util.module_from_spec(spec)
spec.loader.exec_module(up)


class FakeGateway:
    """Fills each order in full at the requested price, unless `fills` pins a
    per-order-id quantity (the close path issues a repriced retry order, so
    fills must be attributable per order, not per gateway)."""

    def __init__(self, fills=None):
        self.orders = []
        self.fills = fills or {}

    def _record(self, *o):
        oid = f"o{len(self.orders) + 1}"
        self.orders.append((oid, *o))
        return oid

    def place_yes(self, market_id, side, price, qty, post_only=False):
        oid = self._record(market_id, side, Decimal(str(price)), qty)
        return {"id": oid, "order": {"order_id": oid}}

    def place_short(self, market_id, price, qty, post_only=False):
        oid = self._record(market_id, "short", Decimal(str(price)), qty)
        return {"id": oid}

    def filled_qty(self, oid):
        if oid in self.fills:
            return self.fills[oid]
        return next((o[4] for o in self.orders if o[0] == oid), 0)


def basket(kside="yes", pside="no", qty=5, cost=4.50, profit=0.50):
    return {"ts": 1.0, "relationship_id": "xvus-test", "title": "t",
            "qty": qty, "cost_usd": cost, "profit_usd": profit,
            "legs": [{"venue": "kalshi", "market_id": "K1", "side": kside},
                     {"venue": "polymarket_us", "market_id": "p1", "side": pside}]}


ROW = {"forward_hold_apr": 1.0, "mark_pnl_usd": 1.0}


@pytest.fixture
def sandbox(monkeypatch, tmp_path):
    """tmp cwd (the ledger path is relative) with venue I/O stubbed out."""
    (tmp_path / "data" / "exec").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(up.time, "sleep", lambda *_: None)
    monkeypatch.setattr(up, "Alerter", lambda *_a, **_k: type(
        "A", (), {"alert": lambda *_: None})())
    monkeypatch.setattr(up, "load_recorder_config", lambda: type(
        "C", (), {"ntfy_topic": "t"})())
    return tmp_path


def set_book(monkeypatch, k_bid, k_ask, p_bid, p_ask):
    monkeypatch.setattr(up, "kalshi_books",
                        lambda c, ids: {"K1": (Decimal(k_bid), Decimal(k_ask))})
    monkeypatch.setattr(up, "pmus_topbook",
                        lambda c, slug: (Decimal(p_bid), Decimal(p_ask)))


def ledger_records(tmp_path):
    f = tmp_path / "data" / "exec" / "trades.jsonl"
    return [json.loads(x) for x in f.read_text().splitlines()] if f.exists() else []


def test_proceeds_are_net_of_both_legs_exit_fees(sandbox, monkeypatch):
    set_book(monkeypatch, "0.60", "0.62", "0.48", "0.50")
    up.close_basket(basket(), ROW, Decimal("0.60"), Decimal("0.50"),
                    FakeGateway(), FakeGateway(), live=True)
    (rec,) = ledger_records(sandbox)
    # gross = 0.60*5 (Kalshi YES sold) + (1-0.50)*5 (PM NO closed) = 5.50
    # fees  = ceil(0.07*5*0.60*0.40)=0.09  +  0.06*0.50*0.50*5=0.075
    assert rec["gross_proceeds_usd"] == pytest.approx(5.50)
    assert rec["exit_fees_usd"] == pytest.approx(0.165)
    assert rec["proceeds_usd"] == pytest.approx(5.335)
    # the kernel takes realized at face value — it must be net too
    assert rec["realized_pnl_usd"] == pytest.approx(5.335 - 4.50)


def test_guard_rejects_a_close_that_only_clears_on_gross_proceeds(sandbox, monkeypatch):
    """cost 5.40 vs gross liq 5.50: +10c gross, but -6.5c after the real 16.5c
    round trip. The old flat-1c assumption cleared this and lost money."""
    set_book(monkeypatch, "0.60", "0.62", "0.48", "0.50")
    up.close_basket(basket(cost=5.40), ROW, Decimal("0.60"), Decimal("0.50"),
                    FakeGateway(), FakeGateway(), live=True)
    assert ledger_records(sandbox) == []


def test_inverted_guard_prices_the_legs_it_actually_closes(sandbox, monkeypatch):
    """A K-NO + PM-YES basket closes by BUYING Kalshi yes @ ask and SELLING PM
    yes @ bid. Pricing that guard off (k_bid, p_ask) — as it used to — reads a
    trade we never place and skipped profitable inverted unwinds."""
    set_book(monkeypatch, "0.20", "0.40", "0.55", "0.75")
    up.close_basket(basket(kside="no", pside="yes", cost=5.00), ROW,
                    Decimal("0.20"), Decimal("0.75"),
                    FakeGateway(), FakeGateway(), live=True,
                    k_ask=Decimal("0.40"), p_bid=Decimal("0.55"))
    (rec,) = ledger_records(sandbox)
    # gross = 0.55*5 (PM YES sold) + (1-0.40)*5 (Kalshi NO closed) = 5.75
    # fees  = ceil(0.07*5*0.40*0.60)=0.09 + 0.06*0.55*0.45*5=0.07425
    assert rec["gross_proceeds_usd"] == pytest.approx(5.75)
    assert rec["proceeds_usd"] == pytest.approx(5.75 - 0.16425)


def test_partial_kalshi_close_fees_follow_each_legs_filled_qty(sandbox, monkeypatch):
    """PM closes 5, Kalshi only 3: fees are charged per leg on what filled."""
    set_book(monkeypatch, "0.60", "0.62", "0.48", "0.50")
    kgw = FakeGateway(fills={"o1": 3, "o2": 0})  # first close 3/5, retry misses
    up.close_basket(basket(), ROW, Decimal("0.60"), Decimal("0.50"),
                    FakeGateway(), kgw, live=True)
    (rec,) = ledger_records(sandbox)
    # gross = 0.60*3 + (1-0.50)*5 = 4.30
    # fees  = ceil(0.07*3*0.60*0.40)=0.06 (Kalshi on 3) + 0.075 (PM on 5)
    assert rec["gross_proceeds_usd"] == pytest.approx(4.30)
    assert rec["exit_fees_usd"] == pytest.approx(0.135)
    assert rec["stranded_kalshi_qty"] == 2


def test_unpriced_basket_is_skipped_not_fatal(monkeypatch, tmp_path, capsys):
    """A basket with no cost basis must be skipped and counted, not crash the pass.

    The Rust engine books a basket at place time and never reads fill reports,
    so its records carry no cost_usd/profit_usd. compute_row subscripts those
    keys directly, so one such record aborts main() — taking with it every
    hard-unwind the pass had not reached yet. This bit mark_positions.py on
    2026-07-28; unwind_positions.py carried the same latent crash because it had
    been switched off since 2026-07-26, and it fired the moment it was re-armed
    (5 maker-hedge baskets, 3 pending hard unwinds behind them).
    """
    priced = basket(cost=5.00)
    priced["profit_usd"] = 1.0
    unpriced = basket(cost=5.00)
    del unpriced["cost_usd"]          # engine-booked record: neither key present
    unpriced["relationship_id"] = "xvus-engine-booked-no-cost"

    monkeypatch.setattr(up, "LEDGER", tmp_path / "trades.jsonl")
    (tmp_path / "trades.jsonl").write_text("")
    monkeypatch.setattr(up, "parse_lines", lambda _lines: [])
    # unpriced FIRST, so a regression aborts before reaching the priced basket
    monkeypatch.setattr(up, "open_baskets", lambda _recs: [unpriced, priced])
    monkeypatch.setattr(up, "httpx", type("H", (), {"Client": lambda **kw: None}))
    monkeypatch.setattr(up, "kalshi_books", lambda _c, _ids: {})
    monkeypatch.setattr(up, "pmus_topbook", lambda _c, _mid: (None, None))
    monkeypatch.setattr(sys, "argv", ["unwind_positions.py"])   # dry run

    up.main()   # must not raise

    out = capsys.readouterr().out
    assert "1 unpriced" in out, out
    assert "xvus-engine-booked-no-cost" not in out, out
    # and the pass still reached the basket behind it
    assert "checked 2 open baskets" in out, out


@pytest.mark.parametrize("costs,qtys,expect_soft_unwind", [
    # $300 exposure, 346 contracts, threshold $325.85: the contract count clears
    # the dollar threshold but the dollars do not. Pre-fix this unwound; the
    # correct answer is to hold.
    ([300.24], [346], False),
    # genuinely capital-constrained in dollars -> soft displacement is allowed
    ([330.00], [346], True),
])
def test_soft_displacement_gates_on_dollars_not_contract_count(
        monkeypatch, tmp_path, capsys, costs, qtys, expect_soft_unwind):
    """The capital-constrained displacement branch must measure DOLLARS.

    CLASS_BUDGET is bankroll_usd(980) x per_class_cap(0.35) = $343, so the 0.95
    threshold is $325.85. Summing `qty` compared contracts against that, and
    since average price is well under $1 the count crosses first — on 2026-07-29
    346 contracts read as "constrained" on $300.24 of real exposure and
    early-unwound a soft-signal position the policy says to hold.
    """
    soft = basket(cost=costs[0])
    soft["profit_usd"] = 1.0
    soft["qty"] = qtys[0]
    soft["relationship_id"] = "xvus-soft-signal-only"

    monkeypatch.setattr(up, "LEDGER", tmp_path / "trades.jsonl")
    (tmp_path / "trades.jsonl").write_text("")
    monkeypatch.setattr(up, "parse_lines", lambda _lines: [])
    monkeypatch.setattr(up, "open_baskets", lambda _recs: [soft])
    monkeypatch.setattr(up, "httpx", type("H", (), {"Client": lambda **kw: None}))
    monkeypatch.setattr(up, "kalshi_books", lambda _c, _ids: {})
    monkeypatch.setattr(up, "pmus_topbook", lambda _c, _mid: (None, None))
    # soft signal only: below the 12% hurdle, but above the 4% hard floor
    monkeypatch.setattr(up, "compute_row", lambda *a, **k: {
        "unwind_hard": False, "unwind_signal": True, "forward_hold_apr": 10.9,
        "mark_pnl_usd": 1.23})
    monkeypatch.setattr(sys, "argv", ["unwind_positions.py"])   # dry run

    up.main()

    out = capsys.readouterr().out
    assert ("1 hard-unwind" in out) is expect_soft_unwind, out
