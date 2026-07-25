"""mark_positions.compute_row — the position-marking math. This script
shipped two live bugs (inverted baskets marked as standard -> phantom
REVERSE flag on our own entry; and the marks feeding unwind signals), so the
direction handling and both unwind tiers get pinned here.
"""

import datetime
import importlib.util
import time
from decimal import Decimal
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "mark_positions", Path(__file__).parent.parent / "scripts" / "mark_positions.py")
mp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mp)

D = Decimal


def trade(kside="yes", pside="no", qty=5, cost=4.5, profit=0.5,
          resolves_days=365, held_days=30, rel="test-unknown-pair"):
    now = time.time()
    return {
        "relationship_id": rel, "qty": qty, "cost_usd": cost, "profit_usd": profit,
        "status": "open", "ts": now - held_days * 86400,
        "resolves_by": (datetime.date.today()
                        + datetime.timedelta(days=resolves_days)).isoformat(),
        "legs": [{"venue": "kalshi", "market_id": "K1", "side": kside},
                 {"venue": "polymarket_us", "market_id": "p1", "side": pside}],
    }, now


def test_exit_fees_are_the_published_taker_curves():
    """Card b83b0449: exit fees were a flat 1c/ct Kalshi-only guess. Real cost
    is ceil_cents(0.07*qty*P*(1-P)) on Kalshi + 0.06*P*(1-P)*qty on PM US."""
    # K @ 0.50: ceil(0.07*5*0.25) = ceil(0.0875) = 0.09
    # PM @ 0.55: 0.06*0.55*0.45*5 = 0.07425
    assert mp.exit_fees_usd(D("0.50"), D("0.55"), D(5)) == D("0.09") + D("0.07425")
    # per-leg quantities differ when a close legs out partially
    assert mp.exit_fees_usd(D("0.50"), D("0.55"), D(3), D(5)) == (
        D("0.06") + D("0.07425"))  # ceil(0.07*3*0.25)=ceil(0.0525)=0.06
    # at the money it is 3.3c/ct — 3.3x the old flat assumption
    assert mp.exit_fees_usd(D("0.50"), D("0.50"), D(100)) > D("3.00")


def test_standard_basket_liq_and_mark():
    t, now = trade()  # K YES + PM NO, cost 0.90/ct, locked 0.5
    # liq: sell K YES @ 0.50 bid + PM NO worth 1-0.55 ask = 0.95/ct, less the
    # real both-leg taker exit: 0.09 (Kalshi @ 0.50) + 0.07425 (PM US @ 0.55)
    row = mp.compute_row(t, D("0.50"), D("0.52"), D("0.53"), D("0.55"), now=now)
    assert abs(row["liq_value_usd"] - (0.95 * 5 - 0.16425)) < 1e-9
    assert abs(row["mark_pnl_usd"] - (4.75 - 0.16425 - 4.5)) < 1e-9


def test_inverted_basket_liq_uses_no_bid_and_pm_yes_bid():
    t, now = trade(kside="no", pside="yes")  # Kalshi NO + PM YES (Koch shape)
    # liq: K NO @ (1 - 0.52 ask) + PM YES @ 0.53 bid = 1.01/ct, and the exit
    # fees price off the legs we actually close: Kalshi @ 0.52, PM US @ 0.53
    # -> ceil(0.07*5*0.52*0.48)=0.09 + 0.06*0.53*0.47*5=0.07473
    row = mp.compute_row(t, D("0.50"), D("0.52"), D("0.53"), D("0.55"), now=now)
    assert abs(row["liq_value_usd"] - (1.01 * 5 - 0.16473)) < 1e-9


def test_inverted_basket_no_phantom_reverse():
    """Regression: our own inverted entry must NOT read as a fresh reverse arb.
    Reverse for K-NO+PM-YES = buy K YES @ ask + PM NO @ (1 - PM bid)."""
    t, now = trade(kside="no", pside="yes")
    row = mp.compute_row(t, D("0.17"), D("0.19"), D("0.25"), D("0.27"), now=now)
    # rev = p_bid - k_ask = 0.25 - 0.19 = +6c -> genuinely crossed, signal ok
    assert row["reverse_edge_c"] == 6.0 and row["reverse_signal"]
    row2 = mp.compute_row(t, D("0.17"), D("0.19"), D("0.11"), D("0.13"), now=now)
    # rev = 0.11 - 0.19 = -8c -> no signal (the old bug flagged k_bid - p_ask)
    assert row2["reverse_edge_c"] == -8.0 and not row2["reverse_signal"]


def test_unwind_soft_between_floor_and_hurdle():
    # nearly converged: locked 0.50, mark 0.40 -> remaining 0.10 on liq ~4.9
    # over 0.5yr -> fwd ~4.1% (>4 floor, <12 hurdle) -> soft only
    t, now = trade(cost=4.5, profit=0.5, resolves_days=183)
    row = mp.compute_row(t, D("0.51"), D("0.53"), D("0.51"), D("0.53"), now=now)
    assert row["forward_hold_apr"] is not None
    assert 4 < row["forward_hold_apr"] < 12
    assert row["unwind_signal"] and not row["unwind_hard"]


def test_unwind_hard_below_floor():
    # same convergence but 2 years out -> fwd ~1% -> hard tier fires
    t, now = trade(cost=4.5, profit=0.5, resolves_days=730)
    row = mp.compute_row(t, D("0.51"), D("0.53"), D("0.51"), D("0.53"), now=now)
    assert row["forward_hold_apr"] < 4
    assert row["unwind_hard"] and row["unwind_signal"]


def test_fully_banked_always_unwind():
    # mark >= locked: liq 5.20-0.05 > cost 4.5 + locked 0.5
    t, now = trade(cost=4.5, profit=0.5)
    row = mp.compute_row(t, D("0.56"), D("0.58"), D("0.51"), D("0.47"), now=now)
    assert row["mark_pnl_usd"] >= row["locked_profit_usd"]
    assert row["unwind_signal"]


def test_missing_quotes_yield_null_row():
    t, now = trade()
    row = mp.compute_row(t, None, None, None, None, now=now)
    assert row["mark_pnl_usd"] is None
    assert not row["unwind_signal"] and not row["unwind_hard"]
    assert not row["reverse_signal"]


def test_natural_apr_ignores_exit_costs():
    # locked 0.5 on 4.5 over ~1yr total -> ~11.1%/yr regardless of book quotes
    t, now = trade(held_days=0, resolves_days=365)
    row = mp.compute_row(t, D("0.40"), D("0.60"), D("0.40"), D("0.60"), now=now)
    assert row["natural_hold_apr"] is not None
    assert abs(row["natural_hold_apr"] - 11.1) < 0.3
