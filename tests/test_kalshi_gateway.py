"""Kalshi order gateway: payload correctness (dry-run, no network)."""

from decimal import Decimal

from cryptography.hazmat.primitives.asymmetric import rsa

from arbbot.exec.kalshi_gateway import KalshiOrderGateway

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def gw():
    return KalshiOrderGateway("kid", KEY, live=False)


def test_yes_bid_payload():
    g = gw()
    r = g.place_yes("KXFED-26DEC-T3.50", "bid", Decimal("0.62"), 5)
    b = g.sent[-1]["body"]
    # V2 body: side bid/ask, count+price fixed-point strings, TIF + STP required
    assert b["side"] == "bid" and b["price"] == "0.6200" and b["count"] == "5.00"
    assert b["time_in_force"] == "good_till_canceled" and b["post_only"] is True
    assert b["self_trade_prevention_type"] == "taker_at_cross"
    assert r["status"] == "dry_run" and r["order_id"].startswith("dry-")


def test_yes_ask_sells_yes_on_yes_axis():
    g = gw()
    g.place_yes("KXFED-26DEC-T3.75", "ask", Decimal("0.40"), 3)
    b = g.sent[-1]["body"]
    # V2: side='ask' sells YES at the YES price directly (no NO complement)
    assert b["side"] == "ask" and b["price"] == "0.4000" and b["count"] == "3.00"


def test_taker_hedge_is_ioc():
    g = gw()
    g.place_yes("T", "bid", Decimal("0.50"), 2, post_only=False)
    b = g.sent[-1]["body"]
    assert b["post_only"] is False and b["time_in_force"] == "immediate_or_cancel"


def test_dry_run_never_flags_live():
    g = gw()
    g.place_yes("T", "bid", Decimal("0.50"), 1)
    g.cancel("dry-abc")
    assert all(rec["live"] is False for rec in g.sent)
    assert g.cancel("dry-abc")["status"] == "dry_cancel"


def test_price_formatted_fixed_point():
    g = gw()
    g.place_yes("T", "bid", Decimal("0.626"), 1)
    assert g.sent[-1]["body"]["price"] == "0.6260"  # 4-dp dollars, not cents
