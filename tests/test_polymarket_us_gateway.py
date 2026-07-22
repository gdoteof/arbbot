"""Polymarket US order gateway: body construction + dry-run (no network)."""

import base64
from decimal import Decimal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway

# a throwaway Ed25519 seed, base64-encoded like the real key file
_seed = ed25519.Ed25519PrivateKey.generate().private_bytes(
    encoding=serialization.Encoding.Raw,
    format=serialization.PrivateFormat.Raw,
    encryption_algorithm=serialization.NoEncryption(),
)
KEY_B64 = base64.b64encode(_seed).decode()


def gw():
    return PolymarketUsOrderGateway("kid-123", KEY_B64, live=False)


def test_bid_buys_yes_long_gtc_postonly():
    g = gw()
    r = g.place_yes("some-slug", "bid", Decimal("0.62"), 5)
    b = g.sent[-1]["body"]
    assert b["marketSlug"] == "some-slug"
    assert b["price"] == {"value": "0.6200", "currency": "USD"}
    assert b["quantity"] == 5
    assert b["intent"] == "ORDER_INTENT_BUY_LONG"
    assert b["tif"] == "TIME_IN_FORCE_GOOD_TILL_CANCEL"
    assert b["participateDontInitiate"] is True
    assert b["manualOrderIndicator"] == "MANUAL_ORDER_INDICATOR_AUTOMATIC"
    assert r["state"] == "dry_run" and r["id"].startswith("dry-")


def test_ask_opens_short_buys_no():
    g = gw()
    g.place_yes("s", "ask", Decimal("0.40"), 3)
    # ask = "sell YES" to OPEN = buy NO (BUY_SHORT); we only ever open exposure
    assert g.sent[-1]["body"]["intent"] == "ORDER_INTENT_BUY_SHORT"


def test_place_short_opens_no_position_ioc():
    g = gw()
    r = g.place_short("s", Decimal("0.25"), 100)
    b = g.sent[-1]["body"]
    # BUY_SHORT at YES-axis price: hits a resting YES bid, holds NO at 1-p
    assert b["intent"] == "ORDER_INTENT_BUY_SHORT"
    assert b["price"] == {"value": "0.2500", "currency": "USD"}
    assert b["quantity"] == 100
    assert b["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert b["participateDontInitiate"] is False
    assert r["state"] == "dry_run"


def test_place_short_can_rest_postonly():
    g = gw()
    g.place_short("s", Decimal("0.99"), 1, post_only=True)
    b = g.sent[-1]["body"]
    assert b["intent"] == "ORDER_INTENT_BUY_SHORT"
    assert b["tif"] == "TIME_IN_FORCE_GOOD_TILL_CANCEL"
    assert b["participateDontInitiate"] is True


def test_taker_is_ioc_not_postonly():
    g = gw()
    g.place_yes("s", "bid", Decimal("0.50"), 2, post_only=False)
    b = g.sent[-1]["body"]
    assert b["tif"] == "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
    assert b["participateDontInitiate"] is False


def test_dry_run_never_live_and_cancel():
    g = gw()
    g.place_yes("s", "bid", Decimal("0.5"), 1)
    g.cancel("dry-abc")
    assert all(rec["live"] is False for rec in g.sent)
    assert g.cancel("dry-abc")["state"] == "dry_cancel"


def test_headers_have_pm_auth_fields():
    g = gw()
    h = g._headers("GET", "/v1/account/balances")
    assert h["X-PM-Access-Key"] == "kid-123"
    assert h["X-PM-Timestamp"].isdigit()
    assert len(h["X-PM-Signature"]) > 0
