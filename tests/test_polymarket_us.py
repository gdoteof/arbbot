"""Polymarket US adapter: market + book normalization (offline)."""

from decimal import Decimal

from arbbot.models.core import BookSnapshot, MarketStatus, Trade, Venue
from arbbot.record.polymarket import SeqCounter
from arbbot.record.polymarket_us import (
    normalize_book, normalize_market, parse_ws_message, ws_auth_headers,
)


def test_normalize_market():
    raw = {"slug": "rdc-usfed-fomc-2026-07-29-nochng", "question": "No change?",
           "orderPriceMinTickSize": 0.01, "minimumTradeQty": 0.01,
           "feeCoefficient": 0.06, "active": True, "closed": False,
           "endDate": "2026-08-12T23:00:00Z"}
    m = normalize_market(raw, "macro")
    assert m.venue is Venue.POLYMARKET_US
    assert m.market_id == "rdc-usfed-fomc-2026-07-29-nochng"
    assert m.tick_size == Decimal("0.01")
    assert m.taker_fee_coef_override == Decimal("0.06")
    assert m.status is MarketStatus.ACTIVE
    assert m.category == "macro"


def test_normalize_market_closed():
    raw = {"slug": "x", "closed": True, "active": True}
    assert normalize_market(raw, "macro").status is MarketStatus.CLOSED


def test_normalize_book_uses_offers_for_asks_and_sorts():
    md = {
        "bids": [{"px": {"value": "0.92", "currency": "USD"}, "qty": "260"},
                 {"px": {"value": "0.93", "currency": "USD"}, "qty": "54596"}],
        "offers": [{"px": {"value": "0.95", "currency": "USD"}, "qty": "10"},
                   {"px": {"value": "0.94", "currency": "USD"}, "qty": "4222"}],
        "transactTime": "123",
    }
    b = normalize_book("slug1", md, seq=7)
    assert b.venue is Venue.POLYMARKET_US and b.seq == 7
    # bids best (highest) first, asks best (lowest) first
    assert b.bids[0].price == Decimal("0.93") and b.bids[0].size == Decimal("54596")
    assert b.asks[0].price == Decimal("0.94") and b.asks[0].size == Decimal("4222")


def test_normalize_book_drops_zero_size():
    md = {"bids": [{"px": {"value": "0.5", "currency": "USD"}, "qty": "0"}], "offers": []}
    assert normalize_book("s", md, 1).bids == []


def test_parse_ws_market_data_to_snapshot():
    msg = {"requestId": "book", "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
           "marketData": {"marketSlug": "s1",
                          "bids": [{"px": {"value": "0.93", "currency": "USD"}, "qty": "100"}],
                          "offers": [{"px": {"value": "0.94", "currency": "USD"}, "qty": "50"}]}}
    evs = parse_ws_message(msg, SeqCounter().next)
    assert len(evs) == 1 and isinstance(evs[0], BookSnapshot)
    assert evs[0].venue is Venue.POLYMARKET_US and evs[0].market_id == "s1"
    assert evs[0].bids[0].price == Decimal("0.93")


def test_parse_ws_trade():
    msg = {"requestId": "tape", "subscriptionType": "SUBSCRIPTION_TYPE_TRADE",
           "trade": {"marketSlug": "s1", "price": {"value": "0.93", "currency": "USD"},
                     "quantity": "12", "tradeTime": "t",
                     "taker": {"side": "x", "intent": "ORDER_INTENT_BUY_LONG"}}}
    evs = parse_ws_message(msg, SeqCounter().next)
    assert len(evs) == 1 and isinstance(evs[0], Trade)
    assert evs[0].taker_side == "buy" and evs[0].size == Decimal("12")


def test_parse_ws_ignores_heartbeat_and_error():
    assert parse_ws_message({"heartbeat": {}}, SeqCounter().next) == []
    assert parse_ws_message({"error": "invalid_message"}, SeqCounter().next) == []


def test_ws_auth_headers_present():
    import base64
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519
    seed = ed25519.Ed25519PrivateKey.generate().private_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption())
    h = ws_auth_headers("kid", base64.b64encode(seed).decode())
    assert h["X-PM-Access-Key"] == "kid" and h["X-PM-Timestamp"].isdigit()
    assert len(h["X-PM-Signature"]) > 0
