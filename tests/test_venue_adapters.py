"""Venue normalization tests — the exact spots where convention bugs hide."""

import json
from decimal import Decimal

from arbbot.models.core import MarketStatus, Venue
from arbbot.record.kalshi import normalize_market, normalize_orderbook, normalize_ws_trade
from arbbot.record.polymarket import (
    SeqCounter,
    normalize_gamma_market,
    normalize_price_changes,
    parse_ws_frame,
    subscribe_frame,
)


def test_kalshi_no_bids_become_yes_asks():
    snap = normalize_orderbook(
        "T",
        {"yes_dollars": [["0.4000", "100.00"]], "no_dollars": [["0.5500", "80.00"]]},
        seq=1,
        ts_local_ns=1,
    )
    assert snap.bids[0].price == Decimal("0.4000")
    # NO bid at 0.55 == YES ask at 0.45
    assert snap.asks[0].price == Decimal("0.4500")
    assert snap.asks[0].size == Decimal("80.00")


def test_kalshi_orderbook_sorted_and_zero_filtered():
    snap = normalize_orderbook(
        "T",
        {
            "yes_dollars": [["0.10", "5"], ["0.30", "5"], ["0.20", "0"]],
            "no_dollars": [["0.50", "1"], ["0.80", "2"]],
        },
        seq=1,
        ts_local_ns=1,
    )
    assert [l.price for l in snap.bids] == [Decimal("0.30"), Decimal("0.10")]
    # NO 0.80 -> ask 0.20 (best first), NO 0.50 -> ask 0.50
    assert [l.price for l in snap.asks] == [Decimal("0.20"), Decimal("0.50")]


def test_kalshi_market_status_mapping():
    m = normalize_market({"ticker": "X", "status": "finalized", "title": "t"})
    assert m.status is MarketStatus.RESOLVED
    assert normalize_market({"ticker": "X", "status": "active"}).status is MarketStatus.ACTIVE


def test_kalshi_ws_trade_yes_denominated():
    t = normalize_ws_trade(
        {
            "market_ticker": "M",
            "yes_price_dollars": "0.360",
            "no_price_dollars": "0.640",
            "count_fp": "136.00",
            "taker_side": "no",
            "ts_ms": 1669149841000,
        },
        seq=7,
    )
    assert t.price == Decimal("0.360")
    assert t.taker_side == "sell"
    assert t.seq == 7


def test_gamma_market_parses_json_encoded_fields():
    raw = {
        "question": "Will X happen?",
        "clobTokenIds": json.dumps(["111", "222"]),
        "outcomes": json.dumps(["Yes", "No"]),
        "orderPriceMinTickSize": 0.001,
        "orderMinSize": 5,
        "active": True,
        "closed": False,
        "endDate": "2026-12-31T00:00:00Z",
        "events": [{"category": "Politics"}],
    }
    m = normalize_gamma_market(raw)
    assert m.market_id == "111" and m.no_market_id == "222"
    assert m.tick_size == Decimal("0.001")
    assert m.min_order_size == Decimal("5")
    assert m.category == "politics"
    assert m.status is MarketStatus.ACTIVE
    assert m.venue is Venue.POLYMARKET


def test_gamma_outcome_order_not_assumed():
    raw = {
        "question": "q",
        "clobTokenIds": json.dumps(["no-tok", "yes-tok"]),
        "outcomes": json.dumps(["No", "Yes"]),
        "active": True,
        "closed": False,
    }
    m = normalize_gamma_market(raw)
    assert m.market_id == "yes-tok" and m.no_market_id == "no-tok"


def test_price_change_size_is_new_total_and_zero_removes():
    seq = SeqCounter()
    deltas = normalize_price_changes(
        {
            "event_type": "price_change",
            "timestamp": "1",
            "price_changes": [
                {"asset_id": "111", "price": "0.5", "size": "200", "side": "BUY"},
                {"asset_id": "111", "price": "0.52", "size": "0", "side": "SELL"},
            ],
        },
        seq.next,
    )
    assert deltas[0].side == "bid" and deltas[0].size == Decimal("200")
    assert deltas[1].side == "ask" and deltas[1].size == Decimal("0")
    assert [d.seq for d in deltas] == [1, 2]  # synthesized per-market seq


def test_parse_ws_frame_array_and_pong_and_unknown():
    seq = SeqCounter()
    assert parse_ws_frame("PONG", seq) == []
    frame = json.dumps(
        [
            {
                "event_type": "book",
                "asset_id": "111",
                "bids": [{"price": ".48", "size": "30"}],
                "asks": [{"price": ".52", "size": "25"}],
                "timestamp": "1",
            },
            {"event_type": "tick_size_change", "asset_id": "111"},
            {
                "event_type": "last_trade_price",
                "asset_id": "111",
                "price": "0.5",
                "size": "10",
                "side": "SELL",
                "timestamp": "2",
            },
        ]
    )
    events = parse_ws_frame(frame, seq)
    assert [e.kind for e in events] == ["snapshot", "trade"]
    assert events[0].bids[0].price == Decimal("0.48")
    assert events[1].taker_side == "sell"


def test_subscribe_frame_shape():
    f = json.loads(subscribe_frame(["a", "b"]))
    assert f == {"type": "market", "assets_ids": ["a", "b"], "custom_feature_enabled": True}
