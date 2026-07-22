"""_record_maker_fill: the trade-ledger writer. Two live bugs lived here
(2026-07-21/22): PM async fills recorded as avg_price=0 (create responses
carry no execution data), and maker-BID hedges (which SELL YES) were costed
at h_px instead of 1-h_px, overstating profit ($3.20 fake vs $0.19 real on
the first Mamdani maker-bid fill). These pin both.
"""

import json
from decimal import Decimal
from pathlib import Path

from arbbot.exec.main import _record_maker_fill
from arbbot.models.core import Venue
from arbbot.registry.model import (
    Leg, Relationship, RelationshipType, Verdict, VettedBy,
)


def rel():
    return Relationship(
        id="xvus-test-pair", type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[Leg(venue=Venue.KALSHI, market_id="KXTEST-26"),
              Leg(venue=Venue.POLYMARKET_US, market_id="test-slug")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN)


class FakePmGw:
    """PM-shaped hedge gateway: first re-fetch races the async fill (cum 0),
    the next returns the authoritative order — mirrors live behavior."""
    def __init__(self, avg_px="0.2600", commission="0.0500", cum=4, misses=1):
        self.calls = 0
        self.misses = misses
        self.order = {"id": "BDTEST1", "cumQuantity": cum,
                      "avgPx": {"value": avg_px},
                      "commissionNotionalTotalCollected": {"value": commission}}

    def get_order(self, oid):
        self.calls += 1
        if self.calls <= self.misses:  # fill not landed yet
            return {"id": oid, "cumQuantity": 0, "avgPx": {"value": "0"}}
        return self.order


def _last_row(tmp_path):
    return json.loads((tmp_path / "data/exec/trades.jsonl").read_text()
                      .splitlines()[-1])


def test_pm_hedge_retries_until_async_fill_lands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    gw = FakePmGw(misses=1)
    # maker BID fill on Kalshi @0.20 x4; hedge SOLD YES on PM @0.26 (Mamdani)
    _record_maker_fill(None, rel(), 0, "bid", 4, "0.20",
                       {"hedge_order": {"id": "BDTEST1", "executions": []}},
                       hedge_gw=gw)
    assert gw.calls >= 2, "must retry past the async-fill race"
    row = _last_row(tmp_path)
    assert row["legs"][1]["avg_price"] == "0.2600"
    assert row["legs"][1]["fees"] == "0.0500"  # TOTAL, not x4


def test_maker_bid_hedge_costs_one_minus_price(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _record_maker_fill(None, rel(), 0, "bid", 4, "0.20",
                       {"hedge_order": {"id": "BDTEST1", "executions": []}},
                       hedge_gw=FakePmGw(misses=0))
    row = _last_row(tmp_path)
    # maker 0.20*4 + hedge NO (1-0.26)*4 + fee 0.05 = 3.81 ; profit 0.19
    assert abs(row["cost_usd"] - 3.81) < 1e-9
    assert abs(row["profit_usd"] - 0.19) < 1e-9


def test_maker_ask_hedge_costs_price(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # maker ASK on Kalshi @0.09 (opens NO at 0.91) x3; hedge BUYS YES @0.03
    _record_maker_fill(None, rel(), 0, "ask", 3, "0.09",
                       {"hedge_order": {"id": "BDTEST1", "executions": []}},
                       hedge_gw=FakePmGw(avg_px="0.0300", commission="0.0100",
                                         cum=3, misses=0))
    row = _last_row(tmp_path)
    # maker (1-0.09)*3 + hedge 0.03*3 + fee 0.01 = 2.83 ; profit 0.17 (Koch)
    assert abs(row["cost_usd"] - 2.83) < 1e-9
    assert abs(row["profit_usd"] - 0.17) < 1e-9


def test_kalshi_shaped_hedge_needs_no_refetch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Kalshi hedge responses carry average_fill_price + PER-CONTRACT fee
    _record_maker_fill(None, rel(), 1, "ask", 5, "0.13",
                       {"hedge_order": {"order_id": "k1",
                                        "average_fill_price": "0.1000",
                                        "average_fee_paid": "0.0063"}})
    row = _last_row(tmp_path)
    # maker (1-0.13)*5 + hedge 0.10*5 + fees 0.0063*5
    assert abs(row["cost_usd"] - (0.87 * 5 + 0.5 + 0.0063 * 5)) < 1e-9
    assert row["legs"][1]["order_id"] == "k1"
