"""Venue-quirk contract pins — gap rows from docs/venue-quirks-test-gaps.md
(card 7fab301e). MockTransport only; nothing touches a venue.

Kalshi:
  K6. order CREATE must POST /portfolio/events/orders (the legacy
      /portfolio/orders POST now 410s); cancel DELETEs
      /portfolio/events/orders/{id}; list/get stay on /portfolio/orders.
Polymarket US:
  P6. live cancel POSTs /v1/order/{id}/cancel with {"marketSlug": ...} in the
      body; missing slug raises ValueError (no open_orders fallback);
      a non-2xx cancel RAISES (a swallowed failure orphans a live order).
  P7. order CREATE posts the BARE body; only /v1/order/preview wraps in
      {"request": ...} (mismatched wrappers give a misleading
      "Market not found").
Polymarket intl (CLOB REST):
  P8. /book arrays arrive with the BEST level at the END (asks high->low,
      bids low->high) — normalization must sort best-first, never assume.
"""

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.record.polymarket import ClobRest

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PM_SEED_B64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def kalshi_gw(handler):
    return KalshiOrderGateway("kid", KEY, live=True,
                              client=httpx.Client(transport=httpx.MockTransport(handler)))


def pm_gw(handler):
    return PolymarketUsOrderGateway("kid", PM_SEED_B64, live=True,
                                    client=httpx.Client(transport=httpx.MockTransport(handler)))


# ---------------------------------------------------------------- Kalshi ----

def test_k6_create_posts_events_orders_endpoint():
    seen = []

    def handler(req):
        seen.append((req.method, req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"order": {"order_id": "o1"}})

    g = kalshi_gw(handler)
    g.place_yes("KXTIME-26-ZOH", "bid", Decimal("0.20"), 5)
    method, path, body = seen[0]
    assert method == "POST"
    # create moved: the legacy POST /portfolio/orders now 410s
    assert path == "/trade-api/v2/portfolio/events/orders"
    assert body["ticker"] == "KXTIME-26-ZOH"


def test_k6_cancel_deletes_events_orders_id():
    seen = []

    def handler(req):
        seen.append((req.method, req.url.path))
        return httpx.Response(200, json={})

    g = kalshi_gw(handler)
    g.cancel("OID123")
    assert seen == [("DELETE", "/trade-api/v2/portfolio/events/orders/OID123")]


def test_k6_list_and_get_stay_on_portfolio_orders():
    seen = []

    def handler(req):
        seen.append((req.method, req.url.path))
        if req.url.path.endswith("/portfolio/orders"):
            return httpx.Response(200, json={"orders": []})
        return httpx.Response(200, json={"order": {"fill_count_fp": "2.00"}})

    g = kalshi_gw(handler)
    g.resting_orders()
    assert g.filled_qty("OID123") == 2
    assert seen[0] == ("GET", "/trade-api/v2/portfolio/orders")
    assert seen[1] == ("GET", "/trade-api/v2/portfolio/orders/OID123")


# ---------------------------------------------------------- Polymarket US ----

def test_p6_cancel_requires_market_slug_in_body():
    seen = []

    def handler(req):
        seen.append((req.method, req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={})

    g = pm_gw(handler)
    g.cancel("BD819026675P", market_slug="tpoyc-2026-zohmam")
    method, path, body = seen[0]
    assert method == "POST"
    assert path == "/v1/order/BD819026675P/cancel"
    assert body == {"marketSlug": "tpoyc-2026-zohmam"}


def test_p6_cancel_without_slug_raises_never_hits_wire():
    hit = []
    g = pm_gw(lambda req: hit.append(1) or httpx.Response(200, json={}))
    with pytest.raises(ValueError):
        g.cancel("BD819026675P")  # no open_orders fallback (429 hammer)
    assert not hit


def test_p6_cancel_non_2xx_raises():
    g = pm_gw(lambda req: httpx.Response(500, json={}))
    with pytest.raises(httpx.HTTPStatusError):
        g.cancel("BD819026675P", market_slug="tpoyc-2026-zohmam")


def test_p7_create_bare_body_preview_wrapped():
    seen = []

    def handler(req):
        seen.append((req.url.path, json.loads(req.content)))
        return httpx.Response(200, json={"id": "X", "executions": []})

    g = pm_gw(handler)
    g.place_short("tpoyc-2026-zohmam", Decimal("0.26"), 4, post_only=False)
    g.preview("tpoyc-2026-zohmam", "bid", Decimal("0.26"), 4)
    create_path, create_body = seen[0]
    preview_path, preview_body = seen[1]
    assert create_path == "/v1/orders"
    assert "request" not in create_body, "create takes the BARE body"
    assert create_body["marketSlug"] == "tpoyc-2026-zohmam"
    assert preview_path == "/v1/order/preview"
    assert set(preview_body) == {"request"}, "only preview wraps in {'request': ...}"
    assert preview_body["request"]["marketSlug"] == "tpoyc-2026-zohmam"


# ------------------------------------------------------- Polymarket intl ----

def test_p8_clob_book_best_at_end_normalized_best_first():
    # live /book shape: asks high->low, bids low->high (best at the END)
    fixture = {"bids": [{"price": "0.01", "size": "10"},
                        {"price": "0.40", "size": "5"},
                        {"price": "0.48", "size": "7"}],
               "asks": [{"price": "0.99", "size": "10"},
                        {"price": "0.60", "size": "5"},
                        {"price": "0.52", "size": "7"}],
               "timestamp": "1"}
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=fixture)))
    snap = asyncio.run(ClobRest(client=client).book("tok1", seq=1))
    assert [l.price for l in snap.bids] == [Decimal("0.48"), Decimal("0.40"), Decimal("0.01")]
    assert [l.price for l in snap.asks] == [Decimal("0.52"), Decimal("0.60"), Decimal("0.99")]
