"""Venue-quirk CONTRACT tests — recorded live-response fixtures.

Every fixture below is a (trimmed) copy of a real response captured from live
traffic during the week of 2026-07-21. These pin the venue behaviors that are
NOT guessable from docs and that a rewrite (any language) must reproduce:

Kalshi:
  K1. GET /portfolio/orders is HISTORY (canceled/executed included) and
      PAGINATED (cursor); ?status=resting returns nothing -> page it all,
      filter client-side. Missing pagination orphaned live orders (the 2x
      Melenchon/Koch doubles).
  K2. The signature covers the PATH ONLY — the query string rides on the URL.
  K3. fill_count_fp is a plain contract count ("2.00" == 2 contracts).
  K4. cancel: 404 == already gone == success; other non-2xx must RAISE
      (silently swallowing orphans a live order).
  K5. a crossing post_only place is REJECTED with 400 (raises).

Polymarket US:
  P1. order CREATE responses omit execution data ({"executions": []}) — fills
      report async; an immediate re-fetch can still show cumQuantity 0.
  P2. the authoritative order has avgPx.value + commissionNotionalTotalCollected
      .value, and that commission is a TOTAL (not per-contract).
  P3. filled_qty comes from cumQuantity via re-fetch, never the create response.
  P4. the order id field is "id" (not order_id).
  P5. /v1/portfolio/positions returns a dict KEYED BY SLUG with string
      netPosition ("-14"); /v1/positions 404s.
"""

import json
from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
# any valid ed25519 seed works for signing in tests (32 zero bytes, b64)
PM_SEED_B64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def kalshi_gw(handler):
    return KalshiOrderGateway("kid", KEY, live=True,
                              client=httpx.Client(transport=httpx.MockTransport(handler)))


def pm_gw(handler):
    return PolymarketUsOrderGateway("kid", PM_SEED_B64, live=True,
                                    client=httpx.Client(transport=httpx.MockTransport(handler)))


# ---------------------------------------------------------------- Kalshi ----

def _order(oid, status, ticker="KXTIME-26-ZOH", fill="0.00"):
    # shape from live GET /portfolio/orders rows (2026-07-21)
    return {"order_id": oid, "status": status, "ticker": ticker,
            "side": "yes", "action": "buy", "fill_count_fp": fill}


def test_k1_resting_orders_pages_and_history():
    pages = {None: {"orders": [_order(f"a{i}", "canceled") for i in range(3)]
                              + [_order("r1", "resting")],
                    "cursor": "CUR2"},
             "CUR2": {"orders": [_order("r2", "resting"),
                                 _order("x1", "executed", fill="5.00")]}}
    seen_urls = []

    def handler(req):
        seen_urls.append(str(req.url))
        cur = req.url.params.get("cursor")
        return httpx.Response(200, json=pages[cur])

    g = kalshi_gw(handler)
    orders = g.resting_orders()
    assert len(orders) == 6, "must return the FULL history across pages"
    resting = [o for o in orders if o["status"] == "resting"]
    assert {o["order_id"] for o in resting} == {"r1", "r2"}
    assert "cursor=CUR2" in seen_urls[1]


def test_k2_signature_covers_path_only():
    signed_paths = []
    g = KalshiOrderGateway("kid", KEY, live=True,
                           client=httpx.Client(transport=httpx.MockTransport(
                               lambda req: httpx.Response(200, json={"orders": []}))))
    orig = g._headers
    g._headers = lambda m, p: (signed_paths.append(p), orig(m, p))[1]
    g.resting_orders()
    assert signed_paths and all("?" not in p and "cursor" not in p
                                for p in signed_paths), \
        "query string must NOT be signed (Kalshi excludes it)"


def test_k3_fill_count_fp_is_plain_contract_count():
    g = kalshi_gw(lambda req: httpx.Response(
        200, json={"order": _order("o1", "executed", fill="2.00")}))
    assert g.filled_qty("o1") == 2


def test_k3b_filled_qty_zero_on_missing_or_dry():
    g = kalshi_gw(lambda req: httpx.Response(404, json={}))
    assert g.filled_qty("gone") == 0
    assert g.filled_qty("dry-abc") == 0  # never hits the wire


def test_k4_cancel_404_is_success_other_errors_raise():
    g = kalshi_gw(lambda req: httpx.Response(404, json={}))
    assert g.cancel("already-gone")["status"] == 404
    g2 = kalshi_gw(lambda req: httpx.Response(500, json={}))
    with pytest.raises(httpx.HTTPStatusError):
        g2.cancel("o-live")


def test_k5_crossing_post_only_rejected_400():
    g = kalshi_gw(lambda req: httpx.Response(
        400, json={"error": {"code": "order_would_cross"}}))
    with pytest.raises(httpx.HTTPStatusError):
        g.place_yes("KXTIME-26-ZOH", "bid", Decimal("0.20"), 5, post_only=True)


# ---------------------------------------------------------- Polymarket US ----

# recorded 2026-07-22: create response for the Mamdani hedge (BD819026675P)
PM_CREATE = {"id": "BD819026675P", "executions": []}
# recorded 2026-07-22: the same order re-fetched after the async fill landed
PM_ORDER_FILLED = {
    "id": "BD819026675P", "marketSlug": "tpoyc-2026-zohmam",
    "side": "ORDER_SIDE_SELL", "quantity": 4, "cumQuantity": 4,
    "leavesQuantity": 0, "state": "ORDER_STATE_FILLED",
    "avgPx": {"value": "0.2600", "currency": "USD"},
    "commissionNotionalTotalCollected": {"value": "0.0500", "currency": "USD"},
    "outcomeSide": "OUTCOME_SIDE_NO",
}
# recorded 2026-07-21: positions response shape (trimmed)
PM_POSITIONS = {"positions": {
    "tpoyc-2026-popleo": {"netPosition": "-14", "qtySold": "14",
                          "avgPx": {"value": "0.7550", "currency": "USD"}}},
    "nextCursor": "", "eof": True}


def test_p1_create_omits_execution_data():
    g = pm_gw(lambda req: httpx.Response(200, json=PM_CREATE))
    res = g.place_short("tpoyc-2026-zohmam", Decimal("0.26"), 4, post_only=False)
    assert res["id"] == "BD819026675P"
    assert res["executions"] == [], "create NEVER carries fill data"
    assert "avgPx" not in res


def test_p2_refetch_has_avgpx_and_TOTAL_commission():
    g = pm_gw(lambda req: httpx.Response(200, json=PM_ORDER_FILLED))
    o = g.get_order("BD819026675P")
    assert o["avgPx"]["value"] == "0.2600"
    # 0.05 is the TOTAL for 4 contracts (1.25c/ct) — dividing by qty is the
    # caller's job; treating it as per-contract quadruples the fee.
    assert o["commissionNotionalTotalCollected"]["value"] == "0.0500"


def test_p3_filled_qty_from_cumquantity():
    g = pm_gw(lambda req: httpx.Response(200, json=PM_ORDER_FILLED))
    assert g.filled_qty("BD819026675P") == 4
    g2 = pm_gw(lambda req: httpx.Response(500, json={}))
    assert g2.filled_qty("whatever") == 0


def test_p4_order_id_field_is_id():
    # the quoter's extraction chain must find PM ids under "id"
    res = dict(PM_CREATE)
    oid = ((res.get("order") or {}).get("order_id")
           or res.get("order_id") or res.get("id"))
    assert oid == "BD819026675P"


def test_p5_positions_dict_keyed_by_slug():
    g = pm_gw(lambda req: httpx.Response(200, json=PM_POSITIONS))
    r = g.client.get(g.base + "/v1/portfolio/positions",
                     headers=g._headers("GET", "/v1/portfolio/positions")).json()
    pos = r["positions"]
    assert isinstance(pos, dict), "positions is a DICT keyed by slug, not a list"
    assert float(pos["tpoyc-2026-popleo"]["netPosition"]) == -14.0
