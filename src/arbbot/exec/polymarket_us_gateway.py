"""Polymarket US (QCX DCM) order gateway: Ed25519-signed order placement.

DRY-RUN by default: logs the exact order it WOULD send and returns a synthetic
id, never touching the venue. Only live=True actually POSTs. Mirrors the Kalshi
gateway interface so the Quoter can route to it via the gateways map.

Auth: sign "{ts}{METHOD}{path}" with Ed25519, headers X-PM-Access-Key /
-Timestamp / -Signature (base64). Key file is base64; seed = first 32 bytes.
Signing + HTTP live in arbbot.venues.pmus (shared client); every request here
is priority="critical" — the order path never waits on the shared rate budget.

Order API (api.polymarket.us):
  POST /v1/orders              create  {marketSlug, type, price{value,currency},
                                         quantity, tif, intent, manualOrderIndicator,
                                         participateDontInitiate}
  POST /v1/order/preview       preview (fills+fees, NEVER submits) — safe to call
  POST /v1/order/{id}/cancel   cancel
  GET  /v1/orders/open         list resting orders
price.value is the YES/long price (0.01-0.99). We quote YES: side='bid' buys
YES (intent BUY_LONG); side='ask' sells YES (intent SELL_LONG).
"""

from __future__ import annotations

import time
import uuid
from decimal import Decimal

import httpx

from arbbot.venues.pmus import API_BASE, PmusSession


class PolymarketUsOrderGateway:
    def __init__(self, key_id: str, secret_key_b64: str, live: bool = False,
                 base: str = API_BASE, client: httpx.Client | None = None):
        self.kid = key_id
        self.session = PmusSession(key_id, secret_key_b64, base=base, client=client)
        self.live = live
        self.base = base
        self.client = self.session.client
        self.sent: list[dict] = []  # audit log (dry-run and live)

    def _headers(self, method: str, path: str) -> dict:
        # signing delegated to the shared client; Content-Type kept
        # byte-identical to the pre-consolidation order path
        return {**self.session.headers(method, path),
                "Content-Type": "application/json"}

    def _order_body(self, slug: str, side: str, price: Decimal, count: int,
                    post_only: bool) -> dict:
        return {
            "marketSlug": slug,
            "type": "ORDER_TYPE_LIMIT",
            "price": {"value": f"{price:.4f}", "currency": "USD"},
            "quantity": count,
            "tif": ("TIME_IN_FORCE_GOOD_TILL_CANCEL" if post_only
                    else "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"),
            # We only ever OPEN exposure: bid = buy YES (BUY_LONG); ask = "sell
            # YES" to open = buy NO (BUY_SHORT). price is always the YES-axis
            # price. (Closing a held long via SELL_LONG isn't used by the arb
            # engine, which opens both legs and holds to resolution.)
            "intent": "ORDER_INTENT_BUY_LONG" if side == "bid" else "ORDER_INTENT_BUY_SHORT",
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            "participateDontInitiate": post_only,  # post-only: don't cross
        }

    def preview(self, slug: str, side: str, price: Decimal, count: int,
                post_only: bool = True) -> dict:
        """POST /v1/order/preview — projected fills + fees, NEVER submits.
        Works regardless of self.live; used to validate the order path safely."""
        body = self._order_body(slug, side, price, count, post_only)
        r = self.session.post("/v1/order/preview", json={"request": body},
                              headers=self._headers("POST", "/v1/order/preview"),
                              priority="critical")
        r.raise_for_status()
        return r.json()

    def place_short(self, slug: str, price: Decimal, count: int,
                    post_only: bool = False) -> dict:
        """OPEN a NO position (intent BUY_SHORT) at YES-axis `price` — hitting a
        resting YES bid at p receives p and holds NO at cost 1-p (fully
        collateralized). Default taker/IOC: this exists for crossing the spread
        on the short leg of a cross-venue basket."""
        return self._submit(self._order_body(slug, "ask", price, count, post_only))

    def place_yes(self, slug: str, side: str, price: Decimal, count: int,
                  post_only: bool = True) -> dict:
        """Rest a maker order (post_only) or send a taker hedge (post_only=False).
        side='bid' buys YES at price; side='ask' sells YES at price (closing a
        held long — to OPEN a short use place_short)."""
        body = self._order_body(slug, side, price, count, post_only)
        return self._submit(body)

    def _submit(self, body: dict) -> dict:
        rec = {"ts": time.time(), "op": "place", "live": self.live, "body": body}
        self.sent.append(rec)
        if not self.live:
            rec["result"] = {"id": "dry-" + uuid.uuid4().hex, "state": "dry_run"}
            return rec["result"]
        # create takes the BARE order body; only /v1/order/preview wraps in
        # {"request": ...} (verified against the official SDK — mismatched
        # wrappers give a misleading "Market not found")
        r = self.session.post("/v1/orders", json=body,
                              headers=self._headers("POST", "/v1/orders"),
                              priority="critical")
        r.raise_for_status()
        rec["result"] = r.json()
        return rec["result"]

    def cancel(self, order_id: str, market_slug: str | None = None) -> dict:
        rec = {"ts": time.time(), "op": "cancel", "live": self.live, "order_id": order_id}
        self.sent.append(rec)
        if not self.live or (order_id or "").startswith("dry-"):
            rec["result"] = {"state": "dry_cancel"}
            return rec["result"]
        # cancel requires the order's marketSlug in the body. The caller MUST
        # pass it — we do NOT self-resolve via open_orders (that hammered the
        # API into 429s on every reprice). raise_for_status so a failed cancel
        # is never mistaken for success (which caused stray-order accumulation).
        if not market_slug:
            raise ValueError("cancel needs market_slug (no open_orders fallback)")
        path = f"/v1/order/{order_id}/cancel"
        r = self.session.post(path, json={"marketSlug": market_slug},
                              headers=self._headers("POST", path),
                              priority="critical")
        r.raise_for_status()
        rec["result"] = {"status": r.status_code}
        return rec["result"]

    def cancel_all_open(self) -> dict:
        """Cancel every resting order in one call (kill-switch cleanup). Safe,
        idempotent, single request."""
        if not self.live:
            return {"state": "dry_cancel_all"}
        r = self.session.post("/v1/orders/open/cancel", json={},
                              headers=self._headers("POST", "/v1/orders/open/cancel"),
                              priority="critical")
        r.raise_for_status()
        return r.json()

    def get_order(self, order_id: str) -> dict:
        """Authoritative order status (create response omits cumQuantity for
        IOC fills — must re-fetch to learn the true filled quantity)."""
        r = self.session.get(f"/v1/order/{order_id}",
                             headers=self._headers("GET", f"/v1/order/{order_id}"),
                             priority="critical")
        r.raise_for_status()
        j = r.json()
        return j.get("order", j)

    def filled_qty(self, order_id: str) -> int:
        """Actual filled contract count for an order (0 if unknown)."""
        try:
            o = self.get_order(order_id)
            return int(float(o.get("cumQuantity") or 0))
        except (httpx.HTTPError, ValueError, TypeError):
            return 0

    def open_orders(self) -> list[dict]:
        r = self.session.get("/v1/orders/open",
                             headers=self._headers("GET", "/v1/orders/open"),
                             priority="critical")
        r.raise_for_status()
        j = r.json()
        return j if isinstance(j, list) else j.get("orders", [])

    def rehearse(self, slug: str) -> dict:
        """Live end-to-end validation with ~zero fill risk: place ONE contract
        at 1c (far below any real bid), confirm it rests, cancel it."""
        if not self.live:
            return {"skipped": "dry-run"}
        placed = self.place_yes(slug, "bid", Decimal("0.01"), 1)
        oid = placed.get("id") or placed.get("order_id")
        time.sleep(1.0)
        rested = any((o.get("id") or o.get("orderId")) == oid for o in self.open_orders())
        cancelled = self.cancel(oid, market_slug=slug)
        return {"order_id": oid, "rested": rested, "cancelled": cancelled}
