"""Kalshi order gateway: RSA-signed order placement/cancel.

DRY-RUN by default: logs the exact order it WOULD send and returns a synthetic
id, never touching the venue. Only live=True actually POSTs. Every real order
goes through the RiskManager before this gateway is called (see quoter).

Kalshi order API (trade-api/v2, create is V2):
  POST   /portfolio/events/orders   place  {ticker, side (bid|ask), count (str),
                                             price (str dollars), time_in_force,
                                             self_trade_prevention_type,
                                             client_order_id, post_only}
  DELETE /portfolio/events/orders/{id}   cancel (V2); GET /portfolio/orders lists
count and price are fixed-point STRINGS ("5.00", "0.0520"). price is on the
YES axis: side='bid' buys YES at price; side='ask' sells YES at price (V2
handles the NO conversion natively — no complementing needed).
"""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from typing import Optional

import httpx

from arbbot.record.kalshi import REST_BASE, sign_headers

WS_PATH = "/trade-api/v2"


class KalshiOrderGateway:
    def __init__(self, api_key_id: str, private_key, live: bool = False,
                 base: str = REST_BASE, client: httpx.Client | None = None):
        self.kid = api_key_id
        self.key = private_key
        self.live = live
        self.base = base
        self.client = client or httpx.Client(timeout=15)
        self.sent: list[dict] = []  # audit log (dry-run and live)

    def _headers(self, method: str, path: str) -> dict:
        return sign_headers(self.kid, self.key, method, "/trade-api/v2" + path)

    def place_yes(self, ticker: str, side: str, price: Decimal, count: int,
                  post_only: bool = True) -> dict:
        """Order on the YES price axis (V2 API). side='bid' -> buy YES at
        `price`; side='ask' -> sell YES at `price` (== buy NO at 1-price).
        price is a Decimal probability. post_only=True rests as a maker
        (good_till_canceled); post_only=False is a taker hedge (IOC)."""
        coid = uuid.uuid4().hex
        body = {"ticker": ticker, "side": side,
                "count": f"{count:.2f}", "price": f"{price:.4f}",
                "time_in_force": "good_till_canceled" if post_only else "immediate_or_cancel",
                "self_trade_prevention_type": "taker_at_cross",
                "client_order_id": coid, "post_only": post_only}
        rec = {"ts": time.time(), "op": "place", "live": self.live, "body": body}
        self.sent.append(rec)
        if not self.live:
            rec["result"] = {"order_id": "dry-" + coid, "status": "dry_run"}
            return rec["result"]
        # create moved to /portfolio/events/orders (the legacy /portfolio/orders
        # POST now 410s; GET/list + cancel still use /portfolio/orders)
        r = self.client.post(self.base + "/portfolio/events/orders",
                             json=body, headers=self._headers("POST", "/portfolio/events/orders"))
        r.raise_for_status()
        rec["result"] = r.json()
        return rec["result"]

    def cancel(self, order_id: str, market_slug: str | None = None) -> dict:  # slug ignored (Kalshi cancels by id)
        rec = {"ts": time.time(), "op": "cancel", "live": self.live, "order_id": order_id}
        self.sent.append(rec)
        if not self.live or order_id.startswith("dry-"):
            rec["result"] = {"status": "dry_cancel"}
            return rec["result"]
        r = self.client.delete(self.base + f"/portfolio/events/orders/{order_id}",
                               headers=self._headers("DELETE", f"/portfolio/events/orders/{order_id}"))
        # RAISE on a real failure so the quoter keeps the order tracked+hedgeable
        # instead of orphaning it. 404 == already gone == successfully cancelled.
        if r.status_code >= 300 and r.status_code != 404:
            r.raise_for_status()
        rec["result"] = {"status": r.status_code}
        return rec["result"]

    def resting_orders(self) -> list[dict]:
        """ALL orders across pages. /portfolio/orders is paginated (100/page +
        cursor) and ?status=resting returns nothing, so we page the full list
        and callers filter status client-side. Without pagination the sweep
        misses older resting orders and orphans them (naked-fill risk)."""
        out: list[dict] = []
        cursor = None
        for _ in range(50):  # hard cap ~10k orders
            # sign the PATH ONLY (Kalshi excludes the query string from the
            # signature); the query rides on the request URL.
            query = "?limit=200" + (f"&cursor={cursor}" if cursor else "")
            r = self.client.get(self.base + "/portfolio/orders" + query,
                                headers=self._headers("GET", "/portfolio/orders"))
            r.raise_for_status()
            j = r.json()
            out.extend(j.get("orders", []))
            cursor = j.get("cursor")
            if not cursor:
                break
        return out

    def filled_qty(self, order_id: str) -> int:
        """Cumulative filled contracts for an order — the poll's maker-fill
        signal on the Kalshi leg. fill_count_fp is a plain contract count
        ("3.00"), not a scaled fixed-point. Returns 0 if the order is gone."""
        if not self.live or order_id.startswith("dry-"):
            return 0
        r = self.client.get(self.base + f"/portfolio/orders/{order_id}",
                            headers=self._headers("GET", f"/portfolio/orders/{order_id}"))
        if r.status_code != 200:
            return 0
        o = r.json().get("order", {})
        return int(float(o.get("fill_count_fp") or 0))

    def cancel_all_open(self) -> dict:
        """Kill-switch sweep: cancel every RESTING order (parity with PM US).
        /portfolio/orders returns history (canceled/executed too), so filter to
        status=='resting' — never try to cancel an already-dead order."""
        if not self.live:
            return {"skipped": "dry-run"}
        ids = [o.get("order_id") for o in self.resting_orders()
               if o.get("status") == "resting" and o.get("order_id")]
        for oid in ids:
            self.cancel(oid)
        return {"cancelled": ids}

    def rehearse(self, ticker: str) -> dict:
        """Live end-to-end validation with ~zero fill risk: place ONE contract
        at 1c (far below any real bid), confirm it rests, cancel it. Proves
        the signed POST/DELETE path works on the real account."""
        if not self.live:
            return {"skipped": "dry-run"}
        placed = self.place_yes(ticker, "bid", Decimal("0.01"), 1)
        oid = placed.get("order", {}).get("order_id") or placed.get("order_id")
        time.sleep(1.0)
        resting = any(o.get("order_id") == oid for o in self.resting_orders())
        cancelled = self.cancel(oid)
        return {"order_id": oid, "rested": resting, "cancelled": cancelled}
