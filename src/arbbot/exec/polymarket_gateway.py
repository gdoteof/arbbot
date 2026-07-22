"""Polymarket CLOB order gateway (via py-clob-client), matching the Kalshi
gateway interface so the Quoter can route to either venue.

DRY-RUN by default: logs the intended order, returns a synthetic id, never
calls the venue. Only live=True posts real orders.

YES-token semantics (all orders are on the YES token id):
  side='bid' -> BUY yes at price   (resting maker bid)
  side='ask' -> SELL yes at price  (resting maker ask; selling shares held)
Price is a Decimal probability (tick 0.001); size is contracts (>= min 5).
"""

from __future__ import annotations

import time
import uuid
from decimal import Decimal
from typing import Optional


class PolymarketOrderGateway:
    def __init__(self, private_key: str, live: bool = False,
                 host: str = "https://clob.polymarket.com", chain_id: int = 137):
        self.live = live
        self.sent: list[dict] = []
        self._client = None
        if live:
            from py_clob_client.client import ClobClient
            key = private_key if private_key.startswith("0x") else "0x" + private_key
            c = ClobClient(host, key=key, chain_id=chain_id)
            c.set_api_creds(c.create_or_derive_api_creds())
            self._client = c

    def place_yes(self, token_id: str, side: str, price: Decimal, count: int) -> dict:
        coid = uuid.uuid4().hex
        rec = {"ts": time.time(), "op": "place", "live": self.live, "venue": "polymarket",
               "token": token_id, "side": side, "price": str(price), "count": count,
               "coid": coid}
        self.sent.append(rec)
        if not self.live:
            rec["result"] = {"order_id": "dry-" + coid, "status": "dry_run"}
            return rec["result"]
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL
        args = OrderArgs(token_id=token_id, price=float(price), size=float(count),
                         side=BUY if side == "bid" else SELL)
        signed = self._client.create_order(args)
        rec["result"] = self._client.post_order(signed, OrderType.GTC)
        return rec["result"]

    def cancel(self, order_id: str, market_slug: str | None = None) -> dict:  # slug ignored
        rec = {"ts": time.time(), "op": "cancel", "live": self.live, "order_id": order_id}
        self.sent.append(rec)
        if not self.live or order_id.startswith("dry-"):
            rec["result"] = {"status": "dry_cancel"}
            return rec["result"]
        rec["result"] = self._client.cancel(order_id)
        return rec["result"]

    def resting_orders(self) -> list[dict]:
        return self._client.get_orders() if self.live else []

    def rehearse(self, token_id: str) -> dict:
        """Live validation with ~zero fill risk: BUY 5 (PM min size) yes at
        0.01 (far below any real ask), confirm it rests, cancel it."""
        if not self.live:
            return {"skipped": "dry-run"}
        placed = self.place_yes(token_id, "bid", Decimal("0.01"), 5)
        oid = placed.get("orderID") or placed.get("order_id")
        time.sleep(1.0)
        resting = any((o.get("id") or o.get("order_id")) == oid
                      for o in self.resting_orders())
        cancelled = self.cancel(oid) if oid else {"status": "no_id"}
        return {"order_id": oid, "rested": resting, "cancelled": cancelled, "raw": placed}
