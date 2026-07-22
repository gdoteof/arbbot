"""Polymarket US (QCX DCM) market-data adapter (verified live 2026-07-21).

Regulated US venue, CFTC DCM. Different API from the international CLOB:
- Public gateway (NO auth): gateway.polymarket.us/v1
    GET /events?tag_slug=<tag>           -> events with nested markets (metadata)
    GET /markets/{slug}/book             -> {"marketData":{bids,offers,...}}
    GET /markets/{slug}/bbo              -> best bid/offer
  (there is no by-slug metadata endpoint; /markets/{slug} 404s — metadata must
  come from the tag route.)
- Market-data WebSocket REQUIRES auth (401 unauthenticated), so we use REST
  polling here (credential-free), mirroring kalshi_poll_task. When API creds
  exist a WS task can replace this (see task: PM US authenticated gateway).

Markets are per-outcome YES/NO contracts (like Kalshi), market_id == slug.
Prices are dollar strings in {px:{value},qty}; the ask side is under "offers".
feeCoefficient is the venue-reported per-market taker coefficient (k in
k*P*(1-P)*size), stored as taker_fee_coef_override.
"""

from __future__ import annotations

import base64
import time
from decimal import Decimal
from typing import Any

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.models.core import (
    BookEvent,
    BookSnapshot,
    Level,
    Market,
    MarketStatus,
    Trade,
    Venue,
    now_local_ns,
)

GATEWAY_BASE = "https://gateway.polymarket.us/v1"
WS_MARKETS_URL = "wss://api.polymarket.us/v1/ws/markets"
WS_MARKETS_PATH = "/v1/ws/markets"


def _status(raw: dict[str, Any]) -> MarketStatus:
    if raw.get("closed"):
        return MarketStatus.CLOSED
    if raw.get("active"):
        return MarketStatus.ACTIVE
    return MarketStatus.UNKNOWN


def normalize_market(raw: dict[str, Any], category: str) -> Market:
    coef = raw.get("feeCoefficient")
    return Market(
        venue=Venue.POLYMARKET_US,
        market_id=raw["slug"],
        title=raw.get("question", ""),
        tick_size=Decimal(str(raw.get("orderPriceMinTickSize", "0.01"))),
        min_order_size=Decimal(str(raw.get("minimumTradeQty", "0.01"))),
        category=(category or "default").lower(),
        status=_status(raw),
        close_time=raw.get("endDate"),
        taker_fee_coef_override=Decimal(str(coef)) if coef is not None else None,
    )


def _levels(raw_levels: list[dict[str, Any]], descending: bool) -> list[Level]:
    lv = []
    for l in raw_levels or []:
        px = Decimal(str(l["px"]["value"]))
        sz = Decimal(str(l["qty"]))
        if sz > 0:
            lv.append(Level(price=px, size=sz))
    return sorted(lv, key=lambda x: x.price, reverse=descending)


def normalize_book(slug: str, market_data: dict[str, Any], seq: int) -> BookSnapshot:
    return BookSnapshot(
        venue=Venue.POLYMARKET_US,
        market_id=slug,
        bids=_levels(market_data.get("bids"), descending=True),
        asks=_levels(market_data.get("offers"), descending=False),  # ask side = "offers"
        seq=seq,
        ts_local_ns=now_local_ns(),
        ts_venue=str(market_data.get("transactTime", "")),
    )


def ws_auth_headers(key_id: str, secret_key_b64: str,
                    path: str = WS_MARKETS_PATH) -> dict[str, str]:
    """Signed handshake headers for the market-data WS (auth required — the
    only PM US key is trade-capable, but this task only reads market data).
    Ed25519 over '{ts}GET{path}'; timestamp must be within 30s of server."""
    priv = ed25519.Ed25519PrivateKey.from_private_bytes(
        base64.b64decode(secret_key_b64)[:32])
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(priv.sign(f"{ts}GET{path}".encode())).decode()
    return {"X-PM-Access-Key": key_id, "X-PM-Timestamp": ts, "X-PM-Signature": sig}


def subscribe_frame(request_id: str, sub_type: str, slugs: list[str]) -> str:
    import json
    return json.dumps({"subscribe": {"requestId": request_id,
                                     "subscriptionType": sub_type,
                                     "marketSlugs": slugs}})


def parse_ws_message(msg: dict[str, Any], next_seq) -> list[BookEvent]:
    """One PM US markets-WS message -> normalized events. MARKET_DATA carries a
    full book snapshot (marketData.bids/offers, same shape as REST /book);
    TRADE carries a single print. heartbeat/error/lite are ignored here."""
    if "marketData" in msg:
        md = msg["marketData"]
        slug = md.get("marketSlug", "")
        return [normalize_book(slug, md, next_seq(slug))]
    if "trade" in msg:
        t = msg["trade"]
        slug = t.get("marketSlug", "")
        taker = (t.get("taker") or {}).get("intent", "")
        return [Trade(
            venue=Venue.POLYMARKET_US,
            market_id=slug,
            price=Decimal(str(t["price"]["value"])),
            size=Decimal(str(t["quantity"])),
            taker_side="buy" if "BUY" in taker else "sell",
            seq=next_seq(slug + "|tape"),
            ts_local_ns=now_local_ns(),
            ts_venue=str(t.get("tradeTime", "")),
        )]
    return []


class PolymarketUsCatalog:
    """Auth-free public catalog + orderbook client (gateway.polymarket.us)."""

    def __init__(self, client: httpx.AsyncClient | None = None, base: str = GATEWAY_BASE):
        self.base = base
        self.client = client or httpx.AsyncClient(timeout=20)

    async def markets_by_tags(self, tags: list[str]) -> list[Market]:
        """All markets under the given tag_slugs (e.g. fed-decision, cpi)."""
        out: list[Market] = []
        seen: set[str] = set()
        for tag in tags:
            r = await self.client.get(
                f"{self.base}/events", params={"tag_slug": tag, "limit": 200}
            )
            r.raise_for_status()
            for e in r.json().get("events", []):
                cat = e.get("category") or "default"
                for m in e.get("markets", []) or []:
                    slug = m.get("slug")
                    if slug and slug not in seen:
                        seen.add(slug)
                        out.append(normalize_market(m, cat))
        return out

    async def book(self, slug: str, seq: int) -> BookSnapshot:
        r = await self.client.get(f"{self.base}/markets/{slug}/book")
        r.raise_for_status()
        return normalize_book(slug, r.json().get("marketData", {}), seq)
