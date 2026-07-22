"""Polymarket market-data adapter (verified against live API 2026-07-20).

- Gamma API (no auth): catalog. `clobTokenIds` / `outcomes` are JSON-encoded
  strings; `description` is the resolution rules text.
- CLOB REST (no auth): /book snapshots. NOTE: live API returns asks high->low
  and bids low->high (best at the END); we sort, never assume.
- WS market channel (no auth): book / price_change / last_trade_price /
  tick_size_change / market_resolved. No sequence numbers on this feed —
  we synthesize a per-market seq and rely on periodic REST re-snapshots for
  integrity. Keepalive: literal "PING" text frame every ~10s.

All books are YES-token-denominated: we subscribe/poll the YES token id only;
a NO-token quote is representable as YES at 1-q, but Polymarket books for the
two tokens of a market are mirrors, so one token suffices.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, AsyncIterator, Callable

import httpx

from arbbot.models.core import (
    BookDelta,
    BookEvent,
    BookSnapshot,
    Level,
    Market,
    MarketStatus,
    Trade,
    Venue,
    now_local_ns,
)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def normalize_gamma_market(raw: dict[str, Any]) -> Market:
    token_ids = json.loads(raw["clobTokenIds"])
    outcomes = json.loads(raw.get("outcomes", '["Yes", "No"]'))
    by_outcome = {o.lower(): t for o, t in zip(outcomes, token_ids)}
    yes_token = by_outcome.get("yes", token_ids[0])
    no_token = by_outcome.get("no", token_ids[1] if len(token_ids) > 1 else None)
    if raw.get("closed"):
        status = MarketStatus.RESOLVED if raw.get("umaResolutionStatus") == "resolved" else MarketStatus.CLOSED
    elif raw.get("active"):
        status = MarketStatus.ACTIVE
    else:
        status = MarketStatus.UNKNOWN
    category = raw.get("category") or ""
    if not category:
        events = raw.get("events") or []
        category = (events[0].get("category") or "default") if events else "default"
    return Market(
        venue=Venue.POLYMARKET,
        market_id=str(yes_token),
        title=raw.get("question", ""),
        tick_size=Decimal(str(raw.get("orderPriceMinTickSize", "0.001"))),
        min_order_size=Decimal(str(raw.get("orderMinSize", "5"))),
        category=category.lower(),
        status=status,
        close_time=raw.get("endDate"),
        no_market_id=str(no_token) if no_token else None,
        condition_id=raw.get("conditionId"),
    )


def _levels(raw_levels: list[dict[str, Any]], descending: bool) -> list[Level]:
    lv = [
        Level(price=Decimal(l["price"]), size=Decimal(l["size"]))
        for l in raw_levels
        if Decimal(l["size"]) > 0
    ]
    return sorted(lv, key=lambda l: l.price, reverse=descending)


def normalize_book_event(msg: dict[str, Any], seq: int) -> BookSnapshot:
    return BookSnapshot(
        venue=Venue.POLYMARKET,
        market_id=str(msg["asset_id"]),
        bids=_levels(msg.get("bids") or [], descending=True),
        asks=_levels(msg.get("asks") or [], descending=False),
        seq=seq,
        ts_local_ns=now_local_ns(),
        ts_venue=str(msg.get("timestamp", "")),
    )


def normalize_price_changes(
    msg: dict[str, Any], next_seq: Callable[[str], int]
) -> list[BookDelta]:
    """price_change carries the NEW total size per level (0 removes)."""
    out = []
    for ch in msg.get("price_changes") or []:
        asset = str(ch["asset_id"])
        out.append(
            BookDelta(
                venue=Venue.POLYMARKET,
                market_id=asset,
                side="bid" if ch.get("side", "").upper() == "BUY" else "ask",
                price=Decimal(ch["price"]),
                size=Decimal(ch["size"]),
                seq=next_seq(asset),
                ts_local_ns=now_local_ns(),
                ts_venue=str(msg.get("timestamp", "")),
            )
        )
    return out


def normalize_last_trade(msg: dict[str, Any], seq: int) -> Trade:
    return Trade(
        venue=Venue.POLYMARKET,
        market_id=str(msg["asset_id"]),
        price=Decimal(msg["price"]),
        size=Decimal(msg["size"]),
        taker_side="buy" if msg.get("side", "").upper() == "BUY" else "sell",
        seq=seq,
        ts_local_ns=now_local_ns(),
        ts_venue=str(msg.get("timestamp", "")),
    )


class SeqCounter:
    """Synthesized per-market monotonic sequence (Polymarket WS has none)."""

    def __init__(self) -> None:
        self._n: dict[str, int] = {}

    def next(self, market_id: str) -> int:
        self._n[market_id] = self._n.get(market_id, 0) + 1
        return self._n[market_id]


def parse_ws_frame(frame: str, seq: SeqCounter) -> list[BookEvent]:
    """One WS text frame -> normalized events. Frames may be a single event
    object or a JSON array of events; unknown event types are ignored."""
    if frame == "PONG":
        return []
    data = json.loads(frame)
    msgs = data if isinstance(data, list) else [data]
    out: list[BookEvent] = []
    for m in msgs:
        et = m.get("event_type")
        if et == "book":
            out.append(normalize_book_event(m, seq.next(str(m["asset_id"]))))
        elif et == "price_change":
            out.extend(normalize_price_changes(m, seq.next))
        elif et == "last_trade_price":
            out.append(normalize_last_trade(m, seq.next(str(m["asset_id"]))))
        # tick_size_change / market_resolved handled by the catalog refresher
    return out


def subscribe_frame(token_ids: list[str]) -> str:
    return json.dumps(
        {"type": "market", "assets_ids": token_ids, "custom_feature_enabled": True}
    )


class GammaCatalog:
    def __init__(self, client: httpx.AsyncClient | None = None, base: str = GAMMA_BASE):
        self.base = base
        self.client = client or httpx.AsyncClient(timeout=15)

    async def markets_by_token_ids(self, token_ids: list[str]) -> list[Market]:
        out = []
        for i in range(0, len(token_ids), 20):
            # Gamma expects REPEATED clob_token_ids params (comma-joined 422s)
            r = await self.client.get(
                f"{self.base}/markets",
                params=[("clob_token_ids", t) for t in token_ids[i : i + 20]],
            )
            r.raise_for_status()
            out.extend(normalize_gamma_market(m) for m in r.json())
        return out

    async def search_markets(self, **params: Any) -> list[dict[str, Any]]:
        """Raw Gamma market dicts (incl. `description` rule text) for the
        candidate-mining pipeline."""
        r = await self.client.get(f"{self.base}/markets", params=params)
        r.raise_for_status()
        return r.json()


class ClobRest:
    def __init__(self, client: httpx.AsyncClient | None = None, base: str = CLOB_BASE):
        self.base = base
        self.client = client or httpx.AsyncClient(timeout=15)

    async def taker_fee_overrides(
        self, markets: list[Market]
    ) -> list[Market]:
        """Venue-reported per-market fees: CLOB /markets/{condition_id}
        taker_base_fee. 0 => the venue declares the market FEE-FREE (override
        Decimal 0); nonzero => keep the schedule (override None) since the
        base-fee unit mapping is unverified. Returns updated Market copies."""
        out = []
        for m in markets:
            override = None
            if m.venue is Venue.POLYMARKET and m.condition_id:
                try:
                    r = await self.client.get(f"{self.base}/markets/{m.condition_id}")
                    r.raise_for_status()
                    if int(r.json().get("taker_base_fee", -1)) == 0:
                        override = Decimal("0")
                except (httpx.HTTPError, ValueError, TypeError):
                    pass  # keep schedule on any doubt
            out.append(
                m.model_copy(update={"taker_fee_coef_override": override})
                if override is not None else m
            )
        return out

    async def book(self, token_id: str, seq: int) -> BookSnapshot:
        r = await self.client.get(f"{self.base}/book", params={"token_id": token_id})
        r.raise_for_status()
        msg = r.json()
        return BookSnapshot(
            venue=Venue.POLYMARKET,
            market_id=str(token_id),
            bids=_levels(msg.get("bids") or [], descending=True),
            asks=_levels(msg.get("asks") or [], descending=False),
            seq=seq,
            ts_local_ns=now_local_ns(),
            ts_venue=str(msg.get("timestamp", "")),
        )
