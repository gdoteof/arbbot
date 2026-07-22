"""Kalshi market-data adapter.

Two data paths (verified against live API 2026-07-20):
- REST (no auth): catalog + per-market orderbook. Used for credential-less
  recording — emits one BookSnapshot per poll. No trade tape in this mode.
- WS (auth REQUIRED, RSA-PSS): orderbook_delta + trade channels. Wired when
  API keys exist; signing helper lives here so the switch is config-only.

Kalshi orderbook convention: `orderbook_fp.yes_dollars` and `.no_dollars` are
both BID ladders. A NO bid at price q is a YES ask at 1-q (same size) — the
normalization below makes every book a YES-denominated Book.

    Kalshi orderbook_fp                 normalized YES book
    yes_dollars: [[0.40, 100]]   -->    bids: [(0.40, 100)]
    no_dollars:  [[0.55,  80]]   -->    asks: [(0.45,  80)]
"""

from __future__ import annotations

import base64
import json
import time
from decimal import Decimal
from typing import Any

import httpx

from arbbot.models.core import (
    BookDelta,
    BookSnapshot,
    Level,
    Market,
    MarketStatus,
    Trade,
    Venue,
    now_local_ns,
)

REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
DEMO_REST_BASE = "https://demo-api.kalshi.co/trade-api/v2"
DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"

_STATUS_MAP = {
    "active": MarketStatus.ACTIVE,
    "open": MarketStatus.ACTIVE,
    "closed": MarketStatus.CLOSED,
    "determined": MarketStatus.RESOLVED,
    "finalized": MarketStatus.RESOLVED,
    "settled": MarketStatus.RESOLVED,
}


def normalize_market(raw: dict[str, Any]) -> Market:
    return Market(
        venue=Venue.KALSHI,
        market_id=raw["ticker"],
        title=raw.get("title", ""),
        tick_size=Decimal("0.01"),  # Kalshi quotes whole cents
        min_order_size=Decimal("1"),
        category=(raw.get("category") or "default").lower(),
        status=_STATUS_MAP.get(raw.get("status", ""), MarketStatus.UNKNOWN),
        close_time=raw.get("close_time"),
    )


def normalize_orderbook(
    ticker: str, orderbook_fp: dict[str, Any], seq: int, ts_local_ns: int | None = None
) -> BookSnapshot:
    """orderbook_fp -> YES-denominated snapshot. Levels arrive as
    [price_dollars_str, qty_str] pairs; both ladders are bids."""
    yes_bids = [
        Level(price=Decimal(p), size=Decimal(q))
        for p, q in (orderbook_fp.get("yes_dollars") or [])
        if Decimal(q) > 0
    ]
    yes_asks = [
        Level(price=Decimal("1") - Decimal(p), size=Decimal(q))
        for p, q in (orderbook_fp.get("no_dollars") or [])
        if Decimal(q) > 0
    ]
    return BookSnapshot(
        venue=Venue.KALSHI,
        market_id=ticker,
        bids=sorted(yes_bids, key=lambda l: l.price, reverse=True),
        asks=sorted(yes_asks, key=lambda l: l.price),
        seq=seq,
        ts_local_ns=ts_local_ns if ts_local_ns is not None else now_local_ns(),
    )


def normalize_ws_trade(msg: dict[str, Any], seq: int) -> Trade:
    """WS `trade` message payload -> Trade (YES-denominated price)."""
    return Trade(
        venue=Venue.KALSHI,
        market_id=msg["market_ticker"],
        price=Decimal(msg["yes_price_dollars"]),
        size=Decimal(msg["count_fp"]),
        taker_side="buy" if msg.get("taker_side") == "yes" else "sell",
        seq=seq,
        ts_local_ns=now_local_ns(),
        ts_venue=str(msg.get("ts_ms", "")),
    )


class KalshiCatalog:
    """Auth-free REST catalog + orderbook client."""

    def __init__(self, client: httpx.AsyncClient | None = None, base: str = REST_BASE):
        self.base = base
        self.client = client or httpx.AsyncClient(timeout=15)

    async def markets(self, tickers: list[str]) -> list[Market]:
        out: list[Market] = []
        for i in range(0, len(tickers), 50):
            r = await self.client.get(
                f"{self.base}/markets", params={"tickers": ",".join(tickers[i : i + 50])}
            )
            r.raise_for_status()
            out.extend(normalize_market(m) for m in r.json().get("markets", []))
        return out

    async def orderbook(self, ticker: str, seq: int) -> BookSnapshot:
        r = await self.client.get(
            f"{self.base}/markets/{ticker}/orderbook", params={"depth": 0}
        )
        r.raise_for_status()
        fp = r.json().get("orderbook_fp") or {}
        return normalize_orderbook(ticker, fp, seq)


WS_PATH = "/trade-api/ws/v2"


def sign_headers(api_key_id: str, private_key, method: str, path: str) -> dict[str, str]:
    """Kalshi auth headers: RSA-PSS(SHA256) over timestamp_ms + METHOD + path.
    `private_key` is a loaded cryptography key object (loaded once, never
    re-parsed per request)."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    ts_ms = str(int(time.time() * 1000))
    message = (ts_ms + method + path).encode()
    sig = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def load_private_key(private_key_pem: bytes):
    from cryptography.hazmat.primitives import serialization

    return serialization.load_pem_private_key(private_key_pem, password=None)


def ws_auth_headers(api_key_id: str, private_key_pem: bytes) -> dict[str, str]:
    """Back-compat wrapper: sign the WS-upgrade handshake."""
    return sign_headers(api_key_id, load_private_key(private_key_pem), "GET", WS_PATH)


class KalshiWsBook:
    """Translates Kalshi's WS feed into YES-denominated normalized events.

    Kalshi quirks handled here:
    - orderbook_delta carries a CHANGE in quantity (delta_fp), not a new total;
      we maintain running sizes per (ticker, side, price) and emit the RESULTING
      total so the shared BookBuilder (which expects new-total deltas) works.
    - both yes_dollars and no_dollars ladders are BIDS; a NO bid at q is a YES
      ask at 1-q (size preserved).
    - `seq` is per-subscription (sid), increments by 1; a gap means resync.
    """

    def __init__(self) -> None:
        # (ticker, side, price_cents) -> size ; side is "yes"/"no" (raw Kalshi)
        self._sizes: dict[tuple[str, str, str], Decimal] = {}

    def _yes_delta(self, ticker: str, side: str, price: Decimal, new_size: Decimal,
                   seq: int) -> BookDelta:
        # map raw Kalshi side -> YES-denominated (venue.core) bid/ask + price
        if side == "yes":
            yes_side, yes_price = "bid", price
        else:  # no bid at q == yes ask at 1-q
            yes_side, yes_price = "ask", Decimal("1") - price
        return BookDelta(
            venue=Venue.KALSHI, market_id=ticker, side=yes_side,
            price=yes_price, size=new_size, seq=seq, ts_local_ns=now_local_ns(),
        )

    def on_snapshot(self, msg: dict, seq: int) -> BookSnapshot:
        ticker = msg["market_ticker"]
        # reset running sizes for this ticker
        self._sizes = {k: v for k, v in self._sizes.items() if k[0] != ticker}
        for side in ("yes", "no"):
            for price, qty in (msg.get(f"{side}_dollars_fp") or msg.get(side + "_dollars") or []):
                self._sizes[(ticker, side, str(price))] = Decimal(qty)
        return normalize_orderbook(
            ticker,
            {"yes_dollars": msg.get("yes_dollars_fp") or msg.get("yes_dollars") or [],
             "no_dollars": msg.get("no_dollars_fp") or msg.get("no_dollars") or []},
            seq,
        )

    def on_delta(self, msg: dict, seq: int) -> BookDelta:
        ticker = msg["market_ticker"]
        side = msg["side"]
        price = Decimal(str(msg["price_dollars"]))
        change = Decimal(str(msg["delta_fp"]))
        key = (ticker, side, str(price))
        new_size = self._sizes.get(key, Decimal("0")) + change
        if new_size <= 0:
            new_size = Decimal("0")
            self._sizes.pop(key, None)
        else:
            self._sizes[key] = new_size
        return self._yes_delta(ticker, side, price, new_size, seq)


def ws_subscribe_frame(tickers: list[str], msg_id: int = 1) -> str:
    return json.dumps({
        "id": msg_id, "cmd": "subscribe",
        "params": {"channels": ["orderbook_delta", "trade"], "market_tickers": tickers},
    })


def ws_snapshot_request(sids: list[int], msg_id: int = 99) -> str:
    return json.dumps({
        "id": msg_id, "cmd": "update_subscription",
        "params": {"sids": sids, "action": "get_snapshot"},
    })
