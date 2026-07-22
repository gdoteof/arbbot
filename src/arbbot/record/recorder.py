"""The recorder daemon: credential-free market-data capture, 24/7.

    +--------------------+     +---------------------+
    | Polymarket WS      |     | Kalshi REST poller  |
    | (book/deltas/tape) |     | (snapshots, no tape)|
    +---------+----------+     +----------+----------+
              |   normalized BookEvents   |
              +------------+--------------+
                           v
                 +--------------------+
                 |  Recorder core     |
                 |  - BookBuilder     |
                 |  - LivenessTracker |
                 +----+-----------+---+
                      |           |
              JSONL append   Unix-socket rebroadcast
              (per venue/day)   (trader subscribes)

Liveness is measured per CONNECTION (time since last received message or
successful poll), never per book — quiet markets are healthy, silent
connections are not (eng review, Issue on staleness definition).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import websockets

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookEvent, BookSnapshot, Venue
from arbbot.record.kalshi import KalshiCatalog
from arbbot.record.polymarket import (
    WS_MARKET_URL,
    ClobRest,
    SeqCounter,
    parse_ws_frame,
    subscribe_frame,
)
from arbbot.record.store import JsonlWriter


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class LivenessTracker:
    def __init__(self, stale_after_s: float = 10.0):
        self.stale_after_s = stale_after_s
        self._last: dict[str, float] = {}
        self._stale_seconds: dict[str, float] = {}

    def beat(self, conn: str) -> None:
        self._last[conn] = time.monotonic()

    def check(self) -> dict[str, bool]:
        """conn -> is_stale; accumulates stale time for the uptime criterion."""
        now = time.monotonic()
        out = {}
        for conn, last in self._last.items():
            stale = (now - last) > self.stale_after_s
            out[conn] = stale
            if stale:
                self._stale_seconds[conn] = self._stale_seconds.get(conn, 0) + 1.0
        return out

    def stale_seconds(self) -> dict[str, float]:
        return dict(self._stale_seconds)


class UnixBroadcaster:
    """Fan-out of normalized events to local subscribers (the trader).
    Slow subscribers are disconnected rather than allowed to backpressure
    the recorder — the recorder's write path must never block on a reader.

    welcome_events: called on each new connection; returns the current book
    state as snapshot events so a (re)connecting subscriber is synced
    immediately instead of waiting for organic snapshots (Kalshi WS sends
    them only on subscribe/gap — a late subscriber would wait forever)."""

    MAX_BUFFER = 1_000_000  # bytes per subscriber

    def __init__(self, socket_path: str | Path,
                 welcome_events=None):
        self.socket_path = str(socket_path)
        self.welcome_events = welcome_events
        self._writers: set[asyncio.StreamWriter] = set()
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        Path(self.socket_path).unlink(missing_ok=True)
        self._server = await asyncio.start_unix_server(self._on_connect, self.socket_path)

    async def _on_connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if self.welcome_events:
            with contextlib.suppress(Exception):
                for ev in self.welcome_events():
                    writer.write((ev.model_dump_json() + "\n").encode())
                await writer.drain()
        self._writers.add(writer)
        with contextlib.suppress(Exception):
            await reader.read()  # subscribers never send; returns on disconnect
        self._writers.discard(writer)
        with contextlib.suppress(Exception):
            writer.close()

    def rebroadcast_snapshots(self) -> None:
        """Push current book state to ALL live subscribers (not just new
        connections). Heals a subscriber whose book gapped and would
        otherwise wait forever — the scanner-stuck-book bug, 2026-07-20."""
        if not self._writers or not self.welcome_events:
            return
        with contextlib.suppress(Exception):
            for ev in self.welcome_events():
                self.publish(ev)

    def publish(self, event: BookEvent) -> None:
        if not self._writers:
            return
        line = (event.model_dump_json() + "\n").encode()
        for w in list(self._writers):
            if w.transport.get_write_buffer_size() > self.MAX_BUFFER:
                self._writers.discard(w)
                w.close()
                continue
            with contextlib.suppress(Exception):
                w.write(line)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        Path(self.socket_path).unlink(missing_ok=True)


class RecorderCore:
    """Venue-agnostic sink: builds books, persists, rebroadcasts."""

    def __init__(self, writer: JsonlWriter, broadcaster: UnixBroadcaster | None = None):
        self.writer = writer
        self.broadcaster = broadcaster
        self.books = BookBuilder()
        self.gap_count = 0

    def snapshot_events(self) -> list[BookSnapshot]:
        """Current book state as synthetic snapshots (welcome payload)."""
        out = []
        for (venue, market_id), b in list(self.books._books.items()):
            out.append(BookSnapshot(
                venue=b.venue, market_id=b.market_id, bids=b.bids, asks=b.asks,
                seq=b.seq, ts_local_ns=b.ts_local_ns, ts_venue=b.ts_venue,
            ))
        return out

    def on_event(self, event: BookEvent) -> str | None:
        """Persist + apply + rebroadcast. Returns market_id needing a
        re-snapshot when a gap/desync is detected, else None."""
        self.writer.write(event, utc_day())
        if self.broadcaster:
            self.broadcaster.publish(event)
        resync: str | None = None
        if isinstance(event, BookSnapshot):
            self.books.apply_snapshot(event)
        elif isinstance(event, BookDelta):
            try:
                self.books.apply_delta(event)
            except (GapDetected, NotSynced):
                self.gap_count += 1
                resync = event.market_id
        return resync


async def polymarket_ws_task(
    core: RecorderCore,
    liveness: LivenessTracker,
    token_ids: list[str],
    clob: ClobRest,
    ws_url: str = WS_MARKET_URL,
    resnapshot_interval_s: float = 300.0,
) -> None:
    """PM WS loop: subscribe, PING keepalive, synthesized seq, gap -> REST
    re-snapshot, periodic integrity re-snapshot (feed has no seq numbers)."""
    seq = SeqCounter()
    while True:
        try:
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                await ws.send(subscribe_frame(token_ids))
                liveness.beat("polymarket-ws")
                last_ping = last_resnap = time.monotonic()
                while True:
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        frame = None
                    now = time.monotonic()
                    if now - last_ping > 10:
                        await ws.send("PING")
                        last_ping = now
                    if frame is not None:
                        liveness.beat("polymarket-ws")
                        for event in parse_ws_frame(
                            frame if isinstance(frame, str) else frame.decode(), seq
                        ):
                            need = core.on_event(event)
                            if need:
                                core.on_event(await clob.book(need, seq.next(need)))
                    if now - last_resnap > resnapshot_interval_s:
                        for tid in token_ids:
                            core.on_event(await clob.book(tid, seq.next(tid)))
                        last_resnap = now
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)  # reconnect with a beat of backoff


async def kalshi_ws_task(
    core: RecorderCore,
    liveness: LivenessTracker,
    tickers: list[str],
    api_key_id: str,
    private_key_pem: bytes,
    ws_url: str | None = None,
    catalog: "KalshiCatalog | None" = None,
) -> None:
    """Real-time Kalshi feed (auth required). Subscribes orderbook_delta+trade,
    tracks per-sid seq, requests a fresh snapshot on gap. Needs a Kalshi API
    key (market data requires auth). Falls back to nothing — the caller chooses
    ws vs poll based on whether credentials exist."""
    import websockets

    from arbbot.record.kalshi import (
        WS_URL,
        KalshiWsBook,
        load_private_key,
        normalize_ws_trade,
        sign_headers,
        ws_snapshot_request,
        ws_subscribe_frame,
    )
    from arbbot.record.polymarket import SeqCounter

    url = ws_url or WS_URL
    key = load_private_key(private_key_pem)
    while True:
        try:
            headers = sign_headers(api_key_id, key, "GET", "/trade-api/ws/v2")
            async with websockets.connect(url, additional_headers=headers) as ws:
                await ws.send(ws_subscribe_frame(tickers))
                liveness.beat("kalshi-ws")
                book = KalshiWsBook()
                sid_seq: dict[int, int] = {}
                last_resnap = time.monotonic()
                # Per-MARKET seq for the shared BookBuilder: the wire `seq`
                # is per-SUBSCRIPTION; feeding it per-market makes every
                # interleaved delta look like a gap and kills book state
                # (live incident 2026-07-20 11:45 — scanner went silent).
                mseq = SeqCounter()
                while True:
                    # periodic integrity re-snapshot for ALL sids: heals any
                    # subscriber that gapped (e.g. the welcome-write race) —
                    # Kalshi sends snapshots only on subscribe/gap otherwise
                    if time.monotonic() - last_resnap > 300 and sid_seq:
                        await ws.send(ws_snapshot_request(list(sid_seq)))
                        last_resnap = time.monotonic()
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        # quiet market, connection still open (websockets runs
                        # its own ping/pong): liveness = connection health, not
                        # message arrival. Beat and keep waiting.
                        liveness.beat("kalshi-ws")
                        continue
                    liveness.beat("kalshi-ws")
                    m = json.loads(frame)
                    typ, sid = m.get("type"), m.get("sid")
                    seq = m.get("seq")
                    if typ in ("orderbook_snapshot", "orderbook_delta") and sid is not None and seq is not None:
                        expected = sid_seq.get(sid)
                        if typ == "orderbook_delta" and expected is not None and seq != expected + 1:
                            await ws.send(ws_snapshot_request([sid]))  # gap -> resync
                            sid_seq.pop(sid, None)
                            continue
                        sid_seq[sid] = seq
                    payload = m.get("msg", {})
                    if typ == "orderbook_snapshot":
                        t = payload.get("market_ticker", "")
                        core.on_event(book.on_snapshot(payload, mseq.next(t)))
                    elif typ == "orderbook_delta":
                        t = payload.get("market_ticker", "")
                        need = core.on_event(book.on_delta(payload, mseq.next(t)))
                        if need and catalog is not None:
                            # gap: REST snapshot resync (auth-free, proven) —
                            # the WS get_snapshot path produced zero snapshots
                            # in practice (adversarial review, 2026-07-20)
                            try:
                                core.on_event(await catalog.orderbook(need, mseq.next(need)))
                            except Exception:
                                pass
                    elif typ == "trade":
                        t = payload.get("market_ticker", "")
                        # trades get their OWN seq stream: sharing the book's
                        # per-market counter gapped (and killed) the book on
                        # every first trade — the traded-markets-die P1
                        core.on_event(normalize_ws_trade(payload, mseq.next(t + "|tape")))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)  # reconnect with backoff


async def kalshi_poll_task(
    core: RecorderCore,
    liveness: LivenessTracker,
    tickers: list[str],
    catalog: KalshiCatalog,
    interval_s: float = 5.0,
) -> None:
    """Credential-less Kalshi path: REST orderbook snapshots round-robin.
    ~len(tickers)/interval requests/sec — keep well under public limits."""
    seq = SeqCounter()
    while True:
        try:
            for ticker in tickers:
                snap = await catalog.orderbook(ticker, seq.next(ticker))
                core.on_event(snap)
                liveness.beat("kalshi-rest")
                await asyncio.sleep(interval_s / max(len(tickers), 1))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)


async def polymarket_us_ws_task(
    core: RecorderCore,
    liveness: LivenessTracker,
    slugs: list[str],
    key_id: str,
    secret_key_b64: str,
) -> None:
    """Authenticated Polymarket US market-data WS: full order-book snapshots +
    trades, real-time. Auth = signed handshake headers (the only PM US key is
    trade-capable; this task only reads market data — no order code path).
    MARKET_DATA pushes a full book on each change, so each message is a
    self-contained snapshot (no delta/seq reconstruction, no gap risk)."""
    import websockets

    from arbbot.record.polymarket import SeqCounter
    from arbbot.record.polymarket_us import (
        WS_MARKETS_URL, parse_ws_message, subscribe_frame, ws_auth_headers,
    )

    seq = SeqCounter()
    while True:
        try:
            async with websockets.connect(
                WS_MARKETS_URL,
                additional_headers=ws_auth_headers(key_id, secret_key_b64),
            ) as ws:
                # chunked subscriptions: unknown venue-side caps on marketSlugs
                # per request — several smaller subscriptions (distinct
                # requestIds) avoid silent partial coverage on wide universes
                for i in range(0, len(slugs), 150):
                    chunk = slugs[i:i + 150]
                    await ws.send(subscribe_frame(f"book{i}", "SUBSCRIPTION_TYPE_MARKET_DATA", chunk))
                    await ws.send(subscribe_frame(f"tape{i}", "SUBSCRIPTION_TYPE_TRADE", chunk))
                liveness.beat("polymarket_us-ws")
                while True:
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        liveness.beat("polymarket_us-ws")  # connection health
                        continue
                    liveness.beat("polymarket_us-ws")
                    msg = json.loads(frame if isinstance(frame, str) else frame.decode())
                    for event in parse_ws_message(msg, seq.next):
                        core.on_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)  # reconnect with backoff


async def polymarket_us_poll_task(
    core: RecorderCore,
    liveness: LivenessTracker,
    slugs: list[str],
    catalog: "PolymarketUsCatalog",
    interval_s: float = 4.0,
) -> None:
    """Credential-free Polymarket US path: REST orderbook snapshots round-robin
    (the market-data WS requires auth). ~len(slugs)/interval requests/sec —
    keep well under the public gateway's rate limit (429s seen at high rate)."""
    from arbbot.record.polymarket import SeqCounter

    seq = SeqCounter()
    while True:
        try:
            for slug in slugs:
                snap = await catalog.book(slug, seq.next(slug))
                core.on_event(snap)
                liveness.beat("polymarket_us-rest")
                await asyncio.sleep(interval_s / max(len(slugs), 1))
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(2)


async def health_task(
    liveness: LivenessTracker,
    health_path: str | Path,
    alert: Callable[[str], None] | None = None,
    interval_s: float = 1.0,
    broadcaster: "UnixBroadcaster | None" = None,
    resync_every_s: float = 30.0,
) -> None:
    alerted: set[str] = set()
    since_resync = 0.0
    while True:
        await asyncio.sleep(interval_s)
        since_resync += interval_s
        if broadcaster is not None and since_resync >= resync_every_s:
            since_resync = 0.0
            broadcaster.rebroadcast_snapshots()
        status = liveness.check()
        for conn, stale in status.items():
            if stale and conn not in alerted:
                alerted.add(conn)
                if alert:
                    alert(f"arbbot recorder: feed '{conn}' stale")
            elif not stale:
                alerted.discard(conn)
        with open(health_path, "a") as f:
            f.write(
                json.dumps(
                    {
                        "ts": time.time(),
                        "stale": {k: v for k, v in status.items()},
                        "stale_seconds_total": liveness.stale_seconds(),
                    }
                )
                + "\n"
            )
