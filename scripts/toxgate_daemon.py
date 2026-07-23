"""Toxicity-gate daemon: shadow scoring for the runner's maker quotes.

Subscribes to the MAIN recorder socket, tracks the quote-time toxicity
features (book state, trade flow, mid vol, cross-venue basis, staleness) for
every registry-paired Kalshi market, and every 2s publishes per-side gate
scores to data/exec/toxgate.json (atomic replace):

    {"ts": ..., "model": "toxicity_gate_model.txt",
     "markets": {"KX...": {"bid": p, "ask": p}, ...}}

`bid` = P(a fill on a resting BID loses money at 60s) — high means an
incoming seller is likely informed; `ask` = same for a resting ask.

SHADOW ONLY: this process places no orders and changes nothing. A later
~10-line consumer in the quoter (trading workstream, config-flagged) can skip
quoting a side when its gate exceeds a cut. History is sampled to
data/scan/toxgate-shadow.jsonl (30s/market) so gated-vs-ungated fill quality
can be measured against the runner's own fills before enforcement.

Validated offline (fills.parquet, day-holdout train 07-20/21 test 07-22):
gate keeps ~84% of maker fills and flips size-weighted PnL positive.

    .venv313/bin/python scripts/toxgate_daemon.py
"""

import asyncio
import contextlib
import json
import time
from collections import defaultdict, deque
from pathlib import Path

import numpy as np

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.jsonl import parse_event

SOCKET_PATH = "data/arbbot.sock"
GATE_MODEL = Path("data/research/toxicity_gate_model.txt")
OUT = Path("data/exec/toxgate.json")
SHADOW_LOG = Path("data/scan/toxgate-shadow.jsonl")
HEALTH = "data/health.jsonl"

GATE_FEATS = ["spread", "mid", "trades_60s", "vol_60s", "trades_300s",
              "vol_300s", "mid_vol_300s", "hour", "aligned_basis", "pm_stale_s"]

_health_cache: dict = {}


def stale_feeds(path=HEALTH, ttl=3.0):
    now = time.time()
    c = _health_cache.get(path)
    if c and now - c[0] < ttl:
        return c[1]
    try:
        with open(path, "rb") as f:
            with contextlib.suppress(OSError):
                f.seek(-4096, 2)
            d = json.loads(f.read().splitlines()[-1])
        out = {k for k, v in d.get("stale", {}).items() if v}
        if now - d.get("ts", 0) > 15:
            out = {"__health__"}
    except Exception:
        out = {"__health__"}
    _health_cache[path] = (now, out)
    return out


def top(book):
    bid = max(book.bids, key=lambda l: l.price, default=None)
    ask = min(book.asks, key=lambda l: l.price, default=None)
    if bid is None or ask is None or ask.price <= bid.price:
        return None
    return float(bid.price), float(ask.price)


def load_universe():
    """kalshi ticker -> (pm_venue, pm_market_id) for every registry pair."""
    pairs = json.loads(Path("data/research/pairs.json").read_text())
    uni = {}
    for p in pairs:
        if p["family"] == "registry":
            uni.setdefault(p["kalshi"], (p["pm_venue"], p["pm"]))
    return uni


class GateDaemon:
    def __init__(self):
        import lightgbm as lgb
        self.model = lgb.Booster(model_file=str(GATE_MODEL))
        self.uni = load_universe()
        self.books = BookBuilder()
        self.trades = defaultdict(deque)   # ticker -> (ts_ns, size)
        self.mids = defaultdict(deque)     # ticker -> (ts_ns, mid)
        self.last_shadow = defaultdict(float)
        print(f"universe: {len(self.uni)} paired kalshi markets", flush=True)

    def feats(self, kt, now_ns):
        book = self.books.get("kalshi", kt)
        if book is None:
            return None
        t = top(book)
        if t is None:
            return None
        bid, ask = t
        mid = (bid + ask) / 2
        cut60, cut300 = now_ns - int(60e9), now_ns - int(300e9)
        tq, mq = self.trades[kt], self.mids[kt]
        while tq and tq[0][0] < cut300:
            tq.popleft()
        while mq and mq[0][0] < cut300:
            mq.popleft()
        t60 = [x for x in tq if x[0] >= cut60]
        mv = 0.0
        if len(mq) > 1:
            arr = [x[1] for x in mq]
            mv = float(np.abs(np.diff(arr)).sum())
        pm_venue, pm_id = self.uni[kt]
        pm = self.books.get(pm_venue, pm_id)
        basis = stale = None
        if pm is not None:
            pt = top(pm)
            if pt is not None:
                basis = (pt[0] + pt[1]) / 2 - mid
                stale = max(0.0, (now_ns - pm.ts_local_ns) / 1e9)
        return {"spread": ask - bid, "mid": mid,
                "trades_60s": len(t60), "vol_60s": float(sum(s for _, s in t60)),
                "trades_300s": len(tq), "vol_300s": float(sum(s for _, s in tq)),
                "mid_vol_300s": mv,
                "hour": (now_ns / 1e9 % 86400) / 3600.0,
                "basis": basis, "pm_stale_s": stale}

    def score(self, f, sign):
        row = []
        for name in GATE_FEATS:
            if name == "aligned_basis":
                v = (f["basis"] or 0.0) * sign
            else:
                v = f.get(name)
            row.append(float(v if v is not None else 0.0))
        return float(self.model.predict(np.array([row]))[0])

    async def publisher(self):
        while True:
            await asyncio.sleep(2.0)
            now_ns = time.time_ns()
            bad = stale_feeds()
            markets = {}
            for kt in self.uni:
                if bad & {"kalshi-ws", "__health__"}:
                    continue  # gate values would be garbage — publish nothing
                f = self.feats(kt, now_ns)
                if f is None or f["basis"] is None:
                    continue
                # resting bid gets hit by a SELLER (sign -1); ask by a buyer
                markets[kt] = {"bid": round(self.score(f, -1.0), 4),
                               "ask": round(self.score(f, +1.0), 4)}
                if time.time() - self.last_shadow[kt] > 30:
                    self.last_shadow[kt] = time.time()
                    SHADOW_LOG.parent.mkdir(exist_ok=True)
                    with SHADOW_LOG.open("a") as fh:
                        fh.write(json.dumps({
                            "ts": time.time(), "ticker": kt,
                            **markets[kt],
                            **{k: (round(v, 5) if isinstance(v, float) else v)
                               for k, v in f.items()}}) + "\n")
            doc = {"ts": time.time(), "model": GATE_MODEL.name,
                   "stale_feeds": sorted(bad), "markets": markets}
            tmp = OUT.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(doc))
            tmp.replace(OUT)

    async def socket_loop(self):
        while True:
            try:
                reader, _ = await asyncio.open_unix_connection(SOCKET_PATH)
                print("connected to recorder socket", flush=True)
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        ev = parse_event(json.loads(line))
                    except ValueError:
                        continue
                    if isinstance(ev, Trade):
                        if ev.venue.value == "kalshi" and ev.market_id in self.uni:
                            self.trades[ev.market_id].append(
                                (ev.ts_local_ns, float(ev.size)))
                        continue
                    if isinstance(ev, BookSnapshot):
                        self.books.apply_snapshot(ev)
                    elif isinstance(ev, BookDelta):
                        with contextlib.suppress(GapDetected, NotSynced):
                            self.books.apply_delta(ev)
                    else:
                        continue
                    if ev.venue.value == "kalshi" and ev.market_id in self.uni:
                        b = self.books.get("kalshi", ev.market_id)
                        t = top(b) if b else None
                        if t:
                            self.mids[ev.market_id].append(
                                (ev.ts_local_ns, (t[0] + t[1]) / 2))
            except (ConnectionRefusedError, FileNotFoundError):
                await asyncio.sleep(5)
            except Exception as e:
                print(f"socket error: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(2)

    async def run(self):
        await asyncio.gather(self.socket_loop(), self.publisher())


if __name__ == "__main__":
    asyncio.run(GateDaemon().run())
