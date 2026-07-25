"""ML lead-lag live probe: Kalshi sports jump -> take stale PM-US moneyline.

Subscribes to the sports recorder socket (data/arbbot-sports.sock), rebuilds
books, detects anchor-based mid jumps (>= --thresh) on Kalshi long tickers,
scores each event with the trained LightGBM model, and — in --live mode —
IOC-takes the paired PM-US moneyline when p_hat >= --min-p, within hard caps.

DRY-RUN by default: logs every signal + would-be action to
data/exec/leadlag_probe.jsonl without touching a venue.

Caps (live): --clip contracts/trade, --max-open-usd total premium at risk,
--max-trades per session, 2 trades + 180s cooldown per pair, data/KILL halts.
Positions are held to game resolution (tennis/MLB resolve in hours).

    .venv313/bin/python scripts/leadlag_probe.py                # dry run
    .venv313/bin/python scripts/leadlag_probe.py --live --clip 5
"""

import argparse
import asyncio
import contextlib
import json
import time
from collections import defaultdict, deque
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.ledgerdb import dual_append
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.jsonl import parse_event

SOCKET_PATH = "data/arbbot-sports.sock"
SPORTS_MAP = Path("data/scan/sports_equiv_map.json")
MODEL_PATH = Path("data/research/leadlag_model_sports.txt")
PROBE_LOG = Path("data/exec/leadlag_probe.jsonl")
LEDGER = Path("data/exec/trades.jsonl")
KILL = Path("data/KILL")

FEATURES = ["abs_jump", "f_spread", "f_stale_s", "aligned_basis", "leader_mid",
            "f_mid", "hour", "pair_event_no",
            "l_spread", "l_trades_60s", "l_trade_vol_60s", "league"]


def load_pairs():
    smap = json.loads(SPORTS_MAP.read_text())
    pairs = {}
    for m in smap.get("matches", []):
        kt, pm = m.get("kalshi_long_ticker"), m.get("pm_moneyline")
        if kt and pm:
            pairs[kt] = {"pm": pm, "league": m["league"],
                         "teams": m.get("teams", ""), "kalshi": kt}
    return pairs


def top(book):
    bid = max(book.bids, key=lambda l: l.price, default=None)
    ask = min(book.asks, key=lambda l: l.price, default=None)
    if bid is None or ask is None or ask.price <= bid.price:
        return None
    return bid, ask


class Probe:
    def __init__(self, args):
        self.args = args
        self.pairs = load_pairs()
        self.books = BookBuilder()
        self.anchor: dict[str, float] = {}
        self.trades60: dict[str, deque] = defaultdict(deque)
        self.event_no: dict[str, int] = defaultdict(int)
        self.pair_trades: dict[str, int] = defaultdict(int)
        self.pair_last_trade: dict[str, float] = {}
        self.session_trades = 0
        self.open_cost = 0.0
        self.leagues = sorted({p["league"] for p in self.pairs.values()})
        self.model = None
        if MODEL_PATH.exists():
            import lightgbm as lgb
            self.model = lgb.Booster(model_file=str(MODEL_PATH))
            print(f"model loaded: {MODEL_PATH}", flush=True)
        else:
            print("no model file — logging signals only", flush=True)
        self.gw = None
        if args.live:
            from arbbot.ops.config import load_credential
            from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
            us_id = load_credential("polymarket_usa_key_id")
            us_key = load_credential("polymarket_usa_private_key")
            if not (us_id and us_key):
                raise SystemExit("PM-US credentials missing")
            self.gw = PolymarketUsOrderGateway(
                us_id.decode().strip(), us_key.decode().strip(), live=True)
            print("[LIVE] PM-US gateway armed", flush=True)

    def features(self, kt, jump, know, pmb, pma, pm_ts, now_ns):
        k = top(know)
        if k is None:
            return None
        kbid, kask = k
        kmid = float(kbid.price + kask.price) / 2
        pmid = float(pmb.price + pma.price) / 2
        sign = 1.0 if jump > 0 else -1.0
        dq = self.trades60[kt]
        cutoff = now_ns - int(60e9)
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        return {
            "abs_jump": abs(jump),
            "f_spread": float(pma.price - pmb.price),
            "f_stale_s": max(0.0, (now_ns - pm_ts) / 1e9),
            "aligned_basis": (kmid - pmid) * sign,
            "leader_mid": kmid,
            "f_mid": pmid,
            "hour": (now_ns / 1e9 % 86400) / 3600.0,
            "pair_event_no": self.event_no[kt],
            "l_spread": float(kask.price - kbid.price),
            "l_trades_60s": len(dq),
            "l_trade_vol_60s": float(sum(s for _, s in dq)),
            "league": self.pairs[kt]["league"],
        }

    def predict(self, feats):
        if self.model is None:
            return None
        import numpy as np
        row = []
        for f in FEATURES:
            v = feats[f]
            if f == "league":
                v = self.leagues.index(v) if v in self.leagues else -1
            row.append(float(v))
        return float(self.model.predict(np.array([row]))[0])

    def log(self, rec):
        PROBE_LOG.parent.mkdir(exist_ok=True)
        with PROBE_LOG.open("a") as f:
            f.write(json.dumps(rec) + "\n")

    async def maybe_trade(self, kt, jump, feats, p_hat, pmb, pma):
        a = self.args
        pair = self.pairs[kt]
        up = jump > 0
        entry = float(pma.price) if up else float(pmb.price)
        cost = entry * a.clip if up else (1 - entry) * a.clip
        rec = {"ts": time.time(), "pair": pair["pm"], "kalshi": kt,
               "league": pair["league"], "teams": pair["teams"],
               "jump": round(jump, 4), "p_hat": p_hat,
               "entry_px": entry, "side": "yes" if up else "no",
               "clip": a.clip, "cost_usd": round(cost, 2), "action": "signal",
               **{k: (round(v, 5) if isinstance(v, float) else v)
                  for k, v in feats.items()}}
        blocked = None
        if KILL.exists():
            blocked = "KILL"
        elif p_hat is None or p_hat < a.min_p:
            blocked = f"p<{a.min_p}"
        elif self.session_trades >= a.max_trades:
            blocked = "max_trades"
        elif self.open_cost + cost > a.max_open_usd:
            blocked = "max_open_usd"
        elif self.pair_trades[kt] >= 2:
            blocked = "pair_cap"
        elif time.time() - self.pair_last_trade.get(kt, 0) < 180:
            blocked = "cooldown"
        elif not (0.03 <= entry <= 0.97):
            blocked = "extreme_px"
        if blocked:
            rec["action"] = f"skip:{blocked}"
            self.log(rec)
            return
        if not a.live:
            rec["action"] = "dry_run_would_trade"
            self.log(rec)
            print(f"[DRY] {pair['teams']} {rec['side']} @{entry} p={p_hat}", flush=True)
            return

        rec["action"] = "trade"
        t0 = time.time()
        try:
            if up:
                r = self.gw.place_yes(pair["pm"], "bid", Decimal(str(entry)),
                                      a.clip, post_only=False)
            else:
                r = self.gw.place_short(pair["pm"], Decimal(str(entry)),
                                        a.clip, post_only=False)
            oid = r.get("id") or r.get("order_id")
            rec["order_id"] = oid
            filled = 0
            for _ in range(10):
                await asyncio.sleep(0.5)
                filled = self.gw.filled_qty(oid)
                if filled:
                    break
            # PM fill reporting lags (runaway postmortem): re-confirm via order
            with contextlib.suppress(Exception):
                o = self.gw.get_order(oid)
                filled = max(filled, int(float(o.get("cumQuantity", 0) or 0)))
            rec["filled"] = filled
            rec["latency_s"] = round(time.time() - t0, 3)
            if filled:
                self.session_trades += 1
                self.pair_trades[kt] += 1
                self.pair_last_trade[kt] = time.time()
                fill_cost = entry * filled if up else (1 - entry) * filled
                self.open_cost += fill_cost
                ledger = {
                    "ts": time.time(), "relationship_id": f"mlprobe-{pair['pm']}",
                    "title": f"ML lead-lag probe: {pair['teams']} ({pair['league']})",
                    "qty": filled, "strategy": "ml-leadlag-probe",
                    "kalshi_ref": kt,
                    "legs": [{"venue": "polymarket_us", "market_id": pair["pm"],
                              "side": rec["side"], "role": "taker", "qty": filled,
                              "yes_price": str(entry), "cost": str(round(fill_cost, 4)),
                              "order_id": oid}],
                    "cost_usd": round(fill_cost, 4), "payoff_usd": filled,
                    "profit_usd": None, "status": "open",
                    "note": f"p_hat={p_hat} jump={round(jump, 4)} model={MODEL_PATH.name}",
                }
                dual_append(ledger, source="probe:leadlag")
                print(f"[LIVE FILL] {pair['teams']} {rec['side']} {filled}@{entry} "
                      f"p={p_hat}", flush=True)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {e}"
        self.log(rec)

    async def on_event(self, ev):
        if isinstance(ev, Trade):
            if ev.venue.value == "kalshi" and ev.market_id in self.pairs:
                self.trades60[ev.market_id].append(
                    (ev.ts_local_ns, float(ev.size)))
            return
        if isinstance(ev, BookSnapshot):
            self.books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                self.books.apply_delta(ev)
            except (GapDetected, NotSynced):
                return
        else:
            return
        if ev.venue.value != "kalshi" or ev.market_id not in self.pairs:
            return
        kt = ev.market_id
        book = self.books.get("kalshi", kt)
        if book is None:
            return
        t = top(book)
        if t is None:
            return
        mid = float(t[0].price + t[1].price) / 2
        if kt not in self.anchor:
            self.anchor[kt] = mid
            return
        jump = mid - self.anchor[kt]
        if abs(jump) < self.args.thresh:
            return
        self.anchor[kt] = mid
        self.event_no[kt] += 1
        pmbook = self.books.get("polymarket_us", self.pairs[kt]["pm"])
        if pmbook is None:
            return
        pt = top(pmbook)
        if pt is None:
            return
        feats = self.features(kt, jump, book, pt[0], pt[1],
                              pmbook.ts_local_ns, ev.ts_local_ns)
        if feats is None:
            return
        p_hat = self.predict(feats)
        await self.maybe_trade(kt, jump, feats, p_hat, pt[0], pt[1])

    async def run(self):
        n = 0
        while True:
            try:
                reader, _ = await asyncio.open_unix_connection(SOCKET_PATH)
                print("connected to sports recorder socket", flush=True)
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        ev = parse_event(json.loads(line))
                    except ValueError:
                        continue
                    n += 1
                    await self.on_event(ev)
            except (ConnectionRefusedError, FileNotFoundError):
                await asyncio.sleep(5)
            except Exception as e:
                print(f"socket error: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--thresh", type=float, default=0.02)
    ap.add_argument("--min-p", type=float, default=0.65)
    ap.add_argument("--clip", type=int, default=5)
    ap.add_argument("--max-open-usd", type=float, default=50.0)
    ap.add_argument("--max-trades", type=int, default=20)
    args = ap.parse_args()
    asyncio.run(Probe(args).run())


if __name__ == "__main__":
    main()
