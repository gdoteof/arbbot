"""ML fill-toxicity live probe: tiny model-gated maker on liquid Kalshi
markets paired with PM-intl (xv-*), no overlap with the exec runner (xvus-*).

Loop: subscribe to the MAIN recorder socket (kalshi + PM-intl books/trades),
rest a small post-only JOIN quote at best bid; every fill is scored with the
validated fill-toxicity model (data/research/toxicity_model.txt, day-holdout
AUC 0.873):

  p_toxic >= --toxic-cut  ->  flatten IMMEDIATELY (IOC into the bid): the
                              model says the price is moving through us.
  p_toxic <  --toxic-cut  ->  hold --hold-s seconds (benign markout), then
                              exit passively at the ask, IOC fallback.

A quote-time gate (toxicity_gate_model.txt, no fill features) pulls the
resting quote when conditions sour before any fill.

Caps: --clip contracts/quote, total inventory <= --max-inv, <= --max-fills
per session, stop + flatten-all at --max-loss-usd realized; data/KILL halts.
Every fill/exit is appended to data/exec/trades.jsonl (strategy
"ml-toxicity-probe") plus a full feature/score log in
data/exec/toxicity_probe.jsonl.

    .venv313/bin/python scripts/toxicity_probe.py             # dry run
    .venv313/bin/python scripts/toxicity_probe.py --live
"""

import argparse
import asyncio
import contextlib
import json
import time
from collections import defaultdict, deque
from decimal import Decimal
from pathlib import Path

import numpy as np

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.ledgerdb import dual_append
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.jsonl import parse_event

SOCKET_PATH = "data/arbbot.sock"
FILL_MODEL = Path("data/research/toxicity_model.txt")
GATE_MODEL = Path("data/research/toxicity_gate_model.txt")
PROBE_LOG = Path("data/exec/toxicity_probe.jsonl")
LEDGER = Path("data/exec/trades.jsonl")
KILL = Path("data/KILL")

# feature orders MUST match the training scripts exactly
FILL_FEATS = ["spread", "mid", "dist_mid", "size", "trades_60s", "vol_60s",
              "trades_300s", "vol_300s", "mid_vol_300s", "hour",
              "aligned_basis", "pm_stale_s"]
GATE_FEATS = ["spread", "mid", "trades_60s", "vol_60s", "trades_300s",
              "vol_300s", "mid_vol_300s", "hour", "aligned_basis", "pm_stale_s"]

# probe universe: kalshi ticker -> PM-intl token id (from pairs.json)
def load_universe(tickers):
    pairs = json.loads(Path("data/research/pairs.json").read_text())
    uni = {}
    for p in pairs:
        if p["pm_venue"] == "polymarket" and p["kalshi"] in tickers:
            uni[p["kalshi"]] = p["pm"]
    return uni


def top(book):
    bid = max(book.bids, key=lambda l: l.price, default=None)
    ask = min(book.asks, key=lambda l: l.price, default=None)
    if bid is None or ask is None or ask.price <= bid.price:
        return None
    return bid, ask


class MarketState:
    def __init__(self, ticker, pm_token):
        self.ticker = ticker
        self.pm_token = pm_token
        self.trades = deque()          # (ts_ns, size)
        self.mids = deque()            # (ts_ns, mid) for realized vol
        self.order_id = None
        self.order_px = None
        self.order_qty = 0
        self.last_reprice = 0.0
        self.inventory = 0
        self.avg_entry = 0.0
        self.exit_order_id = None
        self.exit_deadline = None
        self.open_ts = None
        self.p_toxic_last = None


class ToxProbe:
    def __init__(self, args):
        self.args = args
        self.uni = load_universe(args.tickers)
        if not self.uni:
            raise SystemExit("no PM-intl pairs found for tickers")
        self.books = BookBuilder()
        self.ms = {t: MarketState(t, tok) for t, tok in self.uni.items()}
        self.session_fills = 0
        self.realized = 0.0
        self.stopped = False
        import lightgbm as lgb
        self.fill_model = lgb.Booster(model_file=str(FILL_MODEL))
        self.gate_model = lgb.Booster(model_file=str(GATE_MODEL))
        self.gw = None
        if args.live:
            from arbbot.ops.config import load_credential
            from arbbot.exec.kalshi_gateway import KalshiOrderGateway
            from arbbot.record.kalshi import load_private_key
            kid = load_credential("kalshi_api_key_id")
            kpem = load_credential("kalshi_private_key.pem")
            if not (kid and kpem):
                raise SystemExit("kalshi trade credentials missing")
            self.gw = KalshiOrderGateway(kid.decode().strip(),
                                         load_private_key(kpem), live=True)
            print("[LIVE] kalshi gateway armed", flush=True)
            self.startup_sweep()
        print(f"universe: {self.uni}", flush=True)

    # ---------- features ----------
    def base_feats(self, m, now_ns):
        book = self.books.get("kalshi", m.ticker)
        if book is None:
            return None
        t = top(book)
        if t is None:
            return None
        bid, ask = t
        mid = float(bid.price + ask.price) / 2
        cut60, cut300 = now_ns - int(60e9), now_ns - int(300e9)
        while m.trades and m.trades[0][0] < cut300:
            m.trades.popleft()
        while m.mids and m.mids[0][0] < cut300:
            m.mids.popleft()
        t60 = [x for x in m.trades if x[0] >= cut60]
        mv = 0.0
        if len(m.mids) > 1:
            arr = [x[1] for x in m.mids]
            mv = float(np.abs(np.diff(arr)).sum())
        pm = self.books.get("polymarket", m.pm_token)
        basis, stale = None, None
        if pm is not None:
            pt = top(pm)
            if pt is not None:
                pmid = float(pt[0].price + pt[1].price) / 2
                basis = pmid - mid
                stale = max(0.0, (now_ns - pm.ts_local_ns) / 1e9)
        return {"bid": float(bid.price), "ask": float(ask.price), "mid": mid,
                "spread": float(ask.price - bid.price),
                "trades_60s": len(t60), "vol_60s": float(sum(s for _, s in t60)),
                "trades_300s": len(m.trades),
                "vol_300s": float(sum(s for _, s in m.trades)),
                "mid_vol_300s": mv,
                "hour": (now_ns / 1e9 % 86400) / 3600.0,
                "basis": basis, "pm_stale_s": stale}

    def score(self, model, feats_order, d):
        row = [float(d.get(f, 0.0) if d.get(f) is not None else 0.0)
               for f in feats_order]
        return float(model.predict(np.array([row]))[0])

    def log(self, rec):
        PROBE_LOG.parent.mkdir(exist_ok=True)
        with PROBE_LOG.open("a") as f:
            f.write(json.dumps(rec) + "\n")

    def intents(self, rec):
        """Order-lifecycle event in the runner's trader-intents schema so the
        dash Order-activity tab shows probe orders alongside everything else."""
        rec.setdefault("ts", time.time())
        rec.setdefault("venue", "kalshi")
        with Path("data/scan/trader-intents-mltox.jsonl").open("a") as f:
            f.write(json.dumps(rec) + "\n")

    def startup_sweep(self):
        """Cancel any of OUR resting orders left by a previous run (only on
        this probe's tickers — the runner quotes different markets)."""
        try:
            resting = self.gw.resting_orders()
        except Exception as e:
            raise SystemExit(f"startup sweep failed: {e}")
        for o in resting:
            tk = o.get("ticker")
            if tk in self.ms:
                oid = o.get("order_id") or o.get("id")
                try:
                    self.gw.cancel(oid)
                    print(f"[SWEEP] cancelled orphan {oid} on {tk}", flush=True)
                    self.intents({"cancel": tk, "side": o.get("side"),
                                  "order_id": oid})
                except Exception as e:
                    raise SystemExit(f"orphan cancel failed {oid}: {e}")

    def ledger(self, rec):
        dual_append(rec, source="probe:toxicity")

    # ---------- order management ----------
    async def manage_quotes(self):
        a = self.args
        while not self.stopped:
            await asyncio.sleep(2.0)
            if KILL.exists():
                print("[KILL] halting, flattening", flush=True)
                await self.flatten_all("kill")
                self.stopped = True
                return
            if self.realized <= -a.max_loss_usd or self.session_fills >= a.max_fills:
                print(f"[STOP] realized={self.realized:.2f} fills={self.session_fills}",
                      flush=True)
                await self.flatten_all("session_stop")
                self.stopped = True
                return
            now_ns = time.time_ns()
            for m in self.ms.values():
                try:
                    await self.manage_market(m, now_ns)
                except Exception as e:
                    print(f"[{m.ticker}] manage error {type(e).__name__}: {e}",
                          flush=True)

    async def manage_market(self, m, now_ns):
        a = self.args
        f = self.base_feats(m, now_ns)
        if f is None or f["basis"] is None:
            return
        # fill-side sign for a resting BID is -1 (incoming taker sells to us)
        gate_in = dict(f, aligned_basis=-f["basis"])
        p_gate = self.score(self.gate_model, GATE_FEATS, gate_in)

        # exits first
        if m.inventory > 0:
            await self.manage_exit(m, f, now_ns)

        want_quote = (p_gate < a.gate_cut and m.inventory == 0
                      and not self.stopped
                      and 0.03 <= f["bid"] <= 0.90)
        if m.order_id is not None:
            filled = await self.check_fill(m, f, now_ns)
            if filled:
                return
            if (not want_quote) or abs(m.order_px - f["bid"]) > 1e-9:
                if time.time() - m.last_reprice > 2.0:
                    await self.cancel_order(m, f, now_ns)
        if want_quote and m.order_id is None and time.time() - m.last_reprice > 2.0:
            await self.place_quote(m, f, p_gate)

    async def place_quote(self, m, f, p_gate):
        a = self.args
        m.last_reprice = time.time()
        px = f["bid"]
        rec = {"ts": time.time(), "ticker": m.ticker, "action": "quote",
               "px": px, "qty": a.clip, "p_gate": round(p_gate, 4),
               **{k: (round(v, 5) if isinstance(v, float) else v)
                  for k, v in f.items()}}
        if not a.live:
            rec["action"] = "dry_quote"
            m.order_id, m.order_px, m.order_qty = "dry", px, a.clip
            self.log(rec)
            return
        try:
            r = await asyncio.to_thread(
                self.gw.place_yes, m.ticker, "bid", Decimal(str(px)), a.clip,
                post_only=True)
            oid = (r.get("order") or {}).get("order_id") or r.get("order_id")
            m.order_id, m.order_px, m.order_qty = oid, px, a.clip
            rec["order_id"] = oid
            self.intents({"place": m.ticker, "side": "bid", "price": str(px),
                          "count": a.clip, "order_id": oid})
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {e}"
            self.intents({"place_failed": m.ticker, "reason": str(e)[:120]})
        self.log(rec)

    async def cancel_order(self, m, f, now_ns):
        m.last_reprice = time.time()
        if not self.args.live:
            m.order_id = None
            return
        oid = m.order_id
        try:
            await asyncio.to_thread(self.gw.cancel, oid)
            self.intents({"cancel": m.ticker, "side": "bid",
                          "price": str(m.order_px), "order_id": oid})
        except Exception:
            pass
        # catch a race: partial fill before the cancel landed
        with contextlib.suppress(Exception):
            got = await asyncio.to_thread(self.gw.filled_qty, oid)
            if got:
                await self.on_fill(m, f, got, m.order_px, now_ns)
        m.order_id = None

    async def check_fill(self, m, f, now_ns):
        if not self.args.live:
            return False  # dry-run: no simulated fills, gate/quote logging only
        try:
            got = await asyncio.to_thread(self.gw.filled_qty, m.order_id)
        except Exception:
            return False
        if got and got >= m.order_qty:
            await self.on_fill(m, f, got, m.order_px, now_ns)
            m.order_id = None
            return True
        return False

    async def on_fill(self, m, f, qty, px, now_ns):
        a = self.args
        self.session_fills += 1
        m.inventory += qty
        m.avg_entry = px
        m.open_ts = time.time()
        fill_in = dict(f, dist_mid=(f["mid"] - px), size=float(qty),
                       aligned_basis=-(f["basis"]))
        p_toxic = self.score(self.fill_model, FILL_FEATS, fill_in)
        m.p_toxic_last = p_toxic
        rec = {"ts": time.time(), "ticker": m.ticker, "action": "fill",
               "px": px, "qty": qty, "p_toxic": round(p_toxic, 4),
               **{k: (round(v, 5) if isinstance(v, float) else v)
                  for k, v in f.items()}}
        self.log(rec)
        self.ledger({
            "ts": time.time(), "relationship_id": f"mltox-{m.ticker}",
            "title": f"ML toxicity probe fill: {m.ticker}",
            "qty": qty, "strategy": "ml-toxicity-probe",
            "legs": [{"venue": "kalshi", "market_id": m.ticker, "side": "yes",
                      "role": "maker", "qty": qty, "avg_price": str(px)}],
            "cost_usd": round(px * qty, 4), "payoff_usd": None,
            "profit_usd": None, "status": "open",
            "note": f"p_toxic={p_toxic:.3f}"})
        print(f"[FILL] {m.ticker} +{qty}@{px} p_toxic={p_toxic:.3f} -> "
              f"{'FLATTEN NOW' if p_toxic >= a.toxic_cut else f'hold {a.hold_s}s'}",
              flush=True)
        if p_toxic >= a.toxic_cut:
            await self.exit_now(m, "toxic")
        else:
            m.exit_deadline = time.time() + a.hold_s

    async def manage_exit(self, m, f, now_ns):
        if m.exit_deadline is not None and time.time() >= m.exit_deadline:
            await self.exit_now(m, "hold_expired")

    async def exit_now(self, m, reason):
        qty = m.inventory
        if qty <= 0:
            return
        f = self.base_feats(m, time.time_ns())
        px = f["bid"] if f else m.avg_entry
        rec = {"ts": time.time(), "ticker": m.ticker, "action": "exit",
               "reason": reason, "qty": qty, "px": px}
        if self.args.live:
            try:
                r = await asyncio.to_thread(
                    self.gw.place_yes, m.ticker, "ask", Decimal(str(px)), qty,
                    post_only=False)
                oid = (r.get("order") or {}).get("order_id") or r.get("order_id")
                rec["order_id"] = oid
                self.intents({"place": m.ticker, "side": "ask", "price": str(px),
                              "count": qty, "order_id": oid})
                await asyncio.sleep(1.5)
                sold = 0
                with contextlib.suppress(Exception):
                    sold = await asyncio.to_thread(self.gw.filled_qty, oid)
                rec["sold"] = sold
                if sold and sold >= qty:
                    pnl = (px - m.avg_entry) * sold
                    self.realized += pnl
                    rec["pnl"] = round(pnl, 4)
                    self.ledger({
                        "ts": time.time(), "relationship_id": f"mltox-{m.ticker}",
                        "strategy": "ml-toxicity-probe", "status": "unwound",
                        "closes_ts": m.open_ts, "qty": qty,
                        "proceeds_usd": round(px * sold, 4),
                        "realized_pnl_usd": round(pnl, 4),
                        "note": f"exit:{reason}"})
                    m.inventory = 0
                    m.exit_deadline = None
                    print(f"[EXIT {reason}] {m.ticker} -{sold}@{px} "
                          f"pnl={pnl:+.3f} session={self.realized:+.2f}", flush=True)
                else:
                    # partial/no IOC fill: retry shortly at the fresh bid
                    m.inventory = qty - (sold or 0)
                    m.exit_deadline = time.time() + 5
            except Exception as e:
                rec["error"] = f"{type(e).__name__}: {e}"
                m.exit_deadline = time.time() + 5
        self.log(rec)

    async def flatten_all(self, reason):
        for m in self.ms.values():
            if self.args.live and m.order_id and m.order_id != "dry":
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(self.gw.cancel, m.order_id)
            m.order_id = None
            if m.inventory > 0:
                await self.exit_now(m, reason)

    # ---------- event loop ----------
    async def socket_loop(self):
        while not self.stopped:
            try:
                reader, _ = await asyncio.open_unix_connection(SOCKET_PATH)
                print("connected to recorder socket", flush=True)
                while not self.stopped:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        ev = parse_event(json.loads(line))
                    except ValueError:
                        continue
                    if isinstance(ev, Trade):
                        if ev.venue.value == "kalshi" and ev.market_id in self.ms:
                            self.ms[ev.market_id].trades.append(
                                (ev.ts_local_ns, float(ev.size)))
                        continue
                    if isinstance(ev, BookSnapshot):
                        self.books.apply_snapshot(ev)
                    elif isinstance(ev, BookDelta):
                        with contextlib.suppress(GapDetected, NotSynced):
                            self.books.apply_delta(ev)
                    else:
                        continue
                    if ev.venue.value == "kalshi" and ev.market_id in self.ms:
                        m = self.ms[ev.market_id]
                        b = self.books.get("kalshi", m.ticker)
                        t = top(b) if b else None
                        if t:
                            m.mids.append(
                                (ev.ts_local_ns,
                                 float(t[0].price + t[1].price) / 2))
            except (ConnectionRefusedError, FileNotFoundError):
                await asyncio.sleep(5)
            except Exception as e:
                print(f"socket error: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(2)

    async def run(self):
        await asyncio.gather(self.socket_loop(), self.manage_quotes())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--tickers", nargs="+",
                    default=["KXPRESNOMD-28-GN", "KXPRESPERSON-28-JVAN"])
    ap.add_argument("--clip", type=int, default=5)
    ap.add_argument("--gate-cut", type=float, default=0.35)
    ap.add_argument("--toxic-cut", type=float, default=0.5)
    ap.add_argument("--hold-s", type=float, default=600)
    ap.add_argument("--max-inv", type=int, default=10)
    ap.add_argument("--max-fills", type=int, default=12)
    ap.add_argument("--max-loss-usd", type=float, default=3.0)
    args = ap.parse_args()
    asyncio.run(ToxProbe(args).run())


if __name__ == "__main__":
    main()
