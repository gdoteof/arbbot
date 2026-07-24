#!/usr/bin/env python
"""Maker fill quality: when our resting maker orders filled, could we have
rested one tick more passive (better price) and still been filled?

For each maker fill in data/exec/trades.jsonl (strategy make-take, plus
pmus-maker-probe counted separately):
  - reconstruct the venue book at ts-2s from recorded data
    (kalshi: parquet/raw snapshot+delta replay; polymarket_us: snapshot poll
    stream),
  - gap_ticks = tick distance from our price to the next REAL same-side level
    beyond ours (our own level at p with size <= qty is excluded; levels with
    size < 2 contracts are dust, counted in dust_inside),
  - sweep_evidence: within [ts-2s, ts+60s], was the first real level beyond
    ours materially consumed (or, on kalshi, did a trade print through our
    price)?  That is direct evidence the taker would have paid one more tick.

Emits data/reports/fill_quality.json.  Read-only over market data; writes
only the report.  Run:  PYTHONPATH=src .venv313/bin/python scripts/fill_quality.py
Use --inspect N to dump the reconstructed books around fill #N (spot check).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
LEDGER = ROOT / "data" / "exec" / "trades.jsonl"
REPORT = ROOT / "data" / "reports" / "fill_quality.json"

TICK = 0.01
DUST = 2.0  # contracts; smaller resting size is dust, not a "real" level
PRE_S = 2.0  # book evaluated at (or before) ts - PRE_S
POST_S = 60.0  # sweep-evidence window end: ts + POST_S
LOOKBACK_S = 120.0  # how far back we search for the pre-fill book state
KALSHI_DAYS = ["2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24"]


def day_of(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- ledger


def load_maker_fills() -> list[dict]:
    fills = []
    with open(LEDGER) as f:
        for line in f:
            r = json.loads(line)
            strat = r.get("strategy")
            if strat not in ("make-take", "pmus-maker-probe"):
                continue
            for leg in r.get("legs", []):
                if leg.get("role") != "maker":
                    continue
                fills.append(
                    {
                        "ts": r["ts"],
                        "strategy": strat,
                        "venue": leg["venue"],
                        "market": leg["market_id"],
                        "side": leg["side"],  # yes -> we rest on YES bids; no -> YES asks
                        "px": float(leg["yes_price"]),
                        "qty": float(leg["qty"]),
                    }
                )
    fills.sort(key=lambda x: x["ts"])
    return fills


# ---------------------------------------------------------------- data loading


def _levels_to_dict(levels) -> dict[float, float]:
    out = {}
    for lv in levels or []:
        if isinstance(lv, str):
            lv = json.loads(lv)
        out[round(float(lv["price"]), 4)] = float(lv["size"])
    return out


def load_pmus_snapshots(days: set[str], markets: set[str]) -> dict[tuple[str, str], list]:
    """{(market, day): [(ts_s, bids{p:s}, asks{p:s})] sorted}. Snapshot-poll stream."""
    import duckdb

    from arbbot.record.archive import source_for

    out: dict[tuple[str, str], list] = {}
    con = duckdb.connect()
    mlist = ", ".join(f"'{m}'" for m in sorted(markets))
    for day in sorted(days):
        src = source_for(RAW_DIR, "polymarket_us", day)
        if src is None:
            continue
        kind, path = src
        reader = (
            f"read_parquet('{path}')"
            if kind == "parquet"
            else f"read_json_auto('{path}', format='newline_delimited', "
            f"union_by_name=true, sample_size=-1, maximum_object_size=20000000)"
        )
        cur = con.execute(
            f"SELECT market_id, ts_local_ns, bids, asks FROM {reader} "
            f"WHERE kind = 'snapshot' AND market_id IN ({mlist}) "
            f"ORDER BY ts_local_ns"
        )
        for mid, ts_ns, bids, asks in cur.fetchall():
            out.setdefault((mid, day), []).append(
                (ts_ns / 1e9, _levels_to_dict(bids), _levels_to_dict(asks))
            )
    con.close()
    return out


def load_kalshi_events(markets: set[str]) -> dict[str, list]:
    """All recorded events per market across all days, time-ordered.
    Seq chains span UTC days, so replay always starts from the earliest day."""
    import duckdb

    from arbbot.record.archive import source_for

    out: dict[str, list] = {m: [] for m in markets}
    con = duckdb.connect()
    mlist = ", ".join(f"'{m}'" for m in sorted(markets))
    for day in KALSHI_DAYS:
        src = source_for(RAW_DIR, "kalshi", day)
        if src is None:
            continue
        kind, path = src
        reader = (
            f"read_parquet('{path}')"
            if kind == "parquet"
            else f"read_json_auto('{path}', format='newline_delimited', "
            f"union_by_name=true, sample_size=-1, maximum_object_size=20000000)"
        )
        cur = con.execute(
            f"SELECT * FROM {reader} "
            f"WHERE market_id IN ({mlist}) ORDER BY ts_local_ns, seq"
        )
        cols = [d[0] for d in cur.description]
        for tup in cur.fetchall():
            d = dict(zip(cols, tup))
            k, mid, side = d["kind"], d["market_id"], d.get("side")
            price, size, seq = d.get("price"), d.get("size"), d["seq"]
            ts_ns, taker_side = d["ts_local_ns"], d.get("taker_side")
            bids, asks = d.get("bids"), d.get("asks")
            ev = {"kind": k, "ts": ts_ns / 1e9, "seq": seq}
            if k == "snapshot":
                ev["bids"] = _levels_to_dict(bids)
                ev["asks"] = _levels_to_dict(asks)
            elif k == "delta":
                ev["side"] = side  # "bid"/"ask", already YES-denominated
                ev["price"] = round(float(price), 4)
                ev["size"] = float(size)
            else:  # trade, YES-denominated price
                ev["price"] = round(float(price), 4)
                ev["size"] = float(size)
                ev["taker_side"] = taker_side
            out[mid].append(ev)
    for m in out:
        out[m].sort(key=lambda e: (e["ts"], e["seq"]))
    con.close()
    return out


class KalshiReplay:
    """Tolerant snapshot+delta replay for one market (BookBuilder semantics:
    new-total deltas, contiguous seq; on a gap we go unsynced until the next
    snapshot rather than raising)."""

    def __init__(self, events: list):
        self.events = events
        self.i = 0
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.seq = -1
        self.synced = False
        self.last_ts = None

    def step(self, on_event=None):
        """Apply exactly one event."""
        ev = self.events[self.i]
        self.i += 1
        self._apply(ev)
        if on_event is not None:
            on_event(ev, self)

    def advance(self, until_ts: float, on_event=None):
        while self.i < len(self.events) and self.events[self.i]["ts"] <= until_ts:
            self.step(on_event)

    def advance_index(self, until_i: int):
        while self.i < min(until_i, len(self.events)):
            self.step()

    def _apply(self, ev):
            if ev["kind"] == "snapshot":
                self.bids = dict(ev["bids"])
                self.asks = dict(ev["asks"])
                self.seq = ev["seq"]
                self.synced = True
                self.last_ts = ev["ts"]
            elif ev["kind"] == "delta":
                if not self.synced or ev["seq"] <= self.seq:
                    pass  # pre-snapshot / stale: drop
                elif ev["seq"] != self.seq + 1:
                    self.synced = False  # gap: book untrustworthy until snapshot
                else:
                    levels = self.bids if ev["side"] == "bid" else self.asks
                    if ev["size"] > 0:
                        levels[ev["price"]] = ev["size"]
                    else:
                        levels.pop(ev["price"], None)
                    self.seq = ev["seq"]
                    self.last_ts = ev["ts"]


# ---------------------------------------------------------------- analysis


def gap_calc(levels: dict[float, float], px: float, qty: float, passive_dir: int):
    """passive_dir: -1 our level is a bid (more passive = lower price),
    +1 our level is an ask (more passive = higher price).

    Returns (gap_ticks, dust_inside, residual_at_p, next_real_px,
             our_level_present).  gap_ticks == 0 means real size shares our
    price level; None means no real level beyond ours at all."""
    size_at_p = levels.get(round(px, 4), 0.0)
    our_level_present = size_at_p > 0
    residual = max(0.0, size_at_p - qty)
    dust_inside = 1 if 0 < residual < DUST else 0
    if residual >= DUST:
        return 0, dust_inside, residual, round(px, 4), our_level_present
    beyond = sorted(
        (p for p in levels if (p - px) * passive_dir > 1e-9),
        reverse=(passive_dir < 0),
    )
    for p in beyond:
        if levels[p] >= DUST:
            gap = round(abs(p - px) / TICK)
            return gap, dust_inside, residual, p, our_level_present
        dust_inside += 1
    return None, dust_inside, residual, None, our_level_present


def consumed(baseline: float, min_size: float, qty: float) -> bool:
    """Material consumption of the reference level: wiped out, or reduced by
    at least our fill quantity AND at least 20% of the level (a >=qty drop
    alone is routine churn on a several-hundred-lot level)."""
    if baseline < DUST:
        return False
    drop = baseline - min_size
    return min_size < DUST or (drop >= max(2.0, qty) and drop >= 0.2 * baseline)


def analyze_fill(fill: dict, pmus_snaps, kalshi_evs, inspect: bool = False):
    ts, px, qty = fill["ts"], fill["px"], fill["qty"]
    side_is_bid = fill["side"] == "yes"  # yes maker = YES bid; no maker = YES ask
    passive_dir = -1 if side_is_bid else +1
    ts_pre, ts_end = ts - PRE_S, ts + POST_S
    row = {
        "ts": iso(ts),
        "strategy": fill["strategy"],
        "venue": fill["venue"],
        "market": fill["market"],
        "side": fill["side"],
        "px": px,
        "qty": qty,
    }

    # The ledger ts is FILL DETECTION time; the fill itself can precede it by
    # more than PRE_S (observed: ~2.2s on kalshi).  Anchor the "pre" book at
    # the last state <= ts-PRE_S within a LOOKBACK_S window where our resting
    # level (size >= qty at px) was still visible; fall back to the plain
    # ts-PRE_S state if it never was.  The sweep window starts right after the
    # anchored pre state so the fill events themselves are inside it.
    pxr = round(px, 4)

    def has_us(bids, asks):
        return (bids if side_is_bid else asks).get(pxr, 0.0) >= qty - 1e-9

    if fill["venue"] == "polymarket_us":
        days = {day_of(ts - LOOKBACK_S), day_of(ts_end)}
        snaps = []
        for d in sorted(days):
            snaps.extend(pmus_snaps.get((fill["market"], d), []))
        pre = [s for s in snaps if s[0] <= ts_pre]
        if not pre:
            row["status"] = "no_data"
            return row
        withus = [s for s in pre if s[0] >= ts - LOOKBACK_S and has_us(s[1], s[2])]
        pre_ts, pre_bids, pre_asks = withus[-1] if withus else pre[-1]
        row["pre_offset_s"] = round(ts - pre_ts, 1)
        book_side = pre_bids if side_is_bid else pre_asks
        opp = pre_asks if side_is_bid else pre_bids
        post = [s for s in snaps if pre_ts < s[0] <= ts_end]
        post_sizes = lambda q: [  # noqa: E731
            (s[1] if side_is_bid else s[2]).get(q, 0.0) for s in post
        ]
        if inspect:
            _dump_book("PRE  (snapshot %.1fs before fill-detect ts)" % (ts - pre_ts),
                       pre_bids, pre_asks, px)
            for s in post[:6]:
                _dump_book("POST %+.1fs" % (s[0] - ts), s[1], s[2], px)
    else:  # kalshi
        evs = kalshi_evs.get(fill["market"], [])
        rep = KalshiReplay(evs)
        rep.advance(ts - LOOKBACK_S)
        best_idx = rep.i if (rep.synced and has_us(rep.bids, rep.asks)) else None
        while rep.i < len(evs) and evs[rep.i]["ts"] <= ts_pre:
            rep.step()
            if rep.synced and has_us(rep.bids, rep.asks):
                best_idx = rep.i
        rep2 = KalshiReplay(evs)
        if best_idx is not None:
            rep2.advance_index(best_idx)
        else:
            rep2.advance(ts_pre)
        if not rep2.synced or rep2.last_ts is None or rep2.last_ts < ts - 3600 * 6:
            row["status"] = "no_data"
            return row
        row["pre_offset_s"] = round(ts - rep2.last_ts, 1)
        pre_bids, pre_asks = dict(rep2.bids), dict(rep2.asks)
        book_side = pre_bids if side_is_bid else pre_asks
        opp = pre_asks if side_is_bid else pre_bids
        # continue replay through the sweep window, tracking sizes + trades
        track: dict[float, list] = {}
        trades: list = []

        def watch(ev, r):
            if ev["kind"] == "trade":
                trades.append(ev)
            for q in track:
                sz = (r.bids if side_is_bid else r.asks).get(q, 0.0)
                track[q].append(sz)

        # (reference prices registered after gap calc below)
        row["_kalshi"] = (rep2, watch, track, trades)
        if inspect:
            _dump_book("PRE (replayed, %.1fs before fill-detect ts)"
                       % (ts - rep2.last_ts), pre_bids, pre_asks, px)

    gap, dust_inside, residual, next_px, our_lvl = gap_calc(
        book_side, px, qty, passive_dir
    )
    row.update(
        {
            "status": "ok",
            "gap_ticks": gap,
            "dust_inside": dust_inside,
            "residual_at_px": round(residual, 2),
            "our_level_present": our_lvl,
            "next_real_px": next_px,
            "opp_touch": (min(opp) if not side_is_bid else max(opp)) if opp else None,
        }
    )
    # opp touch: opposite side best (for our bid: best ask = min ask; for our
    # ask: best bid = max bid)
    if opp:
        row["opp_touch"] = min(opp) if side_is_bid else max(opp)

    # ---- sweep / trade-through evidence.
    # Track every level strictly beyond ours up to and including the next REAL
    # level: consumption of the next real level (or a printed trade through our
    # price on kalshi) is "sweep_evidence" per spec; consumption of ANY level
    # beyond ours (dust included) is "one_tick_evidence" — the taker
    # demonstrably paid at least one tick past where we stood.
    sweep = "unknown"
    one_tick = "unknown"
    if next_px is not None and gap != 0:
        tracked = sorted(
            (
                q
                for q in book_side
                if (q - px) * passive_dir > 1e-9
                and abs(q - px) <= abs(next_px - px) + 1e-9
            ),
            reverse=(passive_dir < 0),
        )
        tt = False
        if fill["venue"] == "polymarket_us":
            minafter = {q: min(post_sizes(q), default=None) for q in tracked}
            have_post = any(v is not None for v in minafter.values())
        else:
            rep, watch, track, trades = row.pop("_kalshi")
            for q in tracked:
                track[q] = []
            rep.advance(ts_end, on_event=watch)
            minafter = {q: (min(track[q]) if track[q] else None) for q in tracked}
            have_post = any(v is not None for v in minafter.values())
            tt = any(
                (t["price"] < px - 1e-9) if side_is_bid else (t["price"] > px + 1e-9)
                for t in trades
            )
            row["trade_through"] = tt
            if inspect:
                print("  trades in window:", [(round(t["ts"] - ts, 1), t["price"],
                                               t["size"], t["taker_side"])
                                              for t in trades])
        def level_consumed(q):
            b = book_side.get(q, 0.0)
            m = minafter.get(q)
            if m is None:
                return None
            return m == 0.0 if b < DUST else consumed(b, m, qty)

        if have_post or tt:
            ref = level_consumed(next_px)
            sweep = bool(tt or (ref is True))
            one_tick = bool(tt or any(level_consumed(q) for q in tracked))
        row["ref_baseline"] = book_side.get(next_px, 0.0)
        row["ref_min_after"] = minafter.get(next_px)
        row["consumed_beyond"] = [q for q in tracked if level_consumed(q)]
    else:
        row.pop("_kalshi", None)
    row["sweep_evidence"] = sweep
    row["one_tick_evidence"] = one_tick
    return row


def _dump_book(label: str, bids: dict, asks: dict, px: float, depth: int = 6):
    print(f"  [{label}]  our px marked *")
    a = sorted(asks)[:depth]
    for p in reversed(a):
        mark = "*" if abs(p - px) < 1e-9 else " "
        print(f"    ask {p:6.2f}{mark} x {asks[p]:10.1f}")
    print("    " + "-" * 30)
    for p in sorted(bids, reverse=True)[:depth]:
        mark = "*" if abs(p - px) < 1e-9 else " "
        print(f"    bid {p:6.2f}{mark} x {bids[p]:10.1f}")


# ---------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inspect", type=int, default=None,
                    help="dump reconstructed books for fill #N (0-based) and exit")
    args = ap.parse_args()

    fills = load_maker_fills()
    pmus_markets: dict = {}
    pmus_days: set = set()
    kalshi_markets: set = set()
    for f in fills:
        if f["venue"] == "polymarket_us":
            pmus_markets.setdefault(f["market"], set())
            pmus_days.add(day_of(f["ts"] - PRE_S))
            pmus_days.add(day_of(f["ts"] + POST_S))
        else:
            kalshi_markets.add(f["market"])

    print(f"{len(fills)} maker fills; loading books "
          f"({len(pmus_markets)} PM-US markets x {len(pmus_days)} days, "
          f"{len(kalshi_markets)} kalshi markets) ...")
    pmus_snaps = load_pmus_snapshots(pmus_days, set(pmus_markets))
    kalshi_evs = load_kalshi_events(kalshi_markets)

    if args.inspect is not None:
        f = fills[args.inspect]
        print(f"fill #{args.inspect}: {json.dumps(f)}")
        row = analyze_fill(f, pmus_snaps, kalshi_evs, inspect=True)
        print(json.dumps(row, indent=2, default=str))
        return

    rows = [analyze_fill(f, pmus_snaps, kalshi_evs) for f in fills]

    def aggregate(sel):
        ok = [r for r in sel if r.get("status") == "ok"]
        gaps = [r["gap_ticks"] for r in ok if r["gap_ticks"] is not None]
        g2 = [r for r in ok if (r["gap_ticks"] or 0) >= 2 or r["gap_ticks"] is None]
        # gap None = nothing beyond us at all: could have priced better but
        # unbounded; excluded from cents sums, counted in pct_gap_ge2.
        money = sum((r["gap_ticks"] - 1) * TICK * r["qty"] for r in ok
                    if r["gap_ticks"] is not None and r["gap_ticks"] >= 2)
        money_sw = sum((r["gap_ticks"] - 1) * TICK * r["qty"] for r in ok
                       if r["gap_ticks"] is not None and r["gap_ticks"] >= 2
                       and r["sweep_evidence"] is True)
        sweep_t = [r for r in ok if r["sweep_evidence"] is True]
        sweep_f = [r for r in ok if r["sweep_evidence"] is False]
        ot_t = [r for r in ok if r.get("one_tick_evidence") is True]
        usd_one_tick = sum(TICK * r["qty"] for r in ot_t)
        gaps_s = sorted(gaps)
        return {
            "n_fills": len(sel),
            "n_ok": len(ok),
            "n_no_data": sum(1 for r in sel if r.get("status") == "no_data"),
            "median_gap_ticks": gaps_s[len(gaps_s) // 2] if gaps_s else None,
            "mean_gap_ticks": round(sum(gaps) / len(gaps), 2) if gaps else None,
            "pct_gap_ge2": round(100 * len(g2) / len(ok), 1) if ok else None,
            "n_gap_unbounded": sum(1 for r in ok if r["gap_ticks"] is None),
            "pct_sweep_evidence": round(100 * len(sweep_t) / len(ok), 1) if ok else None,
            "n_sweep_true": len(sweep_t),
            "n_sweep_false": len(sweep_f),
            "n_sweep_unknown": len(ok) - len(sweep_t) - len(sweep_f),
            "pct_one_tick_evidence": round(100 * len(ot_t) / len(ok), 1) if ok else None,
            "n_one_tick_evidence": len(ot_t),
            "usd_left_by_construction": round(money, 4),
            "usd_left_with_sweep_evidence": round(money_sw, 4),
            "usd_one_tick_where_evidence": round(usd_one_tick, 4),
        }

    mt = [r for r in rows if r["strategy"] == "make-take"]
    probe = [r for r in rows if r["strategy"] == "pmus-maker-probe"]
    report = {
        "generated": iso(dt.datetime.now(dt.timezone.utc).timestamp()),
        "params": {"tick": TICK, "dust_lt_contracts": DUST, "pre_s": PRE_S,
                   "post_s": POST_S, "lookback_s": LOOKBACK_S},
        "caveats": [
            "gap_ticks=0 means real (non-dust) size shared our price level: we "
            "were joining a queue, not alone at the edge; no improvement possible.",
            "polymarket_us has no recorded trade tape (snapshot polls ~2-30s): "
            "level consumption there cannot distinguish trades from cancels, so "
            "sweep_evidence on PM US is inferential; kalshi has printed trades.",
            "ledger ts is fill DETECTION time; the pre-book is anchored at the "
            "last state (<= ts-2s, within 120s) where our resting level was "
            "still visible, and the sweep window starts there.",
            "pmus-maker-probe fills are in markets the recorder did not cover "
            "(no_data).",
        ],
        "aggregate": {
            "make_take": aggregate(mt),
            "make_take_kalshi": aggregate([r for r in mt if r["venue"] == "kalshi"]),
            "make_take_pmus": aggregate([r for r in mt if r["venue"] == "polymarket_us"]),
            "pmus_maker_probe": aggregate(probe),
        },
        "fills": rows,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report["aggregate"], indent=2))
    print(f"wrote {REPORT}")


if __name__ == "__main__":
    main()
