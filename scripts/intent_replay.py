#!/usr/bin/env python3
"""Intent-parity harness — the Python reference side of the quoter port gate.

The golden digest gate (golden_scan.py) covers the scanner only. This harness
replays a merged tape (the export_merged_tape.py JSONL stream) through the
PYTHON Quoter deterministically and emits its decision stream — every
place/replace/cancel intent — as canonical JSON plus a SHA256 digest. A Rust
quoter port must reproduce the stream byte-for-byte over the same tape before
it can touch money paths: _target, self-exclusion, hysteresis, the
min_requote_s throttle, and the hedge-depth guard are all exercised.

Determinism seams (no src/ change — patched here):
  * arbbot.exec.quoter.time -> a tape clock: time() and monotonic() both
    return the current event's ts_local_ns/1e9, so the requote throttle runs
    on tape time and intent "ts" fields are reproducible.
  * gateways -> one MockGateway shared by every quoter, so order ids
    m1, m2, ... are globally sequential in processing order.
  * risk -> an inert RiskManager (huge caps and balances, kill file at a
    nonexistent path). The run FAILS if any risk skip appears — the gate
    must stay inert, not exercised, in this harness.
  * All jitter params are left at 0, so the Quoter makes no random calls.

  python scripts/intent_replay.py --tape merged-2026-07-20.jsonl \
      --registry config/registry.yaml --out intents.jsonl
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import arbbot.exec.quoter as quoter_module
from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.quoter import Quoter
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookDelta, BookSnapshot, Market, Venue
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry

BIG = Decimal("1000000000")


class TapeClock:
    """Stand-in for the quoter's `time` module: both clocks read tape time."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


class MockGateway:
    """Order ids globally sequential in processing order; never fails,
    never touches a venue. One instance is shared across ALL quoters."""

    def __init__(self):
        self.placed = 0

    def place_yes(self, market_id, side, price, count, post_only=True):
        self.placed += 1
        return {"order_id": f"m{self.placed}"}

    def cancel(self, order_id, market_slug=None):
        return None


def inert_risk():
    """A RiskManager that allows everything: the harness pins quoter logic,
    not the risk gate (which has its own tests)."""
    from arbbot.risk.manager import RiskConfig, RiskManager

    cfg = RiskConfig(
        bankroll=BIG,
        per_rel_cap=BIG,
        per_class_cap=Decimal("1"),
        global_cap=Decimal("1"),
        kill_switch_file="/nonexistent/arbbot-intent-replay-KILL",
    )
    m = RiskManager(config=cfg)
    m.balances = {v: BIG for v in Venue}
    return m


def build_quoters(rels, min_apr=0.0, resolve_years=None, suppress=frozenset()):
    """One Quoter per relationship (clip=5, all jitter off => deterministic),
    indexed by every (venue, market_id) its legs reference — the same
    by_market convention as golden_scan.py, DEFAULT-constructed Markets."""
    markets = {}
    for r in rels:
        for leg in r.legs:
            markets.setdefault((leg.venue, leg.market_id),
                               Market(venue=leg.venue, market_id=leg.market_id))
    gw = MockGateway()
    gateways = {Venue.KALSHI: gw, Venue.POLYMARKET: gw, Venue.POLYMARKET_US: gw}
    risk = inert_risk()
    by_market = defaultdict(list)
    for r in rels:
        q = Quoter(rel=r, gateways=gateways, risk=risk, markets=markets,
                   fees=FeeSchedule(), clip=5,
                   min_apr=min_apr, resolve_years=resolve_years,
                   suppress=set(suppress))
        for leg in r.legs:
            by_market[(leg.venue.value, leg.market_id)].append(q)
    return by_market


def replay(raw_events, rels, out_path, max_events: int = 0,
           min_apr=0.0, resolve_years=None, suppress=frozenset()) -> dict:
    """Drive the tape through the quoters; write canonical intent JSONL and
    return {events, book_events, intents, sha256}. Event handling mirrors
    golden_scan.py exactly (snapshot/delta apply, gaps skip, trades skip)."""
    by_market = build_quoters(rels, min_apr, resolve_years, suppress)
    books = BookBuilder()
    clock = TapeClock()
    digest = hashlib.sha256()
    n_ev = n_book = n_int = 0
    skips = []
    saved_time = quoter_module.time
    quoter_module.time = clock
    out = open(out_path, "w") if out_path else None
    try:
        for raw in raw_events:
            n_ev += 1
            if max_events and n_ev > max_events:
                break
            try:
                ev = parse_event(raw)
            except ValueError:
                continue
            if isinstance(ev, BookSnapshot):
                books.apply_snapshot(ev)
            elif isinstance(ev, BookDelta):
                try:
                    books.apply_delta(ev)
                except (GapDetected, NotSynced):
                    continue
            else:
                continue
            n_book += 1
            clock.now = ev.ts_local_ns / 1e9
            for q in by_market.get((ev.venue.value, ev.market_id), ()):
                q.on_book(books)
                for intent in q.intents:  # drain in production order
                    # toxgate skips are part of the decision stream (parity-
                    # visible); anything else firing means the risk gate was
                    # not inert — still a harness failure.
                    if "skip" in intent and not all(
                            str(r).startswith("toxgate ") for r in intent["skip"]):
                        skips.append(intent)
                    line = json.dumps(intent, sort_keys=True, separators=(",", ":"))
                    digest.update(line.encode() + b"\n")
                    n_int += 1
                    if out:
                        out.write(line + "\n")
                q.intents.clear()
    finally:
        quoter_module.time = saved_time
        if out:
            out.close()
    if skips:
        raise RuntimeError(
            f"risk gate fired {len(skips)}x — it must stay inert in this "
            f"harness (first reasons: {[s.get('skip') for s in skips[:5]]})")
    return {"events": n_ev, "book_events": n_book, "intents": n_int,
            "sha256": digest.hexdigest()}


def iter_tape(path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape", required=True,
                    help="merged tape JSONL (scripts/export_merged_tape.py)")
    ap.add_argument("--registry", default="config/registry.yaml")
    ap.add_argument("--out", default="intents.jsonl",
                    help="canonical intent stream JSONL")
    ap.add_argument("--max-events", type=int, default=0,
                    help="stop after N tape events (quick runs)")
    ap.add_argument("--min-apr", type=float, default=0.0,
                    help="APR hurdle %%/yr for every quoter (0 = off)")
    ap.add_argument("--resolve-years", type=float, default=None,
                    help="time-to-resolution used by the APR hurdle")
    ap.add_argument("--toxgate-file", default=None,
                    help="toxgate JSON fixture (patches quoter TOXGATE_FILE)")
    ap.add_argument("--suppress", action="append", default=[],
                    help="market_id:side owned elsewhere (repeatable)")
    args = ap.parse_args()
    if args.toxgate_file:
        quoter_module.TOXGATE_FILE = Path(args.toxgate_file)
    suppress = set()
    for s in args.suppress:
        m, _, side = s.rpartition(":")
        assert side in ("bid", "ask"), f"bad --suppress {s}"
        suppress.add((m, side))
    reg = Registry.load(args.registry)
    rels = [r for r in reg.relationships
            if len(r.legs) == 2 and r.verdict.value != "rejected"]
    res = replay(iter_tape(args.tape), rels, args.out, args.max_events,
                 args.min_apr, args.resolve_years, suppress)
    res["out"] = args.out
    print(json.dumps(res))


if __name__ == "__main__":
    main()
