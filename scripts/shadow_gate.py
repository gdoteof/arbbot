#!/usr/bin/env python3
"""P1 shadow gate: compare the Rust recorder's tape against the Python
recorder's tape for one day.

Checks (per plan Stage/P1 gate):
  (a) parse-compat — Python parse_event must parse 100% of Rust lines;
  (b) volume — per-venue event counts and byte sizes side by side;
  (c) book health — replay each tape through BookBuilder; count gaps and
      report per-venue snapshot/delta/trade mix;
  (d) TOB agreement — sample each market's best bid/ask every --sample-s
      seconds on both replays; report how often they agree within
      --tolerance (loose for poll-mode shadows, tight for WS parity).

  python scripts/shadow_gate.py --day 2026-07-24 \
      --py-dir data/raw --rs-dir data/raw-rs [--sample-s 60] [--tolerance 0.01]
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot
from arbbot.record.jsonl import parse_event

VENUES = ("kalshi", "polymarket", "polymarket_us")


def replay(path: Path, sample_ns: int):
    """-> (stats dict, {ts_bucket: {market: (bid, ask)}})"""
    stats = Counter()
    samples: dict[int, dict[str, tuple]] = defaultdict(dict)
    books = BookBuilder()
    unparsed = 0
    lines = path.read_text().splitlines() if path.exists() else []
    for i, ln in enumerate(lines):
        if not ln.strip():
            continue
        try:
            ev = parse_event(json.loads(ln))
        except (ValueError, KeyError) as e:
            if i == len(lines) - 1:
                continue  # torn final line
            unparsed += 1
            if unparsed <= 3:
                print(f"  UNPARSED {path.name}:{i + 1}: {e}")
            continue
        stats[ev.kind] += 1
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                stats["gaps"] += 1
                continue
        else:
            continue
        b = books.get(ev.venue.value, ev.market_id)
        if b and b.bids and b.asks:
            bucket = ev.ts_local_ns // sample_ns
            samples[bucket][ev.market_id] = (b.bids[0].price, b.asks[0].price)
    stats["unparsed"] = unparsed
    stats["lines"] = len(lines)
    return stats, samples


def tob_agreement(py_s, rs_s, tolerance: Decimal):
    """Carry-forward align the two sample streams; compare closest buckets."""
    agree = differ = 0
    worst: list[tuple] = []
    last_rs: dict[str, tuple] = {}
    for bucket in sorted(set(py_s) | set(rs_s)):
        for mkt, tob in rs_s.get(bucket, {}).items():
            last_rs[mkt] = tob
        for mkt, (pb, pa) in py_s.get(bucket, {}).items():
            if mkt not in last_rs:
                continue
            rb, ra = last_rs[mkt]
            db, da = abs(pb - rb), abs(pa - ra)
            if db <= tolerance and da <= tolerance:
                agree += 1
            else:
                differ += 1
                worst.append((float(max(db, da)), mkt, bucket))
    worst.sort(reverse=True)
    return agree, differ, worst[:5]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--py-dir", type=Path, default=Path("data/raw"))
    ap.add_argument("--rs-dir", type=Path, default=Path("data/raw-rs"))
    ap.add_argument("--sample-s", type=int, default=60)
    ap.add_argument("--tolerance", type=Decimal, default=Decimal("0.01"))
    args = ap.parse_args()

    sample_ns = args.sample_s * 1_000_000_000
    ok = True
    for venue in VENUES:
        py_path = args.py_dir / f"{venue}-{args.day}.jsonl"
        rs_path = args.rs_dir / f"{venue}-{args.day}.jsonl"
        if not py_path.exists() and not rs_path.exists():
            continue
        print(f"== {venue}")
        py_stats, py_samples = replay(py_path, sample_ns)
        rs_stats, rs_samples = replay(rs_path, sample_ns)
        for label, st in (("python", py_stats), ("rust", rs_stats)):
            print(f"  {label:7s} lines={st['lines']:>9} snap={st['snapshot']:>8} "
                  f"delta={st['delta']:>9} trade={st['trade']:>7} "
                  f"gaps={st['gaps']:>4} unparsed={st['unparsed']}")
        if rs_stats["unparsed"]:
            print(f"  FAIL: {rs_stats['unparsed']} Rust lines unparseable by Python")
            ok = False
        if rs_stats["lines"]:
            agree, differ, worst = tob_agreement(py_samples, rs_samples, args.tolerance)
            total = agree + differ
            pct = 100.0 * agree / total if total else 0.0
            print(f"  TOB agreement (tol {args.tolerance}): {agree}/{total} = {pct:.1f}%")
            for w, mkt, bucket in worst:
                print(f"    worst: {mkt} bucket={bucket} diff={w:.4f}")
    print("RESULT:", "PASS" if ok else "FAIL",
          "(volume/TOB numbers are judged per shadow mode — poll-mode lags are expected)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
