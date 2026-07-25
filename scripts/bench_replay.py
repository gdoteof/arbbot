#!/usr/bin/env python3
"""Recorder ingestion benchmark — the Python half of the P1 recorder gate.

Measures the per-event cost of the recorder/consumer hot path over a real
tape slice, stage by stage (readline, json.loads, parse_event/pydantic,
BookBuilder apply), plus the tape's burst profile (peak events per 1s and
100ms window by ts_local_ns). The P1 gate compares the Rust recorder's same
numbers over the same slice: sustained events/sec without falling behind is
the gap-frequency lever (the burst that fills you is the burst that gaps
your book — 2026-07-23 probe postmortem).

  python scripts/bench_replay.py --tape SLICE.jsonl [--max-events N]
"""

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot
from arbbot.record.jsonl import parse_event


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape", required=True, type=Path)
    ap.add_argument("--max-events", type=int, default=0)
    args = ap.parse_args()

    lines = []
    t0 = time.perf_counter()
    with args.tape.open() as f:
        for ln in f:
            lines.append(ln)
            if args.max_events and len(lines) >= args.max_events:
                break
    t_read = time.perf_counter() - t0
    n = len(lines)

    t0 = time.perf_counter()
    raws = [json.loads(ln) for ln in lines]
    t_json = time.perf_counter() - t0

    t0 = time.perf_counter()
    events = []
    for r in raws:
        try:
            events.append(parse_event(r))
        except ValueError:
            events.append(None)
    t_parse = time.perf_counter() - t0

    books = BookBuilder()
    kinds = Counter()
    gaps = 0
    t0 = time.perf_counter()
    for ev in events:
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
            kinds["snapshot"] += 1
        elif isinstance(ev, BookDelta):
            kinds["delta"] += 1
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                gaps += 1
        else:
            kinds["other"] += 1
    t_book = time.perf_counter() - t0

    # burst profile from recorded arrival times
    ts = sorted(r["ts_local_ns"] for r in raws if "ts_local_ns" in r)
    def peak(window_ns):
        best = lo = 0
        for hi in range(len(ts)):
            while ts[hi] - ts[lo] > window_ns:
                lo += 1
            best = max(best, hi - lo + 1)
        return best
    peak_1s, peak_100ms = peak(1_000_000_000), peak(100_000_000)

    t_total = t_json + t_parse + t_book
    us = lambda t: t / n * 1e6
    print(json.dumps({
        "tape": str(args.tape), "events": n, "kinds": dict(kinds),
        "gap_or_notsynced": gaps,
        "stage_us_per_event": {
            "readline": round(us(t_read), 2), "json_loads": round(us(t_json), 2),
            "parse_event_pydantic": round(us(t_parse), 2),
            "book_apply": round(us(t_book), 2),
            "total_hot_path": round(us(t_total), 2)},
        "throughput_eps": {
            "hot_path": int(n / t_total),
            "json_only": int(n / t_json)},
        "tape_burst_profile": {
            "span_s": round((ts[-1] - ts[0]) / 1e9, 1) if ts else 0,
            "mean_eps": int(len(ts) / max((ts[-1] - ts[0]) / 1e9, 1e-9)) if ts else 0,
            "peak_eps_1s_window": peak_1s,
            "peak_eps_100ms_window": peak_100ms * 10},
        "headroom_at_peak": round((n / t_total) / max(peak_1s, 1), 1),
    }, indent=1))


if __name__ == "__main__":
    main()
