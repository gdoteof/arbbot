"""Partition (dutch-book) arb scanner. A multi-candidate market where exactly
one wins is a probability partition: true P's sum to 1. Two riskless books:

  BUY-ALL-NO (sell-all-YES): edge = Sum(yes_bid) - 1 - fees.  ROBUST — pays >= N-1
    even if the listed field is INCOMPLETE (an unlisted winner only means MORE
    NOs pay). This is the safe, auto-executable book.
  BUY-ALL-YES:              edge = 1 - Sum(yes_ask) - fees.  FRAGILE — only riskless
    if the partition is EXHAUSTIVE (an unlisted winner => all YES lose). Reported
    for information; NOT safe to auto-execute without a completeness guarantee.

Read-only dry scan. Sums each venue's partition independently (intra-venue book).
"""

import argparse
import math
import time
from decimal import Decimal

import httpx

from arbbot.record.kalshi import REST_BASE

# partition markets to scan: (label, kalshi series_ticker, pm_us tag_slug + slug prefix)
PARTITIONS = [
    ("TIME Person of the Year 2026", "KXTIME", "culture", "tpoyc-2026"),
]


def kalshi_fee(p):  # taker: ceil(0.07 * p*(1-p)) cents/contract
    return math.ceil(0.07 * float(p) * (1 - float(p)) * 100) / 100.0


def pm_fee(p):      # ~0.6c/contract observed
    return 0.006


def scan_kalshi(c, series):
    ms = c.get(f"{REST_BASE}/markets", params={"series_ticker": series, "status": "open", "limit": 300}).json().get("markets", [])
    rows = []
    for m in ms:
        b, a = m.get("yes_bid_dollars"), m.get("yes_ask_dollars")
        if b is None or a is None:
            continue
        rows.append((m["ticker"], float(b), float(a),
                     float(m.get("yes_bid_size_fp") or 0), float(m.get("yes_ask_size_fp") or 0)))
    return rows


def scan_pmus(c, tag, prefix):
    j = c.get(f"https://gateway.polymarket.us/v1/events?tag_slug={tag}", timeout=25).json()
    slugs = set()

    def walk(d):
        if isinstance(d, dict):
            if str(d.get("slug", "")).startswith(prefix):
                slugs.add(d["slug"])
            for v in d.values():
                walk(v)
        elif isinstance(d, list):
            for x in d:
                walk(x)
    walk(j)
    rows = []
    for s in sorted(slugs):
        try:
            b = c.get(f"https://gateway.polymarket.us/v1/markets/{s}/bbo", timeout=15).json().get("marketData", {})
        except Exception:
            continue
        bid, ask = (b.get("bestBid") or {}).get("value"), (b.get("bestAsk") or {}).get("value")
        if bid is None or ask is None:
            continue
        rows.append((s, float(bid), float(ask), 0.0, 0.0))
        time.sleep(0.08)
    return rows


def report(label, venue, rows, fee_fn, years, mutex):
    if not rows:
        print(f"  {venue}: no candidates"); return
    n = len(rows)
    sbid = sum(r[1] for r in rows)
    sask = sum(r[2] for r in rows)
    fee_no = sum(fee_fn(1 - r[1]) for r in rows)
    # BUY-ALL-NO (robust): profit = Sum(bid)-1-fees, but CAPITAL = Sum(1-bid) = cost of
    # all N NOs (this is the real cost — the overround is NOT free money).
    profit_no = sbid - 1 - fee_no
    cap_no = n - sbid
    roc_no = (profit_no / cap_no * 100) if cap_no > 0 else 0
    apr_no = (roc_no / years) if years else 0
    print(f"  {venue}: {n} candidates  Sum(bid)={sbid:.3f} Sum(ask)={sask:.3f}  mutually_exclusive={mutex}")
    tag = "  <<< beats 13% bar" if apr_no > 13 else "  (below bar)"
    print(f"     buy-all-NO: profit=${profit_no:.2f} on ${cap_no:.2f} capital = {roc_no:.1f}% RoC "
          f"-> {apr_no:.1f}%/yr{tag if profit_no>0 else '  (no overround)'}")
    print(f"     (safe only if <=1 candidate wins; capital-heavy — {cap_no:.0f}x the profit)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="(reserved) execute robust buy-all-NO books")
    args = ap.parse_args()
    import datetime
    c = httpx.Client(timeout=25)
    for label, kseries, pmtag, pmprefix in PARTITIONS:
        print(f"\n=== {label} ===")
        ev = c.get(f"{REST_BASE}/events/{kseries}-26").json().get("event", {})
        mutex = ev.get("mutually_exclusive")
        yrs = max((datetime.date(2026, 12, 9) - datetime.date.today()).days, 1) / 365.25  # TIME PotY ~2nd wk Dec
        report(label, "Kalshi", scan_kalshi(c, kseries), kalshi_fee, yrs, mutex)
        report(label, "PM US ", scan_pmus(c, pmtag, pmprefix), pm_fee, yrs, "?")


if __name__ == "__main__":
    main()
