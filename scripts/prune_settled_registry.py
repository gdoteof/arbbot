#!/usr/bin/env python3
"""Move settled relationships out of config/registry.yaml into config/registry-settled.yaml.

The recorder subscribes every market the registry names, at startup, and nothing
else ever removes one. A relationship moves only when ALL of these hold:

  * every leg is on kalshi or polymarket_us (other venues cannot be checked here);
  * every Kalshi leg is `finalized` (paid), or the venue no longer lists it
    (absent from /markets and /markets/{t} answers 404);
  * every PM-US leg is MARKET_STATUS_RESOLVED (/market/slug/{slug});
  * the ledger holds no open lot for it (corrections applied, unwinds netted);
  * the ledger has no entry for it in the last --keep-days (default 28, the
    arb-exitability window, which calibrates on our own lots).

Entries move as text, so every entry that stays is byte-identical. The result is
checked by re-parsing both files before anything is written.

Usage:
  python scripts/prune_settled_registry.py            # dry run: prints the plan
  python scripts/prune_settled_registry.py --write    # backs up, then writes

Read-only against both venues' public market endpoints. The trader and the
recorder read the registry at startup only, so a write takes effect on restart.
"""

import argparse
import json
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

import yaml

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
PMUS = "https://gateway.polymarket.us/v1"
ENTRY = re.compile(r"^- id: *(\S+)", re.M)


def get(url):
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                return 200, json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            return e.code, None
    raise AssertionError("unreachable")


def kalshi_settled(tickers):
    status = {}
    for i in range(0, len(tickers), 50):
        code, body = get(f"{KALSHI}/markets?" + urllib.parse.urlencode({"tickers": ",".join(tickers[i:i + 50])}))
        if code != 200:
            raise SystemExit(f"kalshi /markets answered {code}; nothing written")
        status.update((m["ticker"], m["status"]) for m in body["markets"])
        time.sleep(0.5)
    for t in tickers:
        if t not in status:
            code, _ = get(f"{KALSHI}/markets/{t}")
            status[t] = "purged" if code == 404 else f"unknown ({code})"
            time.sleep(0.5)
    return {t for t, s in status.items() if s in ("finalized", "purged")}


def pmus_resolved(slugs):
    out = set()
    for s in slugs:
        code, body = get(f"{PMUS}/market/slug/{s}")
        if code == 200 and body.get("market", {}).get("status") == "MARKET_STATUS_RESOLVED":
            out.add(s)
        time.sleep(0.5)
    return out


def ledger_holds(path, keep_days):
    """(ids with an open lot, ids with an entry in the last keep_days)."""
    recs = [json.loads(line) for line in open(path) if line.strip()]
    fixes = defaultdict(dict)
    for r in recs:
        if r.get("status") == "correction" and r.get("corrects_ts") is not None:
            fixes[(r.get("relationship_id"), r["corrects_ts"])].update(r.get("fields") or {})
    recs = [dict(r, **fixes.get((r.get("relationship_id"), r.get("ts")), {}))
            for r in recs if r.get("status") != "correction"]
    unwound = defaultdict(float)
    for r in recs:
        if r.get("status") == "unwound" and r.get("closes_ts") is not None:
            unwound[(r["relationship_id"], r["closes_ts"])] += float(r.get("qty") or 0)
    open_ids, recent_ids = set(), set()
    cutoff = time.time() - keep_days * 86400
    for r in recs:
        rid, ts = r.get("relationship_id"), r.get("ts")
        if r.get("status") != "open" or not rid or ts is None:
            continue
        if float(r.get("qty") or 0) - unwound[(rid, ts)] > 1e-9:
            open_ids.add(rid)
        if ts >= cutoff:
            recent_ids.add(rid)
    return open_ids, recent_ids


def split(text):
    """(header, [(id, block_text)]) with every entry's text exactly as written."""
    starts = [m.start() for m in ENTRY.finditer(text)]
    header = text[:starts[0]] if starts else text
    bounds = zip(starts, starts[1:] + [len(text)])
    return header, [(ENTRY.match(text, a).group(1), text[a:b]) for a, b in bounds]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="config/registry.yaml")
    ap.add_argument("--archive", default="config/registry-settled.yaml")
    ap.add_argument("--ledger", default="data/exec/trades.jsonl")
    ap.add_argument("--keep-days", type=int, default=28)
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()

    text = Path(a.registry).read_text()
    header, blocks = split(text)
    if header.strip() != "relationships:":
        raise SystemExit(f"unexpected registry header {header!r}; nothing written")
    rels = yaml.safe_load(text)["relationships"]
    if [r["id"] for r in rels] != [i for i, _ in blocks]:
        raise SystemExit("entry text and parsed ids disagree; nothing written")

    checkable = [r for r in rels if r.get("legs")
                 and all(l["venue"] in ("kalshi", "polymarket_us") for l in r["legs"])]
    tickers = sorted({l["market_id"] for r in checkable for l in r["legs"] if l["venue"] == "kalshi"})
    k_done = kalshi_settled(tickers)
    k_settled = [r for r in checkable
                 if all(l["market_id"] in k_done for l in r["legs"] if l["venue"] == "kalshi")]
    slugs = sorted({l["market_id"] for r in k_settled for l in r["legs"] if l["venue"] == "polymarket_us"})
    p_done = pmus_resolved(slugs)
    settled = [r["id"] for r in k_settled
               if all(l["market_id"] in p_done for l in r["legs"] if l["venue"] == "polymarket_us")]

    open_ids, recent_ids = ledger_holds(a.ledger, a.keep_days)
    held = [i for i in settled if i in open_ids]
    recent = [i for i in settled if i in recent_ids and i not in open_ids]
    move = set(settled) - open_ids - recent_ids

    print(f"registry {len(rels)} | settled on every leg {len(settled)} | "
          f"kept, open lot {len(held)} | kept, entry in last {a.keep_days}d {len(recent)} | move {len(move)}")
    for i in held:
        print(f"  kept (open lot): {i}")
    if not move:
        return

    kept_text = header + "".join(b for i, b in blocks if i not in move)
    archive = Path(a.archive)
    old_archive = archive.read_text() if archive.exists() else "relationships:\n"
    archive_text = old_archive + "".join(b for i, b in blocks if i in move)

    # Re-parse both before writing: every entry lands in exactly one file, unchanged.
    kept = yaml.safe_load(kept_text)["relationships"]
    moved = yaml.safe_load(archive_text)["relationships"]
    before = {r["id"]: r for r in rels}
    if [r["id"] for r in kept] != [r["id"] for r in rels if r["id"] not in move]:
        raise SystemExit("kept ids are not the registry minus the move set; nothing written")
    if any(r != before[r["id"]] for r in kept):
        raise SystemExit("a kept entry changed; nothing written")
    if sorted(r["id"] for r in moved[len(moved) - len(move):]) != sorted(move):
        raise SystemExit("archive tail is not the move set; nothing written")
    if any(r != before[r["id"]] for r in moved[len(moved) - len(move):]):
        raise SystemExit("an archived entry changed; nothing written")

    if not a.write:
        print("dry run: pass --write to back up and apply")
        return
    backup = f"{a.registry}.bak-{date.today():%Y%m%d}-settled"
    shutil.copy2(a.registry, backup)
    archive.write_text(archive_text)
    Path(a.registry).write_text(kept_text)
    print(f"backed up to {backup}; moved {len(move)} to {a.archive}")


if __name__ == "__main__":
    main()
