"""Complete a stranded Kalshi close leg from a recorded unwind (one-off).

2026-07-23 recon page: the 13:50:50 fedcut displacement unwind recorded both
legs closed, but the Kalshi sell never filled — 4 YES stranded on
KXRATECUT-26DEC31 while the ledger booked proceeds at 0.1930. This sells the
stranded qty IOC and appends a correction record adjusting the unwind's
proceeds/realized to the actual fill (ledger stays append-only + auditable).

Dry-run by default; --live to execute.
"""

import argparse
import json
import pathlib
import time
from decimal import Decimal

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.record.kalshi import load_private_key

D = pathlib.Path.home() / ".arbbot-credentials"
LEDGER = pathlib.Path("data/exec/trades.jsonl")
TICKER = "KXRATECUT-26DEC31"
QTY = 4
RECORDED_PX = Decimal("0.1930")   # what the unwind record booked
LIMIT = Decimal("0.17")           # floor: current bid 0.175, one level of room


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    recs = [json.loads(l) for l in LEDGER.read_text().splitlines() if l.strip()]
    target = next(r for r in recs if r.get("status") == "unwound"
                  and "fedcut" in str(r.get("relationship_id"))
                  and r.get("qty") == 4
                  and time.strftime("%H:%M:%S", time.localtime(r["ts"])) == "13:50:50")
    print(f"target unwind ts={target['ts']} recorded realized ${target['realized_pnl_usd']:.2f}")
    print(f"sell {QTY} YES {TICKER} IOC limit {LIMIT} (booked at {RECORDED_PX})")
    if not args.live:
        print("dry-run — no orders placed")
        return

    kid = (D / "kalshi_api_key_id").read_text().strip()
    kkey = load_private_key((D / "kalshi_private_key.pem").read_bytes())
    gw = KalshiOrderGateway(kid, kkey, live=True)
    r = gw.place_yes(TICKER, "ask", LIMIT, QTY, post_only=False)
    oid = (r.get("order") or {}).get("order_id") or r.get("order_id")
    filled = 0
    for _ in range(8):   # confirmed fill only (fill-lag LAW)
        try:
            filled = gw.filled_qty(oid)
        except Exception:
            pass
        if filled >= QTY:
            break
        time.sleep(0.5)
    print(f"filled {filled}/{QTY}")
    if filled < 1:
        print("unfilled — book moved; rerun later")
        return
    px = Decimal("0.175")  # conservative: the bid we crossed
    delta = (px - RECORDED_PX) * filled
    corr = {"ts": time.time(), "relationship_id": target["relationship_id"],
            "status": "correction", "corrects_ts": target["ts"],
            "fields": {
                "proceeds_usd": float(Decimal(str(target["proceeds_usd"])) + delta),
                "realized_pnl_usd": float(Decimal(str(target["realized_pnl_usd"])) + delta)},
            "reason": f"Kalshi close leg never filled (4 YES stranded, recon page); "
                      f"completed x{filled} @ ~{px} vs booked {RECORDED_PX} "
                      f"(delta ${float(delta):.3f})"}
    with open(LEDGER, "a") as f:
        f.write(json.dumps(corr) + "\n")
    print("correction appended:", json.dumps(corr["fields"]))


if __name__ == "__main__":
    main()
