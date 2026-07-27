"""One-off: offset the +2 residual on aec-itfme-alvtud-patalv-2026-07-23
(pair-window vs reconciler race, 2026-07-23) and ledger the whole episode
from venue order truth. BUY_SHORT 2 IOC at bid-2c pad.
"""

import json
import sys
import time
from decimal import Decimal

sys.path.insert(0, "src")
import httpx

from arbbot.ops.config import load_credential
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway

SLUG = "aec-itfme-alvtud-patalv-2026-07-23"


def main() -> None:
    gw = PolymarketUsOrderGateway(
        load_credential("polymarket_usa_key_id").decode().strip(),
        load_credential("polymarket_usa_private_key").decode().strip(), live=True)
    # exact fills for the episode
    oids = set()
    for line in open("data/exec/pmus_maker_probe.jsonl"):
        d = json.loads(line)
        if d.get("pm") == SLUG and d.get("order_id"):
            oids.add(d["order_id"])
        if d.get("pm") == SLUG and d.get("unwind_order"):
            oids.add(d["unwind_order"])
    rows = []
    for oid in oids:
        try:
            o = gw.get_order(oid)
            cum = int(float(o.get("cumQuantity", 0) or 0))
            if cum:
                px = o.get("price", {}).get("value") if isinstance(o.get("price"), dict) else o.get("price")
                rows.append((o.get("createTime", "?"), o.get("intent"), cum, float(px)))
        except Exception:
            pass
    for r in sorted(rows):
        print(r)
    bbo = httpx.get(f"https://gateway.polymarket.us/v1/markets/{SLUG}/bbo",
                    timeout=15).json()
    bid = None
    def walk(d):
        nonlocal bid
        if isinstance(d, dict):
            for k, v in d.items():
                if k.lower() in ("bid", "bestbid"):
                    bid = v
                walk(v)
        elif isinstance(d, list):
            for x in d:
                walk(x)
    walk(bbo)
    if not isinstance(bid, dict):
        print("no bid — cannot offset now; position holds to resolution")
        return
    px = max(0.01, round(float(bid["value"]) - 0.02, 2))
    r = gw.place_short(SLUG, Decimal(str(px)), 2, post_only=False)
    oid = r.get("id") or r.get("order_id")
    time.sleep(2)
    o = gw.get_order(oid)
    cum = int(float(o.get("cumQuantity", 0) or 0))
    print(f"offset: {cum}/2 @ >= {px} ({oid})")
    cash = 0.0
    for _, intent, q, p in rows:
        cash += q * p if intent == "ORDER_INTENT_BUY_SHORT" else -q * p
    if cum:
        cash += cum * px
    fees = 0.06 * sum(q * p * (1 - p) for _, i, q, p in rows
                      if i == "ORDER_INTENT_BUY_LONG") + 0.06 * px * (1 - px) * cum
    now = time.time()
    with open("data/exec/trades.jsonl", "a") as f:
        f.write(json.dumps({
            "ts": now, "relationship_id": f"pmm-{SLUG}",
            "title": "maker probe pair-window/recon race (retro): Tudorica vs Alvarado",
            "qty": 5, "strategy": "pmus-maker-probe",
            "legs": [{"venue": "polymarket_us", "market_id": SLUG,
                      "side": "mixed", "role": "maker+taker", "qty": 5}],
            "cost_usd": None, "payoff_usd": None, "profit_usd": None,
            "status": "open",
            "note": "sell fill 5@0.60 -> recon raced pair window (flatten) + "
                    "expired-window unwind double-bought -> +2 residual offset"}) + "\n")
        f.write(json.dumps({
            "ts": now + 0.001, "relationship_id": f"pmm-{SLUG}",
            "strategy": "pmus-maker-probe", "status": "unwound",
            "closes_ts": now, "qty": 5,
            "realized_pnl_usd": round(cash - fees, 4),
            "note": f"venue-reconstructed cash {cash:+.3f} - fees {fees:.3f}"}) + "\n")
    print(f"ledgered realized: {cash - fees:+.4f}")


if __name__ == "__main__":
    main()
