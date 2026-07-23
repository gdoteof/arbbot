"""One-off cleanup under approved board card 6b54d1ce (joint recon):
buy back the maker probe's own leftover -5 short on
aec-itfme-vindul-rapper-2026-07-23 (probe's orphaned fill, per joint recon
trading engines were never involved). IOC limit 2c through the ask.
Read-only if the market is closed/settled. Appends honest ledger records.
"""

import json
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "src")
import httpx

from arbbot.ops.config import load_credential
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway

SLUG = "aec-itfme-vindul-rapper-2026-07-23"
QTY = 5
LEDGER = Path("data/exec/trades.jsonl")


def main() -> None:
    gw = PolymarketUsOrderGateway(
        load_credential("polymarket_usa_key_id").decode().strip(),
        load_credential("polymarket_usa_private_key").decode().strip(), live=True)
    pth = "/v1/portfolio/positions"
    p = gw.client.get(gw.base + pth, headers=gw._headers("GET", pth)) \
          .json().get("positions", {}).get(SLUG)
    net = int(float(p.get("netPosition", 0))) if p else 0
    print("current net:", net)
    if net >= 0:
        print("nothing to buy back")
        return
    bbo = httpx.get(f"https://gateway.polymarket.us/v1/markets/{SLUG}/bbo",
                    timeout=15).json()
    ask = None
    def walk(d):
        nonlocal ask
        if isinstance(d, dict):
            for k, v in d.items():
                if k.lower() in ("ask", "bestask", "offer"):
                    ask = v
                walk(v)
        elif isinstance(d, list):
            for x in d:
                walk(x)
    walk(bbo)
    if not isinstance(ask, dict):
        print("no ask (market closed?) — will settle at resolution; ledgering open")
        return
    px = min(0.99, round(float(ask["value"]) + 0.02, 2))
    r = gw.place_yes(SLUG, "bid", Decimal(str(px)), -net, post_only=False)
    oid = r.get("id") or r.get("order_id")
    time.sleep(2)
    o = gw.get_order(oid)
    cum = int(float(o.get("cumQuantity", 0) or 0))
    print(f"buyback {oid}: {cum}/{-net} @ <= {px}")
    if cum:
        # avg short entry from venue: qtySold vs cost is aggregate; use the
        # 0.768 avg implied by cost 3.84/5 shorts (joint-recon numbers)
        entry = 0.768
        pnl = round((entry - px) * cum - 0.06 * px * (1 - px) * cum, 4)
        now = time.time()
        with LEDGER.open("a") as f:
            f.write(json.dumps({
                "ts": now, "relationship_id": f"pmm-{SLUG}",
                "title": "maker probe orphaned short (retro, card 6b54d1ce)",
                "qty": cum, "strategy": "pmus-maker-probe",
                "legs": [{"venue": "polymarket_us", "market_id": SLUG,
                          "side": "no", "role": "maker", "qty": cum,
                          "yes_price": str(entry)}],
                "cost_usd": round((1 - entry) * cum, 4), "payoff_usd": cum,
                "profit_usd": None, "status": "open",
                "note": "orphaned fill from leaked order (incident #2)"}) + "\n")
            f.write(json.dumps({
                "ts": now + 0.001, "relationship_id": f"pmm-{SLUG}",
                "strategy": "pmus-maker-probe", "status": "unwound",
                "closes_ts": now, "qty": cum,
                "realized_pnl_usd": pnl,
                "note": f"buyback IOC {oid} @ <= {px} (card 6b54d1ce)"}) + "\n")
        print("ledgered, approx realized:", pnl)


if __name__ == "__main__":
    main()
