"""Snapshot Polymarket liquidity-reward configs for our universe into
data/scan/rewards.json (dashboard reads it; refreshed by the daily report
timer). Rewards docs: score ~ ((v-s)/v)^2 * size, two-sided, paid daily."""

import asyncio
import json
from datetime import datetime, timezone

import httpx

from arbbot.models.core import Venue
from arbbot.registry.model import Registry


async def main() -> None:
    reg = Registry.load("config/registry.yaml")
    tokens = sorted(m for v, m in reg.market_ids() if v is Venue.POLYMARKET)
    out = []
    async with httpx.AsyncClient(timeout=20) as c:
        # token -> condition id + title via gamma
        cond = {}
        for i in range(0, len(tokens), 20):
            r = await c.get("https://gamma-api.polymarket.com/markets",
                            params=[("clob_token_ids", t) for t in tokens[i:i+20]])
            for m in r.json():
                try:
                    toks = json.loads(m["clobTokenIds"])
                    cond[m["conditionId"]] = (m.get("question", ""), toks[0])
                except Exception:
                    pass
        for cid, (title, tok) in cond.items():
            try:
                r = await c.get(f"https://clob.polymarket.com/markets/{cid}")
                m = r.json()
                rw = m.get("rewards") or {}
                rates = rw.get("rates") or []
                daily = sum(float(x.get("rewards_daily_rate", 0)) for x in rates)
                if daily > 0:
                    out.append({
                        "title": title, "token": tok,
                        "daily_pool_usd": daily,
                        "min_size": rw.get("min_size"),
                        "max_spread_c": rw.get("max_spread"),
                        "taker_fee_free": int(m.get("taker_base_fee", -1)) == 0,
                    })
            except Exception:
                continue
    out.sort(key=lambda x: -x["daily_pool_usd"])
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "markets_with_pools": len(out), "universe_pm_markets": len(tokens),
           "pools": out}
    with open("data/scan/rewards.json", "w") as f:
        json.dump(doc, f, indent=2)
    print(f"{len(out)}/{len(tokens)} universe markets carry reward pools; "
          f"top: {[(p['title'][:40], p['daily_pool_usd']) for p in out[:5]]}")


if __name__ == "__main__":
    asyncio.run(main())
