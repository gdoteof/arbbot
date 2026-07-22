"""Watch for PM US to RESUME listing live sports. Cross-venue sports arb is the
best-shaped opportunity (clean 'team X wins' equivalence + games settle in hours
= fast capital recycle), but it's gated on PM US actually running a live sports
book concurrent with Kalshi. As of 2026-07-21 PM US sports is dormant (all
listings archived). This prints SPORTS-LIVE only when PM US has near-term active
games — the trigger to build the cross-venue matcher + add pairs. Read-only.
"""

import datetime

import httpx

SPORTS = ["mlb", "wnba", "soccer", "tennis", "nba", "nfl", "nhl"]
HORIZON_DAYS = 10  # "near-term" = a real upcoming game, not a far-future stub


def main():
    c = httpx.Client(timeout=20)
    today = datetime.date.today()
    horizon = (today + datetime.timedelta(days=HORIZON_DAYS)).isoformat()
    live = {}
    for tag in SPORTS:
        try:
            j = c.get(f"https://gateway.polymarket.us/v1/events?tag_slug={tag}", timeout=15).json()
        except Exception:
            continue
        evs = []

        def walk(d):
            if isinstance(d, dict):
                if d.get("slug") and d.get("active") is not None:
                    evs.append(d)
                for v in d.values():
                    walk(v)
            elif isinstance(d, list):
                for x in d:
                    walk(x)
        walk(j)
        near = [g for g in evs if g.get("active") and not g.get("closed") and not g.get("ended")
                and today.isoformat() <= g.get("startDate", "")[:10] <= horizon]
        if near:
            live[tag] = len(near)
    if live:
        print(f"SPORTS-LIVE on PM US: {live} — cross-venue sports arb window OPEN; build the matcher")


if __name__ == "__main__":
    main()
