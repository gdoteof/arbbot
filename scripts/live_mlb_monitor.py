"""Live cross-venue MLB basis sampler. Sports are efficient AT REST, but an
in-game event (run scores, lead change) can move one venue before the other
catches up — a transient dislocation a fast bot could capture. This samples
today's matched games and prints a DISLOCATION line only when a LIQUID game
shows a real net-of-fee cross-venue edge. Silent when efficient. Read-only.

Run once per invocation (a Monitor loop calls it every few seconds).
"""

import datetime
import re

import httpx

from arbbot.record.kalshi import REST_BASE

MON = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
       "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
FEE = 0.02
MIN_EDGE = 0.02       # only flag meaningful dislocations (>=2c net); smaller ones close in seconds
MAX_SPREAD = 0.03     # only trust tight (liquid) books


def parse(ticker):
    rest = ticker[len("KXMLBGAME") + 1:]
    key, team = rest.rsplit("-", 1)
    m = re.match(r"(\d{2})([A-Z]{3})(\d{2})(\d{4})?(.+)", key)
    if not m:
        return None
    yy, mon, dd, _t, concat = m.groups()
    try:
        return datetime.date(2000 + int(yy), MON[mon], int(dd)).isoformat(), concat, team
    except (KeyError, ValueError):
        return None


def main():
    c = httpx.Client(timeout=15)
    today = datetime.date.today().isoformat()
    ms = c.get(f"{REST_BASE}/markets", params={"series_ticker": "KXMLBGAME", "status": "open", "limit": 400}).json().get("markets", [])
    games = {}
    for x in ms:
        p = parse(x["ticker"])
        if not p or p[0] != today:
            continue
        date, concat, team = p
        games.setdefault((date, concat), {}).update({team: (x.get("yes_bid_dollars"), x.get("yes_ask_dollars"))})
    worst = 0.0
    for (date, concat), sides in games.items():
        if len(sides) != 2:
            continue
        away = next((t for t in sides if concat.startswith(t)), None)
        home = next((t for t in sides if t != away), None)
        if not away or sides[away][0] is None:
            continue
        kb, ka = float(sides[away][0]), float(sides[away][1])
        try:
            r = c.get(f"https://gateway.polymarket.us/v1/markets/aec-mlb-{away.lower()}-{home.lower()}-{date}/bbo", timeout=8)
            if r.status_code != 200:
                continue
            md = r.json().get("marketData", {})
            pb, pa = (md.get("bestBid") or {}).get("value"), (md.get("bestAsk") or {}).get("value")
        except Exception:
            continue
        if not pb or not pa:
            continue
        pb, pa = float(pb), float(pa)
        edge = max(pb - ka, kb - pa) - FEE
        worst = max(worst, edge)
        if edge >= MIN_EDGE and (ka - kb) <= MAX_SPREAD and (pa - pb) <= MAX_SPREAD:
            side = "buy PM/sell K" if (kb - pa) >= (pb - ka) else "buy K/sell PM"
            print(f"DISLOCATION {away}@{home}: K={kb:.2f}/{ka:.2f} PM={pb:.2f}/{pa:.2f} edge={edge*100:+.1f}c ({side})")
    print(f"MLB-XV scan: {len(games)} games, best edge {worst*100:+.1f}c")


if __name__ == "__main__":
    main()
