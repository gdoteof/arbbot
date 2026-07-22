"""Cross-venue sports basis scanner (generalized). Kalshi runs per-game/match
winner markets; PM US runs the same as aec-<sport>-<a>-<b>-<YYYY-MM-DD>. Match by
teams/players + date, compare the moneyline across venues, report net-of-fee basis.

Clean equivalence (who wins) + fast settlement (hours) => any persistent basis
annualizes huge. Read-only. NOTE: wide Kalshi spreads on far/illiquid games make
apparent 'edges' that aren't real — only tight two-sided quotes count.
"""

import datetime
import re
import sys

import httpx

from arbbot.record.kalshi import REST_BASE

MON = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
       "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
FEE = 0.02
# sport -> (kalshi series, [PM US slug prefixes to try])
SPORTS = {
    "mlb":   ("KXMLBGAME", ["aec-mlb"]),
    "wnba":  ("KXWNBAGAME", ["aec-wnba"]),
    "wta":   ("KXWTAMATCH", ["aec-tennis", "aec-wta"]),
    "atp":   ("KXATPMATCH", ["aec-tennis", "aec-atp"]),
    "mls":   ("KXMLSGAME", ["aec-mls", "aec-soccer"]),
}


def parse(ticker, series):
    # KX...-<26MON DD>[HHMM]<teamsconcat>-<TEAM>
    rest = ticker[len(series) + 1:]
    key, team = rest.rsplit("-", 1)
    m = re.match(r"(\d{2})([A-Z]{3})(\d{2})(\d{4})?(.+)", key)
    if not m:
        return None
    yy, mon, dd, _t, concat = m.groups()
    try:
        date = datetime.date(2000 + int(yy), MON[mon], int(dd)).isoformat()
    except (KeyError, ValueError):
        return None
    return date, concat, team


def scan(sport, series, prefixes, c):
    ms = c.get(f"{REST_BASE}/markets", params={"series_ticker": series, "status": "open", "limit": 400}).json().get("markets", [])
    games = {}
    for x in ms:
        p = parse(x["ticker"], series)
        if not p:
            continue
        date, concat, team = p
        if team == "TIE":  # 3-way (soccer) — skip the draw leg for moneyline compare
            continue
        g = games.setdefault((date, concat), {"date": date, "concat": concat, "sides": {}})
        g["sides"][team] = (x.get("yes_bid_dollars"), x.get("yes_ask_dollars"))
    print(f"\n=== {sport.upper()} ({len(games)} games) ===")
    hits = matched = 0
    for (date, concat), g in sorted(games.items()):
        sides = g["sides"]
        if len(sides) != 2:
            continue
        away = next((t for t in sides if concat.startswith(t)), None)
        home = next((t for t in sides if t != away), None)
        if not away or not home or sides[away][0] is None or sides[away][1] is None:
            continue
        kb, ka = float(sides[away][0]), float(sides[away][1])
        pb = pa = None
        for pref in prefixes:
            slug = f"{pref}-{away.lower()}-{home.lower()}-{date}"
            try:
                r = c.get(f"https://gateway.polymarket.us/v1/markets/{slug}/bbo", timeout=8)
            except Exception:
                continue
            if r.status_code == 200:
                md = r.json().get("marketData", {})
                pb = (md.get("bestBid") or {}).get("value")
                pa = (md.get("bestAsk") or {}).get("value")
                break
        if pb is None or pa is None:
            continue
        matched += 1
        pb, pa = float(pb), float(pa)
        kspread = ka - kb
        edge = max(pb - ka, kb - pa) - FEE
        liq = "" if kspread <= 0.03 else "  (illiquid — Kspread %.0fc, edge not real)" % (kspread * 100)
        tag = "  <<< NET BASIS" if (edge > 0 and kspread <= 0.03) else ""
        if edge > 0 and kspread <= 0.03:
            hits += 1
        print(f"  {away}@{home} {date[5:]}  K={kb:.2f}/{ka:.2f} PM={pb:.2f}/{pa:.2f}  edge={edge*100:+.1f}c{tag}{liq}")
    print(f"  matched {matched} games cross-venue; {hits} with real (liquid) net basis")


def main():
    c = httpx.Client(timeout=20)
    want = sys.argv[1:] or list(SPORTS)
    for s in want:
        if s in SPORTS:
            scan(s, SPORTS[s][0], SPORTS[s][1], c)


if __name__ == "__main__":
    main()
