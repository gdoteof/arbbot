"""Generic cross-venue sports equivalence discovery (Geoff 2026-07-22:
"treat all of sports as the same thing").

One framework for every league: a game/match-winner market is equivalent
across venues iff (league, date±1, unordered participant pair) match, with
participants compared as normalized name-token sets (handles "Petro Kolesnik"
vs "Kolesnik Petro", "Ipswich Town FC" vs "Ipswich FC").

Output: per-league coverage (PM games, Kalshi games, matched) + a JSON map of
matched games for downstream spread/edge scanning (sports_equiv_scan).
Read-only; identity mistakes lose money, so anything not exactly matched is
reported unmatched, never guessed.
"""

import json
import re
import sys
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import httpx

from arbbot.models.core import Role, Side, Venue
from arbbot.registry.model import (Leg, Registry, Relationship,
                                   RelationshipType, Verdict, VettedBy)

K = "https://api.elections.kalshi.com/trade-api/v2"
P = "https://gateway.polymarket.us/v1"
OUT = Path("data/scan/sports_equiv_map.json")
REG_DIR = Path("data/registry")

# pm slug prefix -> kalshi series (curated; extend as leagues appear)
LEAGUES = [
    ("mlb", "KXMLBGAME"), ("kbo", "KXKBOGAME"), ("npb", "KXNPBGAME"),
    ("clbf", "KXCLUBFGAME"), ("uecl", "KXUECLGAME"), ("mls", "KXMLSGAME"),
    ("bra", "KXBRASILEIROGAME"), ("ekst", "KXEKSTRAKLASAGAME"),
    ("arg2", "KXARGNACBGAME"), ("uslc", "KXUSLGAME"), ("sclc", "KXSCOCUPGAME"),
    ("itfme", "KXITFMATCH"), ("itfwo", "KXITFWMATCH"),
    ("wta", "KXWTAMATCH"), ("atp", "KXATPMATCH"),
    ("lol", "KXLOLGAME"), ("cs2", "KXCS2GAME"),
    ("valorant", "KXVALORANTGAME"), ("dota2", "KXDOTA2GAME"),
]

STOP = {"fc", "cf", "ca", "sc", "se", "ac", "cd", "afc", "club", "clube",
        "de", "the", "town", "city", "united", "esports", "team", "1913",
        "fk", "pfk", "sv", "spvgg", "tsv", "vfb", "vfl", "sk", "bk", "if"}


def norm_tokens(name: str) -> frozenset:
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    toks = re.sub(r"[^a-z0-9 ]", " ", s.lower()).split()
    core = frozenset(t for t in toks if t not in STOP)
    return core or frozenset(toks)


def team_match(a: str, b: str) -> bool:
    """Subset match: Kalshi 'Pittsburgh' ~ PM 'Pittsburgh Pirates'; tennis
    'Fitriadi' ~ 'M Rifqi Fitriadi'. Never matches on stop-words alone."""
    ta, tb = norm_tokens(a), norm_tokens(b)
    return bool(ta and tb) and (ta <= tb or tb <= ta)


def game_match(pa: str, pb: str, ka: str, kb: str) -> bool:
    return ((team_match(pa, ka) and team_match(pb, kb))
            or (team_match(pa, kb) and team_match(pb, ka)))


def pm_games(c, prefix):
    """slug-prefixed 'A vs. B' events -> [(date, teamA, teamB, slug, markets)]."""
    out, offset = [], 0
    while True:
        j = c.get(f"{P}/events?closed=false&limit=500&offset={offset}").json()
        evs = j.get("events") or []
        if not evs:
            break
        for e in evs:
            slug = e.get("slug") or ""
            if not slug.startswith(prefix + "-"):
                continue
            title = e.get("title") or ""
            if " vs. " not in title and " vs " not in title:
                continue
            a, b = re.split(r" vs\.? ", title, maxsplit=1)
            date = (e.get("startTime") or e.get("startDate") or "")[:10]
            ml = next((m for m in e.get("markets") or []
                       if m.get("marketType") == "moneyline"
                       and (m.get("slug") or "").startswith("aec-")), None)
            long_team = None
            if ml:
                long_team = next(((s.get("team") or {}).get("name") or s.get("description")
                                  for s in ml.get("marketSides") or [] if s.get("long")), None)
            out.append({"date": date, "a": a.strip(), "b": b.strip(),
                        "slug": slug,
                        "moneyline": ml.get("slug") if ml else None,
                        "long_team": long_team,
                        "start_time": e.get("startTime"),
                        "markets": [m.get("slug") for m in e.get("markets") or []]})
        offset += len(evs)
        if len(evs) < 500:
            break
    return out


def kalshi_games(c, series):
    """open events for a series -> [(date, teamA, teamB, event_ticker, markets)]."""
    out, cursor = [], None
    while True:
        params = {"series_ticker": series, "status": "open", "limit": 200,
                  "with_nested_markets": "true"}
        if cursor:
            params["cursor"] = cursor
        j = c.get(f"{K}/events", params=params).json()
        evs = j.get("events") or []
        for e in evs:
            title = (e.get("title") or "").rstrip("?")
            if " vs " not in title.lower():
                continue
            a, b = re.split(r" [Vv]s\.? ", title, maxsplit=1)
            mkts = e.get("markets") or []
            # game date: embedded in the event ticker (SERIES-YYMONDD...);
            # market close_time is unreliable (set days after the game)
            date = ""
            dm = re.search(r"-(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})",
                           e.get("event_ticker") or "")
            if dm:
                mon = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7,
                       "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}[dm.group(2)]
                date = f"20{dm.group(1)}-{mon:02d}-{int(dm.group(3)):02d}"
            if not date:
                dates = sorted(m.get("close_time", "")[:10] for m in mkts if m.get("close_time"))
                date = dates[0] if dates else ""
            out.append({"date": date, "a": a.strip(), "b": b.strip(),
                        "ticker": e.get("event_ticker"),
                        "markets": [{"ticker": m.get("ticker"),
                                     "team": m.get("yes_sub_title")} for m in mkts]})
        cursor = j.get("cursor")
        if not cursor or not evs:
            break
    return out


def date_near(d1: str, d2: str) -> bool:
    if not d1 or not d2:
        return False
    if d1 == d2:
        return True
    import datetime as dt
    try:
        a, b = dt.date.fromisoformat(d1), dt.date.fromisoformat(d2)
    except ValueError:
        return False
    return abs((a - b).days) <= 1


def emit_registry_yaml(map_path=OUT, out_dir=REG_DIR):
    """Relationship-shaped mirror of the matched-game map for the registry
    compiler (scripts/compile_registry.py): data/registry/sports-<utcdate>.yaml,
    vetted_by=mechanical (tradable only via the sports carve-out in
    v_tradable_relationships). Ids match the sports_arb.py ledger shape
    sports-<league>-<away>@<home>; the JSON map stays the probes' interface.
    Missing/corrupt map -> no file, no error (offline-safe)."""
    try:
        doc = json.loads(Path(map_path).read_text())
    except (OSError, ValueError):
        return None
    rels, seen = [], set()
    for m in doc.get("matches", []):
        if not (m.get("pm_moneyline") and m.get("kalshi_long_ticker")):
            continue
        a, _, b = (m.get("teams") or "").partition(" vs ")
        # away/home exactly as sports_arb.load_equiv_games builds its game key
        away = (m.get("pm_long_team") or a)[:20]
        home = (b if m.get("pm_long_team", a) != b else a)[:20]
        rid = f"sports-{m['league']}-{away}@{home}"
        if rid in seen:
            continue
        seen.add(rid)
        rels.append(Relationship(
            id=rid, type=RelationshipType.CROSS_VENUE_EQUIVALENT,
            legs=[Leg(venue=Venue.KALSHI, market_id=m["kalshi_long_ticker"],
                      side=Side.YES, role=Role.TAKER),
                  Leg(venue=Venue.POLYMARKET_US, market_id=m["pm_moneyline"],
                      side=Side.NO, role=Role.TAKER)],
            direction=f"Kalshi {away} YES == PM {away} moneyline YES "
                      f"(basket: Kalshi YES + PM NO)",
            verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.MECHANICAL,
            vetted_at=doc.get("generated_at"),
            category="sports", topic=m["league"],
            event_date=m.get("date") or None))
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"sports-{time.strftime('%Y-%m-%d', time.gmtime())}.yaml"
    Registry(relationships=rels).dump(str(out))
    print(f"{len(rels)} relationships -> {out}")
    return out


def main():
    c = httpx.Client(timeout=30)
    report, matches = [], []
    for prefix, series in LEAGUES:
        pg = pm_games(c, prefix)
        kg = kalshi_games(c, series)
        matched, un_pm = [], []
        for g in pg:
            # doubles ("Dong - Markovina") subset-match singles sharing a
            # surname — identity risk; skip pair entities entirely (v1)
            if " - " in g["a"] or " - " in g["b"]:
                continue
            cands = [k for k in kg
                     if date_near(g["date"], k["date"])
                     and game_match(g["a"], g["b"], k["a"], k["b"])]
            if len(cands) == 1:
                # kalshi market for the PM moneyline's LONG (YES) team — the
                # ready-to-trade pairing: PM-YES == that kalshi market's YES
                k_long = None
                if g.get("long_team"):
                    k_long = next((mk["ticker"] for mk in cands[0]["markets"]
                                   if mk.get("team") and team_match(mk["team"], g["long_team"])),
                                  None)
                matched.append({"league": prefix, "kalshi_series": series,
                                "date": g["date"], "teams": f"{g['a']} vs {g['b']}",
                                "pm_slug": g["slug"], "pm_markets": g["markets"],
                                "pm_moneyline": g.get("moneyline"),
                                "pm_long_team": g.get("long_team"),
                                "start_time": g.get("start_time"),
                                "kalshi_event": cands[0]["ticker"],
                                "kalshi_long_ticker": k_long,
                                "kalshi_markets": cands[0]["markets"]})
            else:
                un_pm.append(g)
        # bijection: a Kalshi event claimed by >1 PM game is ambiguous — drop all
        claims = defaultdict(list)
        for m in matched:
            claims[m["kalshi_event"]].append(m)
        matched = [ms[0] for ms in claims.values() if len(ms) == 1]
        report.append({"league": prefix, "kalshi_series": series,
                       "pm_games": len(pg), "kalshi_games": len(kg),
                       "matched": len(matched),
                       "unmatched_pm_sample": [f"{g['date']} {g['a']} vs {g['b']}"
                                               for g in un_pm[:3]]})
        matches.extend(matched)
        print(f"{prefix:10s} ↔ {series:22s} pm={len(pg):4d} kalshi={len(kg):4d} "
              f"matched={len(matched):4d}")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                               "report": report, "matches": matches}, indent=1))
    print(f"\n{len(matches)} matched games -> {OUT}")
    emit_registry_yaml()


if __name__ == "__main__":
    if "--emit-only" in sys.argv:  # offline: regenerate YAML from existing map
        emit_registry_yaml()
    else:
        main()
