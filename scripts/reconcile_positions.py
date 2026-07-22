"""Position-level reconciliation safety net. For every cross-venue-equivalent
basket, our Kalshi-YES holding must be offset 1:1 by a PM-US-NO holding
(kalshi_pos == -pm_net). Any imbalance = a NAKED, unhedged directional leg —
regardless of how it arose (poll miss, restart orphan, failed hedge). Prints a
single line; a NAKED result is the actionable alert. Read-only.
"""

import base64
import pathlib
import time

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.models.core import Venue
from arbbot.record.kalshi import REST_BASE, load_private_key, sign_headers
from arbbot.registry.model import Registry

D = pathlib.Path.home() / ".arbbot-credentials"


def _retry(fn, tries=3, delay=0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — transient venue error
            last = e
            if i < tries - 1:
                time.sleep(delay * (i + 1))
    raise last


def main():
    c = httpx.Client(timeout=20)
    kid = (D / "kalshi_api_key_id").read_text().strip()
    kkey = load_private_key((D / "kalshi_private_key.pem").read_bytes())

    def kalshi_pos():
        r = c.get(REST_BASE + "/portfolio/positions",
                  headers=sign_headers(kid, kkey, "GET", "/trade-api/v2/portfolio/positions"))
        r.raise_for_status()
        return {p["ticker"]: float(p.get("position_fp") or 0)
                for p in r.json().get("market_positions", [])}

    def pmus_pos():
        KID = (D / "polymarket_usa_key_id").read_text().strip()
        KEY = (D / "polymarket_usa_private_key").read_text().strip()
        priv = ed25519.Ed25519PrivateKey.from_private_bytes(base64.b64decode(KEY)[:32])
        ts = str(int(time.time() * 1000))
        pth = "/v1/portfolio/positions"
        sig = base64.b64encode(priv.sign(f"{ts}GET{pth}".encode())).decode()
        r = c.get("https://api.polymarket.us" + pth,
                  headers={"X-PM-Access-Key": KID, "X-PM-Timestamp": ts, "X-PM-Signature": sig})
        r.raise_for_status()
        return {k: float(v.get("netPosition") or 0)
                for k, v in r.json().get("positions", {}).items()}

    try:
        kpos, ppos = _retry(kalshi_pos), _retry(pmus_pos)
    except Exception as e:
        print(f"RECON error {type(e).__name__} — could not fetch positions")
        return

    # --- SPORTS: cross-venue MLB game legs must net to zero per game ---
    # Kalshi KXMLBGAME-<date><time><away+home>-<TEAM> vs PM aec-mlb-<away>-<home>-<date>.
    # A take-take holds short-away one venue + long-away the other => sum 0.
    import re as _re
    _MON = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    naked = []
    for kt, kq in ((t, q) for t, q in kpos.items() if t.startswith("KXMLBGAME") and q != 0):
        m = _re.match(r"KXMLBGAME-(\d{2})([A-Z]{3})(\d{2})\d{4}([A-Z]+)-([A-Z]+)", kt)
        if not m:
            continue
        yy, mon, dd, concat, team = m.groups()
        if not concat.startswith(team):
            continue  # only reconcile the AWAY leg (it covers the game); home leg would double-count
        away, home = team, concat[len(team):]
        try:
            date = f"20{yy}-{_MON[mon]:02d}-{int(dd):02d}"
        except (KeyError, ValueError):
            continue
        pq = ppos.get(f"aec-mlb-{away.lower()}-{home.lower()}-{date}", 0)
        if abs(kq + pq) >= 1:  # short-away one venue + long-away other must net 0
            naked.append(f"MLB {away}@{home}:kAWAY{kq:+.0f}/pmAWAY{pq:+.0f}/imb{kq+pq:+.0f}")

    reg = Registry.load("config/registry.yaml")
    balanced, seen = 0, set()
    for r in reg.relationships:
        kt = next((l.market_id for l in r.legs if l.venue is Venue.KALSHI), None)
        ps = next((l.market_id for l in r.legs if l.venue is Venue.POLYMARKET_US), None)
        if not kt or not ps or (kt, ps) in seen:
            continue
        seen.add((kt, ps))
        kq, pq = kpos.get(kt, 0), ppos.get(ps, 0)
        if kq == 0 and pq == 0:
            continue
        imb = kq + pq  # hedged basket => kq == -pq => 0
        if abs(imb) >= 1:
            naked.append(f"{r.id}:kYES{kq:+.0f}/pmNet{pq:+.0f}/imb{imb:+.0f}")
        else:
            balanced += 1
    if naked:
        print("RECON NAKED " + " ".join(naked))
    else:
        print(f"RECON ok — {balanced} baskets balanced")


if __name__ == "__main__":
    main()
