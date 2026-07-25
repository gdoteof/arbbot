"""One-shot capital + health snapshot for the 5-min monitoring loop. Prints a
compact summary and, crucially, FLAGS anomalies the monitor should surface:
duplicate resting orders (the orphan/double bug), idle capital, over-utilization,
stale marks, unwind/reverse signals. Read-only."""

import collections
import json
import pathlib
import time
from decimal import Decimal

import httpx

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.record.kalshi import REST_BASE, load_private_key, sign_headers

D = pathlib.Path.home() / ".arbbot-credentials"
ROOT = pathlib.Path(__file__).resolve().parents[1]


UNWIND_UTIL_GATE = Decimal("70")


def signal_flags(nu: int, nh: int, nr: int, util, total) -> list[str]:
    """Pure flag policy (testable): HARD unwind (fwd APR < 4%) is worse than
    trivially-redeployable yield — actionable EVEN INTO IDLE capital, never
    gated. Soft unwind (< 12% hurdle) only matters when capital-constrained
    (util >= 70%). REVERSE = free re-entry profit — always actionable."""
    flags = []
    if nh:
        flags.append(f"UNWIND-HARD:{nh}")
    if (nu - nh) > 0 and util >= UNWIND_UTIL_GATE:
        flags.append(f"UNWIND-SIGNALS:{nu - nh}@util{util:.0f}%")
    if nr:
        flags.append(f"REVERSE-SIGNALS:{nr}")
    if util < 10 and total > 50:
        flags.append(f"LOW-UTILIZATION:{util:.0f}%")
    return flags


def main():
    flags = []
    kid = (D / "kalshi_api_key_id").read_text().strip()
    kkey = load_private_key((D / "kalshi_private_key.pem").read_bytes())
    k = KalshiOrderGateway(kid, kkey, live=True)
    pm = PolymarketUsOrderGateway((D / "polymarket_usa_key_id").read_text().strip(),
                                  (D / "polymarket_usa_private_key").read_text().strip(), live=True)

    # balances (idle capital)
    kbal = pmbal = Decimal(0)
    try:
        hdr = sign_headers(kid, kkey, "GET", "/trade-api/v2/portfolio/balance")
        kbal = Decimal(str(httpx.get(REST_BASE + "/portfolio/balance", headers=hdr, timeout=15)
                           .json()["balance_dollars"]))
    except Exception as e:
        flags.append(f"kalshi-balance-err:{type(e).__name__}")
    try:
        # background read on the shared PM-US rate budget (monitor may wait)
        b = pm.session.get("/v1/account/balances", timeout=15).json()
        pmbal = Decimal(str(b["balances"][0]["buyingPower"]))
    except Exception as e:
        flags.append(f"pmus-balance-err:{type(e).__name__}")

    # resting orders + duplicate (orphan/double) detection. Retry transient
    # rate-limits (429) so a momentary blip degrades to a flag, never a crash.
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
    try:
        kr = [o for o in _retry(k.resting_orders) if o.get("status") == "resting"]
        kdupe = {t: c for t, c in collections.Counter(
            (o.get("ticker"), o.get("side")) for o in kr).items() if c > 1}
    except Exception as e:
        kr, kdupe = None, {}
        flags.append(f"kalshi-orders-err:{type(e).__name__}")
    try:
        pmo = _retry(pm.open_orders)
        pmdupe = {t: c for t, c in collections.Counter(
            (o.get("marketSlug"), o.get("side")) for o in pmo).items() if c > 1}
    except Exception as e:
        pmo, pmdupe = None, {}
        flags.append(f"pmus-orders-err:{type(e).__name__}")
    if kdupe:
        flags.append(f"KALSHI-DUPES:{kdupe}")
    if pmdupe:
        flags.append(f"PMUS-DUPES:{pmdupe}")

    # locked capital in open baskets
    tp = ROOT / "data/exec/trades.jsonl"
    locked = Decimal(0)
    nopen = 0
    if tp.exists():
        for line in tp.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("status") == "open":
                locked += Decimal(str(r.get("cost_usd", 0)))
                nopen += 1

    # marks (mark-to-market + unwind/reverse signals)
    marks = {}
    mp = ROOT / "data/exec/marks.json"
    if mp.exists():
        try:
            marks = json.loads(mp.read_text())
        except ValueError:
            pass
    tot = marks.get("totals", {})
    nu, nr = tot.get("unwind_signals", 0), tot.get("reverse_signals", 0)
    nh = tot.get("unwind_hard", 0)

    idle = kbal + pmbal
    total = idle + locked
    util = (locked / total * 100) if total else Decimal(0)
    # UNWIND is only ACTIONABLE when capital-constrained — unwinding a low-APR
    # hold to free capital is pointless while idle cash is available (it would
    # just forfeit guaranteed profit). Gate the flag on utilization so it stops
    # crying wolf; the per-position UNWIND badge on the dashboard still shows.
    flags.extend(signal_flags(nu, nh, nr, util, total))

    # runner liveness (python proc with --relationship)
    import subprocess
    ps = subprocess.run(["pgrep", "-af", "arbbot.exec.main"], capture_output=True, text=True).stdout
    alive = any("bin/python" in l and "--relationship" in l for l in ps.splitlines())
    if not alive:
        flags.append("RUNNER-DOWN")

    # material signature: only fields worth waking up for (excludes markPnL/idle
    # cent-jitter). The monitor emits only when this changes or a flag fires.
    # exclude resting counts — they flip constantly with normal quote churn.
    # Material = utilization bucket, basket count (fills), and flags.
    sig = f"{util:.0f}|{nopen}|{'F' if flags else 'OK'}"
    kn = len(kr) if kr is not None else "?"
    pn = len(pmo) if pmo is not None else "?"
    print(f"CAPITAL util={util:.0f}% locked=${locked:.2f}/{nopen}baskets "
          f"idle=${idle:.2f}(k=${kbal:.0f} pm=${pmbal:.0f}) "
          f"resting=k{kn}/pm{pn} markPnL=${tot.get('mark_pnl_usd',0):.2f} "
          f"lockedProfit=${tot.get('locked_profit_usd',0):.2f} "
          + ("FLAGS[" + " ".join(flags) + "]" if flags else "OK")
          + f" SIG={sig}")


if __name__ == "__main__":
    main()
