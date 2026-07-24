"""Mark open arbitrage baskets to live market and watch for exit/reverse signals.

For each open basket (long Kalshi YES + long PM US NO), fetch live top-of-book
and compute:
  * liquidation value now = Kalshi YES bid + PM US NO bid (1 - PM YES ask)
  * mark P&L = liquidation - cost - unwind fees  (starts slightly negative from
    the round-trip spread; climbs toward locked profit as prices converge)
  * converged % = mark_pnl / locked_profit
  * UNWIND signal: early exit already banks >= locked profit (free the capital)
  * REVERSE signal: the OPPOSITE basket (buy PM YES + Kalshi NO on the same
    outcome) is now itself profitable -> a fresh independent arb as prices ebb

Writes data/exec/marks.json for the dashboard. Read-only on venues (public
quotes); no orders. Runs on a short timer.
"""

import json
import time
from decimal import Decimal
from pathlib import Path

import httpx

from arbbot.exec.resolve_dates import resolve_date
from arbbot.record.kalshi import REST_BASE

KALSHI_FEE = Decimal("0.01")  # ~taker fee/ct to unwind the Kalshi leg
LEDGER = Path("data/exec/trades.jsonl")
OUT = Path("data/exec/marks.json")


def kalshi_books(client, tickers):
    out = {}
    for i in range(0, len(tickers), 50):
        r = client.get(f"{REST_BASE}/markets", params={"tickers": ",".join(tickers[i:i + 50])})
        for m in r.json().get("markets", []):
            out[m["ticker"]] = (Decimal(str(m.get("yes_bid_dollars") or 0)),
                                Decimal(str(m.get("yes_ask_dollars") or 0)))
    return out


def pmus_topbook(client, slug):
    b = client.get(f"https://gateway.polymarket.us/v1/markets/{slug}/bbo").json().get("marketData", {})
    bid, ask = (b.get("bestBid") or {}), (b.get("bestAsk") or {})
    return (Decimal(bid["value"]) if bid.get("value") else None,
            Decimal(ask["value"]) if ask.get("value") else None)


# opportunity cost of locked capital (%/yr). A hold whose REMAINING return
# annualizes below this is a worse use of capital than fresh riskless arb
# (~15-18%/yr) — flag it for unwind even before full convergence.
HURDLE_APR = 12.0
# hard floor: below this the hold is worse than trivially-redeployable yield,
# so unwind EVEN INTO IDLE CAPITAL (the soft 12% hurdle stays gated on being
# capital-constrained; this one is unconditional).
HARD_FLOOR_APR = 4.0


def _years_to(resolves_by):
    import datetime
    if not resolves_by:
        return None
    try:
        d = datetime.date.fromisoformat(str(resolves_by)[:10])
    except ValueError:
        return None
    return max((d - datetime.date.today()).days, 1) / 365.25


def compute_row(t: dict, k_bid, k_ask, p_bid, p_ask, now: float | None = None) -> dict:
    """Pure per-position marking: trade record + top-of-book quotes -> row
    (mark, APRs, unwind/reverse signals). Extracted from main() so the math
    is testable — this file shipped two live bugs (inverted-basket marking,
    phantom reverse) that unit tests would have caught."""
    now = time.time() if now is None else now
    kleg = next(l for l in t["legs"] if l["venue"] == "kalshi")
    qty = Decimal(str(t["qty"]))
    cost = Decimal(str(t["cost_usd"]))
    locked = Decimal(str(t["profit_usd"]))
    # resolve date: curated estimate (authoritative for known families) else
    # whatever the trade record stamped. `estimated` marks it speculative.
    rdate, rest = resolve_date(t["relationship_id"])
    resolves = rdate or t.get("resolves_by")
    estimated = rest if rdate else bool(t.get("resolves_estimated"))
    if not resolves and str(t.get("relationship_id", "")).startswith("sports-"):
        # sports baskets resolve the same day the game ends — model as
        # entry + 1 day so APR columns populate (Geoff 2026-07-22). The
        # resulting APRs are huge by construction; that IS the point of
        # fast-settling arb (capital recycles daily).
        import datetime as _dt2
        resolves = (_dt2.date.fromtimestamp(float(t.get("ts", now)))
                    + _dt2.timedelta(days=1)).isoformat()
        estimated = True
    if not resolves:
        # probe-originated baskets (card e5696107) stamp no resolves_by, but
        # their event slugs embed the date (aec-...-2026-07-23): event date
        # + 1 day, same fast-settle model. Records with no date anywhere stay
        # None (a wrong entry+1d guess on a long-dated market would fire
        # false hard-unwind signals).
        import datetime as _dt2
        import re as _re2
        hay = str(t.get("relationship_id", "")) + " " + " ".join(
            str(l.get("market_id") or "") for l in t.get("legs", []))
        m2 = _re2.search(r"(20\d{2}-\d{2}-\d{2})", hay)
        if m2:
            try:
                resolves = (_dt2.date.fromisoformat(m2.group(1))
                            + _dt2.timedelta(days=1)).isoformat()
                estimated = True
            except ValueError:
                pass
    row = {"relationship_id": t["relationship_id"], "ts": t.get("ts"), "title": t.get("title"),
           "qty": int(qty), "cost_usd": float(cost), "locked_profit_usd": float(locked),
           "resolves_by": resolves, "resolves_estimated": estimated}
    # basket direction: standard = long Kalshi YES + long PM NO; Kalshi-maker
    # ask-side fills produce the INVERSE (Kalshi NO + PM YES) — marking those
    # as standard flags our own entry as a phantom "reverse" arb.
    inverted = kleg.get("side") == "no"
    if inverted and not (k_ask is None or p_bid is None):
        # liquidate: sell Kalshi NO @ NO-bid (=1-YES-ask); sell PM YES @ bid
        liq_ct = (Decimal(1) - k_ask) + p_bid
    elif not inverted and k_bid is not None and p_ask is not None:
        # liquidate: sell Kalshi YES @ its bid; close PM NO by buying YES @ ask -> NO worth 1-ask
        liq_ct = k_bid + (Decimal(1) - p_ask)
    else:
        liq_ct = None
    if liq_ct is not None:
        liq = liq_ct * qty - KALSHI_FEE * qty
        mark = liq - cost
        # forward APR of CONTINUING to hold: remaining profit annualized over
        # the capital currently tied up (the liq value we'd otherwise free).
        yrs = _years_to(resolves)
        remaining = float(locked) - float(mark)
        fwd_apr = (remaining / float(liq) / yrs * 100) if (yrs and liq > 0) else None
        # realized APR to date = the rate you'd LOCK IN by unwinding now:
        # mark gain on cost, annualized over the holding period so far
        # (floored at 1h so a just-opened position doesn't annualize to
        # infinity). Noisy for fresh positions — pair it with hold APR.
        held_yr = max(now - float(t.get("ts", now)), 3600.0) / 31557600.0
        unwind_apr = (float(mark) / float(cost) / held_yr * 100) if cost > 0 else None
        # NATURAL hold APR: the intrinsic riskless yield if held to resolution —
        # locked profit / cost / (entry->resolution), ignoring exit taker fees &
        # slippage entirely (you don't pay them if you hold). This is the "true"
        # arb yield, vs forward_hold_apr which is depressed by the liquidation mark.
        held_real = max(0.0, now - float(t.get("ts", now))) / 31557600.0
        total_yr = (held_real + yrs) if yrs else None
        natural_apr = (float(locked) / float(cost) / total_yr * 100) if (cost > 0 and total_yr) else None
        # unwind when we've banked the full locked profit OR we're in profit
        # and the remaining hold has become a low-APR use of capital (early
        # convergence on a long-dated position — the time-value case).
        is_sports = str(t.get("relationship_id", "")).startswith("sports-")
        unwind = bool(mark >= locked) or bool(
            mark > 0 and fwd_apr is not None and fwd_apr < HURDLE_APR)
        if is_sports:
            # sports baskets resolve in HOURS and their books move/freeze fast:
            # hold to settlement (the sweeper realizes them). APR-based
            # unwind/reverse machinery is for the slow political book — a
            # marks-snapshot signal here nearly churned a converged basket for
            # +2c against frozen-book and fill-lag risk (2026-07-22 Preston).
            unwind = False
        # hard unwind: forward APR below the floor — worse than trivially
        # redeployable yield, exit regardless of utilization. (fwd_apr is
        # naturally high when mark is deeply negative — the remaining gain
        # is large — so this only fires on near-converged positions.)
        unwind_hard = bool(fwd_apr is not None and fwd_apr < HARD_FLOOR_APR) and not is_sports
        row.update(liq_value_usd=float(liq), mark_pnl_usd=float(mark),
                   converged_pct=float(mark / locked * 100) if locked else 0.0,
                   forward_hold_apr=(round(fwd_apr, 1) if fwd_apr is not None else None),
                   natural_hold_apr=(round(natural_apr, 1) if natural_apr is not None else None),
                   unwind_apr=(round(unwind_apr, 1) if unwind_apr is not None else None),
                   unwind_signal=unwind, unwind_hard=unwind_hard)
        # reverse basket = the OPPOSITE direction of what we hold:
        # standard (K YES + PM NO) -> buy PM YES @ ask + Kalshi NO @ (1-Kbid)
        # inverted (K NO + PM YES) -> buy Kalshi YES @ ask + PM NO @ (1-Pbid)
        if inverted:
            rev = (p_bid - k_ask) if (p_bid is not None and k_ask is not None) else None
        else:
            rev = (k_bid - p_ask) if (k_bid is not None and p_ask is not None) else None
        row["reverse_edge_c"] = float(rev * 100) if rev is not None else None
        # sports reverse signals stay dark too: the WS engine owns sports
        # crossings at sub-second resolution — a 2-min marks snapshot of a
        # fast book is noise, not an actionable re-entry.
        row["reverse_signal"] = bool(rev is not None and rev >= Decimal("0.03")) and not is_sports
        # maker-exit delta (Geoff 2026-07-24, Exits view): per-contract profit
        # of the OPPORTUNISTIC exit right now — rest a Kalshi ask one tick
        # inside the competing ask, close the PM NO one tick through its ask
        # on fill (matches the runner's maker-unwind pricing). Positive =
        # a passive exit locks money today; eligible = the runner would
        # actually rest it (unwind-flagged AND mark-positive).
        if not inverted and k_ask is not None and p_ask is not None and qty:
            mx = (k_ask - Decimal("0.01")) + (Decimal(1) - (p_ask + Decimal("0.01"))) \
                 - cost / qty
            row["maker_exit_ct"] = round(float(mx), 4)
            row["maker_exit_eligible"] = bool((unwind or unwind_hard) and mark > 0)
        else:
            row["maker_exit_ct"] = None
            row["maker_exit_eligible"] = False
    else:
        row.update(liq_value_usd=None, mark_pnl_usd=None, converged_pct=None,
                   forward_hold_apr=None, natural_hold_apr=None, unwind_apr=None,
                   unwind_signal=False, unwind_hard=False,
                   reverse_signal=False, reverse_edge_c=None)
    return row


def main():
    from arbbot.exec.ledger import open_baskets, parse_lines
    trades = parse_lines(LEDGER.read_text().splitlines()) if LEDGER.exists() else []
    open_t = open_baskets(trades)  # open records net of appended unwind records
    c = httpx.Client(timeout=20)
    ktickers = [next(l["market_id"] for l in t["legs"] if l["venue"] == "kalshi")
                for t in open_t if len(t.get("legs", [])) >= 2]
    kb = kalshi_books(c, ktickers) if ktickers else {}

    positions, tot_cost, tot_mark, tot_locked = [], 0.0, 0.0, 0.0
    for t in open_t:
        if len(t.get("legs", [])) < 2:
            continue  # single-leg records (pm-lean riders) are directional — no basket mark
        kleg = next(l for l in t["legs"] if l["venue"] == "kalshi")
        pleg = next(l for l in t["legs"] if l["venue"] == "polymarket_us")
        k_bid, k_ask = kb.get(kleg["market_id"], (None, None))
        p_bid, p_ask = pmus_topbook(c, pleg["market_id"])
        row = compute_row(t, k_bid, k_ask, p_bid, p_ask)
        if row.get("mark_pnl_usd") is not None:
            tot_mark += row["mark_pnl_usd"]
        tot_cost += float(t["cost_usd"]); tot_locked += float(t["profit_usd"])
        positions.append(row)

    doc = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "positions": positions,
           "totals": {"cost_usd": round(tot_cost, 2), "mark_pnl_usd": round(tot_mark, 2),
                      "locked_profit_usd": round(tot_locked, 2),
                      "n_open": len(open_t),
                      "unwind_signals": sum(1 for p in positions if p.get("unwind_signal")),
                      "unwind_hard": sum(1 for p in positions if p.get("unwind_hard")),
                      "reverse_signals": sum(1 for p in positions if p.get("reverse_signal"))}}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, indent=0))
    # exit-delta time series for the dash Exits view (marks.json is an
    # overwritten snapshot; this appends one compact line per run)
    hist = {"t": time.time(), "rows": [
        {"rel": p["relationship_id"], "ts0": p["ts"], "qty": p["qty"],
         "mx": p["maker_exit_ct"], "mark": p.get("mark_pnl_usd"),
         "elig": p.get("maker_exit_eligible", False)}
        for p in positions if p.get("maker_exit_ct") is not None]}
    if hist["rows"]:
        with open("data/exec/marks_history.jsonl", "a") as f:
            f.write(json.dumps(hist) + "\n")
    print(json.dumps(doc["totals"]))


if __name__ == "__main__":
    main()
