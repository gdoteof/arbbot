"""Position-level reconciliation safety net. For every cross-venue-equivalent
basket, our Kalshi-YES holding must be offset 1:1 by a PM-US-NO holding
(kalshi_pos == -pm_net). Any imbalance = a NAKED, unhedged directional leg —
regardless of how it arose (poll miss, restart orphan, failed hedge). Prints a
single line; a NAKED result is the actionable alert. Read-only.
"""

import pathlib
import time

import httpx

from arbbot.exec import ledgerdb
from arbbot.models.core import Venue
from arbbot.record.kalshi import REST_BASE, load_private_key, sign_headers
from arbbot.registry.model import Registry
from arbbot.venues.pmus import PmusSession, get_bbo

D = pathlib.Path.home() / ".arbbot-credentials"


def _migrate_dotfiles(db, d=pathlib.Path("data/exec")):
    """One-time import of the legacy .recon_*.json state blobs into the
    recon_* tables (semantics preserved); each imported dot-file is renamed
    to <name>.migrated so this never runs twice."""
    import json as _j
    now = time.time_ns()

    def _empty(table):
        return not db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()

    f = d / ".recon_pm_n.json"          # ratcheting last-known-good PM count
    if f.exists() and _empty("recon_baseline"):
        with __import__("contextlib").suppress(Exception):
            n = int(_j.loads(f.read_text()).get("n", 0))
            with db:
                db.execute("INSERT INTO recon_baseline (venue, n_positions, updated_ns)"
                           " VALUES ('polymarket_us', ?, ?)", (n, now))
            f.rename(f.with_name(f.name + ".migrated"))
    f = d / ".recon_state.json"         # last run's naked sightings (confirm set)
    if f.exists() and _empty("recon_sighting"):
        with __import__("contextlib").suppress(Exception):
            entries = _j.loads(f.read_text())
            with db:
                for e in entries:
                    db.execute("INSERT OR IGNORE INTO recon_sighting"
                               " (imbalance_key, first_seen_ns, last_seen_ns, runs)"
                               " VALUES (?, ?, ?, 1)", (e, now, now))
            f.rename(f.with_name(f.name + ".migrated"))
    f = d / ".recon_ignore.json"        # scoped tolerated-dust ignores
    if f.exists() and _empty("recon_ignore"):
        with __import__("contextlib").suppress(Exception):
            entries = _j.loads(f.read_text())
            with db:
                for i in entries:
                    db.execute("INSERT OR IGNORE INTO recon_ignore"
                               " (fragment, max_imb, expires_ns, added_by, note)"
                               " VALUES (?, ?, ?, ?, ?)",
                               (i["fragment"], int(i.get("max_imb", 0)),
                                int(float(i.get("expires", 0)) * 1e9),
                                i.get("added_by"), i.get("note")))
            f.rename(f.with_name(f.name + ".migrated"))
    f = d / ".recon_degraded.json"      # last degraded-read details
    if f.exists() and _empty("recon_incident"):
        with __import__("contextlib").suppress(Exception):
            doc = _j.loads(f.read_text())
            with db:
                db.execute("INSERT INTO recon_incident"
                           " (ts_ns, venue, n_positions, baseline, missing_json)"
                           " VALUES (?, 'polymarket_us', ?, ?, ?)",
                           (int(float(doc.get("ts", 0)) * 1e9), doc.get("pm_n"),
                            doc.get("baseline"), _j.dumps(doc.get("missing"))))
            f.rename(f.with_name(f.name + ".migrated"))


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

    k_info, pm_info = {}, {}

    def kalshi_pos():
        r = c.get(REST_BASE + "/portfolio/positions",
                  headers=sign_headers(kid, kkey, "GET", "/trade-api/v2/portfolio/positions"))
        r.raise_for_status()
        out = {}
        for p in r.json().get("market_positions", []):
            q = float(p.get("position_fp") or 0)
            out[p["ticker"]] = q
            if q:
                k_info[p["ticker"]] = {
                    "basis_ct": float(p.get("market_exposure_dollars") or 0) / abs(q),
                    "realized": float(p.get("realized_pnl_dollars") or 0),
                    "fees": float(p.get("fees_paid_dollars") or 0)}
        return out

    def pmus_pos():
        KID = (D / "polymarket_usa_key_id").read_text().strip()
        KEY = (D / "polymarket_usa_private_key").read_text().strip()
        # the endpoint transiently serves EMPTY (and hours-stale) snapshots
        # (2026-07-22) — we always hold PM positions, so empty = glitch;
        # get_positions raises so _retry re-fetches instead of emitting a
        # false NAKED.
        pos = PmusSession(KID, KEY, client=c).get_positions()
        out = {}
        for k2, v in pos.items():
            out[k2] = float(v.get("netPosition") or 0)
            if out[k2]:
                pm_info[k2] = {"basis_ct": float((v.get("costPerShare") or {}).get("value") or 0)}
        return out

    try:
        kpos, ppos = _retry(kalshi_pos), _retry(pmus_pos)
    except Exception as e:
        print(f"RECON error {type(e).__name__} — could not fetch positions")
        return
    # recon guard state lives in the trading DB (recon_* tables; legacy
    # dot-file blobs are migrated once then renamed *.migrated)
    import json as _jg
    db = None
    try:
        db = ledgerdb.connect()
        _migrate_dotfiles(db)
    except Exception as _de:
        print(f"RECON guard error: {type(_de).__name__}: {_de}")
    # PM platform outages serve PARTIAL position sets (n=1-4 of ~10) that pass
    # the empty-read guard and paint pm+0 under every pair. Track last-known-
    # good count; a collapsed read means DEGRADED, not naked.
    try:
        row = db.execute("SELECT n_positions FROM recon_baseline"
                         " WHERE venue = 'polymarket_us'").fetchone()
        last_good = int(row["n_positions"]) if row else 0
    except Exception:
        last_good = 0
    # the LEDGER defines every PM leg that MUST exist (open baskets with a
    # polymarket_us leg). A read missing any of them is partial/broken —
    # generalizes the canary idea to the full expected set. (Legs from failed
    # captures never reach the ledger, so genuine sports nakedness still
    # evaluates normally.)
    expected = set()
    try:
        for t2 in ledgerdb.open_baskets_db(db):
            for l2 in t2.get("legs", []):
                if l2.get("venue") == "polymarket_us" and float(t2.get("qty") or 0) >= 1:
                    expected.add(l2.get("market_id"))
    except Exception as _ge:
        print(f"RECON guard error: {type(_ge).__name__}: {_ge}")
    def _degraded(pp):
        return (sorted(m2 for m2 in expected if not pp.get(m2)),
                bool(last_good and len(pp) < 0.6 * last_good))

    missing, collapsed = _degraded(ppos)
    if missing or collapsed:
        # STABLE message (no counts) so the alert monitor's consecutive-dedupe
        # suppresses repeats during a long outage; details go to recon_incident.
        with __import__("contextlib").suppress(Exception):
            with db:
                db.execute("INSERT INTO recon_incident"
                           " (ts_ns, venue, n_positions, baseline, missing_json)"
                           " VALUES (?, 'polymarket_us', ?, ?, ?)",
                           (time.time_ns(), len(ppos), last_good, _jg.dumps(missing)))
        print("RECON degraded — PM positions API partial (platform incident); skipping naked evaluation")
        return
    with __import__("contextlib").suppress(Exception):
        # ratchet-up only: a healthy read raises the baseline; genuine
        # portfolio shrinkage requires a manual reset of recon_baseline
        with db:
            db.execute("INSERT INTO recon_baseline (venue, n_positions, updated_ns)"
                       " VALUES ('polymarket_us', ?, ?)"
                       " ON CONFLICT(venue) DO UPDATE SET"
                       " n_positions = MAX(n_positions, excluded.n_positions),"
                       " updated_ns = excluded.updated_ns",
                       (len(ppos), time.time_ns()))

    # --- SPORTS: cross-venue MLB game legs must net to zero per game ---
    # Kalshi KXMLBGAME-<date><time><away+home>-<TEAM> vs PM aec-mlb-<away>-<home>-<date>.
    # A take-take holds short-away one venue + long-away the other => sum 0.
    import re as _re
    _MON = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

    reg = Registry.load("config/registry.yaml")

    def _find_naked(kpos, ppos, rows=None):
        naked = []
        naked_kt = {}
        def _row(**kw):
            if rows is not None:
                rows.append(kw)
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
            _row(name=f"MLB {away}@{home}", type="sports-mlb", kt=kt,
                 pm=f"aec-mlb-{away.lower()}-{home.lower()}-{date}", kq=kq, pq=pq, imb=kq + pq)
            if abs(kq + pq) >= 1:  # short-away one venue + long-away other must net 0
                e = f"MLB {away}@{home}:kAWAY{kq:+.0f}/pmAWAY{pq:+.0f}/imb{kq+pq:+.0f}"
                naked.append(e); naked_kt[e] = kt
        # generic sports pairings (sports_equiv_map): kalshi long-team YES must
        # net 1:1 against the PM moneyline position (take-take holds opposite
        # sides). MLB is excluded — the KXMLBGAME block above covers it.
        try:
            smap = __import__("json").loads(
                pathlib.Path("data/scan/sports_equiv_map.json").read_text())
            for m in smap.get("matches", []):
                if m.get("league") == "mlb":
                    continue
                kt, ps = m.get("kalshi_long_ticker"), m.get("pm_moneyline")
                if not kt or not ps:
                    continue
                kq, pq = kpos.get(kt, 0), ppos.get(ps, 0)
                if kq == 0 and pq == 0:
                    continue
                _row(name=m["teams"][:40], type=f"sports-{m['league']}", kt=kt, pm=ps,
                     kq=kq, pq=pq, imb=kq + pq)
                if abs(kq + pq) >= 1:
                    e = (f"sports-{m['league']} {m['teams'][:30]}:"
                         f"k{kq:+.0f}/pm{pq:+.0f}/imb{kq+pq:+.0f}")
                    naked.append(e); naked_kt[e] = kt
        except Exception:
            pass

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
            _row(name=r.id, type="political", kt=kt, pm=ps, kq=kq, pq=pq, imb=imb)
            if abs(imb) >= 1:
                e = f"{r.id}:kYES{kq:+.0f}/pmNet{pq:+.0f}/imb{imb:+.0f}"
                naked.append(e); naked_kt[e] = kt
            else:
                balanced += 1
        # orphans: venue positions no pair explains (visibility, not alerting)
        if rows is not None:
            seen_kt = {row.get("kt") for row in rows}
            seen_pm = {row.get("pm") for row in rows}
            for t2, q2 in kpos.items():
                if q2 != 0 and t2 not in seen_kt:
                    _row(name=t2, type="orphan-kalshi", kt=t2, pm=None, kq=q2, pq=None, imb=None)
            for s2, q2 in ppos.items():
                if q2 != 0 and s2 not in seen_pm:
                    _row(name=s2, type="orphan-pm", kt=None, pm=s2, kq=None, pq=q2, imb=None)
        _find_naked.kt_map = naked_kt
        return naked, balanced

    naked, balanced = _find_naked(kpos, ppos)
    if naked:
        # PM's positions API serves transient EMPTY and hours-STALE snapshots,
        # and the staleness is STICKY (a server-side cache survives back-to-back
        # reads — 2026-07-22 false alarms) — so a same-run confirm fetch is not
        # enough. Only alert an imbalance seen on TWO CONSECUTIVE RUNS (~60s
        # apart); the cache expires between runs, real nakedness persists.
        time.sleep(2)
        try:
            kpos2, ppos2 = _retry(kalshi_pos), _retry(pmus_pos)
            m2_, c2_ = _degraded(ppos2)
            if m2_ or c2_:
                # the CONFIRM read is partial — recomputing from it would paint
                # false nakedness (the hole behind the Mamdani ghost pages).
                # A first-read-naked that can't be confirmed on clean data
                # stays UNCONFIRMED this run.
                print("RECON degraded — PM positions API partial (platform incident); skipping naked evaluation")
                return
            kpos, ppos = kpos2, ppos2
            naked, balanced = _find_naked(kpos, ppos)
        except Exception:
            pass  # keep first-read result if the confirm fetch fails
    # positions snapshot for the dashboard Positions tab (venue truth)
    try:
        import json as _json
        pos_f = pathlib.Path("data/exec/positions.json")
        prev_doc = {}
        with __import__("contextlib").suppress(Exception):
            prev_doc = _json.loads(pos_f.read_text())
        # PM's positions API serves partial/stale snapshots: if this run's PM
        # position count collapsed vs the previous snapshot, the read is
        # degraded — keep the previous snapshot rather than painting false
        # NAKED rows on the dashboard.
        if prev_doc.get("pm_n") and len(ppos) < 0.6 * prev_doc["pm_n"]:
            raise RuntimeError("degraded PM read — keeping previous snapshot")
        rows = []
        _find_naked(kpos, ppos, rows)
        holds = []
        with __import__("contextlib").suppress(Exception):
            holds = _json.loads(pathlib.Path("data/exec/sports_naked.json").read_text())
        watched_kt = {h.get("kt"): h.get("qty") for h in holds if h.get("qty", 0) > 0}
        # venue finalization skew: an imbalanced row whose Kalshi market is no
        # longer active is SETTLING (the sweeper's job), not naked
        imb_kts = [r.get("kt") for r in rows
                   if r.get("kt") and r.get("imb") is not None and abs(r["imb"]) >= 1]
        kstat = {}
        for i in range(0, len(imb_kts), 50):
            with __import__("contextlib").suppress(Exception):
                for m in c.get(REST_BASE + "/markets",
                               params={"tickers": ",".join(imb_kts[i:i+50])}).json().get("markets", []):
                    kstat[m["ticker"]] = m.get("status")
        # live books for every positioned row (kalshi batched; pm per slug)
        all_kts = [r2.get("kt") for r2 in rows if r2.get("kt")]
        kbooks = {}
        for i in range(0, len(all_kts), 50):
            with __import__("contextlib").suppress(Exception):
                for m in c.get(REST_BASE + "/markets",
                               params={"tickers": ",".join(all_kts[i:i+50])}).json().get("markets", []):
                    kbooks[m["ticker"]] = (m.get("yes_bid_dollars"), m.get("yes_ask_dollars"),
                                           m.get("status"))
        hold_ref = {}
        for h in holds:
            if h.get("qty", 0) > 0:
                hold_ref[(h.get("kt"), h.get("side"))] = h.get("ref_px")
        for row in rows:
            row["watched_qty"] = watched_kt.get(row.get("kt"), 0)
            kb = kbooks.get(row.get("kt"), (None, None, None))
            st = kstat.get(row.get("kt")) or kb[2]
            row["settling"] = bool(st and st != "active")
            row["k_bid"], row["k_ask"] = kb[0], kb[1]
            ki = k_info.get(row.get("kt")) or {}
            pi = pm_info.get(row.get("pm")) or {}
            row["k_basis_ct"] = round(ki.get("basis_ct"), 4) if ki.get("basis_ct") is not None else None
            row["pm_basis_ct"] = pi.get("basis_ct")
            pmq = row.get("pq")
            with __import__("contextlib").suppress(Exception):
                b = get_bbo(c, row["pm"], timeout=8)
                row["pm_bid"] = (b.get("bestBid") or {}).get("value")
                row["pm_ask"] = (b.get("bestAsk") or {}).get("value")
            # locked/ct for hedged pairs: $1 payoff minus both held-side bases
            kq2 = row.get("kq") or 0
            if row.get("imb") is not None and abs(row["imb"]) < 1 and kq2 and                     row.get("k_basis_ct") is not None and row.get("pm_basis_ct") is not None:
                row["locked_ct"] = round(1.0 - row["k_basis_ct"] - row["pm_basis_ct"], 4)
            # hedge/flatten distance for watched holds (positive = cents away,
            # <=0 = available right now)
            side = "bid" if (row.get("imb") or 0) > 0 else "ask"
            ref = hold_ref.get((row.get("kt"), side))
            if ref is not None and row.get("imb") is not None and abs(row["imb"]) >= 1:
                margin = 0.015  # fee + min lock
                try:
                    if side == "bid":
                        if row.get("pm_bid") is not None:
                            row["hedge_away_c"] = round(((ref + margin) - float(row["pm_bid"])) * 100, 1)
                        if row.get("k_bid") is not None:
                            row["flatten_away_c"] = round(((ref + margin) - float(row["k_bid"])) * 100, 1)
                    else:
                        if row.get("pm_ask") is not None:
                            row["hedge_away_c"] = round((float(row["pm_ask"]) - (ref - margin)) * 100, 1)
                        if row.get("k_ask") is not None:
                            row["flatten_away_c"] = round((float(row["k_ask"]) - (ref - margin)) * 100, 1)
                except (TypeError, ValueError):
                    pass
        pos_f.write_text(_json.dumps(
            {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "pm_n": len(ppos), "rows": rows}, indent=1))
    except Exception:
        pass

    prev = set()
    try:
        prev = {r["imbalance_key"] for r in
                db.execute("SELECT imbalance_key FROM recon_sighting")}
    except Exception:
        pass
    cur = set(naked)
    try:
        kt_map0 = getattr(_find_naked, "kt_map", {})
        now_ns = time.time_ns()
        with db:
            if cur:
                db.execute("DELETE FROM recon_sighting WHERE imbalance_key NOT IN (%s)"
                           % ",".join("?" * len(cur)), tuple(cur))
            else:
                db.execute("DELETE FROM recon_sighting")
            for e in cur:
                db.execute("INSERT INTO recon_sighting"
                           " (imbalance_key, kalshi_ticker, first_seen_ns, last_seen_ns)"
                           " VALUES (?, ?, ?, ?)"
                           " ON CONFLICT(imbalance_key) DO UPDATE SET"
                           " last_seen_ns = excluded.last_seen_ns, runs = runs + 1",
                           (e, kt_map0.get(e), now_ns, now_ns))
    except Exception:
        pass
    confirmed = sorted(cur & prev)
    # settlement skew: an imbalance whose Kalshi market is FINALIZED is the
    # settlement sweeper's domain (one venue settles before the other) — not
    # a directional risk. Suppress those.
    try:
        kt_map = getattr(_find_naked, "kt_map", {})
        cks = sorted({kt_map[e] for e in confirmed if kt_map.get(e)})
        fin = set()
        for i in range(0, len(cks), 50):
            for m in c.get(REST_BASE + "/markets",
                           params={"tickers": ",".join(cks[i:i+50])}).json().get("markets", []):
                if m.get("status") != "active":
                    fin.add(m["ticker"])
        confirmed = [e for e in confirmed if kt_map.get(e) not in fin]
    except Exception:
        pass
    # probe-owned positions: the overnight research probes (pmus maker probe,
    # toxicity probe — separate workstream, board-approved) hold intentional
    # single-leg PM positions logged in their own files. Not naked — theirs.
    try:
        import json as _jp
        owned = set()
        for pf in ("data/exec/pmus_maker_probe.jsonl", "data/exec/toxicity_probe.jsonl",
                   "data/exec/leadlag_probe.jsonl"):
            with __import__("contextlib").suppress(Exception):
                for ln in pathlib.Path(pf).read_text().splitlines()[-800:]:
                    try:
                        r2 = _jp.loads(ln)
                    except ValueError:
                        continue
                    slug = r2.get("pm") or r2.get("pair")
                    if slug and r2.get("action") not in ("dry_quote", "dry_run_would_trade"):
                        owned.add(slug)
        smap2 = _jp.loads(pathlib.Path("data/scan/sports_equiv_map.json").read_text())
        slug_team = {m3["pm_moneyline"]: m3["teams"][:30] for m3 in smap2.get("matches", [])
                     if m3.get("pm_moneyline")}
        owned_frags = {slug_team[sl] for sl in owned if sl in slug_team}
        confirmed = [e for e in confirmed
                     if not (("k+0/" in e or "kYES+0/" in e)
                             and any(fr[:16] in e for fr in owned_frags))]
    except Exception:
        pass
    # scoped ignores: known tolerated dust (fragment + max size + expiry) —
    # growth beyond max_imb still pages
    try:
        import re as _re4
        ig = db.execute("SELECT fragment, max_imb FROM recon_ignore"
                        " WHERE expires_ns > ?", (time.time_ns(),)).fetchall()
        def _ignored(e):
            m4 = _re4.search(r"imb([+-]?\d+)", e)
            sz = abs(int(m4.group(1))) if m4 else 99
            return any(i["fragment"][:16] in e and sz <= i["max_imb"] for i in ig)
        confirmed = [e for e in confirmed if not _ignored(e)]
    except Exception:
        pass
    # suppress imbalances already REGISTERED as watched naked holds (the sports
    # rehedge watcher owns them; alerting every minute on a known hold is
    # noise) — only page when the imbalance EXCEEDS the watched quantity.
    try:
        import json as _json
        holds = _json.loads(pathlib.Path("data/exec/sports_naked.json").read_text())
        watched = {}
        for h in holds:
            watched[h.get("kt", "")] = watched.get(h.get("kt", ""), 0) + abs(float(h.get("qty", 0)))
        # simpler: rebuild suppression from the map — an entry is watched if a
        # hold's game name prefix appears in it and |imb| <= watched qty
        sup = []
        for e in confirmed:
            keep = True
            for h in holds:
                g0 = h.get("game", "")[:20]
                if g0 and (g0.replace("@", " vs ")[:14] in e or g0[:14] in e):
                    import re as _re3
                    m3 = _re3.search(r"imb([+-]?\d+)", e)
                    if m3 and abs(int(m3.group(1))) <= abs(float(h.get("qty", 0))):
                        keep = False
                        break
            if keep:
                sup.append(e)
        confirmed = sup
    except Exception:
        pass
    if confirmed:
        print("RECON NAKED " + " ".join(confirmed))
    elif cur:
        pass  # 1st sighting — stay silent; alert only if it persists next run
    else:
        print(f"RECON ok — {balanced} baskets balanced")


if __name__ == "__main__":
    main()
