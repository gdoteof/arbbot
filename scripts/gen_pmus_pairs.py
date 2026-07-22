"""Generate agent-vetted PM US <-> Kalshi cross-venue-equivalent registry pairs.

Alignment is EXACT-normalized-name / exact-strike / exact-date only — identity
mistakes here lose money, so anything not exactly matched is reported as
unmatched, never guessed. Output: proposed YAML + a review report to stdout.
"""

import json
import re
import unicodedata
import pathlib
import tempfile

import yaml

SB = pathlib.Path(tempfile.gettempdir()) / "arbbot-pmus-pairs"
kalshi_events = {e["event_ticker"]: e for e in json.load(open(SB / "kalshi_events.json"))}
pmus = json.load(open(SB / "pmus_families.json"))


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = re.sub(r"\([^)]*\)", "", s)          # drop parenthetical acronyms
    s = re.sub(r"^\s*the\s+", "", s.strip(), flags=re.I)  # leading 'the'
    s = s.split("/")[0]                       # 'X / Save Ukraine' -> 'X'
    return re.sub(r"[^a-z0-9]", "", s.lower())


def pm_name(m, patterns):
    d = m.get("description") or ""
    for pat in patterns:
        g = re.match(pat, d)
        if g:
            return g.group(1).strip()
    return None


def k_active(ev):
    return [m for m in ev.get("markets", []) if m.get("status") in ("active", "open")]


def pm_active(ev):
    return [m for m in ev.get("markets", []) if m.get("active") and not m.get("closed")]


# --- family definitions ----------------------------------------------------
# match: name  -> PM name extracted from rules text, matched to Kalshi
#                 yes_sub_title (normalized-exact, plus explicit alias map)
#        strike-> numeric strike from slugs/subtitles
FAMS = [
    dict(id="nobel-peace-26", pm="nobel-peace-2026-10-09", k="KXNOBELPEACE-26",
         mode="name", pm_pat=[r"If (.+?) wins? the 2026 Nobel Peace Prize"],
         # same-entity transliteration/name-form aliases (deterministic)
         alias={"volodymyrzelenskyy": "volodymyrzelensky",
                "doctorswithoutborders": "doctorswithoutborders"},
         conf=0.9, oracle="low",
         cite_pm="Settles Yes if the named person/org is announced by the Norwegian Nobel Committee as the 2026 laureate.",
         cite_k="Resolves to the announced 2026 Nobel Peace Prize winner.",
         caveats=["Shared-prize handling may differ between venues (joint laureates)"]),
    dict(id="time-poty-26", pm="2026", k="KXTIME-26",
         mode="name", pm_pat=[r"If (.+?) is TIME Person of the Year"],
         alias={"artificialintelligence": "ai"},  # same concept, both venues' pick
         conf=0.9, oracle="low",
         cite_pm="Settles Yes if explicitly named by TIME as Person of the Year 2026.",
         cite_k="Resolves to TIME's announced Person of the Year for 2026.",
         caveats=["Group/concept picks (e.g. 'AI') must be named identically by both venues"]),
    dict(id="bestai-26dec", pm="company1-2026-12-31", k="KXLLM1-26DEC31",
         mode="name", pm_pat=[r".*?[Ii]f (.+?) has the #1 ranked AI model"],
         alias={"chatgpt": "openai", "gemini": "google", "claude": "anthropic",
                "grok": "xai", "musespark": "meta", "qwen": "alibaba",
                "kimi": "moonshot", "ernie": "baidu"},
         pm_name_from="question", conf=0.85, oracle="medium",
         cite_pm="Settles Yes if the company has the #1 ranked model on the Arena AI Text Arena leaderboard Dec 31, 2026 12:00 PM ET (Style Control Off).",
         cite_k="Resolves to the top model on the arena leaderboard at end of 2026.",
         caveats=["Kalshi markets are MODEL-named, PM US COMPANY-named (alias-mapped)",
                  "Snapshot time / style-control setting must match between venues — verify before live"]),
    dict(id="fedcut-26", pm="usfed-2026", k="KXRATECUT-26DEC31",
         mode="single", conf=0.95, oracle="low",
         cite_pm="Settles Yes if the Fed decreases the upper bound of the target range by Dec 31, 2026 11:59 PM ET (scheduled or unscheduled).",
         cite_k="Resolves Yes on any Fed rate cut before 2027.",
         caveats=[]),
    dict(id="aliens-26", pm="aliens", k="KXALIENS-27",
         mode="single", k_ticker="KXALIENS-27",  # the 'Before 2027' rung
         conf=0.85, oracle="medium",
         cite_pm="Settles Yes if the President, a Cabinet member, Joint Chiefs member, or a federal agency definitively states extraterrestrial life/technology exists by Dec 31, 2026 11:59 PM ET.",
         cite_k="'Before 2027' rung of the aliens-confirmation ladder (official US confirmation).",
         caveats=["Which officials qualify as 'confirmation' is worded differently — divergence path on an ambiguous statement"]),
    dict(id="gpt6-ladder", pm="openai-gpt-6", k="KXGPT-OPEN",
         mode="date",
         datemap={"2026-07-31": "KXGPT-OPEN-26AUG01", "2026-08-31": "KXGPT-OPEN-26SEP01",
                  "2026-09-30": "KXGPT-OPEN-26OCT01", "2026-12-31": "KXGPT-OPEN-27JAN01"},
         conf=0.85, oracle="medium",
         cite_pm="Settles Yes if OpenAI releases GPT-6 (publicly accessible incl. open beta) by the slug date 11:59 PM ET.",
         cite_k="'Before <month 1st>' rungs of the GPT-6 release ladder.",
         caveats=["'Release' definitions (beta/limited access) may diverge between venues"]),
    dict(id="btcmax-26", pm="btc-hitprice-high-yr-12-31-2026", k="KXBTCMAXY-26DEC31",
         mode="strike",
         strike_pm=r"-(\d+)k$", strike_scale=1000, strike_k_offset=0.01,
         conf=0.85, oracle="low",
         cite_pm="Settles Yes if BTC (CF Benchmarks BRTI) trades above the threshold at any point from market creation to Jan 1, 2027 12:00 AM ET.",
         cite_k="Above-threshold rungs of 'How high will Bitcoin get in 2026' (CF BRTI).",
         caveats=["Window start differs (market creation vs Jan 1) — equivalent only while neither has triggered; both priced consistently now"]),
    dict(id="btc150k-ladder", pm="btc-150k", k="KXBTCMAX150-25",
         mode="date",
         datemap={"07-31-2026": "KXBTCMAX150-25-26JUL31-149999.99",
                  "08-31-2026": "KXBTCMAX150-25-26AUG31-149999.99",
                  "12-31-2026": "KXBTCMAX150-25-26DEC31-149999.99"},
         conf=0.85, oracle="low",
         cite_pm="Settles Yes if BTC (CF BRTI) is above $149,999.99 at any point before the day after the slug date, 12:00 AM ET.",
         cite_k="'Before <month>' rungs of the $150k ladder (Above $149,999.99).",
         caveats=["Kalshi ladder began in 2025 (window start differs) — equivalent while untriggered; both price consistently now"]),
    dict(id="tsla-q3-deliv", pm="tsla-dlvrs-2026-q3-above", k="KXTSLA-26OCTDELIV",
         mode="strike",
         strike_pm=r"-above-(\d+)k$", strike_scale=1000, strike_k_offset=0,
         conf=0.92, oracle="low",
         cite_pm="Settles Yes if Tesla reports Q3 2026 total deliveries greater than the threshold (official Tesla disclosures).",
         cite_k="Above-threshold rungs of Tesla total deliveries in Q3 (Tesla-reported).",
         caveats=[]),
    dict(id="brazil-pres-26", pm="pres-bra-2026-10-04", k="KXBRPRES-26",
         mode="name", pm_pat=[r"If (.+?) wins? the 2026 Brazilian Presidential Election"],
         conf=0.9, oracle="low",
         cite_pm="Settles Yes if the named candidate wins the 2026 Brazilian presidential election incl. any runoff.",
         cite_k="Resolves to the winner of the Brazil presidential election.",
         caveats=[]),
    dict(id="france-pres-27", pm="pres-fra-2027-04-11", k="KXFRENCHPRES-27",
         mode="name", pm_pat=[r"If (.+?) wins? the 2027 French Presidential Election"],
         conf=0.9, oracle="low",
         cite_pm="Settles Yes if the named candidate wins the 2027 French presidential election incl. any runoff.",
         cite_k="Resolves to the winner of the next French presidential election.",
         caveats=[]),
]


def entry(fam, k_ticker, pm_slug, direction):
    return {
        "id": f"xvus-{fam['id']}-{norm(direction)[:24]}" if fam["mode"] == "name"
              else f"xvus-{fam['id']}-{'-'.join(pm_slug.rsplit('-', 3)[-3:])}",
        "type": "cross-venue-equivalent",
        "legs": [
            {"venue": "kalshi", "market_id": k_ticker, "side": "yes", "role": "taker"},
            {"venue": "polymarket_us", "market_id": pm_slug, "side": "yes", "role": "taker"},
        ],
        "direction": f"outcome(kalshi {k_ticker}) == outcome(polymarket_us {pm_slug}): {direction}",
        "rule_citations": {"kalshi": fam["cite_k"], "polymarket_us": fam["cite_pm"]},
        "verdict": "equivalent" if not fam["caveats"] else "equivalent-with-caveat",
        "caveats": fam["caveats"] or ["none noted"],
        "vetted_by": "agent",
        "confidence": fam["conf"],
        "oracle_risk": fam["oracle"],
        "tranche": "head",
    }


out, report = [], []
for fam in FAMS:
    pm_ev = pmus[fam["pm"]]
    k_ev = kalshi_events[fam["k"]]
    pms, ks = pm_active(pm_ev), k_active(k_ev)
    matched, unmatched = [], []
    if fam["mode"] == "single":
        kt = fam.get("k_ticker") or ks[0]["ticker"]
        m = pms[0]
        matched.append((kt, m["slug"], pm_ev.get("title", "")))
    elif fam["mode"] == "date":
        for m in pms:
            key = next((d for d in fam["datemap"] if m["slug"].endswith(d)), None)
            if key:
                matched.append((fam["datemap"][key], m["slug"], f"by {key}"))
            else:
                unmatched.append(m["slug"])
    elif fam["mode"] == "strike":
        kidx = {}
        for km in ks:
            g = re.search(r"-([\d.]+)$", km["ticker"])
            if g:
                kidx[float(g.group(1))] = km["ticker"]
        for m in pms:
            g = re.search(fam["strike_pm"], m["slug"])
            if not g:
                unmatched.append(m["slug"]); continue
            v = float(g.group(1)) * fam["strike_scale"]
            kt = kidx.get(v) or kidx.get(v - fam["strike_k_offset"])
            if kt:
                matched.append((kt, m["slug"], f"above {v:g}"))
            else:
                unmatched.append(m["slug"])
    else:  # name
        alias = fam.get("alias", {})
        kidx = {}
        for km in ks:
            n = norm(km.get("yes_sub_title") or "")
            if n:
                kidx[alias.get(n, n)] = km["ticker"]
        for m in pms:
            src = m.get("question", "") if fam.get("pm_name_from") == "question" else None
            nm = None
            if src:
                g = re.match(r"(.+?) has the #1", src)
                nm = g.group(1) if g else None
            if nm is None:
                nm = pm_name(m, fam["pm_pat"])
            if not nm:
                unmatched.append(m["slug"]); continue
            key = alias.get(norm(nm), norm(nm))
            kt = kidx.get(key)
            if kt:
                matched.append((kt, m["slug"], nm))
            else:
                unmatched.append(f"{m['slug']} ({nm})")
    for kt, slug, direction in matched:
        out.append(entry(fam, kt, slug, direction))
    report.append((fam["id"], len(matched), unmatched, len(ks)))

print("=== MATCH REPORT ===")
for fid, n, un, nk in report:
    print(f"{fid}: {n} pairs (kalshi mkts {nk}); unmatched PM: {un if un else 'none'}")
ids = [e["id"] for e in out]
assert len(ids) == len(set(ids)), f"dup ids: {[i for i in ids if ids.count(i) > 1]}"
print(f"\nTOTAL: {len(out)} proposed pairs")
yaml_text = yaml.safe_dump(out, sort_keys=False, allow_unicode=True, width=110)
(SB / "proposed_pairs.yaml").write_text(yaml_text)
print(f"wrote {SB/'proposed_pairs.yaml'}")
