"""Merge the NEH-2026 family (miner pass 3) into config/registry.yaml.

13 accepted pairwise exclusives {NEH-2026 YES, trigger YES} — the miner
verified the NEH PDF bullets are verbatim clones of the standalone markets'
rules (PDF recovered via Wayback; live S3 403s). 4 rejected entries kept as
scan-skipped documentation (ScanLoop skips verdict: rejected): the GOP
trifecta runoff trap, the looser-threshold meteor trap, and two NEH-family
implies with disjoint trigger lists. All vetted_by: agent.
"""

import yaml

from arbbot.registry.model import Registry

NEH = "86831555216352607458811867018842097191310281105831909967661155990095395482445"


def leg(mid):
    return {"venue": "polymarket", "market_id": str(mid), "side": "yes", "role": "taker"}


def E(id_, trigger_tok, direction, cit_ind, cit_neh, verdict, caveats, conf, oracle):
    return {
        "id": id_, "type": "exclusive", "legs": [leg(NEH), leg(trigger_tok)],
        "direction": direction,
        "rule_citations": {"individual": cit_ind, "neh_pdf": cit_neh},
        "verdict": verdict, "caveats": caveats, "vetted_by": "agent",
        "confidence": conf, "oracle_risk": oracle, "tranche": "long-tail",
    }


WINDOW_GUARD = ("Individual market window opens before NEH creation (Jan 6 2026); "
                "pair valid only while the individual remains unresolved (guard: "
                "universe maintenance marks resolved markets inactive)")

rels = [
    E("neh2026-x-trump-out-before-2027",
      "59252515735652674747158950210016502214756531287333895140318848923768750410355",
      "individual YES => NEH bullet 1 fires => NEH-2026 NO; both windows end Dec 31 2026 11:59 PM ET",
      "Resolves Yes if 'Donald Trump ... ceases to be the President of the United States for any period of time by December 31, 2026' ... 'Only permanent removal from office will qualify.'",
      "Bullet 1: identical text including 25th Amendment carve-outs and announcement-counts clause.",
      "equivalent", ["Rule text verbatim-identical", WINDOW_GUARD], 0.95, "low"),
    E("neh2026-x-china-invades-taiwan-2026",
      "94559586571241563470235664821564670251180951772614764383113614156422396181162",
      "individual YES => NEH bullet 2 fires => NEH-2026 NO",
      "'China commences a military offensive intended to establish control over any portion of the Republic of China (Taiwan) by December 31, 2026' ... 'uninhabited islands will not qualify.'",
      "Bullet 2: verbatim-identical criteria and resolution sources.",
      "equivalent", ["Verbatim-identical; $38.8M individual volume"], 0.95, "low"),
    E("neh2026-x-xi-jinping-out-before-2027",
      "32338220190071351435772801779725302244575775216413325951443816017994629993401",
      "individual YES => NEH bullet 3 fires => NEH-2026 NO",
      "'Xi Jinping is removed from power for any length of time between July 3, 2025, and December 31, 2026' with dismissed/detained/disqualified definition.",
      "Bullet 3: identical removal definition.",
      "equivalent-with-caveat", [WINDOW_GUARD], 0.95, "low"),
    E("neh2026-x-us-invades-iran-before-2027",
      "55115078421062885512539156303747803058407616201213034911037320915726138659123",
      "individual YES => NEH bullet 4 fires => NEH-2026 NO",
      "'United States commences a military offensive intended to establish control over any portion of Iran by December 31, 2026' (de-facto baseline Nov 4 2025).",
      "Bullet 4: same offensive test, de-facto baseline at NEH creation (~Jan 6 2026).",
      "equivalent-with-caveat",
      ["Only delta: de-facto-territory baseline date; no US-Iran territorial change between the dates",
       "$45M individual volume, YES ~0.305 — dominant NEH-NO driver, watch this pair first"],
      0.9, "low"),
    E("neh2026-x-iranian-regime-falls-before-2027",
      "10991849228756847439673778874175365458450913336396982752046655649803657501964",
      "individual YES => NEH bullet 5 fires => NEH-2026 NO",
      "'Islamic Republic of Iran's current ruling regime is overthrown, collapsed, or otherwise ceases to govern by December 31, 2026' with core-structures test.",
      "Bullet 5: paragraph-for-paragraph identical.",
      "equivalent", ["Full rules identical (Supreme Leader/IRGC dissolution test)"], 0.95, "medium"),
    E("neh2026-x-btc-1m-by-eoy-2026",
      "18571303954259773157233385333455041418270179723020753158306812650937565180666",
      "individual YES => NEH bullet 6 (BTC up 1M) fires => NEH-2026 NO",
      "Binance BTC/USDT 1m candle High >= $1M between Nov 24 2025 14:00 and Dec 31 2026 23:59 ET.",
      "Bullet 6: same mechanical Binance BTC/USDT 1m High oracle.",
      "equivalent-with-caveat", ["Identical mechanical oracle", WINDOW_GUARD], 0.93, "low"),
    E("neh2026-x-btc-dip-10k-by-eoy-2026",
      "42439528758178981890895605039938161151582654744978607695897321288347422769852",
      "individual YES => NEH bullet 7 (BTC down 10k) fires => NEH-2026 NO",
      "Binance BTC/USDT 1m candle Low <= $10k in the same window.",
      "Bullet 7: same mechanical oracle.",
      "equivalent-with-caveat", ["Identical mechanical oracle", WINDOW_GUARD], 0.93, "low"),
    E("neh2026-x-epstein-alive-before-2027",
      "57692461228141098196519006129212454078633070644580607609751365664388305515775",
      "individual YES => NEH bullet 8 fires => NEH-2026 NO",
      "'incontrovertible proof is publicly revealed that Jeff Epstein ... is still alive' by Dec 31 2026.",
      "Bullet 8: verbatim-identical criterion.",
      "equivalent-with-caveat",
      ["'Incontrovertible proof' is a judgment call, but the SAME judgment on both legs", WINDOW_GUARD],
      0.93, "medium"),
    E("neh2026-x-russia-invades-nato-by-eoy-2026",
      "23530003628454702082168799022501270830098851824883932803252713402311376262723",
      "individual YES => NEH bullet 10 fires => NEH-2026 NO",
      "'Russia commences a military offensive ... any portion of any NATO country by December 31, 2026' (May 28 2025 de-facto baseline, grey-zone clause).",
      "Bullet 10: identical incl. same baseline and grey-zone clause.",
      "equivalent-with-caveat",
      ["METADATA TRAP: Gamma endDate stale (2025-12-31) while description governs Dec 31 2026 — date-sanity filters must not auto-drop this leg"],
      0.93, "low"),
    E("neh2026-x-9pt0-earthquake-before-2027",
      "29637922150133247837494999268850861972506938929073189065972803949119432654860",
      "individual YES => NEH bullet 12 fires => NEH-2026 NO",
      "USGS M>=9.0 anywhere on Earth, Dec 8 2025 - Dec 31 2026, Jan 31 2027 late-listing extension.",
      "Bullet 12: identical USGS source, extension, and 24h revision clause.",
      "equivalent-with-caveat", [WINDOW_GUARD], 0.95, "low"),
    E("neh2026-x-vei6-volcano-2026",
      "107695993624925821118492288099166196746925552704590535397101229736666098667264",
      "individual YES => NEH bullet 13 fires => NEH-2026 NO (same event; oracle snapshots differ)",
      "GVP VEI>=6 in 2026, final figure as of Mar 31 2027 (prior updates not final).",
      "Bullet 13: GVP 'at any point during time frame', Feb 28 2027 fallback.",
      "equivalent-with-caveat",
      ["Different GVP snapshot dates (Feb 28 vs Mar 31 2027): late reclassification can pay both legs — real oracle-timing risk",
       "Individual open to Mar 2027 (capital lockup)"],
      0.8, "medium"),
    E("neh2026-x-1mt-meteor-2026",
      "36842604344917303933906831960510303955156173049735733354009818804376183187668",
      "one-directional: individual (>=1000kt) strictly implies NEH bullet 14 (>=250kt) => exclusivity valid",
      "CNEOS bolide impact energy >= 1 megaton, Jan 1 - Dec 31 2026.",
      "Bullet 14: >= 250 kilotons, identical CNEOS source and Feb 28 2027 fallback.",
      "equivalent-with-caveat",
      ["NOT threshold-matched (1000kt vs 250kt): individual is STRICTER so exclusivity holds, but a 250-999kt bolide loses both legs — exclusivity only, never a partition"],
      0.9, "low"),
    E("neh2026-x-trump-greenland-before-2027",
      "5161623255678193352839985156330393796378434470119114669671615782853260939535",
      "individual YES => NEH bullet 11 fires => NEH-2026 NO",
      "'official announcement ... that the majority of Greenland will come under US sovereignty ... even if the actual transfer of sovereignty is yet to occur'; social media posts excluded. $35M volume.",
      "Bullet 11: verbatim-identical including announcement and social-media clauses.",
      "equivalent", ["Verbatim-identical (miner-verified vs Wayback PDF)"], 0.95, "low"),
    # --- documented traps: kept scan-skipped (verdict rejected) ---
    E("neh2026-x-gop-trifecta-supermajority-REJECTED",
      "115498786152800454118340234964704317968880116149616938823996096991742421071178",
      "REJECTED: runoff-extension clause lets the individual resolve YES after NEH's window closes — both legs can pay",
      "'If a required runoff ... could change the market's outcome, the market will remain open until that runoff is conclusively called.'",
      "Bullet 9: control 'during this market's above-specified time frame' (through Dec 31 2026); new Congress seats Jan 3 2027 — outside the window.",
      "rejected",
      ["TRAP: Jan 2027 runoff pays the individual while NEH-2026 already resolved YES; also seating-date ambiguity => UMA dispute risk high"],
      0.85, "high"),
    E("neh2026-x-100kt-meteor-REJECTED",
      "67801523885917424674618253192019492976876726656774783606239686807904933496712",
      "REJECTED: individual (>=100kt) is LOOSER than NEH bullet 14 (>=250kt) — a 100-249kt bolide pays both legs",
      "'total impact energy greater than or equal to 100 kilotons'.",
      "Bullet 14 requires >= 250 kilotons.",
      "rejected",
      ["Canonical looser-threshold trap; applies a fortiori to the 10kt and 5kt meteor markets"],
      0.97, "low"),
]

# family-implies rejections (documented; disjoint trigger lists)
rels.append({
    "id": "neh2026-implies-neh-july-REJECTED", "type": "implies",
    "legs": [leg(NEH), leg("80493687565122693017908683853372036224638209989752774425535587727719596025842")],
    "direction": "REJECTED: NEH-July trigger list (World Cup, Iran deal, WTI $150, Fed change, Ukraine ceasefire) is DISJOINT from NEH-2026's — no containment, no implication",
    "rule_citations": {"note": "'Fed decides any change in July' makes July-Something near-certain regardless of the 2026 list"},
    "verdict": "rejected", "caveats": ["Family branding does not imply logical nesting"],
    "vetted_by": "agent", "confidence": 0.97, "oracle_risk": "low", "tranche": "long-tail",
})
rels.append({
    "id": "neh2026-implies-neh-satoshi-REJECTED", "type": "implies",
    "legs": [leg(NEH), leg("42047316132442676984544484222930032486176533443316099697295069962907791408801")],
    "direction": "REJECTED: 'Epstein is Satoshi' can be confirmed posthumously (does not imply 'Epstein alive'); Satoshi-wallet movement on neither list",
    "rule_citations": {"note": "Superficially tempting via the Epstein cameo; lists are disjoint"},
    "verdict": "rejected", "caveats": ["Documented for the Stage-4 agent's training set"],
    "vetted_by": "agent", "confidence": 0.95, "oracle_risk": "low", "tranche": "long-tail",
})

reg = yaml.safe_load(open("config/registry.yaml"))
existing = {r["id"] for r in reg["relationships"]}
added = [r for r in rels if r["id"] not in existing]
reg["relationships"].extend(added)
Registry.model_validate(reg)
yaml.safe_dump(reg, open("config/registry.yaml", "w"), sort_keys=False, width=120)

r = Registry.model_validate(reg)
from arbbot.models.core import Venue
pm = [m for v, m in r.market_ids() if v is Venue.POLYMARKET]
print(f"registry: {len(r.relationships)} relationships (+{len(added)}), pm tokens {len(pm)}, "
      f"tradable {sum(1 for x in r.relationships if x.tradable)}")
