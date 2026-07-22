"""Merge miner pass-2 (cross-venue + Kalshi-native) into config/registry.yaml.
All entries vetted_by: agent — not tradable until Geoff's human verdict."""

import yaml

from arbbot.registry.model import Registry


def leg(venue, mid):
    return {"venue": venue, "market_id": str(mid), "side": "yes", "role": "taker"}


def E(id_, type_, legs, direction, citations, verdict, caveats, conf, oracle, tranche):
    return {
        "id": id_, "type": type_, "legs": legs, "direction": direction,
        "rule_citations": citations, "verdict": verdict, "caveats": caveats,
        "vetted_by": "agent", "confidence": conf, "oracle_risk": oracle,
        "tranche": tranche,
    }


K = "kalshi"
P = "polymarket"

XV_NOM_CITE_K = "If {name} wins and accepts the nomination for the Presidency for the {party} party in 2028, then the market resolves to Yes."
XV_NOM_CITE_P = "This market will resolve to “Yes” if the named individual wins and accepts the 2028 nomination of the {party} Party for U.S. president."
NOM_CAVEAT = "PM adds a replacement-of-nominee clause (resolves on original convention winner); Kalshi rules are silent on replacement — post-convention swap is the divergence path"

new_rels = [
    E("xv-aliens-confirmed-before-2027", "cross-venue-equivalent",
      [leg(K, "KXALIENS-27"),
       leg(P, "107505882767731489358349912513945399560393482969656700824895970500493757150417")],
      "outcome(kalshi KXALIENS-27) == outcome(polymarket aliens-before-2027)",
      {"kalshi": "If the President, any member of the Cabinet, any member of the Joint Chiefs of Staff, or any US federal agency definitively states that extraterrestrial life or technology exists before Jan 1, 2027, then the market resolves to Yes.",
       "polymarket": "Resolves Yes if the President, any Cabinet member, any Joint Chiefs member, or any US federal agency definitively states that extraterrestrial life or technology exists by December 31, 2026, 11:59 PM ET."},
      "equivalent",
      ["Criterion sentences word-for-word parallel; deadlines are the same instant",
       "Residual: UMA vs Kalshi judging whether a statement was 'definitive' on a borderline announcement"],
      0.93, "medium", "head"),
    E("xv-venezuela-leader-grenell", "cross-venue-equivalent",
      [leg(K, "KXVENEZUELALEADER-26DEC31-RGRE"),
       leg(P, "63041927234014862550752749844577619438319946530923167906649363118591050055991")],
      "outcome(kalshi RGRE) == outcome(polymarket Grenell)",
      {"kalshi": "If Richard Grenell officially holds the position of the head of state of Venezuela on Dec 31, 2026 at 10:00 AM ET, then the market resolves to Yes.",
       "polymarket": "This market will resolve to the individual who officially holds the position of the head of state of Venezuela on Dec 31, 2026 at 12 PM ET."},
      "equivalent-with-caveat",
      ["TIMING MISMATCH: Kalshi snapshots 10:00 AM ET, PM 12:00 PM ET — 2-hour split window",
       "PM has UN-list fallback + primary-status tiebreak; Kalshi has none — contested government can diverge"],
      0.85, "high", "head"),
    E("xv-venezuela-leader-donovan", "cross-venue-equivalent",
      [leg(K, "KXVENEZUELALEADER-26DEC31-FDON"),
       leg(P, "102488069217181040733050347118082609606893446345922306325497036296077206149582")],
      "outcome(kalshi FDON) == outcome(polymarket Donovan)",
      {"kalshi": "If Frank Donovan officially holds the position of the head of state of Venezuela on Dec 31, 2026 at 10:00 AM ET, then the market resolves to Yes.",
       "polymarket": "'Officially holds' = formally appointed, confirmed (if required), and sworn in, or otherwise confirmed by official government information."},
      "equivalent-with-caveat",
      ["Same 10AM/12PM mismatch; PM's appointed/confirmed/sworn-in test stricter than Kalshi's bare 'officially holds'"],
      0.85, "high", "head"),
    E("xv-putin-out-2026", "cross-venue-equivalent",
      [leg(K, "KXLEADERSOUT-27JAN01-VPUTRUS"),
       leg(P, "350977769852917329387037893294763093471844346281449484439085576212613048126")],
      "outcome(kalshi VPUTRUS) == outcome(polymarket putin-out)",
      {"kalshi": "If Vladimir Putin has either officially announced their intention to leave as President of Russia or has actually left President of Russia before Jan 1, 2027, then the market resolves to Yes.",
       "polymarket": "Detention, effective removal, or being permanently prevented from fulfilling the duties qualifies for Yes."},
      "equivalent-with-caveat",
      ["SCOPE MISMATCH: PM counts de facto removal without official act; Kalshi needs official announcement or actual departure — coup/incapacity can split the pair"],
      0.80, "high", "head"),
    E("xv-dem-nom-2028-newsom", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMD-28-GN"),
       leg(P, "54533043819946592547517511176940999955633860128497669742211153063842200957669")],
      "outcome(kalshi GN) == outcome(polymarket Newsom dem nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="Gavin Newsom", party="Democratic"),
       "polymarket": XV_NOM_CITE_P.format(party="Democratic")},
      "equivalent", [NOM_CAVEAT], 0.92, "low", "head"),
    E("xv-dem-nom-2028-mamdani", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMD-28-ZMAM"),
       leg(P, "19323997520040734631220675039559156784534982703459512097490198935565229528445")],
      "outcome(kalshi ZMAM) == outcome(polymarket Mamdani dem nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="Zohran Mamdani", party="Democratic"),
       "polymarket": XV_NOM_CITE_P.format(party="Democratic")},
      "equivalent", [NOM_CAVEAT], 0.92, "low", "head"),
    E("xv-dem-nom-2028-michelle-obama", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMD-28-MO"),
       leg(P, "83723134981785004242996949681785273460678873813693556756054904164484296299719")],
      "outcome(kalshi MO) == outcome(polymarket Michelle Obama dem nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="Michelle Obama", party="Democratic"),
       "polymarket": XV_NOM_CITE_P.format(party="Democratic")},
      "equivalent", [NOM_CAVEAT], 0.92, "low", "head"),
    E("xv-dem-nom-2028-bernie", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMD-28-BS"),
       leg(P, "113510300123120868861281132282742729065975487298059944817273881893086209196585")],
      "outcome(kalshi BS) == outcome(polymarket Sanders dem nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="Bernie Sanders", party="Democratic"),
       "polymarket": XV_NOM_CITE_P.format(party="Democratic")},
      "equivalent", [NOM_CAVEAT], 0.92, "low", "head"),
    E("xv-rep-nom-2028-vance", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMR-28-JDV"),
       leg(P, "40081275558852222228080198821361202017557872256707631666334039001378518619916")],
      "outcome(kalshi JDV) == outcome(polymarket Vance rep nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="J.D. Vance", party="Republican"),
       "polymarket": XV_NOM_CITE_P.format(party="Republican")},
      "equivalent", [NOM_CAVEAT, "Most liquid pair in the set (Kalshi 39/40c, $3.7M vol)"],
      0.92, "low", "head"),
    E("xv-rep-nom-2028-desantis", "cross-venue-equivalent",
      [leg(K, "KXPRESNOMR-28-RDS"),
       leg(P, "12127975650736113116407758794754973741525044713193258995588171654429754588610")],
      "outcome(kalshi RDS) == outcome(polymarket DeSantis rep nom)",
      {"kalshi": XV_NOM_CITE_K.format(name="Ron DeSantis", party="Republican"),
       "polymarket": XV_NOM_CITE_P.format(party="Republican")},
      "equivalent", [NOM_CAVEAT], 0.92, "low", "head"),
    E("xv-pres-2028-winner-vance", "cross-venue-equivalent",
      [leg(K, "KXPRESPERSON-28-JVAN"),
       leg(P, "16040015440196279900485035793550429453516625694844857319147506590755961451627")],
      "outcome(kalshi JVAN) == outcome(polymarket Vance wins presidency)",
      {"kalshi": "If J.D. Vance is the next person inaugurated as President for the term beginning in 2029, then the market resolves to Yes.",
       "polymarket": "Resolves once AP, Fox, and NBC call the race for the same candidate; inauguration fallback Jan 20, 2029."},
      "equivalent-with-caveat",
      ["SETTLEMENT MISMATCH: Kalshi = who is INAUGURATED; PM = race call (inauguration only fallback) — death/replacement between call and inauguration splits the pair",
       "Timing: PM can resolve Nov 2028, Kalshi waits for inauguration"],
      0.85, "medium", "head"),
    E("xv-pres-2028-winner-newsom", "cross-venue-equivalent",
      [leg(K, "KXPRESPERSON-28-GNEWS"),
       leg(P, "98250445447699368679516529207365255018790721464590833209064266254238063117329")],
      "outcome(kalshi GNEWS) == outcome(polymarket Newsom wins presidency)",
      {"kalshi": "If Gavin Newsom is the next person inaugurated as President for the term beginning in 2029, then the market resolves to Yes.",
       "polymarket": "This market will resolve to the person who wins the 2028 US Presidential Election. Sources: AP, Fox News, NBC."},
      "equivalent-with-caveat",
      ["Same inaugurated-vs-race-call mismatch as the Vance presidency pair"],
      0.85, "medium", "head"),
    # long tail
    E("xv-f1-2026-bearman", "cross-venue-equivalent",
      [leg(K, "KXF1-26-OB"),
       leg(P, "36651695143257976296316443097895676898349243442057744955120299247443104698921")],
      "outcome(kalshi OB) == outcome(polymarket Bearman 2026 F1 champion)",
      {"kalshi": "If Oliver Bearman wins the F1 Drivers Championship, then the market resolves to Yes.",
       "polymarket": "Resolves to the listed driver that finishes 1st in the driver standings for the 2026 F1 season."},
      "equivalent-with-caveat",
      ["Kalshi one-liner omits tie/cancellation/elimination handling PM spells out — canceled season diverges",
       "Volume wildly asymmetric ($12.9M PM vs $8.9k Kalshi)"],
      0.85, "medium", "long-tail"),
    E("kalshi-greenland-purchase-implies-acquisition", "implies",
      [leg(K, "KXGREENLAND-29"), leg(K, "KXGREENTERRITORY-29")],
      "P(US purchases part of Greenland from Denmark before Jan 20 2029) <= P(US acquires any part before Jan 21 2029)",
      {"KXGREENLAND-29": "If the United States purchases at least part of Greenland from Denmark before January 20, 2029, then the market resolves to Yes.",
       "KXGREENTERRITORY-29": "If the United States acquires any part of Greenland before Jan 21, 2029, then the market resolves to Yes."},
      "equivalent-with-caveat",
      ["Requires 'acquires' to encompass purchase; announced-only purchase agreement could be construed differently"],
      0.88, "medium", "long-tail"),
    E("xv-hantavirus-implies-any-pandemic", "implies",
      [leg(P, "51508280778202349361616850684455231843716212176724253736363122559269229712002"),
       leg(K, "KXNEWOUTBREAK-P-26")],
      "P(WHO characterizes Hantavirus as pandemic in 2026) <= P(any disease becomes a pandemic in 2026)",
      {"polymarket": "Resolves Yes if the WHO explicitly characterizes Hantavirus (or HPS/HFRS/related outbreak) as a pandemic in an official public communication by Dec 31, 2026 11:59 PM ET.",
       "kalshi": "If any disease becomes a pandemic in 2026, then the market resolves to Yes."},
      "equivalent-with-caveat",
      ["Kalshi's 'becomes a pandemic' adjudication standard unstated; if it demands more than WHO characterization the implication could leak"],
      0.85, "medium", "long-tail"),
    E("kalshi-aliens-confirmation-date-ladder", "date-ladder",
      [leg(K, t) for t in ("KXALIENS-26AUG", "KXALIENS-27-26SEP", "KXALIENS-26OCT",
                           "KXALIENS-26NOV", "KXALIENS-26DEC", "KXALIENS-27",
                           "KXALIENS-27-28", "KXALIENS-27-29")],
      "P(confirmed before Aug 1 26) <= ... <= P(before Jan 1 27) <= P(before Jan 1 28) <= P(before Jan 20 29)",
      {"kalshi": "Rules differ only in date ('...definitively states that extraterrestrial life or technology exists before <date>') — absolute cumulative criterion, early Yes implies later Yes."},
      "equivalent",
      ["No 'between market creation and' qualifier — genuine monotone ladder",
       "KXALIENS-27 rung doubles as the cross-venue leg ($25.9M vol)"],
      0.95, "low", "long-tail"),
    E("kalshi-iran-deal-date-ladder", "date-ladder",
      [leg(K, t) for t in ("KXUSAIRANAGREEMENT-27-26AUG", "KXUSAIRANAGREEMENT-27-26SEP",
                           "KXUSAIRANAGREEMENT-27-26OCT", "KXUSAIRANAGREEMENT-27-26NOV",
                           "KXUSAIRANAGREEMENT-27-26DEC", "KXUSAIRANAGREEMENT-27",
                           "KXUSAIRANAGREEMENT-27-28", "KXUSAIRANAGREEMENT-27-29JAN20")],
      "P(Iran-US nuclear deal before Aug 1 26) <= ... <= P(before Jan 20 29)",
      {"kalshi": "If the United States has agreed to, signed, or accepted a new Iran-US nuclear deal before <date>, then the market resolves to Yes. (identical per rung, only date changes)"},
      "equivalent",
      ["Cumulative absolute criterion, no per-window reset"],
      0.95, "low", "long-tail"),
    E("kalshi-fed-dec26-t375-implies-t350", "implies",
      [leg(K, "KXFED-26DEC-T3.75"), leg(K, "KXFED-26DEC-T3.50")],
      "P(fed funds upper bound > 3.75%) <= P(> 3.50%) after the Dec 9 2026 FOMC meeting",
      {"kalshi": "If the upper bound of the target federal funds rate published on the Federal Reserve's official website is greater than 3.75% following the Dec 9, 2026 meeting, then the market resolves to Yes. (T3.50 identical with 3.50%)"},
      "equivalent",
      ["Pure threshold nesting on the same published number"],
      0.98, "low", "long-tail"),
    E("kalshi-fed-dec26-t475-implies-t450", "implies",
      [leg(K, "KXFED-26DEC-T4.75"), leg(K, "KXFED-26DEC-T4.50")],
      "P(> 4.75%) <= P(> 4.50%) — bids VIOLATED ordering at catalog snapshot (0.03 vs 0.02)",
      {"kalshi": "Identical threshold wording with 4.75% / 4.50%."},
      "equivalent",
      ["Live near-violation found at snapshot: buy T4.50 @4c, sell T4.75 @3c = 1c cost for never-negative spread; books thin (~$350-870 vol)"],
      0.98, "low", "long-tail"),
    E("kalshi-fed-dec26-decision-partition", "partition",
      [leg(K, t) for t in ("KXFEDDECISION-26DEC-C26", "KXFEDDECISION-26DEC-C25",
                           "KXFEDDECISION-26DEC-H0", "KXFEDDECISION-26DEC-H25",
                           "KXFEDDECISION-26DEC-H26")],
      "P(cut>25)+P(cut25)+P(no change)+P(hike25)+P(hike>25) == 1 for the Dec 9 2026 decision",
      {"kalshi": "If the Federal Reserve does a Hike of 0bps on December 09, 2026, then the market resolves to Yes. (siblings identical per bucket)"},
      "equivalent-with-caveat",
      ["Non-multiple-of-25 move fits no bucket cleanly (Kalshi silent on rounding)",
       "Meeting cancellation/reschedule ambiguity on 'on December 09, 2026'",
       "Bid sum 0.90 / ask sum 1.07 at snapshot — inside no-arb band"],
      0.90, "low", "long-tail"),
    E("kalshi-venezuela-leader-exclusive", "exclusive",
      [leg(K, f"KXVENEZUELALEADER-26DEC31-{s}") for s in
       ("NMAD", "DROD", "MCM", "EGON", "DRON", "VLOP", "JROD", "MRT", "JGUA",
        "DFIG", "RGRE", "FDON", "DJT", "MRUB", "PHEG", "SMIL", "DCAI", "EPET")],
      "at most one of 18 named individuals holds Venezuela head of state at Dec 31 2026 10AM ET — sum <= 1",
      {"kalshi": "If <name> officially holds the position of the head of state of Venezuela on Dec 31, 2026 at 10:00 AM ET, then the market resolves to Yes. (identical per candidate)"},
      "equivalent-with-caveat",
      ["NOT exhaustive (no Other) — sell-side only",
       "Contested government: two candidates could arguably 'officially hold' under rival authorities; Kalshi tiebreak absent from rules"],
      0.85, "high", "long-tail"),
    E("kalshi-dem-nom-2028-exclusive", "exclusive",
      [leg(K, f"KXPRESNOMD-28-{s}") for s in
       ("GN", "AOC", "JOSS", "KH", "REMA", "PB", "JS", "AB", "MK", "RKHA", "JBP",
        "WM", "MO", "JTAL", "JSTE", "GW", "MC", "BS", "CBOO", "CMUR", "RGAL",
        "ESLO", "RW", "HBID", "SAS", "ZMAM", "JF", "TW", "RC", "BOBA", "GR",
        "JAM", "GPLA", "EWAR", "AKLO", "AYAN", "AELS", "BORO", "DJOH", "HCLI",
        "JCRO", "JP", "LC", "LJAM", "MLAN", "PHI")],
      "at most one candidate wins and accepts the 2028 Democratic nomination — sum over 46 legs <= 1",
      {"kalshi": "If <name> wins and accepts the nomination for the Presidency for the Democratic party in 2028, then the market resolves to Yes. (identical per candidate)"},
      "equivalent-with-caveat",
      ["No other/field market — exclusive, sell-side only",
       "Natural hedge inventory for the cross-venue nomination pairs"],
      0.95, "low", "long-tail"),
]

reg = yaml.safe_load(open("config/registry.yaml"))
existing_ids = {r["id"] for r in reg["relationships"]}
added = [r for r in new_rels if r["id"] not in existing_ids]
reg["relationships"].extend(added)
Registry.model_validate(reg)  # hard validation before write
with open("config/registry.yaml", "w") as f:
    yaml.safe_dump(reg, f, sort_keys=False, width=120)

rej = yaml.safe_load(open("config/registry-rejected.yaml"))
rej["relationships"] += [
    {"id": "xv-hantavirus-pheic-vs-pandemic-rejected",
     "why": "PM 'Hantavirus pandemic' rules EXPLICITLY exclude PHEIC-alone; Kalshi triggers on exactly what PM excludes. Only weak implies P(PM)<=P(Kalshi) defensible, not proposed."},
    {"id": "xv-greenland-2026-rejected",
     "why": "Diverges BOTH directions: any-part vs majority-of-territory (Kalshi easier) AND announcement vs actual acquisition (PM easier). Not even implies survives."},
]
with open("config/registry-rejected.yaml", "w") as f:
    yaml.safe_dump(rej, f, sort_keys=False, width=120)

r = Registry.model_validate(reg)
from arbbot.models.core import Venue
k = [m for v, m in r.market_ids() if v is Venue.KALSHI]
p = [m for v, m in r.market_ids() if v is Venue.POLYMARKET]
print(f"registry: {len(r.relationships)} relationships (+{len(added)}), "
      f"kalshi {len(k)} markets, polymarket {len(p)} tokens, "
      f"tradable {sum(1 for x in r.relationships if x.tradable)}")
