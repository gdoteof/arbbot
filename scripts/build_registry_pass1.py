"""Build config/registry.yaml from miner pass-1 proposals with corrected
semantics. Every entry is vetted_by: agent — NOT tradable until Geoff's
human verdict (registry.tradable enforces this).

Type corrections vs the miner's raw output:
- non-exhaustive "partitions" -> exclusive (all-zero state is feasible)
- presidency-implies-nomination + superset-parlay entries -> implies
- cross-collection duplicate parlays -> equivalent-pair
- expired 15-minute crypto entries dropped (closed 2026-07-20T05:00Z)
- rejected traps go to config/registry-rejected.yaml as documentation
"""

import json

import yaml

PM_INDEX = json.load(open("data/catalogs/pm-token-index.json"))

pm_partitions = {
    "pm-fed-july-2026-fomc-exclusive": {
        "note": "Fed July 2026: no-change/+25/+50+ brackets (cut brackets off-catalog)",
        "tokens": [
            "111604417349377875799825956621596386269673370070912696668140891647145772186047",
            "10547381015916960267379463101229159185405356924982461726471550099674011526491",
            "55547507942649396203839937021705347989082320770224485499197798791654376614809",
        ],
        "direction": "sum of P over listed FOMC brackets <= 1 (mutually exclusive, NOT exhaustive)",
        "citation": "This market will resolve to the amount of basis points the upper bound of the target federal funds rate is changed by versus the level it was prior to the Federal Reserve's July 2026 meeting.",
        "caveats": [
            "Only 3 of the event's brackets in catalog; rate-cut brackets missing — invariant is sum <= 1, sell-side only",
            "negRisk=true enforces exclusivity mechanically",
        ],
        "confidence": 0.96,
        "oracle_risk": "low",
        "tranche": "head",
    },
    "pm-ethiopia-next-pm-exclusive": {
        "note": "Next PM of Ethiopia: 7 listed candidates (incumbent off-catalog)",
        "tokens": [
            "26485245961222313575373928818655498088613748810606082451526412785977121014592",
            "27146956652877944551877724690365745048289675287536243265951843487691050802191",
            "4489049127093187435445530909063903861654656526363504166478270583011546818111",
            "85367286745806857961178482075931972831841231758328346969840810630055458089640",
            "2782564244692790193407832532237218722544517902619602647018377360625351910716",
            "9700525976462326306802795530553376348116310383328382626789994562350229436531",
            "80737749707623321656569294238127946200924575060067022053313951954399729244690",
        ],
        "direction": "sum of P(candidate is next PM) over 7 listed candidates <= 1",
        "citation": "This market will resolve to the next individual who officially assumes the office of Prime Minister of Ethiopia following the 2026 General elections.",
        "caveats": [
            "Severely non-exhaustive (incumbent + Other absent) — sell-side only",
            "Interim/caretaker PM does not count; Other backstop Dec 31 2028",
        ],
        "confidence": 0.95,
        "oracle_risk": "medium",
        "tranche": "long-tail",
    },
}

# 2028 candidate exclusives: token lists from miner pass 1
dem_nominee_tokens = [
    "2213957649161627793381994368131485505647723208738124952452819345058597751695",
    "113510300123120868861281132282742729065975487298059944817273881893086209196585",
    "46211769122513729244602558513001215391004042100934296321661285791281233261657",
    "102504892414163237174864879683522880588750452841477290216845150938663289742135",
    "100561779829530520068956424581003844045516012444425572625523659025114550346085",
    "11343337042526652606304508556838778144915122118685013460142819817079620240439",
    "6349038249650226718426900942104344803413105653564595362433352811865798195800",
    "48218088132821386640274752564592889805082274744128282833036735027928992155024",
    "104464678880204778606895232389511543522030897731392516718349574106843434770446",
    "37126434962149084556522721025504254258386171468763869879755961635390358765833",
    "55087250670040717711131370018408221134109122974378698780636020561523521220754",
    "81633484456710374417231462107666791993850637546563366185897100962157481970040",
    "51035537065442638321333063915918274102446051033989435618245745345113522914516",
    "19323997520040734631220675039559156784534982703459512097490198935565229528445",
    "41340056280794024415573839406074658402877379128871144681466050648033201160259",
    "22103094389913052942362639589409218272323168761614999702665821259175535456835",
    "35165603539035270209469650915267170915291414186795231160733227083640059691764",
    "88927218779847397120407909832537568114340511586397698395193736044476021744522",
    "52310257194926233099477326083312959938983447523839326865869372741334281264830",
    "30732552320006664177902085971957467957450485111214036633872573588757974348601",
    "42119164854283656238382173699154739480090965550328051492328192273011556335098",
    "54533043819946592547517511176940999955633860128497669742211153063842200957669",
    "83723134981785004242996949681785273460678873813693556756054904164484296299719",
    "50887272939612765629559172143901565817521391945540156085421963433918821328137",
    "58343456405874365460518182619380252930558598368174305791768670198253267891911",
    "71289010523270989807437428651002770360280908813019627976988032260868574107425",
    "58378309871183461257693288827678410871907006920578755035402846056315798427503",
    "60590045489347122735554346200880179420435533609307820342798544098823516727807",
    "8815924480259634499011776432080696931704057619336093802050596292915996723758",
    "90227223115596293448966158151606153337847900483048338623540402385929720677904",
    "92325249719485031139867422012514654102580961954747753469470405147070256604118",
    "103988345069738240821107916249290759786442703283901164377120414690701074137717",
    "10521981432803570450946212503203364998108630328528216082760514453664705157397",
    "107064985435494333113391038470401719113272800530429703182710416066774068907304",
    "26468656392978559668331516709623917078428425933265692717836103090220693717685",
]
rep_nominee_tokens = [
    "311624663652221737215322113380496984764966764039692273354641152455298576851",
    "110467341601356058460373968989438018821471776813704553223329002744055822742107",
    "46777072088650922464797852907503479539785178123001199763117403684202580813104",
    "70663522560386702809794077554934323058266367604787739541440326847712418734032",
    "45742939946094874824687417208711661205131461760422331277735745393193805349457",
    "35019878675696902444730611385908114488958639217305639231975258218416576501435",
    "42282542821681724515855150589432262778581936160356547088323204270366545065655",
    "97933607699985149274021900377397416175605196663149372937380275158258659161873",
    "10651968542740640896482407431367624536081358925965856338872110525887291512129",
    "46762065358788748743762617584400316865170283087328708370205000691002445395596",
    "76572162491038773682779378100661251853152133818805423848523216103658476280587",
    "51109358167525503124101840618196204295486048568333812245607026343682053354150",
    "16959950591508656523048029588253513417107455151269158025695062757254216051157",
    "21684611233679565633387313395955873024927822291738934804105644401703825367322",
    "56369772478534954338683665819559528414197495274302917800610633957542171787417",
    "36725157385158152303355940271421346899386884953712631735038848833359115722560",
    "75602319799101799832214450648958470414659751617273540482126444460192042893239",
    "109121432238307135312607668741094299588249627783575654034498708660414246884093",
    "85795532452312461688342921814235563171225750416821603832877839950165564679042",
    "7847177314809025001842027337093046201021185723933452855314998541165696902642",
    "12127975650736113116407758794754973741525044713193258995588171654429754588610",
    "40081275558852222228080198821361202017557872256707631666334039001378518619916",
    "111796631871113641071635832853519973638674140424258605036492112910151603434351",
]
pres_winner_tokens = [
    "39223330966352513418907732239455948906399882924787587183466504819170775983376",
    "2638997461401140008448476093092229850740161649668999975101243681118238509713",
    "24863363384036117015867671383779186335047493791693657275104567365292353535818",
    "48067717079255656974334181122173546721823870434119599450595298250019538972254",
    "58585097107933138034126275600204468509993701936571284309823871593297754181479",
    "105367778020161609482094090847463497982204321259381518614669619753184648647559",
    "97633418272282627828834872960145259034624388719803091466146359304286851612172",
    "14253956839582698877884742930225331487593177635330318923853046874994118249367",
    "631059815958854406224163480574507434200946764542129005247984171366597476420",
    "26641906520532802078452346454133721131611596169940893262820937050881742190686",
    "94621929236241392639802280031607240386237812521756048982107414565863464212526",
    "50507496430692162923857760920722624157737138256097150616293040054748564876552",
    "88521269813605705303364598280494261510300497312149869577251450871617962933121",
    "67028631656597977031363620447645908995417871899828777750494099295092202422178",
    "98250445447699368679516529207365255018790721464590833209064266254238063117329",
    "16040015440196279900485035793550429453516625694844857319147506590755961451627",
    "4999047257294547885970323631312419427544062131054733229841947946130772277335",
    "45015916581148592087728250896178171120552786117190685135618507471166113474733",
    "77660944053451874285291959901958937238149180369572698706675030657716494119346",
]


def leg(venue, mid):
    return {"venue": venue, "market_id": str(mid), "side": "yes", "role": "taker"}


def entry(id_, type_, legs, direction, citations, caveats, conf, oracle, tranche):
    return {
        "id": id_,
        "type": type_,
        "legs": legs,
        "direction": direction,
        "rule_citations": citations,
        "verdict": "equivalent-with-caveat",
        "caveats": caveats,
        "vetted_by": "agent",
        "confidence": conf,
        "oracle_risk": oracle,
        "tranche": tranche,
    }


rels = []
for id_, spec in pm_partitions.items():
    rels.append(
        entry(
            id_, "exclusive",
            [leg("polymarket", t) for t in spec["tokens"]],
            spec["direction"], {"polymarket": spec["citation"]},
            spec["caveats"], spec["confidence"], spec["oracle_risk"], spec["tranche"],
        )
    )

rels.append(
    entry(
        "pm-dem-nominee-2028-exclusive", "exclusive",
        [leg("polymarket", t) for t in dem_nominee_tokens],
        "sum of P(named individual wins 2028 Democratic nomination) over 35 legs <= 1",
        {"polymarket": "This market will resolve to “Yes” if the named individual wins and accepts the 2028 nomination of the Democratic Party for U.S. president."},
        ["Non-exhaustive sample of the negRisk event — sell-side (sum of bids > 1) only",
         "Replacement-of-nominee clause: resolves on original convention winner"],
        0.97, "low", "head",
    )
)
rels.append(
    entry(
        "pm-rep-nominee-2028-exclusive", "exclusive",
        [leg("polymarket", t) for t in rep_nominee_tokens],
        "sum of P(named individual wins 2028 Republican nomination) over 23 legs <= 1",
        {"polymarket": "This market will resolve to “Yes” if the named individual wins and accepts the 2028 nomination of the Republican Party for U.S. president."},
        ["Non-exhaustive — sell-side only"],
        0.97, "low", "head",
    )
)
rels.append(
    entry(
        "pm-pres-2028-winner-exclusive", "exclusive",
        [leg("polymarket", t) for t in pres_winner_tokens],
        "sum of P(named individual wins 2028 US Presidential Election) over 19 legs <= 1",
        {"polymarket": "This market will resolve to the person who wins the 2028 US Presidential Election."},
        ["Non-exhaustive — sell-side only",
         "All-three-calls rule with inauguration fallback guarantees one winner"],
        0.97, "low", "head",
    )
)

implications = [
    ("pm-walz-presidency-implies-dem-nom",
     "2638997461401140008448476093092229850740161649668999975101243681118238509713",
     "104464678880204778606895232389511543522030897731392516718349574106843434770446",
     0.85, "Walz"),
    ("pm-lebron-presidency-implies-dem-nom",
     "39223330966352513418907732239455948906399882924787587183466504819170775983376",
     "100561779829530520068956424581003844045516012444425572625523659025114550346085",
     0.80, "LeBron James"),
    ("pm-mamdani-presidency-implies-dem-nom",
     "94621929236241392639802280031607240386237812521756048982107414565863464212526",
     "19323997520040734631220675039559156784534982703459512097490198935565229528445",
     0.82, "Mamdani"),
    ("pm-newsom-presidency-implies-dem-nom",
     "98250445447699368679516529207365255018790721464590833209064266254238063117329",
     "54533043819946592547517511176940999955633860128497669742211153063842200957669",
     0.87, "Newsom"),
    ("pm-tulsi-presidency-implies-rep-nom",
     "97633418272282627828834872960145259034624388719803091466146359304286851612172",
     "111796631871113641071635832853519973638674140424258605036492112910151603434351",
     0.75, "Gabbard"),
    ("pm-michelle-obama-presidency-implies-dem-nom",
     "88521269813605705303364598280494261510300497312149869577251450871617962933121",
     "83723134981785004242996949681785273460678873813693556756054904164484296299719",
     0.85, "Michelle Obama"),
]
for id_, pres_tok, nom_tok, conf, name in implications:
    e = entry(
        id_, "implies",
        [leg("polymarket", pres_tok), leg("polymarket", nom_tok)],
        f"P({name} wins presidency) <= P({name} wins party nomination)",
        {"presidential-election-winner-2028": "This market will resolve to the person who wins the 2028 US Presidential Election.",
         "nomination-market": "Resolves “Yes” if the named individual wins and accepts the nomination; any replacement of the nominee before election day will not change the resolution."},
        ["Third-party/independent path breaks the inequality (tail risk)",
         "Replacement-nominee trap: nomination resolves on original convention winner"],
        conf, "medium", "long-tail",
    )
    rels.append(e)

# Kalshi MVE structural entries that have NOT expired (MLB, close ~Jul 24)
kalshi_entries = [
    ("kalshi-mlb-jul20-8v7-superset-implies",
     "KXMVESPORTSMULTIGAMEEXTENDED-S20262F4DB581045-E4C73056117",
     "KXMVESPORTSMULTIGAMEEXTENDED-S202633D30C7E6EB-AB895F05BD4",
     "P(8-leg superset parlay) <= P(7-leg subset parlay)", 0.93),
    ("kalshi-mlb-jul20-12v4-superset-implies",
     "KXMVESPORTSMULTIGAMEEXTENDED-S2026F4775C29C45-1E47650024E",
     "KXMVESPORTSMULTIGAMEEXTENDED-S20262D6126CFE9F-9FC9182829B",
     "P(12-leg superset parlay) <= P(4-leg subset {NYY,TOR,TEX,SEA})", 0.95),
    ("kalshi-mlb-homer-8v6-superset-implies",
     "KXMVESPORTSMULTIGAMEEXTENDED-S2026858B6F7016C-B92A0CE2B05",
     "KXMVESPORTSMULTIGAMEEXTENDED-S20269BF6DC0D098-DB2A08D203A",
     "P(8-leg superset parlay) <= P(6-leg subset parlay)", 0.90),
]
for id_, sup, sub, direction, conf in kalshi_entries:
    rels.append(
        entry(
            id_, "implies",
            [leg("kalshi", sup), leg("kalshi", sub)],
            direction,
            {"kalshi": "rules_primary empty on MVE markets; dominance proven structurally from mve_selected_legs (superset contains every subset leg with identical side)"},
            ["Void handling of extra legs unknown — a voided extra leg may collapse superset price to subset",
             "Books were empty at catalog snapshot; likely quote-on-demand"],
            conf, "low", "long-tail",
        )
    )
rels.append(
    entry(
        "kalshi-mlb-top-parlays-exclusive", "exclusive",
        [leg("kalshi", "KXMVESPORTSMULTIGAMEEXTENDED-S2026F4775C29C45-1E47650024E"),
         leg("kalshi", "KXMVESPORTSMULTIGAMEEXTENDED-S20262F4DB581045-E4C73056117")],
        "P(A) + P(B) <= 1: conflicting MIN/CLE and TOR/TB moneyline legs — both cannot pay",
        {"kalshi": "structural proof — conflicting legs KXMLBGAME-26JUL201840MINCLE-MIN(yes) vs -CLE(yes)"},
        ["Game void shifts the exclusivity argument; sell-side only and nearly always slack"],
        0.90, "low", "long-tail",
    )
)
rels.append(
    entry(
        "pm-venezuela-leader-exclusive", "exclusive",
        [leg("polymarket", "63041927234014862550752749844577619438319946530923167906649363118591050055991"),
         leg("polymarket", "102488069217181040733050347118082609606893446345922306325497036296077206149582")],
        "P(Grenell) + P(Donovan) <= 1 — one head of state on Dec 31 2026 12PM ET",
        {"polymarket": "This market will resolve to the individual who officially holds the position of the head of state of Venezuela on Dec 31, 2026 at 12 PM ET."},
        ["Grossly non-exhaustive; contested-government scenario is prime UMA-dispute territory"],
        0.90, "high", "long-tail",
    )
)

registry = {"relationships": rels}
with open("config/registry.yaml", "w") as f:
    yaml.safe_dump(registry, f, sort_keys=False, width=120)

# Rejected traps: documentation file (never scanned, never recorded)
rejected = {
    "relationships": [
        {"id": "pm-fed-july-2026-exhaustive-rejected",
         "why": "Treating {hold,+25,+50+} as exhaustive ignores off-catalog cut brackets; rules text proves more 'displayed options' exist. sum==1 is a trap."},
        {"id": "pm-kardashian-presidency-vs-both-noms-rejected",
         "why": "P(pres) <= P(dem nom)+P(rep nom) fails on independent run + 'wins AND accepts' + replacement clause. Canonical trap for presidency-vs-nomination rollups."},
        {"id": "kalshi-crypto5-allyes-allno-complement-rejected",
         "why": "All-yes and all-no parlays are exclusive but NOT complementary (30 mixed combos). Complements require the full 2^n set."},
    ]
}
with open("config/registry-rejected.yaml", "w") as f:
    yaml.safe_dump(rejected, f, sort_keys=False, width=120)
print(f"wrote {len(rels)} relationships")
