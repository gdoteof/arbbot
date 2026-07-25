"""Pins for arbbot.registry.categorize — faithful ports of dash._category and
risk.topic_of. Table entries are real live-registry ids plus ledger-style
sports ids; expected values are the CURRENT inline behavior (verified
differentially against verbatim copies at port time). Quirks are intentional:
category matches families as bare substrings, topic requires dash-delimited
matches; sports-lean-* falls to sports-misc."""

import pytest

from arbbot.registry.categorize import category_of, load_game_league, topic_of

# config/topics.yaml families as of 2026-07-23 (untracked config — pinned
# here explicitly so the tests don't depend on the live file)
FAMILIES = ("nobel-peace-26", "time-poty-26", "fedcut-26",
            "brazil-pres-26", "france-pres-27")

GAME_LEAGUE = {"Yankees@Red Sox": "mlb", "Tampa Bay Rays@Toronto Blue Ja": "mlb"}

CASES = [
    # (relationship_id, category, topic)
    ("pm-fed-july-2026-fomc-exclusive", "other", "other"),
    ("pm-dem-nominee-2028-exclusive", "other", "other"),
    ("kalshi-mlb-top-parlays-exclusive", "other", "other"),
    ("xv-putin-out-2026", "other", "other"),
    ("xv-fed-26jul-cut25", "other", "other"),
    ("xv-f1-2026-bearman", "other", "other"),
    ("kalshi-fed-dec26-decision-partition", "other", "other"),
    ("kalshi-aliens-confirmation-date-ladder", "other", "other"),
    ("neh2026-x-trump-out-before-2027", "other", "other"),
    ("xvus-nobel-peace-26-donaldtrump", "nobel-peace-26", "nobel-peace-26"),
    ("xvus-nobel-peace-26-popeleoxiv", "nobel-peace-26", "nobel-peace-26"),
    ("xvus-time-poty-26-taylorswift", "time-poty-26", "time-poty-26"),
    ("xvus-time-poty-26-darioamodei", "time-poty-26", "time-poty-26"),
    ("xvus-bestai-26dec-anthropic", "bestai-26dec", "other"),  # no topic budget
    ("xvus-fedcut-26-usfed-2026-cut", "fedcut-26", "fedcut-26"),
    ("xvus-brazil-pres-26-luizinacioluladasilva", "brazil-pres-26", "brazil-pres-26"),
    ("xvus-france-pres-27-marinelepen", "france-pres-27", "france-pres-27"),
    ("xvus-btcmax-26-31-2026-150k", "other", "other"),
    ("xvus-gpt6-ladder-2026-08-31", "other", "other"),
    ("xvus-tsla-q3-deliv-q3-above-500k", "other", "other"),
    # sports (sports_arb.py ledger id shapes)
    ("sports-mlb-Tampa Bay Rays@Toronto Blue Ja", "sports-mlb", "other"),
    ("sports-wta-A. Sabalenka@I. Swiatek", "sports-wta", "other"),
    ("sports-atp-X@Y", "sports-atp", "other"),
    ("sports-kbo-X@Y", "sports-kbo", "other"),
    ("sports-mls-A@B", "sports-misc", "other"),  # league not in whitelist
    ("sports-cs2-TeamA@TeamB", "sports-misc", "other"),
    ("sports-lean-mlb-A@B", "sports-misc", "other"),  # lean rider: misc today
    ("sports-rehedge-Yankees@Red Sox", "sports-mlb", "other"),  # via game map
    ("sports-rehedge-Unknown@Game", "sports-misc", "other"),
    ("sports-flatten-Yankees@Red Sox", "sports-mlb", "other"),
    ("sports-mlb", "sports-misc", "other"),  # no game segment
    # category substring vs topic dash-delimited divergence
    ("xfedcut-26-foo", "fedcut-26", "other"),
]


@pytest.mark.parametrize("rid,cat,top", CASES, ids=[c[0] for c in CASES])
def test_pinned(rid, cat, top):
    assert category_of(rid, game_league=GAME_LEAGUE) == cat
    assert topic_of(rid, families=FAMILIES) == top


def test_empty_and_none():
    assert category_of(None) == "other"
    assert category_of("") == "other"
    assert topic_of("anything", families=()) == "other"


def test_topic_longest_family_wins():
    fams = ("pres-27", "france-pres-27")
    assert topic_of("xvus-france-pres-27-lepen", families=fams) == "france-pres-27"


def test_rehedge_without_map_is_misc():
    assert category_of("sports-rehedge-Yankees@Red Sox") == "sports-misc"


def test_load_game_league_missing_file(tmp_path):
    assert load_game_league(tmp_path / "nope.json") == {}
