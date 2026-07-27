"""Approximate resolution dates per relationship family, for hold-APR math.

Some events have no officially-fixed date far in advance (TIME Person of the
Year is announced ~2nd week of December; the French 2027 presidential date
isn't set yet). Kalshi's `expected_expiration_time` is a conservative year-end
close, which understates time-to-resolution. We curate a best estimate and flag
it `estimated=True` so the dashboard can mark the derived APR as speculative.
"""

# (relationship-id prefix, approximate resolve date, estimated?)
# estimated=True  -> event date not officially fixed; APR shown with a ~ / "est".
# estimated=False -> date is firm or a hard market-defined bound.
_RULES = [
    ("xvus-time-poty-26",    "2026-12-09", True),   # TIME PotY announced ~2nd week of Dec
    ("xvus-france-pres-27",  "2027-04-25", True),   # French pres election ~late Apr 2027 (date TBD)
    ("xvus-brazil-pres-26",  "2026-10-25", True),   # Brazil general; runoff ~last Sun of Oct
    ("xvus-nobel-peace-26",  "2026-10-09", True),   # Nobel Peace Prize announced early Oct
    ("xvus-fedcut-26",       "2026-12-31", False),  # hard bound: resolves by year-end per market def
    ("xvus-btcmax-26-31",    "2026-12-31", False),  # hard bound: BTC year-high window ends Dec 31
]


def resolve_date(relationship_id: str) -> tuple[str | None, bool]:
    """-> (YYYY-MM-DD or None, estimated). None when the family is unknown."""
    for prefix, date, estimated in _RULES:
        if relationship_id.startswith(prefix):
            return date, estimated
    return None, False
