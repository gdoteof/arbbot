"""Fee curve tests: schedule-anchor examples + hypothesis property tests.

Anchors come from the published schedules cited in the design doc:
Kalshi taker peaks at 1.75c/contract at P=0.50; maker is a quarter of that
coefficient; Polymarket US is flat 0.10% of premium with a $0.001 floor.
"""

from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from arbbot.fees.curves import FeeSchedule, PolymarketVariant, ceil_cents
from arbbot.models.core import Role, Venue

# Default schedule is INTERNATIONAL (Geoff's venue, confirmed 2026-07-20).
FS = FeeSchedule()
FS_INTL = FS
FS_US = FeeSchedule(polymarket=PolymarketVariant(mode="us"))

prices = st.decimals(min_value="0.01", max_value="0.99", places=2)
sizes = st.decimals(min_value="1", max_value="100000", places=0)


# --- anchors ---------------------------------------------------------------

def test_kalshi_taker_peak_at_50c():
    # 0.07 * 1 * 0.5 * 0.5 = 0.0175 -> ceil to cent = $0.02 for one contract
    assert FS.fee(Venue.KALSHI, Role.TAKER, Decimal("0.50"), Decimal(1)) == Decimal("0.02")
    # 100 contracts: 0.07*100*0.25 = 1.75 exactly -> $1.75 (the published peak)
    assert FS.fee(Venue.KALSHI, Role.TAKER, Decimal("0.50"), Decimal(100)) == Decimal("1.75")


def test_kalshi_maker_quarter_coefficient():
    # 0.0175*100*0.25 = 0.4375 -> ceil -> 0.44
    assert FS.fee(Venue.KALSHI, Role.MAKER, Decimal("0.50"), Decimal(100)) == Decimal("0.44")


def test_kalshi_extreme_price_percentage_burden():
    # 5c contract, 100 contracts: 0.07*100*0.05*0.95 = 0.3325 -> 0.34
    fee = FS.fee(Venue.KALSHI, Role.TAKER, Decimal("0.05"), Decimal(100))
    assert fee == Decimal("0.34")
    # ~6.8% of the $5 cost — the doc's "percentage fee highest at extremes"
    assert fee / (Decimal("0.05") * 100) > Decimal("0.06")


def test_polymarket_us_flat_and_floor():
    # 0.10% of premium: 0.001 * 0.50 * 100 = $0.05
    assert FS_US.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.50"), Decimal(100)) == Decimal("0.05")
    # tiny fill hits the $0.001 floor
    assert FS_US.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.01"), Decimal(1)) == Decimal("0.001")


def test_polymarket_maker_free_both_modes():
    assert FS.fee(Venue.POLYMARKET, Role.MAKER, Decimal("0.50"), Decimal(1000)) == 0
    assert FS_INTL.fee(Venue.POLYMARKET, Role.MAKER, Decimal("0.50"), Decimal(1000)) == 0


def test_polymarket_intl_peak_rates_match_published_schedule():
    # Confirmed 2026-07-20: peak fee per 100 shares at P=0.50.
    peak = lambda cat: FS_INTL.fee(
        Venue.POLYMARKET, Role.TAKER, Decimal("0.50"), Decimal(100), category=cat
    )
    assert peak("geopolitics") == Decimal("0.00")
    assert peak("politics") == Decimal("1.00")
    assert peak("finance") == Decimal("1.00")
    assert peak("sports") == Decimal("1.25")
    assert peak("crypto") == Decimal("1.75")
    # unmapped category -> politics default (0.04) = $1.00 peak (conservative)
    assert peak("nope") == Decimal("1.00")


def test_polymarket_intl_symmetric_around_half():
    # a trade at 30c costs the same dollar fee as at 70c (design doc + schedule)
    at30 = FS_INTL.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.30"), Decimal(100), category="politics")
    at70 = FS_INTL.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.70"), Decimal(100), category="politics")
    assert at30 == at70 == Decimal("0.04") * 100 * Decimal("0.30") * Decimal("0.70")


def test_polymarket_us_still_available_via_config():
    assert FS_US.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.50"), Decimal(100)) == Decimal("0.05")
    assert FS_US.fee(Venue.POLYMARKET, Role.TAKER, Decimal("0.01"), Decimal(1)) == Decimal("0.001")


def test_polymarket_us_venue_maker_free_taker_coef():
    # regulated QCX venue (Venue.POLYMARKET_US): makers free; taker fallback
    # k=0.06 -> 100 contracts at P=0.50: 0.06*100*0.25 = $1.50
    assert FS.fee(Venue.POLYMARKET_US, Role.MAKER, Decimal("0.50"), Decimal(100)) == 0
    assert FS.fee(Venue.POLYMARKET_US, Role.TAKER, Decimal("0.50"), Decimal(100)) == Decimal("1.50")


def test_polymarket_us_venue_override_beats_fallback():
    from arbbot.fees.curves import leg_fee
    # venue-reported feeCoefficient (per-market) wins over the 0.06 fallback
    f = leg_fee(FS, Venue.POLYMARKET_US, Role.TAKER, Decimal("0.50"), Decimal(100),
                taker_coef_override=Decimal("0.02"))
    assert f == Decimal("0.50")


def test_zero_size_charges_nothing():
    for venue in Venue:
        for role in Role:
            assert FS.fee(venue, role, Decimal("0.50"), Decimal(0)) == 0


def test_bad_inputs_rejected():
    with pytest.raises(ValueError):
        FS.fee(Venue.KALSHI, Role.TAKER, Decimal("1.01"), Decimal(1))
    with pytest.raises(ValueError):
        FS.fee(Venue.KALSHI, Role.TAKER, Decimal("0.5"), Decimal(-1))


# --- properties ------------------------------------------------------------

@given(p=prices, n=sizes)
def test_kalshi_fee_nonnegative_and_ceil_stable(p, n):
    fee = FS.fee(Venue.KALSHI, Role.TAKER, p, n)
    assert fee >= 0
    assert fee == ceil_cents(fee)  # always whole cents
    assert fee >= Decimal("0.07") * n * p * (1 - p)  # ceil never under-charges


@given(p=prices, n=sizes)
def test_kalshi_maker_never_exceeds_taker(p, n):
    assert FS.fee(Venue.KALSHI, Role.MAKER, p, n) <= FS.fee(Venue.KALSHI, Role.TAKER, p, n)


@given(p=prices, n=sizes)
def test_kalshi_parabola_symmetry(p, n):
    assert FS.fee(Venue.KALSHI, Role.TAKER, p, n) == FS.fee(Venue.KALSHI, Role.TAKER, 1 - p, n)


@given(p=prices, n=sizes)
def test_fees_scale_monotonically_with_size(p, n):
    for venue in Venue:
        assert FS.fee(venue, Role.TAKER, p, n + 1) >= FS.fee(venue, Role.TAKER, p, n)


@given(p=prices, n=sizes)
def test_polymarket_us_exact_formula(p, n):
    fee = FS_US.fee(Venue.POLYMARKET, Role.TAKER, p, n)
    assert fee == max(Decimal("0.001") * p * n, Decimal("0.001"))


@given(p=prices, n=sizes)
def test_polymarket_intl_exact_formula(p, n):
    fee = FS_INTL.fee(Venue.POLYMARKET, Role.TAKER, p, n, category="politics")
    assert fee == Decimal("0.04") * p * (1 - p) * n
