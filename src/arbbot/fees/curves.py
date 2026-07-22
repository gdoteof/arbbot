"""Exact per-role fee curves for both venues.

Design doc constraints (verify against published schedules before Stage 3):

  Kalshi taker:  ceil_cents(0.07   * contracts * P * (1-P))
  Kalshi maker:  ceil_cents(0.0175 * contracts * P * (1-P))
  Polymarket US taker:   0.10% * P * contracts premium, $0.001 minimum
  Polymarket intl taker: k * P * (1-P) * contracts, k config-driven per
                         category (structure known, coefficients unconfirmed)
  Polymarket maker: 0 on both venue variants

Reconciliation compares venue-charged fees per fill against these curves —
a mismatch alert is the fee-schedule-drift detector.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal

from pydantic import BaseModel, Field

from arbbot.models.core import Role, Venue

CENT = Decimal("0.01")
MILL = Decimal("0.001")


def ceil_cents(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_CEILING)


# Polymarket international taker feeRate per category, CONFIRMED against the
# published schedule 2026-07-20 (docs.polymarket.com/trading/fees):
#   fee = shares * feeRate * P * (1-P); makers free.
# Peak (P=0.50) per 100 shares: $0 geopolitics, $1.00 politics/finance/tech/
# mentions, $1.25 sports/economics/culture/weather, $1.75 crypto.
POLYMARKET_INTL_FEERATES: dict[str, Decimal] = {
    "geopolitics": Decimal("0.00"),
    "politics": Decimal("0.04"),
    "finance": Decimal("0.04"),
    "tech": Decimal("0.04"),
    "mentions": Decimal("0.04"),
    "sports": Decimal("0.05"),
    "economics": Decimal("0.05"),
    "culture": Decimal("0.05"),
    "weather": Decimal("0.05"),
    "crypto": Decimal("0.07"),
    # Default for unmapped categories = politics rate (0.04). Conservative for
    # this political/geopolitical universe: it can only OVER-state fees
    # (under-state edge) relative to a true geopolitics-0 market, never
    # manufacture a phantom opportunity.
    "default": Decimal("0.04"),
}


class PolymarketVariant(BaseModel):
    """Which Polymarket venue the account trades — Open Question 1.
    Geoff's account is INTERNATIONAL (confirmed 2026-07-20, on it since before
    the US venue existed). Both curves implemented; config selects."""

    mode: str = "intl"  # "us" | "intl"
    intl_category_coefficients: dict[str, Decimal] = Field(
        default_factory=lambda: dict(POLYMARKET_INTL_FEERATES)
    )


class FeeSchedule(BaseModel):
    kalshi_taker_coef: Decimal = Decimal("0.07")
    kalshi_maker_coef: Decimal = Decimal("0.0175")
    polymarket: PolymarketVariant = Field(default_factory=PolymarketVariant)
    # PM US (QCX) fallback taker coefficient when no per-market feeCoefficient
    # is available (observed 0.06 on live markets 2026-07-21)
    polymarket_us_taker_coef: Decimal = Decimal("0.06")

    def fee(
        self,
        venue: Venue,
        role: Role,
        price: Decimal,
        size: Decimal,
        category: str = "default",
    ) -> Decimal:
        """Fee in dollars for a fill of `size` contracts at probability `price`."""
        if size < 0 or not (Decimal(0) <= price <= Decimal(1)):
            raise ValueError(f"bad fill: price={price} size={size}")
        if venue is Venue.KALSHI:
            coef = self.kalshi_taker_coef if role is Role.TAKER else self.kalshi_maker_coef
            return ceil_cents(coef * size * price * (Decimal(1) - price))
        if venue is Venue.POLYMARKET_US:
            # Regulated QCX DCM: makers free (order preview reports
            # makerCommissionsBasisPoints 0). Taker fee is the venue-reported
            # per-market feeCoefficient (k*P*(1-P)*size) — leg_fee passes it as
            # the override; this fallback uses 0.06, the coefficient observed
            # on live markets 2026-07-21, so a missing catalog entry can only
            # over-state fees, never manufacture edge.
            if role is Role.MAKER:
                return Decimal("0")
            return self.polymarket_us_taker_coef * price * (Decimal(1) - price) * size
        # Polymarket
        if role is Role.MAKER:
            return Decimal("0")
        pm = self.polymarket
        if pm.mode == "us":
            if size == 0:
                return Decimal("0")
            fee = Decimal("0.001") * price * size  # 0.10% of premium
            return max(fee, MILL)
        k = pm.intl_category_coefficients.get(
            category, pm.intl_category_coefficients["default"]
        )
        return k * price * (Decimal(1) - price) * size


def leg_fee(
    schedule: FeeSchedule,
    venue: Venue,
    role: Role,
    price: Decimal,
    size: Decimal,
    category: str = "default",
    taker_coef_override: Decimal | None = None,
) -> Decimal:
    """Fee for one leg, honoring a VENUE-REPORTED per-market coefficient.

    Polymarket's CLOB reports taker_base_fee per market; 0 means the venue
    itself declares the market fee-free (observed on geopolitics/NEH-class
    markets, 2026-07-20). The venue's own word beats any category table, so
    an override of 0 zeroes the taker fee; a nonzero override is used as k
    in k*P*(1-P)*size. None falls back to the schedule."""
    if (
        venue in (Venue.POLYMARKET, Venue.POLYMARKET_US)
        and role is Role.TAKER
        and taker_coef_override is not None
    ):
        return taker_coef_override * price * (Decimal(1) - price) * size
    return schedule.fee(venue, role, price, size, category=category)
