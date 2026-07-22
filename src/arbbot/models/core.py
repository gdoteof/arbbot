"""Core normalized market/book models shared by recorder, scanner, replay, and trader.

All prices are Decimal probabilities in [0, 1] regardless of venue convention
(Kalshi quotes integer cents 1-99; we normalize to 0.01-0.99). All sizes are
Decimal contract counts. Every event carries a local monotonic receive
timestamp (ts_local_ns) — cross-venue comparisons use local receive time only;
venue timestamps are retained for forensics but never compared across venues.
"""

from __future__ import annotations

import enum
import time
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

ZERO = Decimal("0")
ONE = Decimal("1")


def now_local_ns() -> int:
    """Monotonic-ish wall timestamp: ns since epoch from the local clock.

    time.time_ns() is not strictly monotonic, but recorded events additionally
    carry a per-connection sequence; consumers order by (ts_local_ns, seq).
    """
    return time.time_ns()


class Venue(str, enum.Enum):
    KALSHI = "kalshi"
    POLYMARKET = "polymarket"  # international CLOB
    POLYMARKET_US = "polymarket_us"  # regulated QCX DCM (gateway.polymarket.us)


class Role(str, enum.Enum):
    MAKER = "maker"
    TAKER = "taker"


class Side(str, enum.Enum):
    YES = "yes"
    NO = "no"


class MarketStatus(str, enum.Enum):
    ACTIVE = "active"
    CLOSED = "closed"
    RESOLVED = "resolved"
    UNKNOWN = "unknown"


class Market(BaseModel):
    """Normalized market metadata. tick_size/min_order_size are first-class:
    the scanner must never report an opportunity that requires a price between
    ticks or a size below the venue minimum."""

    model_config = ConfigDict(frozen=True)

    venue: Venue
    market_id: str  # Kalshi ticker or Polymarket token_id (YES token)
    title: str = ""
    tick_size: Decimal = Decimal("0.01")
    min_order_size: Decimal = Decimal("1")
    category: str = "default"  # drives Polymarket intl fee coefficient
    status: MarketStatus = MarketStatus.UNKNOWN
    close_time: Optional[str] = None  # ISO 8601, venue-reported
    no_market_id: Optional[str] = None  # Polymarket NO token_id, if distinct
    condition_id: Optional[str] = None  # Polymarket condition id (fee lookup)
    # Venue-reported taker fee coefficient override (k in k*P*(1-P)*size).
    # None = use the schedule; Decimal(0) = venue reports this market FEE-FREE
    # (CLOB taker_base_fee == 0 — e.g. NEH/geopolitics markets).
    taker_fee_coef_override: Optional[Decimal] = None

    @field_validator("tick_size", "min_order_size")
    @classmethod
    def _positive(cls, v: Decimal) -> Decimal:
        if v <= ZERO:
            raise ValueError("must be positive")
        return v


class Level(BaseModel):
    model_config = ConfigDict(frozen=True)
    price: Decimal
    size: Decimal

    @field_validator("price")
    @classmethod
    def _price_range(cls, v: Decimal) -> Decimal:
        if not (ZERO <= v <= ONE):
            raise ValueError(f"price {v} outside [0,1]")
        return v


class Book(BaseModel):
    """One side-of-market order book for a YES contract.

    bids: sorted descending by price (best first).
    asks: sorted ascending by price (best first).
    A NO quote at price q is representable as a YES quote at 1-q; venue
    adapters do that normalization before events reach this model.
    """

    venue: Venue
    market_id: str
    bids: list[Level] = Field(default_factory=list)
    asks: list[Level] = Field(default_factory=list)
    seq: int = 0
    ts_local_ns: int = 0
    ts_venue: Optional[str] = None

    def best_bid(self) -> Optional[Level]:
        return self.bids[0] if self.bids else None

    def best_ask(self) -> Optional[Level]:
        return self.asks[0] if self.asks else None


class BookSnapshot(BaseModel):
    kind: Literal["snapshot"] = "snapshot"
    venue: Venue
    market_id: str
    bids: list[Level]
    asks: list[Level]
    seq: int
    ts_local_ns: int
    ts_venue: Optional[str] = None


class BookDelta(BaseModel):
    """A single price-level change. size is the NEW total size at the level
    (0 removes the level) — both venues' delta feeds normalize to this."""

    kind: Literal["delta"] = "delta"
    venue: Venue
    market_id: str
    side: Literal["bid", "ask"]
    price: Decimal
    size: Decimal
    seq: int
    ts_local_ns: int
    ts_venue: Optional[str] = None


class Trade(BaseModel):
    kind: Literal["trade"] = "trade"
    venue: Venue
    market_id: str
    price: Decimal
    size: Decimal
    taker_side: Optional[Literal["buy", "sell"]] = None
    seq: int = 0
    ts_local_ns: int = 0
    ts_venue: Optional[str] = None


BookEvent = BookSnapshot | BookDelta | Trade
