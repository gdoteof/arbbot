"""Order book reconstruction from snapshot/delta streams.

State machine per (venue, market):

    UNSYNCED --snapshot--> SYNCED --delta(seq=expected)--> SYNCED
                              |--delta(seq gap)--> UNSYNCED (raise GapDetected;
                                     caller requests a fresh snapshot)
                              |--delta(seq <= current)--> dropped (duplicate/stale)

The recorder and the replay harness both build books through this module —
one implementation, one behavior (the design's "recorder's book model IS the
execution engine's book model").
"""

from __future__ import annotations

from decimal import Decimal

from arbbot.models.core import Book, BookDelta, BookSnapshot, Level


class GapDetected(Exception):
    """Sequence gap: the book is no longer trustworthy; resync required."""

    def __init__(self, venue: str, market_id: str, expected: int, got: int):
        self.expected, self.got = expected, got
        super().__init__(f"{venue}/{market_id}: expected seq {expected}, got {got}")


class NotSynced(Exception):
    """Delta arrived before any snapshot."""


class BookBuilder:
    def __init__(self) -> None:
        self._books: dict[tuple[str, str], Book] = {}

    def get(self, venue: str, market_id: str) -> Book | None:
        return self._books.get((venue, market_id))

    def apply_snapshot(self, snap: BookSnapshot) -> Book:
        book = Book(
            venue=snap.venue,
            market_id=snap.market_id,
            bids=sorted(snap.bids, key=lambda l: l.price, reverse=True),
            asks=sorted(snap.asks, key=lambda l: l.price),
            seq=snap.seq,
            ts_local_ns=snap.ts_local_ns,
            ts_venue=snap.ts_venue,
        )
        self._books[(snap.venue, snap.market_id)] = book
        return book

    def apply_delta(self, delta: BookDelta) -> Book | None:
        """Returns the updated book, or None if the delta was stale/duplicate.

        Raises NotSynced before first snapshot, GapDetected on a sequence gap
        (the caller must drop the book and request a snapshot — the book is
        removed from state here so a dead book can never be read as live).
        """
        key = (delta.venue, delta.market_id)
        book = self._books.get(key)
        if book is None:
            raise NotSynced(f"{key}: delta before snapshot")
        if delta.seq <= book.seq:
            return None  # duplicate or out-of-order stale delta: drop
        if delta.seq != book.seq + 1:
            del self._books[key]
            raise GapDetected(delta.venue, delta.market_id, book.seq + 1, delta.seq)

        levels = book.bids if delta.side == "bid" else book.asks
        levels = [l for l in levels if l.price != delta.price]
        if delta.size > 0:
            levels.append(Level(price=delta.price, size=delta.size))
        levels.sort(key=lambda l: l.price, reverse=(delta.side == "bid"))

        updated = book.model_copy(
            update={
                ("bids" if delta.side == "bid" else "asks"): levels,
                "seq": delta.seq,
                "ts_local_ns": delta.ts_local_ns,
                "ts_venue": delta.ts_venue,
            }
        )
        self._books[key] = updated
        return updated

    def is_crossed(self, venue: str, market_id: str) -> bool:
        """A crossed book (best bid >= best ask) is a data-quality signal,
        not an arb — it means our view is stale or the venue is mid-update."""
        book = self._books.get((venue, market_id))
        if not book or not book.bids or not book.asks:
            return False
        return book.bids[0].price >= book.asks[0].price
