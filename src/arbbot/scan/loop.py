"""Scan loop: runs inside the recorder process (Stage 1 — scanning is
read-only and credential-free; only the TRADER is a separate unit).

On every book change, re-scan the relationships containing the changed
market — the market-id -> relationships keying that Stage 4's dirty-marking
optimization slots into (design doc scale note)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookEvent, Market, Trade, Venue, now_local_ns
from arbbot.registry.model import Registry
from arbbot.scan.scanner import (
    LifetimeTracker,
    MakerViabilityTracker,
    Opportunity,
    probe_relationship,
    scan_relationship,
)


class ScanLoop:
    def __init__(
        self,
        registry: Registry,
        markets: dict[tuple[Venue, str], Market],
        books: BookBuilder,
        fees: FeeSchedule,
        scan_dir: str | Path,
        min_edge_per_contract: Decimal = Decimal("0"),
    ):
        self.registry = registry
        self.markets = markets
        self.books = books
        self.fees = fees
        self.scan_dir = Path(scan_dir)
        self.scan_dir.mkdir(parents=True, exist_ok=True)
        self.min_edge = min_edge_per_contract
        self._trackers: dict[str, LifetimeTracker] = {}
        self._maker = MakerViabilityTracker()
        self._probe: dict[str, dict] = {}   # rel -> {scans, ready, best_edge, last_ns}
        self._probe_flushed = 0.0
        self._probe_day = ""
        self._rewards: dict[str, dict] = {}
        self._load_rewards()

    def _load_rewards(self) -> None:
        try:
            doc = json.loads((self.scan_dir / "rewards.json").read_text())
            self._rewards = {p["token"]: p for p in doc.get("pools", [])}
        except Exception:
            self._rewards = {}

    def _append(self, name: str, day: str, payload: dict) -> None:
        with open(self.scan_dir / f"{name}-{day}.jsonl", "a") as f:
            f.write(json.dumps(payload) + "\n")

    def on_event(self, event: BookEvent, utc_day: str) -> list[Opportunity]:
        """Called by the recorder after each applied event; scans affected
        relationships and persists opportunities + closed lifetimes."""
        if isinstance(event, Trade):
            return []
        ts = now_local_ns()
        emitted: list[Opportunity] = []
        from arbbot.models.core import MarketStatus
        for rel in self.registry.affected_by(event.venue, event.market_id):
            if rel.verdict.value == "rejected":
                continue  # documented traps: their "arbs" are known-false
            if any(
                (m := self.markets.get((l.venue, l.market_id))) is not None
                and m.status not in (MarketStatus.ACTIVE, MarketStatus.UNKNOWN)
                for l in rel.legs
            ):
                continue  # a leg has closed/resolved: no longer a live relationship
            opps = scan_relationship(
                rel, self.books, self.markets, self.fees, ts, self.min_edge
            )
            tracker = self._trackers.setdefault(rel.id, LifetimeTracker())
            for first, lifetime_ns in tracker.observe(opps, ts):
                self._append(
                    "lifetimes",
                    utc_day,
                    {**first.to_json_dict(), "lifetime_ns": lifetime_ns},
                )
            for o in opps:
                self._append("opportunities", utc_day, o.to_json_dict())
            emitted.extend(opps)
            # maker viability: could we rest at/inside top of book on either
            # leg and hedge profitably? Emits completed viability spells.
            for spell in self._maker.observe(
                rel, self.books, self.markets, self.fees, ts,
                rewards=self._rewards,
            ):
                self._append("maker", utc_day, spell)
            # probe: closeness-to-crossing even when nothing clears — the
            # "quiet because tight" vs "quiet because blind" discriminator
            st = self._probe.setdefault(rel.id, {"scans": 0, "ready": 0,
                                                 "best_edge": None, "last_ns": 0,
                                                 "legs_missing": 0})
            st["scans"] += 1
            st["last_ns"] = ts
            missing = sum(
                1 for l in rel.legs
                if self.books.get(l.venue.value, l.market_id) is None
                or self.books.is_crossed(l.venue.value, l.market_id)
            )
            if missing:
                st["legs_missing"] += 1
            edge = probe_relationship(rel, self.books, self.markets, self.fees)
            if edge is not None:
                st["ready"] += 1
                if st["best_edge"] is None or edge > st["best_edge"]:
                    st["best_edge"] = edge
        if utc_day != self._probe_day:
            self._probe = {}  # counts are per-UTC-day (adversarial review P2)
            self._probe_day = utc_day
        import time as _t
        if _t.monotonic() - self._probe_flushed > 60:
            self._probe_flushed = _t.monotonic()
            self._load_rewards()  # picks up the daily snapshot refresh
            with open(self.scan_dir / f"probe-{utc_day}.json", "w") as f:
                json.dump(self._probe, f)
        return emitted
