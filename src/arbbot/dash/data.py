"""Dashboard aggregation: incremental JSONL tailing -> summary JSON.

Files grow all day; re-parsing them per request would melt. Each source file
keeps a byte offset and we parse only appended complete lines. Aggregates are
additive (counts, sums, hourly buckets) plus a bounded reservoir per
relationship for approximate medians — good enough for a glance dashboard,
never for the replay gate (which reads the raw files properly).
"""

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from arbbot.fees.curves import ceil_cents
from arbbot.models.core import Venue
from arbbot.record import archive
from arbbot.registry.model import Registry
from arbbot.report.daily import annualized_return_on_locked_capital

RESERVOIR = 500

# Fee scenarios: recompute each stored opportunity's edge under alternative
# fee assumptions from its per-leg (venue, role, vwap, size, fee) record.
# "scanned" = fees exactly as the scanner charged at observation time.
# "pm-zero" = Polymarket taker legs free (venue-reported zero-fee hypothesis,
#             confirmed for NEH/geopolitics-class markets via CLOB
#             taker_base_fee=0 on 2026-07-20).
# "stress-2x" = every fee doubled (schedule-drift stress).
# "kalshi-maker" = Kalshi legs at maker coefficient (Stage-4 make-take world).
FEE_SCENARIOS = ("scanned", "pm-zero", "stress-2x", "kalshi-maker")


def _scenario_leg_fee(scenario: str, venue: str, role: str,
                      vwap: float, size: float, stored_fee: float) -> float:
    if scenario == "pm-zero":
        return 0.0 if (venue == "polymarket" and role == "taker") else stored_fee
    if scenario == "stress-2x":
        return stored_fee * 2
    if scenario == "kalshi-maker" and venue == "kalshi":
        from decimal import Decimal
        return float(ceil_cents(Decimal("0.0175") * Decimal(str(size))
                                * Decimal(str(vwap)) * (1 - Decimal(str(vwap)))))
    return stored_fee


def _merge_counts(live: dict[str, int], archived: dict[str, int]) -> dict[str, int]:
    """Per-venue event totals = live JSONL counts + archived (Parquet) counts."""
    out = dict(live)
    for venue, n in archived.items():
        out[venue] = out.get(venue, 0) + n
    return out


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[len(s) // 2]


def _scenario_deques() -> dict:
    return {sc: deque(maxlen=RESERVOIR) for sc in FEE_SCENARIOS}


@dataclass
class RelAgg:
    observations: int = 0
    edge_reservoir: dict = field(default_factory=_scenario_deques)
    roc_reservoir: dict = field(default_factory=_scenario_deques)
    size_reservoir: deque = field(default_factory=lambda: deque(maxlen=RESERVOIR))
    lifetimes_s: deque = field(default_factory=lambda: deque(maxlen=RESERVOIR))
    closed: int = 0
    last_seen_ns: int = 0
    sub_minimum: int = 0


class _TailFile:
    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.partial = b""

    def new_line_count(self) -> int:
        """Count appended newlines WITHOUT parsing — raw event files are
        millions of lines and the dashboard only needs the count."""
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return 0
        if size < self.offset:
            self.offset = 0
        n = 0
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            while chunk := f.read(1 << 20):
                n += chunk.count(b"\n")
            self.offset = size
        return n

    def new_lines(self) -> list[dict]:
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return []
        if size < self.offset:  # rotated/truncated: restart
            self.offset = 0
            self.partial = b""
        if size == self.offset:
            return []
        out = []
        with open(self.path, "rb") as f:
            f.seek(self.offset)
            chunk = self.partial + f.read(size - self.offset)
            self.offset = size
        lines = chunk.split(b"\n")
        self.partial = lines.pop()  # incomplete tail (or b"")
        for raw in lines:
            if not raw.strip():
                continue
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return out


class DashboardData:
    def __init__(self, scan_dir: str | Path, raw_dir: str | Path,
                 health_path: str | Path, registry_path: str | Path):
        self.scan_dir = Path(scan_dir)
        self.raw_dir = Path(raw_dir)
        self.health_path = Path(health_path)
        self.registry_path = Path(registry_path)
        self._tails: dict[Path, _TailFile] = {}
        self._raw_counts: dict[str, int] = {}   # venue -> events (all days)
        self._raw_tails: dict[Path, _TailFile] = {}
        self._rel: dict[tuple[str, str], RelAgg] = {}  # (day, rel_id)
        self._maker: dict[tuple, dict] = {}  # (day, rel, leg, side) -> agg
        self._hourly: dict[str, int] = {}  # "YYYY-MM-DDTHH" -> observations
        self._opp_days_seen: set[str] = set()  # days ingested (jsonl or parquet)
        self._registry_cache: tuple[float, Any] | None = None
        self._last_refresh = 0.0

    # -- registry ------------------------------------------------------------
    def registry(self) -> Optional[Registry]:
        try:
            mtime = self.registry_path.stat().st_mtime
        except FileNotFoundError:
            return None
        if self._registry_cache and self._registry_cache[0] == mtime:
            return self._registry_cache[1]
        reg = Registry.load(str(self.registry_path))
        self._registry_cache = (mtime, reg)
        return reg

    # -- ingest --------------------------------------------------------------
    def refresh(self, min_interval_s: float = 3.0) -> None:
        now = time.monotonic()
        if now - self._last_refresh < min_interval_s:
            return
        self._last_refresh = now
        for path in sorted(self.scan_dir.glob("opportunities-*.jsonl")):
            day = path.stem.replace("opportunities-", "")
            self._opp_days_seen.add(day)
            tail = self._tails.setdefault(path, _TailFile(path))
            for o in tail.new_lines():
                self._ingest_opportunity(day, o)
        # archived (Parquet) opportunity days: ingest once, skipping any day
        # already seen live via JSONL (avoids double-count)
        for path in sorted((self.scan_dir.parent / "parquet").glob("opportunities-*.parquet")):
            day = path.stem.replace("opportunities-", "")
            if day in self._opp_days_seen:
                continue
            self._opp_days_seen.add(day)
            for o in archive.iter_day(self.scan_dir, "opportunities", day):
                self._ingest_opportunity(day, o)
        for path in sorted(self.scan_dir.glob("maker-*.jsonl")):
            day = path.stem.replace("maker-", "")
            tail = self._tails.setdefault(path, _TailFile(path))
            for sp in tail.new_lines():
                key = (day, sp.get("relationship_id", "?"),
                       sp.get("maker_leg", "?"), sp.get("quote_side", "?"))
                agg = self._maker.setdefault(key, {"spells": 0, "viable_s": 0.0,
                                                   "dd_s": 0.0, "margins": []})
                agg["spells"] += 1
                agg["viable_s"] += float(sp.get("duration_s", 0))
                if sp.get("rewards_band"):
                    agg["dd_s"] += float(sp.get("duration_s", 0))
                if len(agg["margins"]) < 500:
                    agg["margins"].append(float(sp.get("margin_at_start", 0)))
        for path in sorted(self.scan_dir.glob("lifetimes-*.jsonl")):
            day = path.stem.replace("lifetimes-", "")
            tail = self._tails.setdefault(path, _TailFile(path))
            for o in tail.new_lines():
                agg = self._agg(day, o.get("relationship_id", "?"))
                agg.closed += 1
                agg.lifetimes_s.append(float(o.get("lifetime_ns", 0)) / 1e9)
        for path in sorted(self.raw_dir.glob("*.jsonl")):
            venue = path.stem.split("-")[0]
            tail = self._raw_tails.setdefault(path, _TailFile(path))
            self._raw_counts[venue] = (
                self._raw_counts.get(venue, 0) + tail.new_line_count()
            )

    def _agg(self, day: str, rel_id: str) -> RelAgg:
        return self._rel.setdefault((day, rel_id), RelAgg())

    def _ingest_opportunity(self, day: str, o: dict) -> None:
        rel_id = o.get("relationship_id", "?")
        agg = self._agg(day, rel_id)
        agg.observations += 1
        ts = int(o.get("ts_local_ns", 0))
        agg.last_seen_ns = max(agg.last_seen_ns, ts)
        if o.get("bucket") == "sub_minimum":
            agg.sub_minimum += 1
        try:
            size = float(o.get("size", 0))
            gross = float(o.get("gross_cost", 0))
            payoff = float(o.get("min_payoff", 1)) * size
            legs = o.get("basket") or []
            agg.size_reservoir.append(size)
            from decimal import Decimal
            stored_net = float(o.get("net_edge_total", 0))
            for sc in FEE_SCENARIOS:
                if sc == "scanned" or not legs:
                    # trust the scanner's own number; recompute only when we
                    # have per-leg data AND an alternative scenario is asked
                    net_sc = stored_net
                else:
                    fees_sc = sum(
                        _scenario_leg_fee(sc, l.get("venue", ""), l.get("role", ""),
                                          float(l.get("vwap", 0)), size,
                                          float(l.get("fee", 0)))
                        for l in legs
                    )
                    net_sc = payoff - gross - fees_sc
                if size > 0:
                    agg.edge_reservoir[sc].append(net_sc / size)
                roc = annualized_return_on_locked_capital(
                    Decimal(str(net_sc)), Decimal(str(gross)),
                    ts, o.get("max_hold_close_time"),
                )
                if roc is not None:
                    agg.roc_reservoir[sc].append(float(roc))
        except (TypeError, ValueError, KeyError):
            pass
        hour = datetime.fromtimestamp(ts / 1e9, tz=timezone.utc).strftime("%Y-%m-%dT%H")
        self._hourly[hour] = self._hourly.get(hour, 0) + 1

    # -- output --------------------------------------------------------------
    def _days_in_range(self, rng: str) -> Optional[set[str]]:
        today = datetime.now(timezone.utc).date()
        if rng == "today":
            return {today.isoformat()}
        if rng == "7d":
            from datetime import timedelta
            return {(today - timedelta(days=i)).isoformat() for i in range(7)}
        return None  # all

    def summary(self, rng: str = "today", fee_scenario: str = "scanned") -> dict:
        if fee_scenario not in FEE_SCENARIOS:
            fee_scenario = "scanned"
        self.refresh()
        days = self._days_in_range(rng)
        rel_rows: dict[str, dict] = {}
        total_obs = 0
        closed = 0
        lifetimes: list[float] = []
        sigs_last_seen: dict[str, int] = {}
        for (day, rel_id), agg in self._rel.items():
            if days is not None and day not in days:
                continue
            total_obs += agg.observations
            closed += agg.closed
            lifetimes.extend(agg.lifetimes_s)
            row = rel_rows.setdefault(rel_id, {
                "id": rel_id, "observations": 0, "closed": 0, "sub_minimum": 0,
                "edges": [], "rocs": [], "sizes": [], "lifetimes": [], "last_seen_ns": 0,
            })
            row["observations"] += agg.observations
            row["closed"] += agg.closed
            row["sub_minimum"] += agg.sub_minimum
            row["edges"].extend(agg.edge_reservoir[fee_scenario])
            row["rocs"].extend(agg.roc_reservoir[fee_scenario])
            row["sizes"].extend(agg.size_reservoir)
            row["lifetimes"].extend(agg.lifetimes_s)
            row["last_seen_ns"] = max(row["last_seen_ns"], agg.last_seen_ns)
            sigs_last_seen[rel_id] = row["last_seen_ns"]

        reg = self.registry()
        reg_meta: dict[str, dict] = {}
        vetted = total_rels = 0
        if reg:
            total_rels = len(reg.relationships)
            for r in reg.relationships:
                if r.vetted_by.value == "human" and r.verdict.value != "rejected":
                    vetted += 1
                reg_meta[r.id] = {
                    "type": r.type.value, "tranche": r.tranche.value,
                    "oracle_risk": r.oracle_risk.value,
                    "vetted": r.vetted_by.value, "verdict": r.verdict.value,
                }

        # every registered relationship appears — "watched and quiet" is
        # information (a monotone ladder staying monotone is the healthy state),
        # not absence of coverage.
        for rel_id in reg_meta:
            rel_rows.setdefault(rel_id, {
                "id": rel_id, "observations": 0, "closed": 0, "sub_minimum": 0,
                "edges": [], "rocs": [], "sizes": [], "lifetimes": [], "last_seen_ns": 0,
            })
        rows = []
        for rel_id, row in rel_rows.items():
            meta = reg_meta.get(rel_id, {})
            rows.append({
                "id": rel_id,
                "type": meta.get("type", "?"),
                "tranche": meta.get("tranche", "?"),
                "oracle_risk": meta.get("oracle_risk", "?"),
                "vetted": meta.get("vetted", "?"),
                "observations": row["observations"],
                "closed": row["closed"],
                "sub_minimum": row["sub_minimum"],
                "edge_p50": round(_median(row["edges"]), 4),
                "roc_p50": round(_median(row["rocs"]), 4),
                "size_p50": round(_median(row["sizes"]), 1),
                "lifetime_p50_s": round(_median(row["lifetimes"]), 1),
                "last_seen_ns": row["last_seen_ns"],
            })
        rows.sort(key=lambda r: (-r["observations"], r["id"]))

        hourly = []
        for hour, n in sorted(self._hourly.items()):
            if days is not None and hour[:10] not in days:
                continue
            hourly.append({"hour": hour, "observations": n})

        maker_rows: dict[tuple, dict] = {}
        for (day, rel_id, leg, side), agg in self._maker.items():
            if days is not None and day not in days:
                continue
            row = maker_rows.setdefault((rel_id, leg, side), {
                "relationship_id": rel_id, "maker_leg": leg, "quote_side": side,
                "spells": 0, "viable_s": 0.0, "dd_s": 0.0, "margins": []})
            row["spells"] += agg["spells"]
            row["viable_s"] += agg["viable_s"]
            row["dd_s"] += agg.get("dd_s", 0.0)
            row["margins"].extend(agg["margins"])
        maker = [
            {"relationship_id": r["relationship_id"], "maker_leg": r["maker_leg"],
             "quote_side": r["quote_side"], "spells": r["spells"],
             "viable_s": round(r["viable_s"], 1),
             "double_dip_s": round(r.get("dd_s", 0.0), 1),
             "margin_p50": round(_median(r["margins"]), 4)}
            for r in maker_rows.values()
        ]
        maker.sort(key=lambda r: -r["viable_s"])

        health = self._health()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "range": rng,
            "fee_scenario": fee_scenario,
            "hero": {"observations": total_obs,
                     "distinct_relationships": len([r for r in rows if r["observations"] > 0])},
            "tiles": {
                "events_recorded": _merge_counts(
                    self._raw_counts,
                    archive.load_archived_counts(self.raw_dir.parent / "parquet")),
                "closed_crossings": closed,
                "lifetime_p50_s": round(_median(lifetimes), 1),
                "best_roc_p50": max((r["roc_p50"] for r in rows), default=0.0),
                "universe": {"total": total_rels, "vetted": vetted},
            },
            "hourly": hourly,
            "relationships": rows,
            "maker": maker,
            "probe": self._read_json(self.scan_dir / f"probe-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json") or {},
            "unwind": self._read_json(self.scan_dir / f"unwind-{sorted(days)[-1] if days else datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json"),
            "rewards": self._read_json(self.scan_dir / "rewards.json"),
            "maketake": self._read_json(self.scan_dir / f"maketake-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json")
                        or self._read_json(sorted(self.scan_dir.glob("maketake-*.json"))[-1]
                                           if sorted(self.scan_dir.glob("maketake-*.json")) else "/nonexistent"),
            "health": health,
            "trades": self._trades(),
            "order_activity": self._order_activity(),
            "sports": self._sports(),
            "maker_probe": self._maker_probe(),
            "capacity": self._read_json(self.raw_dir.parent / "reports" / "capacity.json"),
            "positions": self._positions_enriched(),
            "exits": self._exits(),
        }

    def _positions_enriched(self) -> dict:
        """Recon snapshot rows + the maker-exit delta from marks (by rel)."""
        doc = self._read_json(self.raw_dir.parent / "exec" / "positions.json") or {}
        marks = self._read_json(self.raw_dir.parent / "exec" / "marks.json") or {}
        by_kt: dict = {}   # a pair can hold several baskets — keep the BEST exit
        for m in marks.get("positions", []):
            kt = m.get("kalshi_ticker")
            if kt is None or m.get("maker_exit_ct") is None:
                continue
            cur = by_kt.get(kt)
            if cur is None or m["maker_exit_ct"] > cur["maker_exit_ct"]:
                by_kt[kt] = m
        for row in doc.get("rows", []):
            m = by_kt.get(row.get("kt"))
            if m is not None:
                row["mx_ct"] = m.get("maker_exit_ct")
                row["mx_elig"] = m.get("maker_exit_eligible")
        return doc

    def _exits(self) -> dict:
        """Opportunistic maker-exit timeline (Geoff 2026-07-24): per-2-min
        samples of each open basket's passive-exit delta from profit, appended
        by mark_positions to marks_history.jsonl. Shaped like the sports
        history (t / best / tops) so the chart pattern transfers."""
        p = self.raw_dir.parent / "exec" / "marks_history.jsonl"
        try:
            lines = p.read_text().splitlines()[-1500:]  # ~2 days at 2-min cadence
        except OSError:
            return {"history": [], "latest": []}
        history = []
        latest_rows: list = []
        for line in lines:
            try:
                s = json.loads(line)
            except ValueError:
                continue
            rows = [r for r in s.get("rows", []) if r.get("mx") is not None]
            if not rows:
                continue
            rows.sort(key=lambda r: -r["mx"])
            history.append({"t": s["t"],
                            "best": round(rows[0]["mx"] * 100, 2),
                            "n_elig": sum(1 for r in rows if r.get("elig")),
                            "tops": [{"e": round(r["mx"] * 100, 2),
                                      "n": str(r["rel"])[:40]} for r in rows[:5]]})
            latest_rows = rows
        marks = self._read_json(self.raw_dir.parent / "exec" / "marks.json") or {}
        by_key = {(m["relationship_id"], m.get("ts")): m for m in marks.get("positions", [])}
        latest = []
        for r in latest_rows:
            m = by_key.get((r["rel"], r.get("ts0"))) or {}
            latest.append({"rel": r["rel"], "ts0": r.get("ts0"), "qty": r.get("qty"),
                           "mx_c": round(r["mx"] * 100, 2), "elig": bool(r.get("elig")),
                           "mark": r.get("mark"), "title": m.get("title"),
                           "fwd_apr": m.get("forward_hold_apr"),
                           "flagged": bool(m.get("unwind_signal") or m.get("unwind_hard"))})
        latest.sort(key=lambda r: -r["mx_c"])
        return {"history": history, "latest": latest}

    def _sports(self) -> dict:
        """Real-time cross-venue sports lines written by the WS engine
        (scripts/sports_arb.py). Never a fresh REST poll — that shows phantom
        crossings from stale-vs-fresh across venues. `stale_s` lets the UI mark
        the feed dim during the engine's 15-min self-restart gap."""
        doc = self._read_json(self.raw_dir.parent / "exec" / "sports_lines.json")
        if not doc:
            return {"games": [], "n_live": 0, "n_crossed": 0, "stale_s": None, "generated_at": None}
        try:
            gen = datetime.strptime(doc.get("generated_at", ""), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            doc["stale_s"] = round(time.time() - gen.timestamp(), 0)
        except (ValueError, TypeError):
            doc["stale_s"] = None
        doc["history"] = self._sports_history()
        doc["mt_probe"] = self._mt_probe()
        return doc

    def _maker_probe(self) -> dict:
        """Candidate-pair status written by the PM-US maker probe
        (scripts/pmus_maker_probe.py) every 5s: per-pair books, our quote
        targets, guard blocks and distance-to-fill. stale_s marks the feed
        dim when the probe is down."""
        doc = self._read_json(self.raw_dir.parent / "exec" / "pmus_maker_status.json")
        if not doc:
            return {"pairs": [], "session": None, "stale_s": None}
        try:
            gen = datetime.strptime(doc.get("generated_at", ""),
                                    "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            doc["stale_s"] = round(time.time() - gen.timestamp(), 0)
        except (ValueError, TypeError):
            doc["stale_s"] = None
        return doc

    def _mt_probe(self) -> dict:
        """Make-take adverse-selection experiment scorecard: per-fill expected vs
        realized lock, and the aggregate that answers 'do we get picked off?'"""
        p = self.raw_dir.parent / "exec" / "sports_mt_probe.jsonl"
        fills = []
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    fills.append(json.loads(line))
                except ValueError:
                    pass
        # normalize old/new record schemas: derive naked + captured-lock
        for r in fills:
            hedged = r.get("hedged", 0)
            r["_naked"] = r.get("naked", r.get("qty", 0) - hedged)
            cap = r.get("captured_lock_c", r.get("realized_lock_c"))
            r["_captured_c"] = cap if hedged else None
        fills.sort(key=lambda r: -r.get("ts", 0))
        n = len(fills)
        qty = sum(r.get("qty", 0) for r in fills)
        picked = sum(1 for r in fills if r.get("picked_off"))
        naked = sum(r["_naked"] for r in fills)
        captured_fills = [r for r in fills if r["_captured_c"] is not None]
        mean_cap = (round(sum(r["_captured_c"] for r in captured_fills) / len(captured_fills), 2)
                    if captured_fills else None)
        # dollars actually locked on the hedged (captured) portion
        captured_usd = sum((r["_captured_c"] / 100.0) * r.get("hedged", 0) for r in captured_fills)
        return {
            "fills": fills[:40],
            "n": n, "contracts": qty, "cap": 20,
            "picked_off": picked,
            "picked_off_pct": round(picked / n * 100, 0) if n else None,
            "captured": len(captured_fills),
            "naked_held": naked,
            "mean_captured_c": mean_cap,
            "mean_exp_c": round(sum(r.get("exp_lock_c", 0) for r in fills) / n, 2) if n else None,
            "captured_usd": round(captured_usd, 2),
        }

    def _sports_history(self, hours: float = 8, cap: int = 900) -> list:
        """Best-edge-across-live-games time series for today, written by the WS
        engine every ~15s. Bounded to the last `hours` and `cap` points so the
        chart stays snappy on a long day."""
        p = (self.raw_dir.parent / "exec" /
             f"sports_edge-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.jsonl")
        if not p.exists():
            return []
        cutoff = time.time() - hours * 3600
        out = []
        try:
            for line in p.read_text().splitlines()[-(cap * 3):]:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if e.get("t", 0) >= cutoff:
                    out.append(e)
        except OSError:
            return []
        return out[-cap:]

    def _trades(self) -> dict:
        """Executed cross-venue arbitrage baskets from data/exec/trading.db,
        netted against close records (mirrors arbbot.exec.ledger convention):
        a fully-unwound basket renders as CLOSED with its realized P&L."""
        from arbbot.exec import ledgerdb
        conn = ledgerdb.connect(self.raw_dir.parent / "exec" / "trading.db")
        try:
            raw = ledgerdb.all_records_db(conn)   # corrected view, ledger order
            netted = ledgerdb.open_baskets_db(conn)  # open net of closes (aggregates)
        finally:
            conn.close()
        netted_keys = {(r.get("relationship_id"), r.get("ts")) for r in netted}
        unwound_by_key: dict[tuple, dict] = {}
        for r in raw:
            if r.get("status") == "unwound" and r.get("closes_ts") is not None:
                k = (r.get("relationship_id"), r["closes_ts"])
                agg = unwound_by_key.setdefault(k, {"qty": 0, "realized_usd": 0.0, "ts": 0})
                agg["qty"] += float(r.get("qty", 0))
                agg["realized_usd"] += float(r.get("realized_pnl_usd") or 0)
                agg["ts"] = max(agg["ts"], r.get("ts", 0))
        rows = []
        for r in raw:
            if r.get("status") == "unwound":
                continue  # folded into the parent basket's row below
            if r.get("status") == "open":
                u = unwound_by_key.get((r.get("relationship_id"), r.get("ts")))
                if u is None:
                    r["_status"] = "open"
                else:
                    r["closed_qty"] = u["qty"]
                    r["realized_usd"] = round(u["realized_usd"], 2)
                    r["closed_ts"] = u["ts"]
                    r["_status"] = ("partial" if (r.get("relationship_id"), r.get("ts"))
                                    in netted_keys else "closed")
            else:
                r["_status"] = r.get("status") or "?"
            rows.append(r)
        rows.sort(key=lambda r: -r.get("ts", 0))
        open_rows = netted
        # merge live mark-to-market (scripts/mark_positions.py, on a 2-min timer)
        marks = self._read_json(self.raw_dir.parent / "exec" / "marks.json") or {}
        # key by (rel, ts) so multiple records of the same relationship (e.g.
        # several sports rehedge baskets) each get THEIR OWN mark row — a
        # rel-only key pasted one basket's marks onto every sibling
        by_key = {(m["relationship_id"], m.get("ts")): m for m in marks.get("positions", [])}
        by_rel = {m["relationship_id"]: m for m in marks.get("positions", [])}
        for r in rows:
            m = by_key.get((r.get("relationship_id"), r.get("ts"))) or by_rel.get(r.get("relationship_id"))
            if m:
                r["mark"] = {k: m.get(k) for k in
                             ("mark_pnl_usd", "liq_value_usd", "converged_pct",
                              "forward_hold_apr", "natural_hold_apr", "unwind_apr", "resolves_estimated",
                              "unwind_signal", "unwind_hard", "reverse_signal", "reverse_edge_c")}
                # prefer the resolver's (authoritative) date over the record's
                if m.get("resolves_by"):
                    r["resolves_by"] = m["resolves_by"]
        lats = [float(r["hedge_latency_ms"]) for r in rows if r.get("hedge_latency_ms") is not None]
        # portfolio APR: raw RoC annualized over the capital-weighted time to
        # resolution — the "7% locked" expressed as a comparable yearly rate.
        today = datetime.now(timezone.utc).date()
        num = den = 0.0
        for r in open_rows:
            rb = (r.get("mark") or {}).get("resolves_by") or r.get("resolves_by")
            if rb and r.get("cost_usd"):
                try:
                    yrs = max((date.fromisoformat(str(rb)[:10]) - today).days, 1) / 365.25
                except ValueError:
                    continue
                num += r["cost_usd"] * yrs
                den += r["cost_usd"]
        cap = sum(r.get("cost_usd", 0) for r in open_rows)
        prof = sum(r.get("profit_usd", 0) for r in open_rows)
        wavg_yrs = (num / den) if den else None
        portfolio_apr = round(prof / cap / wavg_yrs * 100, 1) if (cap and wavg_yrs) else None
        realized = round(sum(float(r.get("realized_pnl_usd") or 0)
                             for r in raw if r.get("status") in ("unwound", "realized")), 2)

        # per-category breakdown: open capital/locked + realized (incl losses)
        FAMILIES = ("france-pres-27", "time-poty-26", "nobel-peace-26",
                    "brazil-pres-26", "fedcut-26", "bestai-26dec")

        # game-name -> league map so rehedge/flatten records (whose ids carry
        # the game, not the league) still land in their real category
        game_league = {}
        with __import__("contextlib").suppress(Exception):
            smap = json.loads((self.scan_dir / "sports_equiv_map.json").read_text())
            for sm in smap.get("matches", []):
                game_league[sm["teams"].replace(" vs ", "@")[:30]] = sm["league"]

        def _category(rid: str) -> str:
            rid = str(rid or "")
            if rid.startswith("sports-"):
                seg = rid.split("-", 2)[1] if "-" in rid[7:] else "misc"
                if seg in ("mlb", "itfme", "itfwo", "wta", "atp", "kbo", "npb"):
                    return f"sports-{seg}"
                if seg in ("rehedge", "flatten"):
                    game = rid.split("-", 2)[2] if rid.count("-") >= 2 else ""
                    lg = game_league.get(game[:30])
                    return f"sports-{lg}" if lg else "sports-misc"
                return "sports-misc"
            for f2 in FAMILIES:
                if f2 in rid:
                    return f2
            return "other"

        cats: dict[str, dict] = {}
        for r in netted:
            cat = cats.setdefault(_category(r.get("relationship_id")),
                                  {"open": 0, "capital": 0.0, "locked": 0.0,
                                   "realized": 0.0, "losses": 0.0})
            cat["open"] += 1
            cat["capital"] += float(r.get("cost_usd") or 0)
            cat["locked"] += float(r.get("profit_usd") or 0)
        for r in raw:
            if r.get("status") not in ("unwound", "realized"):
                continue
            v = float(r.get("realized_pnl_usd") or 0)
            cat = cats.setdefault(_category(r.get("relationship_id")),
                                  {"open": 0, "capital": 0.0, "locked": 0.0,
                                   "realized": 0.0, "losses": 0.0})
            cat["realized"] += v
            if v < 0:
                cat["losses"] += v
        categories = [{"category": k, "open": v["open"],
                       "capital": round(v["capital"], 2), "locked": round(v["locked"], 2),
                       "realized": round(v["realized"], 2), "losses": round(v["losses"], 2)}
                      for k, v in sorted(cats.items(), key=lambda kv: -kv[1]["capital"])]

        # pickoff -> salvage funnel (sports probe lifecycle)
        funnel = {}
        mp_path = self.raw_dir.parent / "exec" / "sports_mt_probe.jsonl"
        if mp_path.exists():
            import json as _json2
            fills = clean = picked = 0
            qty = clean_q = picked_q = 0
            for line in mp_path.read_text().splitlines():
                if not line.strip():
                    continue
                try:
                    m = _json2.loads(line)
                except ValueError:
                    continue
                fills += 1
                qty += int(m.get("qty") or 0)
                if m.get("picked_off"):
                    picked += 1
                    picked_q += int(m.get("naked") or m.get("qty") or 0)
                else:
                    clean += 1
                    clean_q += int(m.get("qty") or 0)
            salv_n = salv_q = 0
            salv_locked = 0.0
            for r in raw:
                ti = str(r.get("title") or "")
                if ("naked rehedge" in ti or r.get("strategy") == "flatten")                         and r.get("status") in ("open", "realized"):
                    salv_n += 1
                    salv_q += int(r.get("qty") or 0)
                    salv_locked += float(r.get("profit_usd") or r.get("realized_pnl_usd") or 0)
            naked_losses = round(sum(float(r.get("realized_pnl_usd") or 0)
                                     for r in raw if r.get("strategy") == "naked-settlement"), 2)
            outstanding = 0
            with __import__("contextlib").suppress(Exception):
                outstanding = sum(int(h.get("qty") or 0) for h in _json2.loads(
                    (self.raw_dir.parent / "exec" / "sports_naked.json").read_text()))
            funnel = {"fills": fills, "contracts": qty,
                      "clean_captures": clean, "clean_contracts": clean_q,
                      "picked_off": picked, "picked_contracts": picked_q,
                      "salvaged_baskets": salv_n, "salvaged_contracts": salv_q,
                      "salvaged_locked_usd": round(salv_locked, 2),
                      "naked_losses_usd": naked_losses,
                      "outstanding_naked": outstanding}
        return {
            "rows": rows,
            "n": len(rows),
            "n_open": len(open_rows),
            "n_closed": sum(1 for r in rows if r["_status"] == "closed"),
            "realized_usd": realized,
            "categories": categories,
            "funnel": funnel,
            "capital_locked": round(sum(r.get("cost_usd", 0) for r in open_rows), 2),
            "profit_locked": round(sum(r.get("profit_usd", 0) for r in open_rows), 2),
            "marks": marks.get("totals", {}),
            "marks_at": marks.get("generated_at"),
            "hedge_latency_p50": round(_median(lats), 0) if lats else None,
            "hedge_latency_n": len(lats),
            "portfolio_apr": portfolio_apr,
            "wavg_years": round(wavg_yrs, 2) if wavg_yrs else None,
        }

    def _order_activity(self, limit: int = 250) -> dict:
        """Chronological order-lifecycle feed (place / move / cancel / fail)
        merged across every quoter's intent log. Powers the Order Activity view.
        A 'move' is a reprice (place carrying `replaces`)."""
        events = []
        for p in self.scan_dir.glob("trader-intents-*.jsonl"):
            rel_id = p.stem[len("trader-intents-"):]
            try:
                lines = p.read_text().splitlines()[-600:]  # bound work on long logs
            except OSError:
                continue
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if "place" in e:
                    ev = {"action": "move" if e.get("replaces") else "place",
                          "market": e["place"], "price": e.get("price"),
                          "old_price": e.get("old_price"), "count": e.get("count"),
                          "order_id": e.get("order_id")}
                elif "cancel" in e:
                    ev = {"action": "cancel", "market": e["cancel"],
                          "price": e.get("price"), "order_id": e.get("order_id")}
                elif "place_failed" in e:
                    ev = {"action": "fail", "market": e["place_failed"],
                          "reason": e.get("reason")}
                else:
                    continue  # risk 'skip' etc. — not an order lifecycle event
                venue = e.get("venue")
                if not venue:  # older logs pre-date venue tagging — infer from id
                    venue = "kalshi" if str(ev.get("market", "")).startswith("KX") else "polymarket_us"
                ev.update(ts=e.get("ts"), rel=rel_id, venue=venue, side=e.get("side"))
                events.append(ev)
        # maker fills — the "destroyed by fill" outcome — from the trading DB,
        # so the graph shows the full lifecycle (place -> move -> cancel|fill).
        # Bounded to the newest 400 make-take baskets (was: last 400 log lines).
        from arbbot.exec import ledgerdb
        conn = ledgerdb.connect(self.raw_dir.parent / "exec" / "trading.db")
        try:
            trows = [json.loads(row["raw_json"]) for row in conn.execute(
                "SELECT raw_json FROM basket WHERE strategy='make-take'"
                " ORDER BY ts_ns DESC LIMIT 400")]
        finally:
            conn.close()
        for r in trows:
            maker = next((l for l in r.get("legs", []) if l.get("role") == "maker"), None)
            if not maker:
                continue
            events.append({"action": "fill", "market": maker.get("market_id"),
                           "price": maker.get("yes_price"), "count": maker.get("qty"),
                           "ts": r.get("ts"), "rel": r.get("relationship_id"),
                           "venue": maker.get("venue"), "side": maker.get("side")})
        events.sort(key=lambda x: -(x.get("ts") or 0))
        counts: dict[str, int] = {}
        for e in events:
            counts[e["action"]] = counts.get(e["action"], 0) + 1
        return {"events": events[:limit], "counts": counts, "n": len(events)}

    def _read_json(self, path) -> Optional[dict]:
        try:
            return json.loads(Path(path).read_text())
        except Exception:
            return None

    def _health(self) -> dict:
        try:
            with open(self.health_path, "rb") as f:
                f.seek(max(f.seek(0, 2) - 4096, 0))
                lines = [l for l in f.read().split(b"\n") if l.strip()]
            d = json.loads(lines[-1])
            age = time.time() - d["ts"]
            return {
                "age_s": round(age, 1),
                "feeds": {k: ("stale" if v else "ok") for k, v in d.get("stale", {}).items()},
                "stale_seconds_total": d.get("stale_seconds_total", {}),
                "recorder": "ok" if age < 30 else "down",
            }
        except Exception:
            return {"age_s": None, "feeds": {}, "stale_seconds_total": {}, "recorder": "unknown"}
