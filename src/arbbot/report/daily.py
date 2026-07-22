"""Daily report: the Stage 1 deliverable.

Answers, with real numbers: how many post-fee executable opportunities
existed, at what size, how long did they live — by relationship class,
tranche (head vs long-tail), and bucket — plus annualized return on locked
capital (the economic bar: beat the T-bill, not zero).
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from arbbot.record.archive import iter_day

SECONDS_PER_YEAR = Decimal(365 * 24 * 3600)


def _percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {"p50": 0.0, "p90": 0.0, "max": 0.0}
    values = sorted(values)
    return {
        "p50": statistics.quantiles(values, n=100)[49] if len(values) > 1 else values[0],
        "p90": statistics.quantiles(values, n=100)[89] if len(values) > 1 else values[0],
        "max": values[-1],
    }


def annualized_return_on_locked_capital(
    net_edge_total: Decimal, gross_cost: Decimal, ts_ns: int, close_time: str | None
) -> Decimal | None:
    """Edge / locked capital, annualized by time-to-close (capital-lock proxy).
    None when close time is unknown or already passed."""
    if not close_time or gross_cost <= 0:
        return None
    try:
        close = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
    except ValueError:
        return None
    opened = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
    lock_s = Decimal(str((close - opened).total_seconds()))
    if lock_s <= 0:
        return None
    return (net_edge_total / gross_cost) * (SECONDS_PER_YEAR / lock_s)


def iter_scan_records(scan_dir: str | Path, day: str) -> Iterator[dict[str, Any]]:
    # opportunities may be archived to Parquet; lifetimes stays JSONL. iter_day
    # transparently reads whichever exists.
    for stem in ("opportunities", "lifetimes"):
        yield from iter_day(scan_dir, stem, day)


def build_report(scan_dir: str | Path, day: str) -> dict[str, Any]:
    opportunities: list[dict[str, Any]] = []
    lifetimes: list[dict[str, Any]] = []
    for raw in iter_scan_records(scan_dir, day):
        (lifetimes if "lifetime_ns" in raw else opportunities).append(raw)

    by_group: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for o in opportunities:
        key = (o["relationship_id"], o.get("tranche", "?"), o.get("bucket", "?"))
        by_group.setdefault(key, []).append(o)

    groups = []
    for (rel_id, tranche, bucket), items in sorted(by_group.items()):
        rocs = []
        for o in items:
            roc = annualized_return_on_locked_capital(
                Decimal(str(o["net_edge_total"])),
                Decimal(str(o["gross_cost"])),
                int(o["ts_local_ns"]),
                o.get("max_hold_close_time"),
            )
            if roc is not None:
                rocs.append(float(roc))
        groups.append(
            {
                "relationship_id": rel_id,
                "tranche": tranche,
                "bucket": bucket,
                "observations": len(items),
                "size": _percentiles([float(o["size"]) for o in items]),
                "net_edge_per_contract": _percentiles(
                    [float(o["net_edge_per_contract"]) for o in items]
                ),
                "annualized_return_on_locked_capital": _percentiles(rocs),
            }
        )

    lifetime_s = [float(l["lifetime_ns"]) / 1e9 for l in lifetimes]
    return {
        "day": day,
        "opportunity_observations": len(opportunities),
        "distinct_signatures": len({o["signature"] for o in opportunities}),
        "primary_observations": sum(1 for o in opportunities if o.get("bucket") == "primary"),
        "sub_minimum_observations": sum(
            1 for o in opportunities if o.get("bucket") == "sub_minimum"
        ),
        "lifetime_seconds": _percentiles(lifetime_s),
        "closed_opportunities": len(lifetimes),
        "groups": groups,
    }


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"arbbot daily report — {report['day']}",
        f"observations: {report['opportunity_observations']} "
        f"(primary {report['primary_observations']}, "
        f"sub-minimum {report['sub_minimum_observations']}), "
        f"distinct signatures: {report['distinct_signatures']}",
        f"closed: {report['closed_opportunities']}, "
        f"lifetime s p50/p90/max: {report['lifetime_seconds']['p50']:.1f}/"
        f"{report['lifetime_seconds']['p90']:.1f}/{report['lifetime_seconds']['max']:.1f}",
        "",
    ]
    if not report["groups"]:
        lines.append("No opportunities recorded. (A quiet day is data, not an error.)")
    for g in report["groups"]:
        roc = g["annualized_return_on_locked_capital"]
        lines.append(
            f"  {g['relationship_id']:<28} {g['tranche']:<9} {g['bucket']:<11} "
            f"n={g['observations']:<5} size p50={g['size']['p50']:.0f} "
            f"edge/ct p50=${g['net_edge_per_contract']['p50']:.4f} "
            f"ann.RoLC p50={roc['p50']*100:.1f}%"
        )
    return "\n".join(lines)


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--scan-dir", default="data/scan")
    ap.add_argument("--day", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    report = build_report(args.scan_dir, args.day)
    print(json.dumps(report, indent=2) if args.json else render_text(report))


if __name__ == "__main__":
    main()
