"""Parquet archive layer: closed-day JSONL is converted to columnar Parquet
(scripts/archive_to_parquet.py) and the JSONL deleted. This module is the READ
side — it makes Parquet-or-JSONL transparent to every closed-day consumer so
deleting the JSONL never breaks a sim, report, or the dashboard.

The recorder still writes today's data as append-only JSONL (crash-safe, zero
native surface — unchanged). Only closed, static days are archived.

DuckDB stores nested bids/asks/basket as JSON strings; the reader rebuilds them
to the exact dict/list shape parse_event expects, so callers see identical rows
whichever source they came from.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Iterator

PARQUET_DIR = "data/parquet"


def _parquet_dir(directory: str | Path) -> Path:
    """Archive dir is a sibling of raw/ and scan/ under the data root, so it
    tracks whatever directory the caller passed (keeps tests/custom roots from
    leaking into the global data/parquet)."""
    return Path(directory).parent / "parquet"


def _reconstruct(row: dict[str, Any]) -> dict[str, Any]:
    """Rebuild JSON-encoded columns (bids/asks levels, opportunity baskets) to
    the dict/list shapes the JSONL callers expect. DuckDB hands JSON columns
    back as JSON strings or lists of JSON strings; drop SQL NULLs entirely so a
    row matches what json.loads of the original line would have produced."""
    out = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, _dt.datetime):
            # DuckDB auto-types ISO-8601 string fields to TIMESTAMP; restore the
            # original UTC-suffixed string form the JSONL consumers expect
            v = v.isoformat() + ("Z" if v.tzinfo is None else "")
        elif isinstance(v, _dt.date):
            v = v.isoformat()
        elif isinstance(v, str) and v[:1] in "{[":
            try:
                v = json.loads(v)
            except ValueError:
                pass
        elif isinstance(v, list) and v and isinstance(v[0], str) and v[0][:1] in "{[":
            try:
                v = [json.loads(x) for x in v]
            except ValueError:
                pass
        out[k] = v
    return out


def source_for(directory: str | Path, stem: str, day: str) -> tuple[str, Path] | None:
    """('parquet', path) | ('jsonl', path) | None for <stem>-<day> in `directory`
    (parquet archive preferred; falls back to the live JSONL)."""
    pq = _parquet_dir(directory) / f"{stem}-{day}.parquet"
    if pq.exists():
        return ("parquet", pq)
    jl = Path(directory) / f"{stem}-{day}.jsonl"
    if jl.exists():
        return ("jsonl", jl)
    return None


def iter_day(directory: str | Path, stem: str, day: str) -> Iterator[dict[str, Any]]:
    """Yield raw event/record dicts for one <stem>-<day>, from Parquet if
    archived else JSONL, streaming (no full-file load)."""
    src = source_for(directory, stem, day)
    if src is None:
        return
    kind, path = src
    if kind == "jsonl":
        from arbbot.record.jsonl import iter_jsonl
        for _, raw in iter_jsonl(path):
            yield raw
        return
    import duckdb
    con = duckdb.connect()
    try:
        cur = con.execute(f"SELECT * FROM read_parquet('{path}')")
        cols = [d[0] for d in cur.description]
        while True:
            batch = cur.fetchmany(10000)
            if not batch:
                break
            for tup in batch:
                yield _reconstruct(dict(zip(cols, tup)))
    finally:
        con.close()


def iter_day_merged(day: str, venues: list[str], raw_dir: str | Path,
                    ) -> Iterator[dict[str, Any]]:
    """Merged, time-ordered raw events across venues for one day, streamed and
    sorted BY DUCKDB (out-of-core) — replaces load-all-into-a-list-and-sort.
    Sources are Parquet-or-JSONL per venue; today's live JSONL is included."""
    import duckdb
    con = duckdb.connect()
    selects = []
    for v in venues:
        src = source_for(raw_dir, v, day)
        if src is None:
            continue
        kind, path = src
        # sample_size=-1: full-scan schema inference — a sampled read of a
        # heterogeneous tape silently drops columns absent from the sample
        reader = (f"read_parquet('{path}')" if kind == "parquet"
                  else f"read_json_auto('{path}', format='newline_delimited', "
                       f"union_by_name=true, sample_size=-1, maximum_object_size=20000000)")
        selects.append(f"SELECT * FROM {reader}")
    if not selects:
        return
    sql = (" UNION ALL BY NAME ".join(selects) +
           " ORDER BY ts_local_ns, venue, seq")
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        while True:
            batch = cur.fetchmany(10000)
            if not batch:
                break
            for tup in batch:
                yield _reconstruct(dict(zip(cols, tup)))
    finally:
        con.close()


def load_archived_counts(parquet_dir: str | Path = PARQUET_DIR) -> dict[str, int]:
    """Cumulative event counts for days already archived+deleted, so the
    dashboard's per-venue totals survive JSONL deletion."""
    p = Path(parquet_dir) / "archived_counts.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except ValueError:
            return {}
    return {}
