"""Parquet archive layer: closed-day JSONL is converted to columnar Parquet
(scripts/archive_to_parquet.py) and the JSONL deleted. This module is the READ
side — it makes Parquet-or-JSONL transparent to every closed-day consumer so
deleting the JSONL never breaks a sim, report, or the dashboard.

The recorder still writes today's data as append-only JSONL (crash-safe, zero
native surface — unchanged). Only closed, static days are archived.

Raw tapes are archived with a FIXED schema (RAW_JSON_COLUMNS — no type
inference): every scalar stays VARCHAR/BIGINT and bids/asks are JSON-text
columns, so ts_venue survives verbatim (nanosecond precision — auto-inference
used to type it TIMESTAMP and truncate to microseconds). The reader decodes
the bids/asks JSON strings back to the dict/list shape parse_event expects, so
callers see identical rows whichever source they came from. Pre-fix parquet
(STRUCT[] levels, auto-typed columns) remains readable via legacy branches.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any, Iterator

PARQUET_DIR = "data/parquet"

# Fixed read_json schema for raw tapes (union of BookSnapshot/BookDelta/Trade
# fields from models/core.py — the frozen recorder writes exactly these).
# Everything textual stays VARCHAR: prices/sizes are Decimal-as-string in the
# JSONL, bids/asks become the JSON text of the level array, and ts_venue is
# kept verbatim (NEVER TIMESTAMP — that truncates nanoseconds).
RAW_JSON_COLUMNS = {
    "kind": "VARCHAR", "venue": "VARCHAR", "market_id": "VARCHAR",
    "side": "VARCHAR", "price": "VARCHAR", "size": "VARCHAR",
    "bids": "VARCHAR", "asks": "VARCHAR", "taker_side": "VARCHAR",
    "seq": "BIGINT", "ts_local_ns": "BIGINT", "ts_venue": "VARCHAR",
}


def raw_json_reader(path: str | Path) -> str:
    """DuckDB read_json() source clause for a raw-tape JSONL with the fixed
    schema above (no read_json_auto inference)."""
    cols = ", ".join(f"{k}: '{t}'" for k, t in RAW_JSON_COLUMNS.items())
    return (f"read_json('{path}', format='newline_delimited', "
            f"columns={{{cols}}}, maximum_object_size=20000000)")


def _parquet_dir(directory: str | Path) -> Path:
    """Archive dir is a sibling of raw/ and scan/ under the data root, so it
    tracks whatever directory the caller passed (keeps tests/custom roots from
    leaking into the global data/parquet)."""
    return Path(directory).parent / "parquet"


def _reconstruct(row: dict[str, Any]) -> dict[str, Any]:
    """Rebuild JSON-encoded columns (bids/asks levels, opportunity baskets) to
    the dict/list shapes the JSONL callers expect; drop SQL NULLs entirely so a
    row matches what json.loads of the original line would have produced.

    Fixed-schema raw archives only exercise the JSON-string decode (everything
    else, ts_venue included, passes through verbatim). The datetime and
    list-of-JSON-strings branches are LEGACY: pre-fix auto-inferred parquet
    (opportunities' TIMESTAMP max_hold_close_time, JSON[] level columns)."""
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


def source_for(directory: str | Path, stem: str, day: str,
               parquet_dir: str | Path | None = None) -> tuple[str, Path] | None:
    """('parquet', path) | ('jsonl', path) | None for <stem>-<day> in `directory`
    (parquet archive preferred; falls back to the live JSONL). `parquet_dir`
    overrides the default sibling archive dir — needed for roots like
    data/raw-sports whose stems would collide with the prod data/parquet."""
    pq = Path(parquet_dir) if parquet_dir else _parquet_dir(directory)
    pq = pq / f"{stem}-{day}.parquet"
    if pq.exists():
        return ("parquet", pq)
    jl = Path(directory) / f"{stem}-{day}.jsonl"
    if jl.exists():
        return ("jsonl", jl)
    return None


def _iter_sql_rows(sql: str) -> Iterator[dict[str, Any]]:
    """Stream _reconstruct-ed row dicts from a DuckDB query."""
    import duckdb
    con = duckdb.connect()
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


def iter_parquet_rows(path: str | Path) -> Iterator[dict[str, Any]]:
    """Stream one archived parquet file back as event/record dicts — the exact
    rows iter_day would yield for it (archiver verify reads through this)."""
    yield from _iter_sql_rows(f"SELECT * FROM read_parquet('{path}')")


def _parquet_select(path: Path) -> str:
    """SELECT clause for one raw-tape parquet, normalized for UNION with
    fixed-schema sources: legacy auto-inferred files carry bids/asks as
    STRUCT[]/JSON[] — re-encode those to JSON text (byte-identical decode) so
    they union cleanly with VARCHAR columns; fixed-schema files pass through."""
    import duckdb
    con = duckdb.connect()
    try:
        types = dict(
            (d[0], d[1]) for d in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall())
    finally:
        con.close()
    fixes = [f"to_json({c}) AS {c}" for c in ("bids", "asks")
             if types.get(c, "VARCHAR") != "VARCHAR"]
    replace = f" REPLACE ({', '.join(fixes)})" if fixes else ""
    return f"SELECT *{replace} FROM read_parquet('{path}')"


def iter_day(directory: str | Path, stem: str, day: str,
             parquet_dir: str | Path | None = None) -> Iterator[dict[str, Any]]:
    """Yield raw event/record dicts for one <stem>-<day>, from Parquet if
    archived else JSONL, streaming (no full-file load)."""
    src = source_for(directory, stem, day, parquet_dir)
    if src is None:
        return
    kind, path = src
    if kind == "jsonl":
        from arbbot.record.jsonl import iter_jsonl
        for _, raw in iter_jsonl(path):
            yield raw
        return
    yield from iter_parquet_rows(path)


def iter_day_merged(day: str, venues: list[str], raw_dir: str | Path,
                    parquet_dir: str | Path | None = None,
                    ) -> Iterator[dict[str, Any]]:
    """Merged, time-ordered raw events across venues for one day, streamed and
    sorted BY DUCKDB (out-of-core) — replaces load-all-into-a-list-and-sort.
    Sources are Parquet-or-JSONL per venue; today's live JSONL is included and
    read with the same fixed schema as the archive (ts_venue verbatim)."""
    selects = []
    for v in venues:
        src = source_for(raw_dir, v, day, parquet_dir)
        if src is None:
            continue
        kind, path = src
        selects.append(_parquet_select(path) if kind == "parquet"
                       else f"SELECT * FROM {raw_json_reader(path)}")
    if not selects:
        return
    yield from _iter_sql_rows(" UNION ALL BY NAME ".join(selects) +
                              " ORDER BY ts_local_ns, venue, seq")


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
