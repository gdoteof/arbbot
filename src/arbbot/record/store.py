"""Legacy import shim — the JSONL layer lives in arbbot.record.jsonl.

The DuckDB EventStore that used to live here is gone (the Parquet archive +
arbbot.record.archive read layer replaced batch-ingest replay). This module
survives only because the frozen recorder imports JsonlWriter from it; new
code should import from arbbot.record.jsonl directly.
"""

from arbbot.record.jsonl import JsonlWriter, iter_jsonl, parse_event  # noqa: F401 (re-export)
