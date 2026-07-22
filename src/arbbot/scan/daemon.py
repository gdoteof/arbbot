"""Standalone scanner daemon: subscribes to the recorder's Unix-socket
rebroadcast and runs the ScanLoop out-of-process.

Blast-radius isolation (2026-07-20 segfault incident): recording is
irreplaceable — you can re-scan history, you can't re-record it. The scanner
crashing must never take the recorder down, so it lives in its own unit and
rebuilds its own book state from the event stream (snapshots re-sync it the
same way they re-sync the recorder after gaps).

Usage: python -m arbbot.scan.daemon --config config/recorder.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from datetime import datetime, timezone
from decimal import Decimal

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot, Venue
from arbbot.ops.config import load_recorder_config
from arbbot.record.jsonl import parse_event
from arbbot.record.kalshi import KalshiCatalog
from arbbot.record.polymarket import ClobRest, GammaCatalog
from arbbot.record.polymarket_us import PolymarketUsCatalog
from arbbot.registry.model import Registry
from arbbot.scan.loop import ScanLoop


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def run(config_path: str) -> None:
    cfg = load_recorder_config(config_path)
    registry = Registry.load(cfg.registry_path)
    kalshi_tickers = sorted(m for v, m in registry.market_ids() if v is Venue.KALSHI)
    pm_tokens = sorted(m for v, m in registry.market_ids() if v is Venue.POLYMARKET)
    clob = ClobRest()
    pmus = PolymarketUsCatalog()
    markets = {}
    for m in await KalshiCatalog().markets(kalshi_tickers):
        markets[(m.venue, m.market_id)] = m
    for m in await GammaCatalog().markets_by_token_ids(pm_tokens):
        markets[(m.venue, m.market_id)] = m
    # PM US metadata (tick/min/feeCoefficient override) — without it, PM US
    # legs fall back to the intl default coefficient and mis-fee the scan
    if cfg.polymarket_us_tags:
        for m in await pmus.markets_by_tags(cfg.polymarket_us_tags):
            markets[(m.venue, m.market_id)] = m

    books = BookBuilder()
    scan = ScanLoop(
        registry, markets, books, cfg.fee_schedule(), cfg.scan_dir,
        min_edge_per_contract=Decimal(cfg.min_edge_per_contract),
    )

    async def refresh_universe(interval_s: float = 1800.0) -> None:
        while True:
            await asyncio.sleep(interval_s)
            try:
                for m in await KalshiCatalog().markets(kalshi_tickers):
                    markets[(m.venue, m.market_id)] = m
                pm = await GammaCatalog().markets_by_token_ids(pm_tokens)
                pm = await ClobRest().taker_fee_overrides(pm)
                for m in pm:
                    markets[(m.venue, m.market_id)] = m
                if cfg.polymarket_us_tags:
                    for m in await pmus.markets_by_tags(cfg.polymarket_us_tags):
                        markets[(m.venue, m.market_id)] = m
            except Exception:
                pass

    asyncio.get_event_loop().create_task(refresh_universe())
    while True:
        try:
            reader, writer = await asyncio.open_unix_connection(cfg.socket_path)
            while True:
                line = await reader.readline()
                if not line:
                    break  # recorder restarted; reconnect
                try:
                    event = parse_event(json.loads(line))
                except ValueError:
                    # truncated/partial line at recorder cut-over (observed
                    # 2026-07-21: JSONDecodeError killed the daemon mid-restart)
                    continue
                if isinstance(event, BookSnapshot):
                    books.apply_snapshot(event)
                elif isinstance(event, BookDelta):
                    with contextlib.suppress(GapDetected, NotSynced):
                        books.apply_delta(event)
                        # gaps self-heal: the recorder's resync snapshot
                        # arrives on this same stream moments later
                with contextlib.suppress(Exception):
                    scan.on_event(event, utc_day())
            with contextlib.suppress(Exception):
                writer.close()
        except (ConnectionError, FileNotFoundError, OSError):
            pass
        await asyncio.sleep(2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/recorder.yaml")
    args = ap.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
