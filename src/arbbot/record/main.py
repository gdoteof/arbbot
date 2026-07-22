"""Recorder daemon entrypoint: python -m arbbot.record.main [--config path]

Credential-free flight recorder. Records the registry universe and
rebroadcasts on the Unix socket; scanning runs out-of-process
(arbbot.scan.daemon) for blast-radius isolation.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from arbbot.models.core import MarketStatus, Venue
from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_credential, load_recorder_config
from arbbot.record.jsonl import JsonlWriter
from arbbot.record.kalshi import KalshiCatalog
from arbbot.record.polymarket import ClobRest, GammaCatalog
from arbbot.record.polymarket_us import PolymarketUsCatalog
from arbbot.record.recorder import (
    LivenessTracker,
    RecorderCore,
    UnixBroadcaster,
    health_task,
    kalshi_poll_task,
    kalshi_ws_task,
    polymarket_us_poll_task,
    polymarket_us_ws_task,
    polymarket_ws_task,
)
from arbbot.registry.model import Registry


async def run(config_path: str) -> None:
    cfg = load_recorder_config(config_path)
    registry = Registry.load(cfg.registry_path)
    kalshi_tickers = sorted(
        m for v, m in registry.market_ids() if v is Venue.KALSHI
    )
    pm_tokens = sorted(m for v, m in registry.market_ids() if v is Venue.POLYMARKET)

    kalshi = KalshiCatalog()
    gamma = GammaCatalog()
    clob = ClobRest()
    pmus = PolymarketUsCatalog()
    markets = {}
    for m in await kalshi.markets(kalshi_tickers):
        markets[(m.venue, m.market_id)] = m
    pm_markets = await gamma.markets_by_token_ids(pm_tokens)
    pm_markets = await clob.taker_fee_overrides(pm_markets)
    for m in pm_markets:
        markets[(m.venue, m.market_id)] = m
    # Polymarket US (regulated DCM): record all markets under the configured
    # tags (credential-free REST poll; the WS needs auth). Universe is tag-
    # driven since PM US has no by-slug metadata endpoint.
    pmus_slugs: list[str] = []
    if cfg.polymarket_us_tags:
        for m in await pmus.markets_by_tags(cfg.polymarket_us_tags):
            markets[(m.venue, m.market_id)] = m
            if m.status is not MarketStatus.CLOSED:
                pmus_slugs.append(m.market_id)

    Path(cfg.data_dir).mkdir(parents=True, exist_ok=True)
    writer = JsonlWriter(cfg.data_dir)
    broadcaster = UnixBroadcaster(cfg.socket_path)
    # Scanning is OUT of this process (arbbot-scanner.service subscribes to
    # the socket) — the recorder is the flight recorder and carries the
    # minimum possible native-code surface.
    core = RecorderCore(writer, broadcaster)
    broadcaster.welcome_events = core.snapshot_events  # sync new subscribers
    await broadcaster.start()

    async def refresh_universe(interval_s: float = 1800.0) -> None:
        # Universe maintenance (design doc Stage 1): refetch market status;
        # closed/resolved markets leave the book state so the 30s rebroadcast
        # stops re-feeding frozen books (adversarial review P1, 2026-07-20).
        while True:
            await asyncio.sleep(interval_s)
            try:
                for m in await kalshi.markets(kalshi_tickers):
                    markets[(m.venue, m.market_id)] = m
                pm = await gamma.markets_by_token_ids(pm_tokens)
                pm = await clob.taker_fee_overrides(pm)
                for m in pm:
                    markets[(m.venue, m.market_id)] = m
                if cfg.polymarket_us_tags:
                    for m in await pmus.markets_by_tags(cfg.polymarket_us_tags):
                        markets[(m.venue, m.market_id)] = m
                for (venue, mid), m in list(markets.items()):
                    if m.status not in (MarketStatus.ACTIVE, MarketStatus.UNKNOWN):
                        core.books._books.pop((venue.value, mid), None)
            except Exception:
                pass  # transient catalog failure: retry next cycle

    liveness = LivenessTracker()
    alerter = Alerter(cfg.ntfy_topic)

    # Kalshi path: real-time WS when the READ-ONLY recorder credentials are
    # present (Geoff's isolation decision 2026-07-20 — the trade-capable key
    # never enters this process); otherwise credential-free REST polling.
    rec_key_id = load_credential("kalshi_recorder_api_key_id")
    rec_key_pem = load_credential("kalshi_recorder_private_key.pem")
    if rec_key_id and rec_key_pem:
        kalshi_task = kalshi_ws_task(
            core, liveness, kalshi_tickers,
            rec_key_id.decode().strip(), rec_key_pem,
            catalog=kalshi,
        )
        print("kalshi: real-time WS (read-only key)", flush=True)
    else:
        kalshi_task = kalshi_poll_task(
            core, liveness, kalshi_tickers, kalshi, cfg.kalshi_poll_interval_s
        )
        print("kalshi: REST polling (no recorder credentials)", flush=True)

    tasks = [
        asyncio.create_task(
            polymarket_ws_task(core, liveness, pm_tokens, clob)
        ),
        asyncio.create_task(kalshi_task),
        asyncio.create_task(
            health_task(liveness, cfg.health_path, alert=alerter.alert,
                        broadcaster=broadcaster)
        ),
        asyncio.create_task(refresh_universe()),
    ]
    if pmus_slugs:
        # Authenticated WS (real-time full-book + trades) when the PM US key is
        # available; else credential-free REST polling. The WS key is trade-
        # capable but this feed only reads market data (Geoff's call — PM US has
        # no read-only key option).
        us_id = load_credential("polymarket_usa_key_id")
        us_key = load_credential("polymarket_usa_private_key")
        if us_id and us_key:
            tasks.append(asyncio.create_task(
                polymarket_us_ws_task(core, liveness, pmus_slugs,
                                      us_id.decode().strip(), us_key.decode().strip())
            ))
            print(f"polymarket_us: real-time WS {len(pmus_slugs)} markets "
                  f"(tags={cfg.polymarket_us_tags})", flush=True)
        else:
            tasks.append(asyncio.create_task(
                polymarket_us_poll_task(core, liveness, pmus_slugs, pmus)
            ))
            print(f"polymarket_us: REST polling {len(pmus_slugs)} markets "
                  f"(tags={cfg.polymarket_us_tags})", flush=True)
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await broadcaster.stop()
        writer.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/recorder.yaml")
    args = ap.parse_args()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
