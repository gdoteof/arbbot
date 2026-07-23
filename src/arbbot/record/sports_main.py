"""Sports recorder daemon: python -m arbbot.record.sports_main

Records the sports cross-venue universe (Kalshi game tickers + Polymarket US
moneylines from data/scan/sports_equiv_map.json) to data/raw-sports/.
Separate process and data dir from the main recorder — zero blast radius on
the registry feed. No socket broadcaster; this is a pure flight recorder for
research (lead-lag/ML). Exits cleanly when the sports map file changes so
systemd (Restart=always) relaunches it with the fresh universe.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_credential, load_recorder_config
from arbbot.record.jsonl import JsonlWriter
from arbbot.record.kalshi import KalshiCatalog
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
)

SPORTS_MAP = Path("data/scan/sports_equiv_map.json")
DATA_DIR = Path("data/raw-sports")
SOCKET_PATH = Path("data/arbbot-sports.sock")


def universe(map_path: Path = SPORTS_MAP) -> tuple[list[str], list[str]]:
    """(kalshi tickers, pm_us slugs) from the sports equivalence map."""
    smap = json.loads(map_path.read_text())
    kalshi: set[str] = set()
    pmus: set[str] = set()
    for m in smap.get("matches", []):
        for km in m.get("kalshi_markets", []):
            if km.get("ticker"):
                kalshi.add(km["ticker"])
        if m.get("kalshi_long_ticker"):
            kalshi.add(m["kalshi_long_ticker"])
        if m.get("pm_moneyline"):
            pmus.add(m["pm_moneyline"])
    return sorted(kalshi), sorted(pmus)


async def exit_on_map_change(map_path: Path, interval_s: float = 60.0) -> None:
    """End the process when the map regenerates; systemd restarts us with
    the new universe."""
    start_mtime = map_path.stat().st_mtime
    while True:
        await asyncio.sleep(interval_s)
        try:
            if map_path.stat().st_mtime != start_mtime:
                print("sports map changed; exiting for restart", flush=True)
                return
        except FileNotFoundError:
            pass


async def run() -> None:
    cfg = load_recorder_config("config/recorder.yaml")  # ntfy topic wiring only
    kalshi_tickers, pmus_slugs = universe()
    print(f"sports universe: {len(kalshi_tickers)} kalshi tickers, "
          f"{len(pmus_slugs)} pm_us slugs", flush=True)
    if not kalshi_tickers and not pmus_slugs:
        print("empty universe; sleeping until map changes", flush=True)
        await exit_on_map_change(SPORTS_MAP)
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    broadcaster = UnixBroadcaster(SOCKET_PATH)
    core = RecorderCore(JsonlWriter(DATA_DIR), broadcaster)
    broadcaster.welcome_events = core.snapshot_events
    await broadcaster.start()
    liveness = LivenessTracker()
    alerter = Alerter(cfg.ntfy_topic)
    kalshi_cat = KalshiCatalog()

    tasks: list[asyncio.Task] = []
    rec_key_id = load_credential("kalshi_recorder_api_key_id")
    rec_key_pem = load_credential("kalshi_recorder_private_key.pem")
    if kalshi_tickers:
        if rec_key_id and rec_key_pem:
            tasks.append(asyncio.create_task(kalshi_ws_task(
                core, liveness, kalshi_tickers,
                rec_key_id.decode().strip(), rec_key_pem, catalog=kalshi_cat)))
            print("kalshi: real-time WS (read-only key)", flush=True)
        else:
            tasks.append(asyncio.create_task(kalshi_poll_task(
                core, liveness, kalshi_tickers, kalshi_cat, 30.0)))
            print("kalshi: REST polling", flush=True)

    if pmus_slugs:
        us_id = load_credential("polymarket_usa_key_id")
        us_key = load_credential("polymarket_usa_private_key")
        if us_id and us_key:
            tasks.append(asyncio.create_task(polymarket_us_ws_task(
                core, liveness, pmus_slugs,
                us_id.decode().strip(), us_key.decode().strip())))
            print(f"polymarket_us: real-time WS {len(pmus_slugs)} markets", flush=True)
        else:
            tasks.append(asyncio.create_task(polymarket_us_poll_task(
                core, liveness, pmus_slugs, PolymarketUsCatalog())))
            print(f"polymarket_us: REST polling {len(pmus_slugs)} markets", flush=True)

    tasks.append(asyncio.create_task(health_task(
        liveness, DATA_DIR / "health.jsonl", alert=alerter.alert,
        broadcaster=broadcaster)))

    watcher = asyncio.create_task(exit_on_map_change(SPORTS_MAP))
    await watcher
    for t in tasks:
        t.cancel()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
