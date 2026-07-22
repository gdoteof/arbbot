"""Probe the Polymarket US market-data WebSocket to learn the real protocol:
which URL connects, the subscribe message it accepts, and the shape of
orderbook snapshot/delta and trade messages. Prints the first ~12 frames."""

import asyncio
import json

import websockets

SLUG = "rdc-usfed-fomc-2026-07-29-nochng"
URLS = ["wss://api.polymarket.us/v1/ws/markets", "wss://api.polymarket.us/ws"]
SUBS = [
    {"type": "subscribe", "channel": "orderbook", "symbol": SLUG},
    {"type": "subscribe", "channel": "trades", "symbol": SLUG},
]


async def try_url(url):
    print(f"\n=== connecting {url} ===")
    try:
        async with websockets.connect(url, ping_interval=None, open_timeout=10) as ws:
            print("  connected")
            for s in SUBS:
                await ws.send(json.dumps(s))
                print("  sent:", s)
            for i in range(12):
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=8.0)
                except asyncio.TimeoutError:
                    print("  (timeout waiting for frame)")
                    break
                s = frame if isinstance(frame, str) else frame.decode()
                print(f"  frame[{i}]: {s[:400]}")
        return True
    except Exception as e:
        print("  ERR:", repr(e)[:200])
        return False


async def main():
    for url in URLS:
        if await try_url(url):
            break


asyncio.run(main())
