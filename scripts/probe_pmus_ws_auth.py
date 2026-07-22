"""Determine the Polymarket US market-data WebSocket auth protocol (undocumented;
plain connect 401s). Try (A) signed X-PM-* headers on the handshake, across a
few candidate ws paths, then send a subscribe frame and print what comes back."""

import asyncio
import base64
import json
import time
import pathlib

import websockets
from cryptography.hazmat.primitives.asymmetric import ed25519

D = pathlib.Path.home() / ".arbbot-credentials"
KID = (D / "polymarket_usa_key_id").read_text().strip()
PRIV = ed25519.Ed25519PrivateKey.from_private_bytes(
    base64.b64decode((D / "polymarket_usa_private_key").read_text().strip())[:32])
SLUG = "rdc-usfed-fomc-2026-07-29-nochng"

PATHS = ["/v1/ws/markets", "/ws/markets", "/v1/ws", "/ws"]
SUBS = [
    {"type": "subscribe", "channel": "orderbook", "symbol": SLUG},
    {"type": "subscribe", "channel": "orderbook", "marketSlug": SLUG},
    {"action": "subscribe", "channel": "orderbook", "market": SLUG},
]


def hdrs(path):
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(PRIV.sign(f"{ts}GET{path}".encode())).decode()
    return {"X-PM-Access-Key": KID, "X-PM-Timestamp": ts, "X-PM-Signature": sig}


async def try_path(path):
    url = "wss://api.polymarket.us" + path
    try:
        async with websockets.connect(url, additional_headers=hdrs(path),
                                      ping_interval=None, open_timeout=10) as ws:
            print(f"CONNECTED {url}")
            for s in SUBS:
                await ws.send(json.dumps(s))
            for i in range(6):
                try:
                    f = await asyncio.wait_for(ws.recv(), timeout=6.0)
                except asyncio.TimeoutError:
                    print("  (timeout)"); break
                print(f"  frame[{i}]:", (f if isinstance(f, str) else f.decode())[:350])
            return True
    except Exception as e:
        print(f"  {url} -> {repr(e)[:120]}")
        return False


async def main():
    for p in PATHS:
        if await try_path(p):
            break


asyncio.run(main())
