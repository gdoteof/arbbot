"""Find the PM US market-data WS subscribe format. Auth (signed handshake
headers) is known-good; we brute-force the subscribe envelope, one per fresh
connection, and print the server's reply to each. 'invalid_message' = wrong
envelope; anything else (ack / book data / 'unknown channel') = closer."""

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
PATH = "/v1/ws/markets"
URL = "wss://api.polymarket.us" + PATH
S = "rdc-usfed-fomc-2026-07-29-nochng"

CANDIDATES = [
    {"type": "subscribe", "channels": [{"name": "orderbook", "marketSlugs": [S]}]},
    {"type": "subscribe", "subscriptions": [{"channel": "orderbook", "marketSlug": S}]},
    {"type": "subscribe", "channel": "orderbook", "marketSlugs": [S]},
    {"op": "subscribe", "channel": "orderbook", "marketSlug": S},
    {"method": "subscribe", "channel": "orderbook", "marketSlug": S},
    {"event": "subscribe", "channel": "orderbook", "marketSlug": S},
    {"type": "SUBSCRIBE", "channel": "ORDERBOOK", "marketSlug": S},
    {"type": "subscribe", "topic": "orderbook", "marketSlug": S},
    {"type": "subscribe", "channel": "book", "marketSlug": S},
    {"type": "subscribe", "feed": "orderbook", "product_ids": [S]},
    {"type": "subscribe", "channel": "orderbook", "markets": [S]},
    {"messageType": "subscribe", "channel": "orderbook", "marketSlug": S},
]


def hdrs():
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(PRIV.sign(f"{ts}GET{PATH}".encode())).decode()
    return {"X-PM-Access-Key": KID, "X-PM-Timestamp": ts, "X-PM-Signature": sig}


async def try_sub(msg):
    try:
        async with websockets.connect(URL, additional_headers=hdrs(),
                                      ping_interval=None, open_timeout=10) as ws:
            await ws.send(json.dumps(msg))
            replies = []
            for _ in range(2):
                try:
                    f = await asyncio.wait_for(ws.recv(), timeout=3.0)
                    replies.append((f if isinstance(f, str) else f.decode())[:160])
                except asyncio.TimeoutError:
                    break
            tag = json.dumps(msg)[:70]
            print(f"{tag}\n    -> {replies}")
    except Exception as e:
        print(f"{json.dumps(msg)[:70]}\n    ERR {repr(e)[:100]}")


async def main():
    for m in CANDIDATES:
        await try_sub(m)


asyncio.run(main())
