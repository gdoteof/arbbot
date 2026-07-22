"""Probe the Polymarket US PRIVATE WebSocket: connect (signed handshake),
subscribe to ORDER updates, print frames. On connect it should snapshot our
current resting orders; a fill would arrive as an execution update. Validates
the event-driven fill path before wiring it into the runner."""

import asyncio
import json
import pathlib

import websockets

from arbbot.record.polymarket_us import ws_auth_headers

D = pathlib.Path.home() / ".arbbot-credentials"
KID = (D / "polymarket_usa_key_id").read_text().strip()
KEY = (D / "polymarket_usa_private_key").read_text().strip()
PATH = "/v1/ws/private"
URL = "wss://api.polymarket.us" + PATH


async def main():
    async with websockets.connect(URL, additional_headers=ws_auth_headers(KID, KEY, PATH),
                                  ping_interval=None, open_timeout=10) as ws:
        print("connected")
        await ws.send(json.dumps({"subscribe": {"requestId": "orders",
                                                "subscriptionType": "SUBSCRIPTION_TYPE_ORDER"}}))
        for i in range(8):
            try:
                f = await asyncio.wait_for(ws.recv(), timeout=8.0)
            except asyncio.TimeoutError:
                print("(timeout — no more frames)")
                break
            s = f if isinstance(f, str) else f.decode()
            print(f"frame[{i}]: {s[:500]}")


asyncio.run(main())
