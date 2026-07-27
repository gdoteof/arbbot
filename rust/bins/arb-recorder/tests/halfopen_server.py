"""Half-open WebSocket server: complete the handshake, send a few real frames,
then go SILENT while holding the TCP connection open.

This is the failure the recorder could not survive: no close frame, no error,
no data. A client that only reacts to errors waits forever. The stall guard
must notice the silence and reconnect.

Records every connection so the test can assert reconnects actually happened.
"""
import asyncio
import base64
import hashlib
import json
import sys
import time

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
connections = []
SILENT_AFTER = 2  # frames sent before going quiet


def accept_key(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


def frame(payload: bytes) -> bytes:
    """Minimal unmasked server text frame."""
    n = len(payload)
    if n < 126:
        hdr = bytes([0x81, n])
    elif n < 65536:
        hdr = bytes([0x81, 126, n >> 8 & 0xFF, n & 0xFF])
    else:
        hdr = bytes([0x81, 127]) + n.to_bytes(8, "big")
    return hdr + payload


async def handle(reader, writer):
    idx = len(connections) + 1
    connections.append(time.time())
    peer = writer.get_extra_info("peername")
    print(f"[server] connection #{idx} at t+{time.time()-T0:.1f}s", flush=True)

    # --- handshake ---
    req = b""
    while b"\r\n\r\n" not in req:
        chunk = await reader.read(1024)
        if not chunk:
            return
        req += chunk
    key = ""
    for line in req.decode(errors="replace").split("\r\n"):
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
    writer.write(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept_key(key).encode() + b"\r\n\r\n"
    )
    await writer.drain()

    # --- a couple of REAL frames so the client is genuinely subscribed ---
    for i in range(SILENT_AFTER):
        ev = {
            "event_type": "book",
            "asset_id": "TESTTOKEN",
            "market": "TESTTOKEN",
            "bids": [{"price": "0.10", "size": "5"}],
            "asks": [{"price": "0.12", "size": "5"}],
            "timestamp": str(int(time.time() * 1000)),
            "hash": f"h{i}",
        }
        writer.write(frame(json.dumps([ev]).encode()))
        await writer.drain()
        await asyncio.sleep(0.2)

    print(f"[server] conn #{idx} going SILENT (holding TCP open)", flush=True)
    # Never send again, never close. Drain whatever the client sends (PINGs)
    # so its writes keep succeeding — that is what makes this half-open.
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                print(f"[server] conn #{idx} closed BY CLIENT at "
                      f"t+{time.time()-T0:.1f}s", flush=True)
                return
    except Exception:
        return


async def main(port):
    global T0
    T0 = time.time()
    srv = await asyncio.start_server(handle, "127.0.0.1", port)
    print(f"[server] listening on 127.0.0.1:{port}", flush=True)
    async with srv:
        await asyncio.sleep(40)
    print(f"\n[result] total connections: {len(connections)}", flush=True)
    if len(connections) >= 2:
        gaps = [connections[i + 1] - connections[i] for i in range(len(connections) - 1)]
        print(f"[result] reconnect gaps: {[f'{g:.1f}s' for g in gaps]}", flush=True)
        print("[result] PASS — the client detected the silent socket and reconnected")
    else:
        print("[result] FAIL — client never reconnected; it hung on a half-open socket")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1])))
