"""Verify the Polymarket US API credentials with READ-ONLY authenticated
calls (no orders placed). Loads the Ed25519 key to SIGN requests; never prints
the key. Confirms the auth scheme (X-PM-Access-Key/Timestamp/Signature over
{ts}{method}{path}) works against the real account."""

import base64
import pathlib
import time

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

D = pathlib.Path.home() / ".arbbot-credentials"
API = "https://api.polymarket.us"
KEY_ID = (D / "polymarket_usa_key_id").read_text().strip()
_seed = base64.b64decode((D / "polymarket_usa_private_key").read_text().strip())[:32]
_priv = ed25519.Ed25519PrivateKey.from_private_bytes(_seed)


def headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    sig = base64.b64encode(_priv.sign(f"{ts}{method}{path}".encode())).decode()
    return {"X-PM-Access-Key": KEY_ID, "X-PM-Timestamp": ts,
            "X-PM-Signature": sig, "Content-Type": "application/json"}


c = httpx.Client(timeout=20)
for path in ("/v1/account/balances", "/v1/portfolio/positions"):
    r = c.get(API + path, headers=headers("GET", path))
    print(f"GET {path} -> {r.status_code}")
    print("  ", r.text[:400])
