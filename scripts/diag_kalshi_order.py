"""Diagnostic: exercise the Kalshi V2 order path end-to-end with a tiny
far-from-market order (1ct YES @ 1c, cannot fill). Prints raw responses so we
can confirm the create body is accepted and learn the list/cancel shapes."""

import pathlib
import uuid

import httpx

from arbbot.record.kalshi import REST_BASE, sign_headers, load_private_key

D = pathlib.Path.home() / ".arbbot-credentials"
KID = (D / "kalshi_api_key_id").read_text().strip()
KEY = load_private_key((D / "kalshi_private_key.pem").read_bytes())
TICKER = "KXPRESNOMR-28-RDS"


def req(method: str, path: str, body=None):
    full = "/trade-api/v2" + path
    hdr = sign_headers(KID, KEY, method, full)
    r = httpx.request(method, REST_BASE + path, json=body, headers=hdr, timeout=15)
    print(f"{method} {path} -> {r.status_code}")
    print("  body:", r.text[:700])
    return r


body = {"ticker": TICKER, "side": "bid", "count": "1.00", "price": "0.0100",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": uuid.uuid4().hex, "post_only": True}

r = req("POST", "/portfolio/events/orders", body)
print()
if r.status_code // 100 == 2:
    j = r.json()
    oid = (j.get("order") or {}).get("order_id") or j.get("order_id")
    print("order_id:", oid)
    print()
    req("GET", "/portfolio/orders")
    print()
    if oid:
        req("DELETE", f"/portfolio/orders/{oid}")
