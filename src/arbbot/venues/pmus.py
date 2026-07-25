"""Shared Polymarket US (QCX) API client: Ed25519 signing + HTTP + rate budget.

ONE home for the auth scheme (quirk pmus-ed25519-signing): sign
"{ts_ms}{METHOD}{path}" with the Ed25519 key whose SEED is the first 32 bytes
of the base64-decoded key file; headers X-PM-Access-Key / -Timestamp /
-Signature (base64). Timestamp must be within ~30s of server time. The same
scheme signs WS upgrade handshakes (see record.polymarket_us.ws_auth_headers).

Cross-process rate budget: ~9 scripts share one venue API budget with no
shared limiter (429 incident 2026-07-23). Every PmusSession request takes a
token from a per-minute bucket persisted in data/exec/trading.db (table
rate_budget, via ledgerdb.connect) and BLOCKS until the window rolls once the
bucket is spent. priority="critical" (order place/cancel/hedge paths) skips
the budget entirely — those must never wait — and never opens the DB.

Public gateway reads (gateway.polymarket.us, credential-free) are module
functions and are NOT metered — the budget guards api.polymarket.us.
"""

from __future__ import annotations

import base64
import time
from decimal import Decimal

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.exec import ledgerdb

API_BASE = "https://api.polymarket.us"
GATEWAY_BASE = "https://gateway.polymarket.us/v1"

BUDGET_PER_MIN = 30            # background requests/min against api.polymarket.us
WINDOW_NS = 60 * 1_000_000_000


def load_ed25519(secret_key_b64: str) -> ed25519.Ed25519PrivateKey:
    """Key file is base64; the SEED is the first 32 bytes of the decoded blob
    (signing with the full blob yields opaque 401s)."""
    return ed25519.Ed25519PrivateKey.from_private_bytes(
        base64.b64decode(secret_key_b64)[:32])


def sign_headers(key_id: str, priv: ed25519.Ed25519PrivateKey, method: str,
                 path: str, ts: str | None = None) -> dict:
    """Auth headers: Ed25519 over '{ts}{METHOD}{path}' (bare path, ms ts)."""
    ts = ts or str(int(time.time() * 1000))
    sig = base64.b64encode(priv.sign(f"{ts}{method}{path}".encode())).decode()
    return {"X-PM-Access-Key": key_id, "X-PM-Timestamp": ts, "X-PM-Signature": sig}


def consume_budget(db_path=ledgerdb.DB_PATH, venue: str = "polymarket_us",
                   budget: int = BUDGET_PER_MIN, window_ns: int = WINDOW_NS) -> None:
    """Take one token from the shared per-minute bucket (SQLite is the
    cross-process arbiter), sleeping until the window rolls once it's spent."""
    while True:
        conn = ledgerdb.connect(db_path)
        try:
            with conn:
                now = time.time_ns()
                row = conn.execute(
                    "SELECT window_start_ns, used FROM rate_budget WHERE venue = ?",
                    (venue,)).fetchone()
                if row is None or now - row["window_start_ns"] >= window_ns:
                    conn.execute(
                        """INSERT INTO rate_budget (venue, window_start_ns, used)
                           VALUES (?, ?, 1)
                           ON CONFLICT(venue) DO UPDATE SET
                             window_start_ns = excluded.window_start_ns, used = 1""",
                        (venue, now))
                    return
                if row["used"] < budget:
                    conn.execute(
                        "UPDATE rate_budget SET used = used + 1 WHERE venue = ?",
                        (venue,))
                    return
                wait_s = (row["window_start_ns"] + window_ns - now) / 1e9
        finally:
            conn.close()
        time.sleep(max(wait_s, 0.05))


class PmusSession:
    """Signed HTTP session for api.polymarket.us with the shared rate budget.

    priority: "background" (default) consumes a budget token and may block;
    "critical" (order place/cancel/hedge) never waits and never opens the DB.
    """

    def __init__(self, key_id: str, secret_key_b64: str, base: str = API_BASE,
                 client: httpx.Client | None = None, db_path=ledgerdb.DB_PATH,
                 budget_per_min: int = BUDGET_PER_MIN):
        self.kid = key_id
        self.priv = load_ed25519(secret_key_b64)
        self.base = base
        self.client = client or httpx.Client(timeout=15)
        self.db_path = db_path
        self.budget_per_min = budget_per_min

    def headers(self, method: str, path: str) -> dict:
        return sign_headers(self.kid, self.priv, method, path)

    def request(self, method: str, path: str, priority: str = "background",
                headers: dict | None = None, **kw) -> httpx.Response:
        if priority != "critical":
            consume_budget(self.db_path, budget=self.budget_per_min)
        if headers is None:
            headers = self.headers(method, path)
        return self.client.request(method, self.base + path, headers=headers, **kw)

    def get(self, path: str, priority: str = "background", **kw) -> httpx.Response:
        return self.request("GET", path, priority=priority, **kw)

    def post(self, path: str, priority: str = "background", **kw) -> httpx.Response:
        return self.request("POST", path, priority=priority, **kw)

    # ---- shared reads the scripts used to hand-roll ----

    def get_positions(self, allow_empty: bool = False,
                      priority: str = "background") -> dict:
        """GET /v1/portfolio/positions -> dict KEYED BY SLUG with string
        fields (quirk pmus-positions-dict-keyed-by-slug). The endpoint
        transiently serves EMPTY snapshots while positions are held (quirk
        pmus-positions-empty-glitch, seen 4x 2026-07-22): empty raises unless
        allow_empty, so callers' retry loops re-fetch instead of freeing
        phantom risk headroom or painting false NAKED alerts."""
        r = self.get("/v1/portfolio/positions", priority=priority)
        r.raise_for_status()
        pos = r.json().get("positions", {})
        if not pos and not allow_empty:
            raise RuntimeError("PM positions empty — glitch read")
        return pos

    def get_order(self, order_id: str, priority: str = "background") -> dict:
        """Authoritative order record (create responses omit fill data —
        quirk pmus-create-omits-fill-data; poll this until the async fill
        lands)."""
        r = self.get(f"/v1/order/{order_id}", priority=priority)
        r.raise_for_status()
        j = r.json()
        return j.get("order", j)


# ---- public gateway reads (credential-free, gateway.polymarket.us) ----

def get_bbo(client: httpx.Client, slug: str, **kw) -> dict:
    """GET /markets/{slug}/bbo -> marketData dict (bestBid/bestAsk)."""
    return client.get(f"{GATEWAY_BASE}/markets/{slug}/bbo", **kw) \
        .json().get("marketData", {})


def get_book(client: httpx.Client, slug: str, **kw) -> dict:
    """GET /markets/{slug}/book -> marketData dict. The ask side is named
    'offers' (quirk pmus-ws-full-book-snapshots-offers; same shape as WS)."""
    return client.get(f"{GATEWAY_BASE}/markets/{slug}/book", **kw) \
        .json().get("marketData", {})


def top_of_book(client: httpx.Client, slug: str, **kw):
    """(best bid, best ask) as Decimals (None = empty side) — the exact parse
    previously re-rolled by mark/make_take/execute_xv/auto_take_take."""
    b = get_bbo(client, slug, **kw)
    bid, ask = (b.get("bestBid") or {}), (b.get("bestAsk") or {})
    return (Decimal(bid["value"]) if bid.get("value") else None,
            Decimal(ask["value"]) if ask.get("value") else None)
