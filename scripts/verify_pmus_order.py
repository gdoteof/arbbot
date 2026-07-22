"""Validate the PM US order gateway against the live /order/preview endpoint.
Preview projects fills+fees but NEVER submits — proves the signed order path
(auth + body shape) works end-to-end with zero risk. No order is placed."""

import pathlib
from decimal import Decimal

from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway

D = pathlib.Path.home() / ".arbbot-credentials"
gw = PolymarketUsOrderGateway(
    (D / "polymarket_usa_key_id").read_text().strip(),
    (D / "polymarket_usa_private_key").read_text().strip(),
    live=False,  # dry-run; preview() bypasses this and is always safe
)

SLUG = "rdc-usfed-fomc-2026-07-29-nochng"
print("PREVIEW: buy 1 YES @ $0.02 (far below ~0.93 market — would not fill)")
print("  ->", gw.preview(SLUG, "bid", Decimal("0.02"), 1))
print("\nPREVIEW: buy 5 YES @ $0.90 (marketable — shows projected fills+fees)")
print("  ->", gw.preview(SLUG, "bid", Decimal("0.90"), 5))
