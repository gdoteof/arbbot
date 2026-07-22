"""Cancel every resting Kalshi order (operational safety utility). Lists
resting orders and cancels each via the gateway's V2 cancel path."""

import pathlib

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.record.kalshi import load_private_key

D = pathlib.Path.home() / ".arbbot-credentials"
gw = KalshiOrderGateway((D / "kalshi_api_key_id").read_text().strip(),
                        load_private_key((D / "kalshi_private_key.pem").read_bytes()),
                        live=True)

orders = gw.resting_orders()
print(f"{len(orders)} resting order(s)")
for o in orders:
    oid = o.get("order_id")
    print(f"  cancel {oid} {o.get('ticker')} {o.get('side')} "
          f"{o.get('remaining_count_fp')} -> {gw.cancel(oid)}")
