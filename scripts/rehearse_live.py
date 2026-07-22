"""Live order-path rehearsal on BOTH venues — place a far-from-market order,
confirm it rests, cancel it. Near-zero fill risk; validates auth/signing/
permissions and surfaces Polymarket's minimum-order rules empirically.

Run it yourself (live order placement is gated from the agent):
    ARBBOT_CREDENTIALS_DIR=~/.arbbot-credentials \
      .venv313/bin/python scripts/rehearse_live.py
"""

import pathlib
import time
from decimal import Decimal

D = pathlib.Path.home() / ".arbbot-credentials"

KALSHI_TICKER = "KXPRESNOMR-28-RDS"   # DeSantis nomination on Kalshi (~5c)
PM_TOKEN = "12127975650736113116407758794754973741525044713193258995588171654429754588610"


def kalshi_rehearse():
    from arbbot.exec.kalshi_gateway import KalshiOrderGateway
    from arbbot.record.kalshi import load_private_key
    gw = KalshiOrderGateway((D / "kalshi_api_key_id").read_text().strip(),
                            load_private_key((D / "kalshi_private_key.pem").read_bytes()),
                            live=True)
    print("KALSHI: place 1ct YES @ 1c (far below market), confirm, cancel")
    print("  ->", gw.rehearse(KALSHI_TICKER))


def pm_rehearse():
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    key = (D / "polymarket_private_key").read_text().strip()
    if not key.startswith("0x"):
        key = "0x" + key
    c = ClobClient("https://clob.polymarket.com", key=key, chain_id=137)
    c.set_api_creds(c.create_or_derive_api_creds())
    # far below the ~0.025 market so it can't fill; try escalating size to learn
    # any minimum-notional rule (5 -> 40 -> 100 shares @ $0.01).
    for size in (5, 40, 100):
        print(f"POLYMARKET: place {size} YES @ $0.01 (far below ~2.5c market)")
        try:
            args = OrderArgs(token_id=PM_TOKEN, price=0.01, size=float(size), side=BUY)
            resp = c.post_order(c.create_order(args), OrderType.GTC)
            print("  placed:", resp)
            oid = resp.get("orderID") or resp.get("order_id")
            time.sleep(1.0)
            resting = [o for o in c.get_orders() if (o.get("id") or o.get("order_id")) == oid]
            print("  rested:", bool(resting))
            if oid:
                print("  cancel:", c.cancel(oid))
            print(f"  => Polymarket accepts a {size}-share order (notional ${size*0.01:.2f}).")
            return
        except Exception as e:
            print(f"  rejected at size {size}: {str(e)[:160]}")
    print("  => could not place any test order; see rejections above.")


if __name__ == "__main__":
    kalshi_rehearse()
    print()
    pm_rehearse()
