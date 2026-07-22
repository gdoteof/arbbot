"""Diagnose the Polymarket 403. Print the FULL error body (not truncated),
the derived credentials/address, and whether read endpoints work — to tell a
genuine regional block apart from a config problem."""

import pathlib
import traceback

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BalanceAllowanceParams, AssetType
from py_clob_client.order_builder.constants import BUY

D = pathlib.Path.home() / ".arbbot-credentials"
PM_TOKEN = "12127975650736113116407758794754973741525044713193258995588171654429754588610"

key = (D / "polymarket_private_key").read_text().strip()
if not key.startswith("0x"):
    key = "0x" + key

c = ClobClient("https://clob.polymarket.com", key=key, chain_id=137)
creds = c.create_or_derive_api_creds()
c.set_api_creds(creds)

print("== identity ==")
print("  signer address:", c.get_address())
try:
    print("  api key:", creds.api_key)
except Exception as e:
    print("  api creds err:", e)

print("\n== read endpoints (should work even if trading is blocked) ==")
try:
    print("  server ok:", c.get_ok())
except Exception as e:
    print("  get_ok err:", repr(e)[:200])
try:
    ba = c.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    print("  collateral balance:", ba)
except Exception as e:
    print("  balance err:", repr(e)[:300])

print("\n== order attempt (full error) ==")
try:
    args = OrderArgs(token_id=PM_TOKEN, price=0.01, size=5.0, side=BUY)
    signed = c.create_order(args)
    resp = c.post_order(signed, OrderType.GTC)
    print("  PLACED:", resp)
except Exception as e:
    print("  full error:", repr(e))
    traceback.print_exc()
