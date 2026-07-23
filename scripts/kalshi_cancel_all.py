"""Cancel every resting Kalshi order (operational safety utility). Lists
resting orders and cancels each via the gateway's V2 cancel path.

BLANKET cancel on the SHARED account: the political runner, sports engine,
and research probes all rest orders here. Only sensible when everything is
halting, so it refuses to run unless data/KILL exists (card be03a1ff) or
--force is passed with eyes open.
"""

import argparse
import pathlib

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.record.kalshi import load_private_key

ap = argparse.ArgumentParser()
ap.add_argument("--force", action="store_true",
                help="cancel even without data/KILL (wipes runner+probe quotes)")
args = ap.parse_args()

if not pathlib.Path("data/KILL").exists() and not args.force:
    raise SystemExit("refusing blanket cancel: data/KILL absent (live engines are "
                     "quoting this account). touch data/KILL first, or --force.")

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
