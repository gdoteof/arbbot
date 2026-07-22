"""One-shot: wrap this wallet's USDC.e into pUSD via Polymarket's
CollateralOnramp, so the CLOB (V2/pUSD) sees collateral.

Verified against Polymarket's own docs (docs.polymarket.com/concepts/pusd,
/resources/contracts, 2026-07-20):
  onramp  0x93070a847efEf7F70739046A929D47a521F5B8ee
  pUSD    0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
  USDC.e  0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
  wrap(address _asset, address _to, uint256 _amount); approve onramp first.

Run it yourself (the fund-moving broadcast is intentionally gated from the
agent):  .venv313/bin/python scripts/wrap_usdce_to_pusd.py
"""

import pathlib
import time

from eth_account import Account
from web3 import Web3

KEY = pathlib.Path.home().joinpath(".arbbot-credentials/polymarket_private_key").read_text().strip()
if not KEY.startswith("0x"):
    KEY = "0x" + KEY
acct = Account.from_key(KEY)
ADDR = acct.address
USDCE = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
ONRAMP = Web3.to_checksum_address("0x93070a847efEf7F70739046A929D47a521F5B8ee")
PUSD = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
RPCS = ["https://polygon-rpc.com", "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com"]

ERC20 = [
    {"name": "balanceOf", "inputs": [{"type": "address"}], "outputs": [{"type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"name": "approve", "inputs": [{"type": "address"}, {"type": "uint256"}],
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"},
]
ONRAMP_ABI = [{"name": "wrap", "inputs": [{"type": "address"}, {"type": "address"},
              {"type": "uint256"}], "outputs": [], "stateMutability": "nonpayable",
              "type": "function"}]


def main() -> None:
    from web3.middleware import ExtraDataToPOAMiddleware
    w3 = next(w for u in RPCS if (w := Web3(Web3.HTTPProvider(u, request_kwargs={"timeout": 20})))
              .is_connected() and w.eth.chain_id == 137)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)  # Polygon is PoA
    usdce = w3.eth.contract(USDCE, abi=ERC20)
    pusd = w3.eth.contract(PUSD, abi=ERC20)
    onr = w3.eth.contract(ONRAMP, abi=ONRAMP_ABI)
    bal = usdce.functions.balanceOf(ADDR).call()
    print(f"wallet {ADDR}\nUSDC.e {bal/1e6}  pUSD {pusd.functions.balanceOf(ADDR).call()/1e6}")
    assert bal > 0, "no USDC.e"

    def send(fn, desc):
        base = w3.eth.get_block("latest")["baseFeePerGas"]
        prio = w3.to_wei(40, "gwei")
        tx = fn.build_transaction({"from": ADDR, "nonce": w3.eth.get_transaction_count(ADDR),
                                   "maxFeePerGas": base * 2 + prio,
                                   "maxPriorityFeePerGas": prio, "chainId": 137})
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)
        h = w3.eth.send_raw_transaction(acct.sign_transaction(tx).raw_transaction)
        print(f"  {desc} {h.hex()}", end="", flush=True)
        print(" status", w3.eth.wait_for_transaction_receipt(h, timeout=180).status)

    send(usdce.functions.approve(ONRAMP, bal), "approve")
    send(onr.functions.wrap(USDCE, ADDR, bal), "wrap")
    time.sleep(4)
    print(f"DONE. pUSD {pusd.functions.balanceOf(ADDR).call()/1e6}  "
          f"USDC.e {usdce.functions.balanceOf(ADDR).call()/1e6}")


if __name__ == "__main__":
    main()
