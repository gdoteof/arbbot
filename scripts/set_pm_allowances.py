"""Set Polymarket CLOB V2 trading allowances for the bot wallet:
approve pUSD spend + CTF setApprovalForAll to the exchanges the CLOB checks.

Exchanges (from the CLOB balance-allowance response for this account):
  CTF Exchange V2   0xE111180000d2663C0091e4f400237545B87B996B
  NegRisk adapter   0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
  NegRisk Exchange  0xe2222d279d744050d28e00520010520000310F59
pUSD  0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB
CTF   0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
Idempotent: skips an approval that is already set.
"""

import pathlib

from eth_account import Account
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

KEY = pathlib.Path.home().joinpath(".arbbot-credentials/polymarket_private_key").read_text().strip()
if not KEY.startswith("0x"):
    KEY = "0x" + KEY
acct = Account.from_key(KEY)
ADDR = acct.address
PUSD = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
CTF = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
EXCHANGES = [Web3.to_checksum_address(a) for a in (
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296",
    "0xe2222d279d744050d28e00520010520000310F59")]
MAX = 2**256 - 1
RPCS = ["https://polygon-rpc.com", "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com"]

ERC20 = [
    {"name": "allowance", "inputs": [{"type": "address"}, {"type": "address"}],
     "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"name": "approve", "inputs": [{"type": "address"}, {"type": "uint256"}],
     "outputs": [{"type": "bool"}], "stateMutability": "nonpayable", "type": "function"}]
CTF_ABI = [
    {"name": "isApprovedForAll", "inputs": [{"type": "address"}, {"type": "address"}],
     "outputs": [{"type": "bool"}], "stateMutability": "view", "type": "function"},
    {"name": "setApprovalForAll", "inputs": [{"type": "address"}, {"type": "bool"}],
     "outputs": [], "stateMutability": "nonpayable", "type": "function"}]


def main() -> None:
    w3 = next(w for u in RPCS if (w := Web3(Web3.HTTPProvider(u, request_kwargs={"timeout": 20})))
              .is_connected() and w.eth.chain_id == 137)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    pusd = w3.eth.contract(PUSD, abi=ERC20)
    ctf = w3.eth.contract(CTF, abi=CTF_ABI)

    nonce = [w3.eth.get_transaction_count(ADDR, "pending")]

    def send(fn, desc):
        base = w3.eth.get_block("latest")["baseFeePerGas"]
        prio = w3.to_wei(40, "gwei")
        tx = fn.build_transaction({"from": ADDR, "nonce": nonce[0],
                                   "maxFeePerGas": base * 2 + prio, "maxPriorityFeePerGas": prio,
                                   "chainId": 137})
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)
        h = w3.eth.send_raw_transaction(acct.sign_transaction(tx).raw_transaction)
        print(f"  {desc} {h.hex()}", end="", flush=True)
        st = w3.eth.wait_for_transaction_receipt(h, timeout=180).status
        nonce[0] += 1
        print(" status", st)

    for ex in EXCHANGES:
        if pusd.functions.allowance(ADDR, ex).call() < MAX // 2:
            send(pusd.functions.approve(ex, MAX), f"pUSD approve -> {ex[:10]}")
        else:
            print(f"  pUSD already approved -> {ex[:10]}")
        if not ctf.functions.isApprovedForAll(ADDR, ex).call():
            send(ctf.functions.setApprovalForAll(ex, True), f"CTF approveAll -> {ex[:10]}")
        else:
            print(f"  CTF already approved -> {ex[:10]}")
    print("allowances set.")


if __name__ == "__main__":
    main()
