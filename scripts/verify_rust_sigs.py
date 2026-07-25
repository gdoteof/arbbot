#!/usr/bin/env python3
"""Cross-check: verify a RUST-produced Kalshi RSA-PSS signature with the Python
stack. Closes the loop on the salted (nondeterministic) signer — the Rust test
`kalshi_emit_rust_signature_for_python_crosscheck` writes
rust/target/tmp/rust_kalshi_sig.json {message, signature_b64,
public_key_spki_pem}; this verifies it with cryptography, the same library the
live client signs with.

Run from the worktree root AFTER `cargo test -p arb-venue`:
    /home/geoff/claude/arbbot/.venv/bin/python scripts/verify_rust_sigs.py
"""

from __future__ import annotations

import base64
import json
import pathlib
import sys

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature

IN = (pathlib.Path(__file__).resolve().parent.parent
      / "rust/target/tmp/rust_kalshi_sig.json")


def main() -> int:
    if not IN.exists():
        print(f"MISSING {IN} — run `cargo test -p arb-venue` first", file=sys.stderr)
        return 2
    doc = json.loads(IN.read_text())
    pub = serialization.load_pem_public_key(doc["public_key_spki_pem"].encode())
    sig = base64.b64decode(doc["signature_b64"])
    message = doc["message"].encode()
    try:
        # same PSS params the Python client uses: MGF1(SHA256), salt=DIGEST_LENGTH
        pub.verify(
            sig, message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    except InvalidSignature:
        print("FAIL — Python REJECTED the Rust RSA-PSS signature")
        return 1
    print(f"OK — Python verified the Rust RSA-PSS signature over message={doc['message']!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
