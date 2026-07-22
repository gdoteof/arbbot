#!/usr/bin/env bash
# Install Kalshi API credentials for arbbot's trader/recorder with correct perms.
#
# Usage:
#   scripts/install-kalshi-key.sh <API_KEY_ID> <PATH_TO_PRIVATE_KEY.pem>
#
# Get these from Kalshi: Account -> Profile -> API Keys -> "Create API Key".
# Kalshi shows the API Key ID (a UUID) and lets you download an RSA private
# key (.pem) exactly once. Pass the UUID as arg 1 and the downloaded .pem path
# as arg 2. This script copies them to ~/.arbbot-credentials/ (mode 0600) where
# the recorder/trader read them, and restarts the recorder so it upgrades from
# 30s REST polling to the real-time WebSocket feed.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <API_KEY_ID> <PATH_TO_PRIVATE_KEY.pem>" >&2
  exit 1
fi
KEY_ID="$1"; PEM="$2"
[ -f "$PEM" ] || { echo "private key not found: $PEM" >&2; exit 1; }

DIR="$HOME/.arbbot-credentials"
mkdir -p "$DIR"; chmod 700 "$DIR"
printf '%s' "$KEY_ID" > "$DIR/kalshi_api_key_id"
cp "$PEM" "$DIR/kalshi_private_key.pem"
chmod 600 "$DIR/kalshi_api_key_id" "$DIR/kalshi_private_key.pem"
echo "installed -> $DIR (mode 0600)"

# sanity: the key must load and sign
"$HOME/claude/arbbot/.venv313/bin/python" - "$DIR" <<'PY'
import sys, pathlib
from arbbot.record.kalshi import ws_auth_headers
d = pathlib.Path(sys.argv[1])
kid = (d / "kalshi_api_key_id").read_text().strip()
pem = (d / "kalshi_private_key.pem").read_bytes()
h = ws_auth_headers(kid, pem)
assert h["KALSHI-ACCESS-KEY"] and h["KALSHI-ACCESS-SIGNATURE"]
print("signing check: OK (key id %s..., signature %d bytes b64)" % (kid[:8], len(h["KALSHI-ACCESS-SIGNATURE"])))
PY

echo "NOTE: the WS upgrade path is wired in record/kalshi.py (ws_auth_headers) but"
echo "the recorder still runs REST polling until the WS client is switched on."
echo "Credentials are now in place; tell Claude to flip the recorder to WS mode."
