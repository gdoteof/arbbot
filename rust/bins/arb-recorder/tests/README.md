# Half-open socket stress test

Reproduces the failure that caused the 959-second PM-intl outage of
2026-07-25, and proves the stall guard catches it.

`halfopen_server.py` completes a WebSocket handshake, sends a couple of real
frames so the client is genuinely subscribed, then goes **silent while holding
the TCP connection open** — no close frame, no error, no data. It keeps
draining what the client sends so the client's writes keep succeeding, which
is what makes the socket half-open rather than simply broken.

Run (from the repo root, with the release binary built):

```bash
python3 rust/bins/arb-recorder/tests/halfopen_server.py 8899 &
ARBBOT_WS_PMINTL=ws://127.0.0.1:8899/ \
ARBBOT_STALL_RECONNECT_S=5 \
ARBBOT_CREDENTIALS_DIR=/nonexistent \
timeout 35 rust/target/release/arb-recorder \
  --config config/recorder.yaml --data-dir /tmp/stress-raw \
  --socket /tmp/stress.sock --health /tmp/stress-health.jsonl \
  --shadow --kalshi-poll-only --pmus-poll-only
```

Measured 2026-07-27:

| threshold | connections in 36s | verdict |
|---|---|---|
| 5s (guard on)      | 5, gaps of 7.2s | PASS — detects and reconnects |
| 9999s (guard off)  | 1, hung 36s     | FAIL — reproduces the original bug |

The control matters: without it the test could pass for reasons unrelated to
the guard. 7.2s is 5s of stall detection plus the 2s reconnect backoff.

`ARBBOT_WS_*` and `ARBBOT_STALL_RECONNECT_S` exist ONLY for this test.
Production sets neither; the venue URLs stay compiled-in constants and the
threshold defaults to 60s.
