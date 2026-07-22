# arbbot

Personal Polymarket/Kalshi prediction-market arbitrage bot. Staged autonomous
stack: it ships as a measuring instrument, validates its edge against recorded
reality, and earns live trading stage by stage.

**Design doc (source of truth):** kept locally, outside the repo.

## Architecture

```
             +----------------- recorder.service (credential-free) -----------------+
             |                                                                      |
  Polymarket WS ----+                                                               |
  (books/deltas/    |    normalized      +-------------+    JSONL (per venue/day)   |
   tape)            +--> BookEvents ---> | BookBuilder | --> data/raw/*.jsonl ------+--> DuckDB (batch
  Kalshi REST ------+                    |  (gap-safe) |         |                  |    ingest, replay)
  (snapshots; WS    |                    +------+------+         +--> Unix socket   |
   when keys exist) |                           |                     rebroadcast --+--> trader.service
             |                            ScanLoop (per affected                    |    (Stage 3, gated,
             |                            relationship) --> data/scan/*.jsonl ------+     LoadCredential)
             +----------------------------------------------------------------------+
```

- **Registry** (`config/registry.yaml`): the relationship graph — cross-venue
  equivalents, exclusives (at-most-one), partitions (exactly-one), implications,
  ladders, rollups. Agent-proposed entries are recordable/scannable but **never
  tradable** until `vetted_by: human`.
- **Scanner**: generic basket pricer — enumerate side assignments (canonical
  all-YES/all-NO beyond 10 legs), keep baskets with min feasible payoff >= 1,
  price against executable depth net of exact per-role fees, respect tick sizes
  and minimum order sizes (sub-minimum bucket kept separate).
- **Replay** (Stage 2 gate): lag sweep 300/500/1000ms; maker fills only on
  trade-THROUGH; deterministic. Output: capture rate + annualized return on
  locked capital per relationship class/tranche.
- **Exec/Risk** (Stage 3, not enabled): leg-risk state machine with escalation
  ladder and max-age alarms; tail-first sizing (a both-legs-lose event costs
  <= 2% bankroll, scaled down by oracle risk); kill switch (`touch data/KILL`);
  fee-per-fill reconciliation as the fee-schedule-drift detector.

## Running

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/pytest                                  # full suite
.venv/bin/python -m arbbot.record.main            # recorder (foreground)
.venv/bin/python -m arbbot.report.daily           # daily report for today
.venv/bin/python -m arbbot.report.daily --day 2026-07-20 --json
```

systemd (user units, survive logout with `loginctl enable-linger`):
```bash
cp systemd/arbbot-recorder.service ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now arbbot-recorder
```

`arbbot-trader.service` exists but must NOT be enabled until the Stage 2
replay gate passes and credentials are provisioned under `~/.arbbot-credentials/`
(mode 0600; a dedicated trading wallet holding working capital only).

## Stage gates (from the design doc)

1. **Record + scan** (now): >=1 week of data AND minimum event coverage.
2. **Replay validation**: per-class go/no-go from lag-swept replay. A "no"
   for every class is a successful outcome.
3. **Tiny live**: Kalshi demo-env order rehearsal first; live capture rate
   AND per-trade P&L directional-vs-replay over >=50 trades; recorder keeps
   running so gate comparisons use the same wall-clock window.
4. **Scale out**: make-take, agent miner auto-expansion, structural strategies.

## Config

- `config/recorder.yaml` — paths, poll interval, ntfy topic (empty = alerts off).
- `config/registry.yaml` — the universe. Human vetting flips `vetted_by`.
- `config/registry-rejected.yaml` — documented traps (never scanned).

## Dashboard

Instrument panel at **http://127.0.0.1:4748** (`arbbot-dash.service`) — live view
of accumulating data: feed health (calibration plate), crossings observed, per-
relationship crossings/edge/lifetimes, and the full relationship ledger.
Ranges: Today / 7d / All. Auto-refreshes every 10s; `?theme=light|dark` forces
a theme. Read-only; binds 127.0.0.1 only.
