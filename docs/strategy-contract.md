# Strategy Contract v2 — vocabulary, families, sole-executor engine, intent gateway (design-only)

Status: **design-only, v2** (Geoff design feedback, 2026-07-23; supersedes v1
of the same day). Nothing in this doc changes live behavior today. The
contract becomes load-bearing when the P4 engine reads
`config/strategies.yaml`; until then it is the reviewed specification that new
strategies and the engine port are written against.

**What changed v1 → v2 (Geoff's decisions):**

1. **The Rust engine is the sole executor.** There is no `runtime:
   python-daemon` as an end state — in the target architecture Python never
   places orders. Every strategy, whatever its language or location, is an
   intent *proposer*; the engine validates every intent against the manifest
   (ownership claims, caps, venue budgets, kill, no-self-cross, shared risk)
   regardless of source, then executes or rejects-with-reason. Trust
   concentrates in Rust + the manifest; Python becomes untrusted by
   construction instead of trusted by hope.
2. **Strategy substance moves into the YAML via families.** Where a strategy
   is a parameterization of a known shape, the manifest entry IS the
   strategy: a `family` field selects a verified Rust template and typed
   params configure it (§3). Bespoke logic that fits no family is a Rust
   module implementing the `Strategy` trait against its manifest entry.
   Full logic-in-YAML beyond parameterization is explicitly out of scope: a
   YAML dialect expressive enough for arbitrary strategy logic is an
   unverified programming language with no type checker and no parity gate —
   the opposite of concentrating trust.
3. **Python's remaining roles:** (a) research sidecars that propose intents
   through the engine's **intent gateway** (§5) under identical arbitration;
   (b) the read-only diverse auditor (reconcile/mark) — permanently.
   Lifecycle actions (unwind / naked-hedge / settle) eventually submit
   through the gateway too (later-phase).
4. The runtime enum becomes `rust-template(family)` | `rust-module` |
   `external-intents` | `audit-readonly` | `manual-tool`, and every manifest
   entry carries both `target_runtime` and a `today:` field, so the manifest
   documents present *and* target reality per strategy (§2).

> **Census caveat:** this worktree lags `main` by a few commits — `main`
> gained a toxicity gate in the quoter and the clip-25 scale-up. The manifest
> entries for `make-take` (clip, quoter knobs) MUST be re-verified against
> `main` before the manifest becomes load-bearing. All file:line citations
> below are against this worktree.

---

## 1. One vocabulary: canonical `strategy_id`

*(Unchanged from v1 — the census stands.)*

Today strategy identity is spread across **five uncoordinated vocabularies**:

1. ledger `strategy` tags (`"strategy": "make-take"` — `src/arbbot/exec/main.py:104`)
2. `dual_append` `source` values, in **three conventions**: `python-trader`
   (`src/arbbot/exec/main.py:119`), `py:*` (`scripts/sports_arb.py:353`,
   `scripts/hedge_naked_legs.py:224`), `probe:*` (`scripts/toxicity_probe.py:197`,
   `scripts/pmus_maker_probe.py:646`, `scripts/leadlag_probe.py:220`)
3. ownership `owner` (`src/arbbot/exec/schema.sql:114` expects
   `python-trader | rust-trader | probe-<name>`; `claim_ownership`
   (`src/arbbot/exec/ledgerdb.py:295`) has **zero production callers** — only
   `tests/test_ledgerdb_readers.py:116-120`)
4. topic-budget families (`config/topics.yaml`, deliberately **untracked** —
   `src/arbbot/exec/main.py:173-175`; substring-matched by
   `risk/manager.py:105` `topic_of`)
5. dash FAMILIES / `_category` (`src/arbbot/dash/data.py:577-602`)

Vocabularies 4 and 5 are **market/topic** taxonomies, not strategy identity —
they stay separate (a strategy trades many topics). The contract collapses
1–3 into one canonical kebab-case `strategy_id`. **Legacy ledger tags are kept
as-is — no data migration in this track**; the mapping is recorded so joins
are possible.

*(Terminology note: manifest `family` in this doc always means a strategy
family per §3 — a Rust template shape. Topic-budget "families" in
`topics.yaml` are an unrelated market taxonomy and keep their name there.)*

### 1.1 Canonical vocabulary table

| strategy_id | legacy ledger tag(s) | legacy `source` | ownership `owner` (canonical) | code | notes |
|---|---|---|---|---|---|
| `make-take` | `make-take` (main.py:104) | `python-trader` (main.py:119) | `python-trader` | `exec/quoter.py` + `exec/main.py` | runner maker; intents `data/scan/trader-intents-{rel}.jsonl` (main.py:582) |
| `take-take` | `take-take` (main.py:445) | `python-trader` (main.py:455) | `python-trader` | `_take_take`, `exec/main.py:377` | in-runner trigger; same process as make-take |
| `sports-take-take` | `sports-take-take` (sports_arb.py:360) | `py:sports_arb` (sports_arb.py:367) | `sports-engine` (new) | `scripts/sports_arb.py` detector:575 | |
| `pm-lean` | `pm-lean` (sports_arb.py:347) | `py:sports_arb` | `sports-engine` | sports_arb.py lean rider | lifetime-capped experiment |
| `sports-mt-probe` | *(none — logs to `data/exec/sports_mt_probe.jsonl`, sports_arb.py:669, NOT the ledger)* | *(n/a)* | `sports-engine` | sports_arb.py `mt_probe`:662 | opt-in `--mt-probe` |
| `sports-rehedge` | `sports-take-take` (sports_arb.py:980, **tag reuse**) + close-kind `flatten` (sports_arb.py:904) | `py:sports_arb` | `sports-engine` | sports_arb.py `rehedge_watcher`:832 | converts naked holds; ids `sports-rehedge-*`/`sports-flatten-*` |
| `pmus-maker-probe` | `pmus-maker-probe` (pmus_maker_probe.py:641) | `probe:pmus-maker` (:646) | `probe-pmus-maker` | `scripts/pmus_maker_probe.py` | PM-US-only maker |
| `toxicity-probe` | `ml-toxicity-probe` (toxicity_probe.py:326) | `probe:toxicity` (:197) | `probe-toxicity` | `scripts/toxicity_probe.py` | Kalshi maker, ML-gated |
| `leadlag-probe` | `ml-leadlag-probe` (leadlag_probe.py:210) | `probe:leadlag` (:220) | `probe-leadlag` | `scripts/leadlag_probe.py` | directional taker, ML-gated |
| `naked-hedge` | **`take-take` — COLLISION** (hedge_naked_legs.py:214) | `py:hedge_naked_legs` (:224) | `lifecycle` (new) | `scripts/hedge_naked_legs.py` | future tag `naked-hedge` written at migration time; until then hedger baskets are indistinguishable from real take-take in the ledger |
| `unwind` | close-kind `unwind` (unwind_positions.py:103) | `py:unwind_positions` (:108) | `lifecycle` | `scripts/unwind_positions.py` | |
| `settlement` | close-kind `settlement` (settle_baskets.py:55,76) | `py:settle_baskets` (:60,80) | `lifecycle` | `scripts/settle_baskets.py` | |
| `flatten-probe-residual` | `pmus-maker-probe` open+close (flatten_probe_residual.py:70,80 — attributes cleanup to the probe) | `dual_append` default | `probe-pmus-maker` | `scripts/flatten_probe_residual.py` | one-off, board card 6b54d1ce |
| `execute-xv` | *(none — records carry NO `strategy` key at all, execute_xv.py:151-164)* | `py:execute_xv` (:166) | `manual` (new) | `scripts/execute_xv.py` | manual one-shot taker |
| `make-take-manual` | `make-take` (**tag shared with runner**, make_take.py:127) | `py:make_take` (:139) | `manual` | `scripts/make_take.py` | manual one-shot |
| `auto-take-take` | `take-take` (auto_take_take.py:184) | `py:auto_take_take` (:191) | `manual` | `scripts/auto_take_take.py` | **RETIRES** — duplicate of `_take_take` with independently-declared constants (`VETTED`/`FEE_CT` at auto_take_take.py:39-40 vs `TT_VETTED`/`TT_FEE` at main.py:40-41); drift-prone by construction |
| `kalshi-cancel-all` | *(no ledger writes)* | *(n/a)* | `manual` | `scripts/kalshi_cancel_all.py` | **blanket hazard**: cancels EVERY resting Kalshi order on the shared account, all strategies' quotes included, no ownership filter |
| `rehearse-live` | *(no ledger writes)* | *(n/a)* | `manual` | `scripts/rehearse_live.py` | far-from-market place+cancel auth rehearsal |

### 1.2 Disjoint sets: open-strategy tags vs close-kinds

`basket.strategy` and `basket_close.kind` are **disjoint vocabularies**.
`CLOSE_KINDS = ("unwind", "settlement", "naked-settlement", "flatten",
"expiry")` (`src/arbbot/exec/ledgerdb.py:32`); a close record whose `strategy`
is not in that set gets its kind inferred (`ledgerdb.py:185-186`). Canonical
rule: **`strategy_id` identifies who acted; close `kind` identifies how the
position ended.** A close record therefore carries both (the acting
`strategy_id` in `source`/attribution, the `kind` from CLOSE_KINDS). No
`strategy_id` may be added to CLOSE_KINDS and vice versa.

### 1.3 New-tag rule

A ledger `strategy` tag or `source` value that does not map to a manifest
entry is a bug. When `dual_append` gains validation (noted for a later track),
it warns-then-rejects unknown tags; until then, review enforces it: **new
strategies add a manifest entry first.**

---

## 2. Execution model: the engine is the sole executor

**End state: exactly one process places, amends, or cancels orders — the
Rust engine.** Everything else proposes. An intent reaches a venue only after
the engine has validated it against the compiled manifest:

    proposer (any source) ──▶ Intent ──▶ engine arbitration (§6 a–f) ──▶ venue executor
                                             │
                                             └──▶ Reject{reason} event (WAL, alarmed)

The arbitration path is **identical for every source** — an in-process family
template, a bespoke Rust module, a Python research sidecar on the gateway, or
a lifecycle/manual tool. There is no privileged caller and no bypass lane.
This inverts today's trust model: instead of ~11 processes each trusted to
check its own caps and grep the others' logs, one process enforces one
manifest, and everything outside it is untrusted by construction.

### 2.1 Runtime enum (target) and the `today:` field

Every manifest entry declares `target_runtime` (where the strategy ends up)
and `today` (how it actually runs right now — `python-daemon`,
`python-timer`, or `python-manual`). The manifest is therefore an honest
document of both realities; migration progress is the diff between the two
columns.

| `target_runtime` | meaning |
|---|---|
| `rust-template(<family>)` | The manifest entry IS the strategy: the engine instantiates the named family template (§3) with the entry's typed `params`. No strategy-specific code exists. |
| `rust-module` | Bespoke logic that fits no family: a Rust `Strategy` trait impl, registered in the engine, bound to its manifest entry (claims/caps/params still come from the manifest). |
| `external-intents` | A sidecar (Python, typically) that proposes intents through the intent gateway (§5). Subject to identical arbitration; may never place orders directly. |
| `audit-readonly` | Reads venue truth and the ledger through its own code path; never emits intents. The diverse auditor (reconcile/mark) — deliberately does NOT share the engine's code or event stream, so an engine bug cannot blind its own audit. |
| `manual-tool` | Human-invoked one-shot. Later-phase: submits its orders through the gateway like any external proposer; until then it is a documented exception with a manifest entry. |
| `retires` | Sentinel: the entry exists only to map legacy ledger data; the code is deleted, not ported. |

---

## 3. Strategy families — substance in the YAML

A **family** is a verified Rust template: one implementation of a known
strategy shape, parameterized by a typed schema. A strategy whose behavior is
"family shape + these numbers" has **no code of its own** — its manifest
entry (family + params) is the complete definition. The template is verified
once (intent-parity gate per family, see `docs/p3-shell.md`); after that,
adding or tuning a strategy in that family is a config change, reviewed as
data, not a code change.

Initial families, drawn from the census strategies. Param values cited below
are the REAL current knobs (worktree file:line).

### 3.1 `maker-hedge`

Rest post-only maker quotes at an anchor-derived price; on fill, run the
hedge/inventory policy. Covers `make-take` (cross-venue riskless maker,
`exec/quoter.py`) and `pmus-maker-probe` (single-venue maker anchored to the
Kalshi reference mid, `scripts/pmus_maker_probe.py`). `sports-mt-probe` fits
this shape and would promote into it if it graduates from research.

| param | type | semantics | current values |
|---|---|---|---|
| `quote_venue` | enum `kalshi \| polymarket_us \| both` | where maker quotes rest | make-take: both (per relationship legs); pmus probe: polymarket_us |
| `anchor` | enum `riskless-hedge-cost \| reference-mid` | price model: riskless = `min(top+tick, p_max − safety)` from the hedge-cost scan (quoter.py:11); reference-mid = external mid ± margin (pmus_maker_probe.py:391-392) | make-take: riskless-hedge-cost; probe: reference-mid (Kalshi mid) |
| `margin` | decimal | reference-mid only: half-spread vs anchor | probe: 0.04 (pmus_maker_probe.py:747) |
| `safety_ticks` | int | riskless only: extra ticks inside p_max as slippage buffer | make-take: 0 (quoter.py:66) |
| `clip` | int | contracts per resting order | 5 both (quoter.py:64, probe :748) |
| `size_jitter` | int | rest `clip − random(0..N)` | make-take live: 2; probe: 0 |
| `price_jitter_ticks` | int | rest 0..N ticks more passive | make-take: 0 (quoter.py:69) |
| `deadband_ticks` | int | hold profitable quote inside randomized deadband | make-take: 0 (quoter.py:71) |
| `min_requote_s` | float | min rest time before repricing | make-take: 15.0 (quoter.py:67) |
| `quote_ttl_s` | float \| null | hard reprice deadline | mt-probe shape: 120 (sports_arb.py:77) |
| `max_concurrent` | int \| null | resting orders in flight | mt-probe shape: 8 (sports_arb.py:71) |
| `hedge` | enum `cross-venue-anchor-ladder \| none-inventory` | on-fill policy: anchor ladder = quote-time anchor + slippage band → age-bounded stale book → one REST snapshot → unwind (burst-gap postmortem); none-inventory = single-venue, manage inventory/exit | make-take: anchor-ladder; probe: none-inventory |
| `guards` | map | quote-suppression guards: `max_kspread` (0.05), `jump_standdown` (0.04), `px_bounds` ([0.05, 0.95]) (pmus_maker_probe.py:754-757); toxicity gate (main-only — re-verify) | per entry |
| session caps | in entry `caps` | `max_fills`, `max_loss_usd`, `max_baskets`/`max_inv`, bankroll — enforced by the shared risk view (§6c), not the template | per entry |

### 3.2 `take-take-cross`

Cross both venues taker-taker when the locked edge net of fees clears the
bar. Covers `take-take` (`exec/main.py:377`) and `sports-take-take`
(`scripts/sports_arb.py:575`). `auto-take-take` retires against this family.

| param | type | semantics | current values |
|---|---|---|---|
| `universe` | list of rel-id prefixes \| equiv-map query | vetted markets it may cross | take-take: `TT_VETTED` 4 prefixes (main.py:40); sports: matched games from `data/scan/sports_equiv_map.json`, leagues itfme/itfwo/wta/atp/kbo/npb/mlb |
| `edge_model` | enum `apr-bar \| min-edge` | apr-bar: bar floats with class-budget utilization between floor/ceil (main.py:349); min-edge: flat edge threshold | take-take: apr-bar floor 4.0 ceil 16.0 (main.py:50-51); sports: min-edge 0.02 (sports_arb.py:38) |
| `fee_ct` | decimal | both-leg taker fees per contract (conservative model) | 0.02 both (main.py:41, sports_arb.py fee model) |
| `max_clip` | int | contracts per single execution | take-take: 10 (main.py:42); sports: 5 (sports_arb.py:40) |
| `per_market_cap` | int | concentration cap, contracts | take-take: 50/rel (main.py:43); sports: 20/game (sports_arb.py:41) |
| `cooldown_s` | float | between fires on the same market | 30 (main.py:44); sports: 20 (sports_arb.py:42) |
| `max_spread` | decimal \| null | skip books wider than this | sports: 0.03 (sports_arb.py:39) |
| `legging` | enum `record-naked-and-rehedge \| abort` | second-leg-miss policy | sports: record to naked holds + rehedge watcher; take-take: abort/alarm |

### 3.3 `directional-signal`

One-sided taker gated by a model or rule signal. Covers `leadlag-probe`
(`scripts/leadlag_probe.py` — LightGBM model) and `pm-lean` (`sports_arb.py`
lean rider — rule signal, no model file).

| param | type | semantics | current values |
|---|---|---|---|
| `model_ref` | path \| null | exported model artifact evaluated by the template; null = rule trigger only | leadlag: `data/research/leadlag_model_sports.txt` (leadlag_probe.py:35); pm-lean: null (rides sports-take-take captures when PM is dear) |
| `thresh` | decimal | minimum leader jump / trigger magnitude | leadlag: 0.02 (leadlag_probe.py:299) |
| `min_p` | decimal \| null | minimum model probability to fire | leadlag: 0.65 (:300) |
| `clip` | int | contracts per trade | leadlag: 5 (:301); pm-lean: 2 (sports_arb.py:49) |
| `max_open_usd` | decimal \| null | session open-risk cap | leadlag: 50.0 (:302) |
| `lifetime_cap` | int \| null | contracts, then the experiment stops | pm-lean: 20 (sports_arb.py:50) |
| `max_trades` | int \| null | per session | leadlag: 20 (:303) |
| `per_market` | int \| null | trades per pair per session | leadlag: 2 (leadlag_probe.py:12) |
| `cooldown_s` | float | per pair | leadlag: 180 (:164) |
| `exit` | enum `settlement \| hold-s` | position exit policy | both: settlement (settle_baskets covers pm-lean + leadlag today) |

Note on `model_ref`: the template evaluates the exported artifact
deterministically (model bytes are part of config; a model swap is a config
change and re-runs the family parity gate on the affected strategy's tape).
Until LightGBM evaluation exists in the template, `leadlag-probe` runs as
`external-intents` — promotion is a config flip, not new strategy code.

### 3.4 Bespoke: `rust-module`

Logic that fits no family (novel shape, not just novel numbers) is a Rust
`Strategy` trait impl against its manifest entry — same claims, caps, and
arbitration; the only difference is where the fold's body lives. A third
occurrence of a shape is the signal to extract a family.

---

## 4. Manifest schema (`config/strategies.yaml`)

One YAML document, `strategies:` list, one entry per strategy. Tracked in git
(no secrets; topic budgets stay in untracked `topics.yaml`). Compiled — same
pattern as `scripts/compile_registry.py` — to `strategy` + `strategy_claim`
tables in `data/exec/trading.db` for runtime reads and dash joins (compile
step is a later track; the YAML is authored now).

| field | type | semantics |
|---|---|---|
| `id` | string, kebab-case, unique | canonical `strategy_id` (§1.1) |
| `kind` | `maker \| taker \| directional \| lifecycle \| manual \| audit` | economic role |
| `family` | family name (§3) or `bespoke \| external-intents \| audit-readonly \| manual-tool \| retires` | what defines the strategy's substance. A family name means the entry IS the strategy (template + `params`) |
| `params` | map, typed per family schema (§3) | family entries only: the template's configuration, REAL current values |
| `target_runtime` | §2.1 enum | where the strategy executes in the end state |
| `today` | `python-daemon \| python-timer \| python-manual \| none` | how it actually runs right now |
| `claims` | list of `{venue?, rel_ids? \| slug_patterns? \| registry_query?}` | markets it may propose orders in. Compiles to `strategy_claim` rows; the engine rejects intents outside the compiled set (§6a). Empty list = proposes no orders (audit/cancel-only tools) |
| `budget_share` | map venue → note/limit | share of the shared per-venue API budget (PM-US: `venues/pmus.py:34` `BUDGET_PER_MIN = 30`; the `priority="critical"` bypass hazard retires when executors own the buckets, §6f) |
| `caps` | map | capital/size caps, **each marked `per-session` or `lifetime`**; real current values from code; enforced by the shared risk view (§6c) |
| `ledger_tags` | list | `strategy` tags this strategy may write (legacy values until migration) |
| `close_kinds` | list | CLOSE_KINDS values it may write (lifecycle only) |
| `source` | string | its `dual_append` source value (legacy convention preserved) |
| `intents_stream` | path or null | intent/decision log it appends |
| `kill` | `honors \| DOES-NOT-CHECK (gap) \| inherits \| engine \| n/a` | `data/KILL` behavior today. **Contract: MUST honor — no exceptions.** `engine` = enforced by arbitration (§6e) once on the engine/gateway. Gap entries are known violations, surfaced via board cards |
| `state_files` | list | files this strategy owns (writes). No two entries may own the same file |

---

## 5. The intent gateway (`data/intents.sock`)

The engine's admission path for out-of-process proposers. Mirrors the
recorder socket conventions (`data/arbbot.sock`): a unix stream socket owned
by the engine, line-JSON (one UTF-8 JSON object per newline-terminated
line), local-only, filesystem permissions as the auth boundary. Chosen over a
SQLite queue because it matches the protocol conventions the stack already
has, gives sub-second request/reply, and provides a natural push channel for
acks/fills — no polling.

**Session.** A client connects and sends a hello:

```json
{"v": 1, "hello": {"strategy_id": "toxicity-probe"}}
```

The engine accepts only if the id exists in the compiled manifest with
`target_runtime: external-intents` (later-phase: also `manual-tool` and the
lifecycle scripts); otherwise it replies `{"result": "reject", "reason":
"unknown-strategy"}` and closes. All connections for one `strategy_id` share
that strategy's caps and claims — connecting twice buys nothing.

**Intent messages** (client → engine), tagged with a client-unique
`intent_id`:

```json
{"v": 1, "intent_id": "tox-20260723-0001", "action": "place",
 "venue": "kalshi", "market": "KXPRESNOMD-28-GN", "side": "buy",
 "contract": "yes", "price": "0.41", "qty": 5, "tif": "gtc"}
{"v": 1, "intent_id": "tox-20260723-0002", "action": "cancel", "order_id": "..."}
{"v": 1, "intent_id": "tox-20260723-0003", "action": "cancel-all-owned"}
```

Prices are string decimals (the stack's decimal-parity convention). `action`
is `place | cancel | cancel-all-owned` — there is no blanket cancel; a
cancel touches only orders owned by the sending `strategy_id` (this is what
retires the `kalshi_cancel_all` hazard class).

**Replies** (engine → client, one per intent, in order):

```json
{"intent_id": "tox-20260723-0001", "result": "accept"}
{"intent_id": "tox-20260723-0002", "result": "reject", "reason": "claims"}
```

`reason` ∈ `malformed | unknown-strategy | claims | self-cross | risk-cap |
venue-budget | kill | paused | duplicate`. `accept` means the intent passed
arbitration and was handed to the venue executor; venue-level outcomes
arrive as pushed events on the same connection:

```json
{"event": "ack",  "intent_id": "tox-20260723-0001", "order_id": "..."}
{"event": "fill", "order_id": "...", "qty": 5, "price": "0.41", "ts_ns": 0}
```

**Semantics.**

- Every gateway intent enters the engine's single event channel as an
  `ExternalIntent` event; the arbitration verdict is a WAL event. Gateway
  traffic therefore replays byte-exactly through the parity harness like
  everything else, and a sidecar's behavior is auditable from the WAL alone.
- **Identical arbitration** (§6 a–f): a gateway intent gets exactly the
  checks an in-process template's intent gets. Kill, claims, caps, budgets,
  no-self-cross — same code path, same rejection events.
- **Idempotent**: `intent_id` is deduplicated per strategy; a resend after
  reconnect gets `reason: duplicate` plus the original verdict. Safe to
  retry blindly.
- **Never blocks the engine**: bounded per-client queues; a slow or stalled
  client is disconnected (the recorder's slow-subscriber rule), never waited
  on. Disconnection cancels nothing by itself — resting orders remain owned
  by the strategy and governed by the manifest.
- This is the **safe-experimentation lane**: a research sidecar gets real
  execution under enforced caps, with zero ability to exceed its manifest.
  Later-phase, lifecycle scripts (unwind / naked-hedge / settle) and manual
  tools submit through the same socket, at which point no Python code holds
  venue order credentials at all.

---

## 6. Engine arbitration semantics

The engine enforces the following **for every intent regardless of source**
(family template, rust-module, gateway). Each requirement is testable
(harness-replayable, no venue I/O) and cites the ad-hoc guard it structurally
replaces.

**(a) Per-market exclusive ownership from compiled claims.**
An intent for a market not claimed by the emitting strategy — or claimed by
another — is rejected and alarmed (rejection is an event; parity-visible).
*Replaces:* the inert `ownership` table (`schema.sql:112-114`,
`claim_ownership` `ledgerdb.py:295`, zero callers) and all three duplicated
probe-log greps: `PROBE_SLUGS` (`scripts/sports_arb.py:151-176`),
`probe_owned_slugs` (`scripts/hedge_naked_legs.py:107` — the code itself says
"drop this grep once they claim ownership", `hedge_naked_legs.py:121`), and
the reconcile probe filter (`scripts/reconcile_positions.py:423-448`).
*Test:* replay a tape with two registered strategies claiming overlapping
markets → second registration or first foreign intent rejects deterministically.

**(b) No self-crossing.**
The engine sees all resting orders across strategies; a marketable intent
that would cross an own resting order (any strategy, same account) is
rejected + alarmed. Kills the wash-fill class structurally.
*Replaces:* `probe_owned(slug)` pre-checks in sports_arb's detector
(`sports_arb.py:618`) — today's only defense is each taker grepping the
probes' JSONL advertisements before crossing.
*Test:* strategy A rests at 40c, strategy B emits a marketable buy at 41c →
reject event, no order placed.

**(c) One shared risk/exposure view fed by fills-as-events.**
All fills enter the single event stream; one risk state (positions, capital
by class/topic, per-relationship caps) is folded from it and consulted for
every intent. *Replaces:* per-process ad-hoc caps that cannot see each other
— `TT_CAP` (`main.py:43`), `CAP_PER_GAME`/`LEAN_CAP`/`MT_LIFETIME_CAP`
(`sports_arb.py:41,50,69`), per-probe `--max-inv`/`--max-open-usd`/
`--max-baskets` (`toxicity_probe.py:452`, `leadlag_probe.py:302`,
`pmus_maker_probe.py:749`), each blind to the others' exposure on the same
account. *Test:* two strategies filling the same market → the second is
capped by the shared view where today both would fill to their private caps.

**(d) Deterministic strategy evaluation order.**
In-engine strategies are evaluated in registration order; given the same
event sequence (including `ExternalIntent` events at their WAL positions),
the interleaved intent stream is byte-identical. Extends the intent-parity
gates (§ p3-shell) to multi-strategy operation.
*Replaces:* nothing today (today's "ordering" is OS scheduling across 4
daemons — unreproducible by construction). *Test:* golden replay with N
strategies registered → pinned digest over the merged intent stream.

**(e) Per-strategy kill/pause via Control events + global KILL.**
`data/KILL` is a Control event from the file watcher and halts EVERY
strategy — no exceptions, no per-strategy opt-out; gateway intents are
rejected with `reason: kill` while set. Additionally each strategy can be
paused/resumed individually by Control event (gateway: `reason: paused`).
*Replaces:* the inconsistent per-process checks — honored by the runner
(`main.py:186`) and all three probes (`toxicity_probe.py:204`,
`pmus_maker_probe.py:202`, `leadlag_probe.py:154`), **not checked at all**
by `sports_arb.py` (grep KILL: zero hits — circuit breaker only,
`sports_arb.py:119-122`) nor by the order-placing timers
(`unwind_positions.py`, `hedge_naked_legs.py`, `settle_baskets.py`: zero
KILL hits). Once every proposer is on the engine/gateway, per-process KILL
checks become redundant instead of load-bearing. *Test:* inject
Control(KILL) mid-replay → zero intents from any strategy afterward; inject
Control(pause, id) → zero intents from that id only.

**(f) Venue budget shares enforced in the executors.**
Per-venue executor tasks own the rate limiters; each strategy's manifest
`budget_share` is enforced there, so no strategy can starve another's
background reads. It cannot starve another's HEDGE path either, but not by
sharing it out: since 2026-07-29 the order path draws from no local budget at
all (`rust/crates/arb-venue/src/ratelimit.rs` — the critical bucket is gone,
because a bucket the order path must bypass is not a budget). What shapes the
write path is the per-venue executor token bucket in `exec.rs`, which WAITS
rather than refusing.
*Replaces:* the cross-process `rate_budget` table + `consume_budget`
(`src/arbbot/venues/pmus.py:34,53`) whose `priority="critical"` path
bypasses metering entirely (`pmus.py:14` — "critical ... never wait[s] —
and never opens the DB"), i.e. today the shared PM-US budget is advisory for
exactly the calls most likely to burst. *Test:* replay burst load → per-
strategy background request counts within share; critical (hedge) latency
unaffected.

---

## 7. Migration table (v2)

Target runtimes per strategy. "Runs external-intents first" means the
research/current Python code keeps running as a gateway sidecar until the
family template covers it; promotion is then a config flip.

| strategy_id | family | target_runtime | today | gate / notes |
|---|---|---|---|---|
| `make-take` | `maker-hedge` | `rust-template(maker-hedge)` | python-daemon | first family instantiation; per-relationship cutover via claims; family parity gate replays the runner's intent stream. Re-verify quoter knobs against `main` (toxgate, clip 25) first |
| `take-take` | `take-take-cross` | `rust-template(take-take-cross)` | python-daemon (in-runner) | v2 change: was "stays-python sidecar"; now a family parameterization — the engine executes, the weekly-churn policy lives in `params` (data, not code) |
| `auto-take-take` | `retires` | `retires` | python-manual | superseded by the take-take-cross family entry; its independently-declared constants are the drift hazard |
| `sports-take-take` | `take-take-cross` | `rust-template(take-take-cross)` | python-daemon | same template as take-take, different `universe`/`edge_model` params; equiv-map discovery stays a Python research feed |
| `pm-lean` | `directional-signal` | `rust-template(directional-signal)` | python-daemon | rule-signal parameterization (`model_ref: null`); lifetime-capped |
| `sports-mt-probe` | `external-intents` | `external-intents` | python-daemon | research probe; fits `maker-hedge` shape and promotes into it only if it graduates |
| `sports-rehedge` | `external-intents` | `external-intents` | python-daemon | lifecycle; later-phase gateway submitter |
| `pmus-maker-probe` | `maker-hedge` | `rust-template(maker-hedge)` | python-daemon | runs external-intents while research; template covers it via `anchor: reference-mid`, `hedge: none-inventory` |
| `toxicity-probe` | `external-intents` | `external-intents` | python-daemon | ML gate is the strategy and churns with the model — stays a sidecar under enforced caps |
| `leadlag-probe` | `directional-signal` | `rust-template(directional-signal)` | python-daemon | external-intents until the template evaluates the exported LightGBM artifact; then a config flip |
| `naked-hedge` | `external-intents` | `external-intents` (later-phase) | python-timer | gets its own `naked-hedge` ledger tag at migration, ending the `take-take` collision; detection input stays the diverse auditor |
| `unwind` | `external-intents` | `external-intents` (later-phase) | python-timer | latency-irrelevant, densest venue-glitch knowledge; submits closes via gateway later-phase |
| `settlement` | `external-intents` | `external-intents` (later-phase) | python-timer | writes closes only today |
| `flatten-probe-residual` | `manual-tool` | `manual-tool` | python-manual (done) | completed one-off; entry kept for ledger mapping |
| `execute-xv` | `manual-tool` | `manual-tool` | python-manual | later-phase: submits through the gateway; gains a `strategy` key at migration |
| `make-take-manual` | `manual-tool` | `manual-tool` | python-manual | later-phase gateway submitter |
| `kalshi-cancel-all` | `manual-tool` | `manual-tool` | python-manual | end state: replaced by gateway `cancel-all-owned` — the blanket-cancel class ceases to exist; until then the hazard stands (board card) |
| `rehearse-live` | `manual-tool` | `manual-tool` | python-manual | auth rehearsal; may stay direct-to-venue by design (it tests credentials, not strategy) |
| `reconcile-audit` (reconcile_positions + mark_positions) | `audit-readonly` | `audit-readonly` | python-timer | **permanent** — diverse-auditor rule: must NOT share the engine's code or event stream, so an engine bug cannot blind its own audit. Naked-leg *detection* stays here even after hedge *placement* moves |

---

## 8. Census gaps this contract surfaces (board cards, not fixed here)

- `data/KILL` not checked by `sports_arb.py` (own circuit breaker only) — also
  not checked by the order-placing timers `unwind_positions.py` /
  `hedge_naked_legs.py` / `settle_baskets.py`.
- `kalshi_cancel_all.py` blanket cancel over the shared account.
- `ownership` table inert (`claim_ownership` zero callers) — adopt when
  convenient; the manifest `claims` field is its future compile source.
- `hedge_naked_legs.py` writes `strategy: "take-take"` (tag collision).
- `execute_xv.py` writes ledger records with no `strategy` key at all.
- PM-US budget `priority="critical"` bypass (`venues/pmus.py`).
