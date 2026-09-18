Trading-policy audit — 2026-09-18

Scope: the active Rust trader, maker quoting, taker admission, capital gates, and maker exits. Read-only review of production; no trading rules changed by this audit. Evidence includes recent journal/intent logs, the 08:42:55 UTC marks snapshot, public Kalshi market metadata, and isolated probes calling the actual `arb-core` quoter. Repeated log refusals are repeated decisions, not distinct missed trades or lost profit.

1. **Urgent, reproduced: the requote throttle can retain a money-losing quote.**

   `rust/crates/arb-core/src/quoter.rs:788` skips repricing for 15 seconds when a new target exists. It does not first check whether the existing price remains profitable against the new hedge book. The immediate-cancel path only runs when the new target is absent.

   Probe with an 18% APR hurdle: at t=1000 we bid 0.31 on PM-US, hedged against Kalshi's 0.60 bid. At t=1001 the competing PM-US bid drops to 0.10; at t=1002 Kalshi's hedge bid drops to 0.30. A new 0.11 quote is permissible, so `target` remains Some and the throttle retains our 0.31 bid. A fill now loses at least 0.01/contract before fees. Nothing reprices until t=1016. This is a deterministic defect, not a recommendation to quote more aggressively. Live incidence was not measured.

   Fix direction: assess the existing quote against the current hedge and APR constraint before applying traffic throttles. Cancel or reduce unsafe exposure immediately; throttle only optional improvements and re-entry. The risk-refused reprice path also deserves this same separation.

2. **High priority, reproduced: whole-cent pricing disables valid sub-cent markets.**

   `quoter.rs:448` hardcodes 0.01, and lines 488/518 round bids/asks to whole cents. Public metadata for `KXRATECUT-26DEC31` returned `deci_cent`, a 0.0010 grid, bid 0.0410, ask 0.0470. The market is active. With that spread the maker's bid cap is ask minus 0.01 = 0.037, below the 0.041 competitor; the ask floor is bid plus 0.01 = 0.051, above the 0.047 competitor. Both sides are declined irrespective of an otherwise adequate hedge edge.

   A probe with a sufficiently profitable synthetic hedge places a Kalshi bid with ask 0.060 but stops placing it when only the ask narrows to 0.047. The narrower market still has legal passive prices. This is a concrete mechanism for the Fed-cut quoting drought, though it does not establish when every historical missing quote became unavailable.

   Fix direction: carry per-market `price_ranges` through quote construction and rounding, including exits and hedge limits. Round on the actual grid and revalidate passivity afterward. Kalshi explicitly says these ranges are the source of truth: https://docs.kalshi.com/getting_started/fixed_point_migration . Do not replace 0.01 with a different universal constant.

3. **High priority, code-confirmed exposure gap: taker entries do not reserve capital while in flight.**

   `rust/bins/arb-trader/src/engine/mod.rs:2121` calls the risk gate with `rests_on: None`. `risk.rs` reserves only when a resting slot is supplied. Exposure increases only after a fill is booked. The taker cooldown is per relationship, so different relationships can each pass against the same remaining topic/global/cash headroom before any fill updates it. The source comments acknowledge this window. No recent overrun was established by this audit.

   Fix direction: separate transient IOC commitments from maker reservations; reserve at admission and release/convert on terminal execution outcomes. Do this before increasing turnover or relaxing caps.

4. **Material policy bottleneck: exits require 2 cents of realized profit over historical cost.**

   `rust/bins/arb-trader/src/unwind.rs:317` and `maker_exit.rs:368` impose a 0.02/contract floor. At the observed snapshot, 29 non-settled lots / 149 contracts / $141.48 historical cost had forward APR below 18.02% and a modeled positive maker exit below 0.02. They are excluded by that floor. Seven Retailleau lots alone total 84 contracts: modeled exit about 0.0097/contract, forward APR about 5.2–5.6%.

   These are modeled passive exits, not executable guarantees, and some markets may have independent venue restrictions. The economically relevant decision is whether net exit proceeds can be redeployed better than holding, accounting for fees, adverse selection, hedge uncertainty, and concentration. A fixed realized-profit target is not equivalent to that comparison. Its historical justification includes stale marks and missing fee inputs that should be addressed directly.

   Fix direction: separate execution-risk allowance from historical profit; evaluate hold versus a feasible replacement opportunity. Shadow-measure any lower floor before enabling it. Simply dropping the floor while leaving uncertain fees/hedges untouched is not justified.

5. **Material throughput bottleneck: one five-contract exit can monopolize the whole portfolio for an hour.**

   `maker_exit.rs:340`, `:377`, and `:400` set max clip 5, one resting exit, and a 3600-second rotation threshold. Recent logs show one Fed-cut exit resting while 14 candidates are admitted. The rationale bounds simultaneous unhedged exit risk, but it also couples independent markets. Maker entries can already quote clips up to 25 on multiple markets, so the entry/exit concurrency policies are notably asymmetric.

   There is also incomplete prioritization: `unwind.rs:674` groups potentially crossable exits first, then sorts by whole-lot quantity and forward APR; actual executions are capped at five. `Live::target` preserves the incumbent until rotation, even when a newly available candidate could convert immediately. A large original lot is not necessarily the best use of one five-contract slot.

   Fix direction: first allow a demonstrably better executable candidate to preempt a resting incumbent, with a verified cancel before switching. Then consider a bounded per-market exit set backed by a portfolio-wide limit on unhedged exposure and independent pending-close state. Raising `MAX_RESTING` alone is insufficient.

6. **Binding allocation policy: static topic budgets reject opportunities regardless of relative return.**

   The recent 48-hour intent scan contained approximately 85,060 Nobel topic refusals, 22,671 TIME refusals, and 2,072 Fed-cut refusals; another 78,875 refusals were full per-relationship caps. The latter had zero remaining room, so smaller clips would not fix them. Nobel's 189 held contracts plus 7 reserved explain its 196/196 gate. TIME is at 185/185; Fed-cut is at 85 against a 68 budget.

   Journal examples include taker candidates at 89–347% annualized rejected by the Nobel cap. Those APRs are annualized signal estimates, not large dollar profits or evidence concentration limits should be bypassed. Nevertheless, this demonstrates that the APR hurdle is only a filter: it does not allocate capital to the best available opportunity. The generic overflow mechanism exists, but production deliberately passes no opportunity APR to it.

   `config/topics.yaml` is based on an August 19 snapshot. Its explanation that exits free budget only after restart is obsolete: the current trader releases closed exposure on a timer. The BTC issue is narrower than an overall naming mismatch: `btcmax-26` matches the standard rungs, but vetted `xvus-btc150k-ladder-*` relationships fall into `other` and inherit its utilization<0.5 gate instead of the BTC budget.

   Fix direction: validate every eligible relationship's assigned topic, surface current deployed+reserved headroom, and rank feasible replacements within explicit concentration limits. Reallocation is a policy change; do not enable the generic overflow path blindly, since its topic override is not itself a bounded replacement transaction.

7. **Additional conservative layers are not calibrated independently.**

   `taketake.rs:50` floors modeled two-leg fees at 0.02/contract, then `detect` also demands net edge strictly above the 0.01 hedge-slippage allowance and the APR threshold. Under the code's fee schedule, a five-contract 0.04/0.07 crossing costs about 0.007906/contract in fees: its modeled true net is about 0.022094, but the fee floor reports 0.01 and the slip gate rejects it. The floor compensates partly for missing per-market fee metadata, so this example is not proof of achievable live profit. The trader still uses fallback fee coefficients.

   Maker sizing additionally refuses clips below three and shrinks for topic headroom only; other scarce resources can leave a feasible smaller clip unused. No positive tail-cap headroom was observed in the recent rejection sample, so that latter issue is latent rather than the current main blocker.

   Fix direction: ingest authoritative fee/tick metadata, record the individual rejection reasons and foregone candidate economics, and measure reserves against realized hedge costs. Tune each allowance for its own risk rather than stacking fixed cushions without attribution.

Recommended order: fix unsafe-quote throttling; implement market price grids; reserve in-flight taker exposure; then improve exit scheduling and review exit floors/topic allocation using shadow comparisons. Keep settlement-equivalence checks, stale-feed protection, and guaranteed hedge completion as constraints.

Probe artifacts (outside the working tree): `/tmp/arbbot-policy-probe/src/main.rs`, `result.txt`, `src/bin/tick.rs`, `tick-result.txt`, and `fedcut-market.json`. Run with `cargo run --offline --manifest-path /tmp/arbbot-policy-probe/Cargo.toml --bin arbbot-policy-probe` or `--bin tick`. The probes place no real orders. Findings are scoped to the reviewed paths; the audit does not establish expected fill rates or counterfactual realized P&L.

Implementation follow-up — 2026-09-18

Implemented after the user's request to fix the urgent findings and improve exits:

- Existing maker prices are checked against the current hedge and APR before requote throttling or replacement admission. Unsafe bids and asks cancel immediately.
- Live Kalshi maker quotes use each market's published price ranges. Missing/invalid metadata withholds that market's quotes; penny-only replay behavior is preserved for historical tapes without metadata events.
- Armed taker entries reserve an independent order-id slot. Partial fills convert reservations to exposure. Terminal order reads credit any missing cumulative fills and release only the unused remainder. Rejections and undispatched orders release immediately; uncertain outcomes keep their reservation. Dry runs do not acquire IOC commitments.
- Independent exit workers run per Kalshi market, aggregate their entry suppression and backstop stand-off, and also exclude simultaneous owners of a shared PM-US market. Existing resting orders and pending closes remain managed if their marks disappear. A healthy worker cannot hide stale ownership from another worker.
- Passive selection allows lots whose current touch would not meet the profit floor. Orders can rest behind the touch at a valid fee-aware price. Live pricing now requires 0.005/contract net after lot basis, modeled fees and the existing one-cent hedge slippage allowance, rather than a separate two-cent realized-profit target. The diagnostic unwind report retains its old two-cent screen; it is no longer the passive admission list.
- Resting exit maintenance evaluates the actual price directly instead of rounding its economic bound to a penny.
- Cancellation reconciles terminal fills before forgetting an exit. Unconfirmed partial cancellations retain the order for another reconciliation instead of dropping possible further fills. A fill racing an otherwise unfilled pull is closed and booked.

The existing five-contract clip, one order per market, forward-holding-APR test, candidate persistence, hedge freshness checks and unresolved-exposure pause remain. This does not rest every contract or every lot on the same market simultaneously. Remaining allocation/fee-calibration findings above are not represented as fixed. Exit execution still depends on hedge liquidity; the net buffer is a modeled bound, not a guarantee of realized profit.

Validation covers unsafe quote cancellation on both sides, sub-cent placement and metadata loss, IOC partial/terminal/duplicate fills and rejection uncertainty, independent exit ownership and shared-market exclusion, stale-worker detection, off-touch admission, sub-cent exit maintenance, and cancel/fill races. The full Rust workspace suite passed outside the socket-restricted sandbox; the final affected-crate tests and strict Clippy checks also passed.

Terminal PM-US status handling follows the [official order-state documentation](https://docs.polymarket.us/streaming-endpoints/order-stream); unknown states or unreadable cumulative quantities do not release commitments.

Deployment check: release build completed; `arbbot-trader-m3` restart was attempted at approximately 09:16 UTC (05:16 EDT). PM-US was already returning HTTP 503 for order-status/open-order reads before this restart. Its shutdown sweep could not prove the old orders cancelled, and subsequent startup sweeps also returned 503, so the new process refused to arm. The existing systemd on-failure policy retried after 30 seconds, then exhausted its three-starts-per-ten-minutes limit. The service is now failed/stopped and needs another start after the venue recovers; no automatic retry remains scheduled. Kalshi cleanup succeeded. Live multi-market resting-order verification remains blocked by the PM-US response; code/test completion is not evidence that these exits have been observed at the venues. Final full-workspace run: 1,136 tests passed. Strict Clippy on arb-core, arb-trader and arb-venue passed.

### Follow-up: full inventory exits within each market

The five-contract clip and per-market order cap have now been removed. Production discovers one owner per `(relationship_id, opened_ts)` and rests the minimum remaining quantity across that lot's two ledger legs and its marks quantity. Multiple lots on one market may rest simultaneously at their own prices. Same-price orders remain separate so that fill attribution stays exact; this version does not coalesce lots into an aggregate order.

The execution path is `maker_exit/lot.rs`. It requires terminal order receipts before retrying a hedge or releasing inventory. It never attributes account position changes to an individual lot. A partial resting fill cancels and confirms the remainder before hedging; only confirmed hedged quantities are booked against that lot. Entry suppression and reconciliation stand-off are unions across all lot owners, including pending hedges.

Checkpoints alongside the ledger (`<ledger>.maker-exits/*.json`) are written and synced before submission, after acknowledgements, and around booking. After the existing startup cancel sweep, saved orders are reconciled before fresh exits are allowed. Stable booking timestamps make a crash after ledger append but before checkpoint completion idempotent. Do not delete checkpoints while orders or hedges remain unresolved.

PM-US does not carry our client order IDs. Lost PM-US placement acknowledgements remain reserved and require order-identity reconciliation; the old market/quantity heuristic is unsafe with sibling orders and is not used. Known order IDs recover automatically when their status endpoint returns. Unknown or nonterminal fills never become assumed zero fills. This is deliberately distinct from ordinary venue outage recovery.

Forward APR selection, candidate persistence, fresh hedge prices, and modeled fee/profit bounds remain. Full inventory resting increases potential simultaneous hedge demand; execution still depends on venue liquidity. Tests cover same-market concurrent lots, both exit shapes, cancel races, partial and delayed hedge fills, crash replay, startup sweep fills, stale views, and lost acknowledgements.

Live verification also exposed newly accepted orders briefly returning 404 on both venues' terminal-status reads, and concurrent hedge submissions hitting Kalshi HTTP 429. Terminal reads now use the gateways' existing bounded 404 retry policy. Lot transactions share a serialized, paced execution turn (500 ms after an active-order pass or submission); there is still no cap on resting-order count. Idle candidate scans do not incur that delay. The first live batch's 18 crossed exits all completed through receipt-based recovery, with 17 passive orders remaining before deployment of the pacing improvement.

### Follow-up: high holding APR sets the passive price, not eligibility

Passive selection no longer excludes a lot because its current forward holding APR exceeds the hurdle. Immediate-exit diagnostic selection retains that comparison. Passive pricing uses the resolution horizon to require net liquidation value of at least `1 / (1 + hurdle_fraction * years_remaining)` per standard $1-payoff basket, as well as the existing ledger-basis-plus-half-cent profit floor. This is an opportunity-cost model using the current redeployment hurdle, not a guarantee of that future return. Both passive shapes and optional immediate crossing must meet the reservation price; resting-order maintenance recomputes it as time and the hurdle change. Confirmed fills still use the existing hedge completion policy to avoid stranding a leg.

The resolution date is persisted on new orders. Older checkpoints remain readable and are reconciled through the startup sweep before replacements receive the new pricing policy. Freshness, ownership, settlement, supported basket direction, executable hedge prices, and unresolved-order checks remain. The current snapshot has 56 newly admitted lots (382 contracts); admission does not guarantee a valid venue price for both legs.

### Identical-price exit orders consolidated

The earlier per-lot wire orders are now grouped when both venue markets, direction,
and exact limit price match. Prices are neither averaged nor rounded for grouping.
Each group's durable checkpoint retains original lot quantities and cost bases,
ordered by opening time. Confirmed fills consume the oldest inventory first;
hedge retries continue from the already-booked quantity. Per-allocation booking
keys are persisted before the hedge request, making a restart between allocation
records idempotent. Old single-lot checkpoints remain readable.

A batch scheduler prices available lots, shares one Kalshi quote read per market
per pass, and submits one summed quantity per price group. Existing groups retain
queue position unless they need repricing or new inventory joins the same price:
joining first cancels and reconciles the incumbent, then replans from the ledger
on the next pass. Unknown cancel/fill outcomes retain every member reservation.
Crossed IOC exits remain separate. Group maintenance and hedge limits evaluate
individual lot bases; the shared hedge uses the strictest member limit.

Regression coverage includes exact-price grouping and FIFO ordering, partial fills
across lot boundaries, partially filled hedge recovery, a crash between allocation
ledger appends, strict hedge limits, and cancellation races during regrouping.

Validation: 621 trader unit tests and 10 integration tests passed; Clippy passed
with warnings denied. Release rebuilt and `arbbot-trader-m3` restarted at
2026-09-18 13:07:39 EDT (PID 2584976). After placement and a management pass,
28 acknowledged resting orders covered 76 lots / 443 contracts, versus 77 orders
/ 446 contracts before restart; the other 3 contracts exited and were booked.
There were no duplicate quote groups, duplicate lot reservations, or outstanding
exit hedges. Different quotes remained separate; 13 orders contained multiple
lots, with up to 15 lots in one order.
