# Venue Quirk Registry

Port spec + institutional memory. Every surprising venue-API behavior we have
paid to learn is recorded here with the failure it caused, where the current
Python handles it, and what ANY reimplementation (the Rust gateways in
particular) MUST reproduce. File:line references are against this tree.

**Rule: any new venue workaround merged into the codebase MUST get an entry
here in the same change.**

Venue values: `kalshi`, `polymarket_us` (QCX DCM), `polymarket_intl`
(international CLOB, data-only), `both`.

---

## Kalshi — order API

### `kalshi-create-endpoint-410`
- **Venue:** kalshi
- **What the API does:** Order CREATE moved to `POST /portfolio/events/orders`; the legacy `POST /portfolio/orders` now returns 410. GET/list still uses `/portfolio/orders`; cancel is `DELETE /portfolio/events/orders/{id}`.
- **Failure it prevents:** every live order placement 410s if the legacy path is used.
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:62-65` (create), `:76-77` (cancel), `:96-97`/`:112-113` (list/get stay on the legacy path).
- **Port requirement:** Route create/cancel to `/portfolio/events/orders`, list/get to `/portfolio/orders`; never assume one base path for the whole order API.

### `kalshi-orders-list-history-paginated`
- **Venue:** kalshi
- **What the API does:** `GET /portfolio/orders` returns full HISTORY (canceled/executed included), paginated (~100/page + cursor). `?status=resting` returns nothing.
- **Failure it caused:** missing pagination orphaned older resting orders during sweeps (the 2x Melenchon/Koch doubles — naked-fill risk).
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:85-104` (page all, hard cap 50 pages), `:119-129` (filter `status=="resting"` client-side before cancel-all).
- **Port requirement:** Always page to cursor exhaustion and filter status client-side; a one-page read of open orders is a naked-fill bug.

### `kalshi-order-visibility-lags-a-create`
- **Venue:** kalshi
- **What the API does:** A just-accepted order is not immediately visible to the query service. `GET /portfolio/orders/{id}` 404s for a beat — **observed live 2026-07-27, and that date attaches to this read only**. The paginated `GET /portfolio/orders` list is believed to lag further still; that half has **no capture behind it** and is inherited from a code comment (`Settle`, `gateway/mod.rs`), so it is recorded here as the conservative assumption it is, not as an observation.
- **Failure it prevents:** reading absence as a fact. On the recovery path for a place whose RESPONSE was lost — the 15s HTTP timeout, or a 2xx body that will not parse — "not on the account" would mean the venue never took it, and the order then rests under an id this process never learned: no per-order cancel can address it, and a fill on it arrives unattributable.
- **The likelier cause of an absence on this account is not the lag.** Nothing in this repo has ever demonstrated that the order LIST echoes `client_order_id` at all (see `Book::echoes_tags`), and a list that does not echo it answers "absent" to every lookup by our own id, for ever. That case is checked separately and reported as itself — see `kalshi-orders-list-history-paginated`.
- **Current handling:** `rust/crates/arb-venue/src/gateway/mod.rs` (`Settle::retry_404` polls the single-order GET rather than sleeping once, and returns the last 404 rather than a synthesised "missing order"); `rust/crates/arb-venue/src/gateway/kalshi.rs` (`cancel` and `recover_place` both treat `Ours::Absent` as an ERROR — never as a successful cancel, never as "nothing to recover" — and `find_ours` refuses with the tag-echo premise instead when that is what actually failed).
- **Port requirement:** Never let a not-found from either read stand as proof an order does not exist. Poll the single-order GET after a create, and on the lost-response path report absence as UNSETTLED — the safe default is that the venue has the order. Establish whether the LIST echoes the tag before trusting any lookup built on it.

### `kalshi-signature-path-only`
- **Venue:** kalshi
- **What the API does:** The RSA-PSS request signature covers `timestamp_ms + METHOD + path` ONLY — the query string is excluded and rides on the URL.
- **Failure it prevents:** signing the query string yields 401s on any paginated/parameterized call.
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:93-97` (sign path, query on URL), `src/arbbot/record/kalshi.py:135-153` (`sign_headers`).
- **Port requirement:** Sign the bare path; never include `?...` in the signed message.

### `kalshi-fill-count-fp-plain-count`
- **Venue:** kalshi
- **What the API does:** `fill_count_fp` (and `position_fp`, `count`) are fixed-point STRINGS that are plain contract counts (`"3.00"` == 3 contracts), not scaled fixed-point.
- **Failure it prevents:** treating `_fp` as scaled would mis-size every hedge by 100x.
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:106-117` (`int(float(...))`), positions reads `src/arbbot/exec/main.py:353-356`, `scripts/reconcile_positions.py:46-53`.
- **Port requirement:** Parse `*_fp` fields as decimal strings equal to the human contract count; send `count`/`price` as fixed-point strings (`"5.00"`, `"0.0520"`).

### `kalshi-cancel-404-is-success`
- **Venue:** kalshi
- **What the API does:** DELETE on an already-gone order returns 404. Other non-2xx are real failures.
- **Failure it prevents:** swallowing a real cancel failure orphans a live order (untracked, unhedgeable); treating 404 as failure blocks reprice loops.
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:78-82` (404 == success; other >=300 raises so the quoter keeps the order tracked).
- **Port requirement:** Cancel must treat 404 as success and RAISE on anything else — a failed cancel must keep the order tracked.

### `kalshi-postonly-cross-rejected-400`
- **Venue:** kalshi
- **What the API does:** A `post_only` order that touches/crosses the opposite touch is rejected with 400 (`order_would_cross`).
- **Failure it prevents:** on a 1-tick book, naive "one tick better" quoting rejects every place; unthrottled retries spam the venue (429s).
- **Current handling:** `src/arbbot/exec/quoter.py:158-163` (bid clamped strictly below ask), `:176-181` (ask strictly above bid), `:232-235`+`:253-264` (place-failure backoff so a rejected side doesn't retry every tick).
- **Port requirement:** Clamp maker prices strictly inside the opposite touch and back off a side whose placement failed; a 400 is expected traffic, not a crash.

### `kalshi-v2-yes-axis`
- **Venue:** kalshi
- **What the API does:** V2 order API is entirely YES-price-axis: `side='ask'` sells YES at `price` (== buy NO at 1-price) natively — no NO-complement math; body requires `time_in_force`, `self_trade_prevention_type`, `client_order_id`.
- **Failure it prevents:** double-complementing prices on the ask side places orders at wildly wrong prices.
- **Current handling:** `src/arbbot/exec/kalshi_gateway.py:45-56` (+ module docstring `:7-16`).
- **Port requirement:** Keep all order prices on the YES axis end-to-end; the venue does the NO conversion.

### `kalshi-deci-cent-ticks`
- **Venue:** kalshi
- **What the API does:** Most markets tick in whole cents, but some take 0.001 prices; the tick comes from the market's `price_ranges[].step`. Penny markets 400 anything finer; sub-tick prices get rounded by the venue.
- **Failure it caused:** a venue-rounded price stops matching our tracked price, breaking self-recognition in the book (quote walks toward the mid until picked off).
- **Current handling:** `scripts/hedge_naked_legs.py:94-105` (step from `price_ranges`), `src/arbbot/exec/quoter.py:78-85` (venue-reported tick), `:152-154`/`:172-175` (quantize to tick before sending).
- **Port requirement:** Fetch the per-market tick and quantize every outgoing price to it; never hardcode $0.01.

### `kalshi-positions-and-settlement-fields`
- **Venue:** kalshi
- **What the API does:** `/portfolio/positions` returns `market_positions[]` with `position_fp` (signed contract count), `market_exposure_dollars`, `fees_paid_dollars`. Settlement is visible via market `status == "finalized"` + `result in ("yes","no")`.
- **Failure it prevents:** settling/marking off any other field or status string realizes phantom PnL.
- **Current handling:** `scripts/reconcile_positions.py:41-53`, `scripts/settle_baskets.py:47-49` (finalized+result gate), `scripts/hedge_naked_legs.py:52-57`.
- **Port requirement:** Gate settlement on `status=="finalized" AND result in {yes,no}`; read positions from `position_fp`.

## Kalshi — accounting / cash

All six entries below were paid for on 2026-07-27 while building the
double-entry books. Together they make the Kalshi account reconcile to
**exactly $0.000000** residual:
`deposits − buys@yes_price + sells@yes_price − fees + settlement_revenue
− (net_short_contracts × $1.00) = balance_dollars`.

### `kalshi-fills-are-the-fee-authority`
- **Venue:** kalshi
- **What the API does:** `GET /portfolio/fills` (paginated, cursor) returns every fill with `fill_id`, `order_id`, `count_fp`, `is_taker`, `yes_price_dollars`, `no_price_dollars`, and `fee_cost` — **the fee actually charged**, which is the per-order ceil-to-cent of the curve, not the raw curve value.
- **Failure it caused:** nothing in the repo had ever read this endpoint. The ledger stored the raw un-ceiled `fees/curves.py` value, so every Kalshi fee was systematically light — **$11.51 actually charged vs $3.52 booked** across 217 fills.
- **Current handling:** none in Python; this endpoint is unused. The Rust ledger importer is the first consumer.
- **Port requirement:** Book `fee_cost` from the fill as the expense; compute the curve value only as a drift check. Never book a modelled fee when the venue reports the charged one.

### `kalshi-sell-no-is-closing-long-yes`
- **Venue:** kalshi
- **What the API does:** Fills come back only as `action=buy, side=yes` or `action=sell, side=no`. A `sell no` fill is how Kalshi represents **closing a long YES**, and its cash proceeds are `count_fp × yes_price_dollars` — NOT `no_price_dollars`. Position is `Σ buy_yes − Σ sell_no`, which matches `position_fp` on every market including net-short ones.
- **Failure it caused:** pricing `sell no` at `no_price_dollars` put the cash reconciliation **$156.67** off.
- **Current handling:** none — no Python code reconstructs position or cash from fills.
- **Port requirement:** Price both fill directions off the YES axis. Verify against `position_fp` per market; a mismatch means the sign convention is wrong.

### `kalshi-short-collateral-one-dollar-per-contract`
- **Venue:** kalshi
- **What the API does:** A net-short YES position encumbers **exactly $1.00 per contract** of `balance_dollars`. The balance endpoint does not itemize it.
- **Failure it caused:** the last **$15.00** of an otherwise-closed reconciliation — six markets ending net-short 15 contracts total.
- **Current handling:** none.
- **Port requirement:** Cash reconciliation must subtract `Σ max(0, −position) × $1.00`. See the identical PM-US mechanic in `pmus-margin-is-one-dollar-per-short`.

### `kalshi-settlement-fee-restates-fill-fees`
- **Venue:** kalshi
- **What the API does:** `GET /portfolio/settlements` returns `fee_cost` per settled market, and that value **restates the sum of that market's trading fees** — it is NOT an additional settlement charge. Verified 28/28 settled markets match their fill-fee sum exactly.
- **Failure it prevents:** summing fill fees *and* settlement fees injects phantom expense — **$5.52** on the current history.
- **Current handling:** none.
- **Port requirement:** Book fees from fills only. Treat `settlement.fee_cost` as a cross-check, never as an expense posting.

### `kalshi-settlement-revenue-is-net-cents`
- **Venue:** kalshi
- **What the API does:** Settlement `revenue` is in **cents** and reflects the **net** position payout: Kalshi nets YES against NO within a market, so 15 YES + 5 NO with `market_result=yes` pays `revenue=1000` ($10), not $15. `yes_count_fp`/`no_count_fp`/`*_total_cost_dollars` carry the per-side detail; `value` is a different, smaller field — do not use it as the payout.
- **Failure it prevents:** paying out gross rather than net overstates settlement cash.
- **Port requirement:** Credit `revenue/100`; derive realized P&L against the per-side cost fields.

### `kalshi-deposits-endpoint-exists`
- **Venue:** kalshi
- **What the API does:** `GET /portfolio/deposits` and `/portfolio/withdrawals` exist and return funding history (`amount_cents`, `status`, `type`, `created_ts`). `/portfolio/transfers` and `/portfolio/transactions` are 404.
- **Failure it prevents:** without these the cash accounts have no legitimate source and the books open with an unexplained plug.
- **Port requirement:** Seed `equity:capital` from these endpoints rather than asking a human. Contrast `pmus-no-history-endpoints`, where you must.

## Kalshi — market data (WS/REST)

### `kalshi-both-ladders-are-bids`
- **Venue:** kalshi
- **What the API does:** `orderbook_fp.yes_dollars` and `.no_dollars` are BOTH bid ladders. A NO bid at q is a YES ask at 1-q (same size). There is no native ask ladder.
- **Failure it prevents:** reading `no_dollars` as asks inverts half the book — every edge computation is wrong.
- **Current handling:** `src/arbbot/record/kalshi.py:8-16` (convention doc), `:67-89` (`normalize_orderbook`), `:183-193` (WS delta mapping).
- **Port requirement:** Normalize to a YES-denominated book at ingestion: NO-bid(q, sz) -> YES-ask(1-q, sz), everywhere.

### `kalshi-ws-delta-is-change-not-total`
- **Venue:** kalshi
- **What the API does:** WS `orderbook_delta.delta_fp` is a CHANGE in quantity at a level, not the new total (unlike PM's `price_change`).
- **Failure it prevents:** treating the change as a total corrupts every book silently — "the change-vs-total quirk is where a naive port loses money."
- **Current handling:** `src/arbbot/record/kalshi.py:167-177` (docstring), `:209-221` (running per-(ticker,side,price) sizes; emits resulting totals).
- **Port requirement:** Maintain running level sizes and emit resulting totals; clamp negatives to zero and drop the level.

### `kalshi-ws-seq-per-subscription`
- **Venue:** kalshi
- **What the API does:** The wire `seq` is per-SUBSCRIPTION (`sid`), incrementing across all markets in the subscription — not per market.
- **Failure it caused:** feeding the wire seq into a per-market gap detector makes every interleaved delta look like a gap and kills book state (live incident 2026-07-20 11:45 — scanner went silent).
- **Current handling:** `src/arbbot/record/recorder.py:258-264` (remap to synthesized per-market seq), `:284-290` (gap detection stays per-sid), `scripts/sports_arb.py:449-456` (same remap).
- **Port requirement:** Track gaps per-sid on the wire seq; assign your own per-market monotonic seq for downstream book building.

### `kalshi-trade-needs-own-seq-stream`
- **Venue:** kalshi
- **What the API does:** Trades arrive on the same WS with their own sid/seq space, interleaved with book events.
- **Failure it caused:** sharing the book's per-market counter with trades gapped (and permanently killed) the book on every first trade — the traded-markets-die P1 (2026-07-20).
- **Current handling:** `src/arbbot/record/recorder.py:306-311` (trades sequence on `ticker + "|tape"`).
- **Port requirement:** Keep trade and book sequence domains disjoint; a trade must never advance a book's expected seq.

### `kalshi-ws-snapshots-only-on-subscribe`
- **Venue:** kalshi
- **What the API does:** WS sends `orderbook_snapshot` only on subscribe and on explicit `get_snapshot` request — and in practice the WS `get_snapshot` path produced ZERO snapshots (adversarial review, 2026-07-20). The auth-free REST orderbook endpoint works.
- **Failure it caused:** a late/regapped subscriber waits forever for a snapshot (scanner-stuck-book bug, 2026-07-20).
- **Current handling:** `src/arbbot/record/recorder.py:82-85` (welcome snapshots on connect), `:113-121` (30s rebroadcast heal), `:266-271` (periodic 300s re-snapshot request), `:298-305` (gap -> REST snapshot resync, not WS). The Rust recorder — the production stack since 2026-07-28 — sends NO `get_snapshot` at any point: `rust/bins/arb-recorder/src/kalshi.rs` sweeps every book by REST on a 300s cycle (`resnap_slice`) and rebuilds a market whose snapshot was lost by REST from `on_ws_message`. The request was deleted in #32; a call the venue never answers was the only thing the gap branch did, which made a no-op look like a recovery.
- **Port requirement:** On gap, resync via REST snapshot; push synthetic snapshots to (re)connecting subscribers; do not rely on the venue re-sending snapshots.

### `kalshi-market-data-auth-split`
- **Venue:** kalshi
- **What the API does:** REST catalog/orderbook needs NO auth; the market-data WS REQUIRES RSA-PSS auth on the upgrade handshake (verified live 2026-07-20). Catalog `?tickers=` is batched at 50 per request.
- **Failure it prevents:** WS without auth never connects; unbatched catalog reads 4xx on large universes.
- **Current handling:** `src/arbbot/record/kalshi.py:1-16`, `:106-129` (auth-free REST), `:115-118` (batch 50), `src/arbbot/record/main.py:99-110` (WS only when the READ-ONLY recorder key exists — trade key never enters the recorder process).
- **Port requirement:** Support both paths (authed WS, credential-free REST poll) and keep the trade-capable key out of the data-plane process.

### `kalshi-fill-channel-deltas-not-cumulative`
- **Venue:** kalshi
- **What the API does:** The private `fill` WS channel pushes per-fill DELTAS (`count_fp`), not a cumulative total, keyed by Kalshi's own `order_id` with a `trade_id` per fill. Contrast PM-US's private order channel, which sends the venue's own cumulative `cumQuantity`. **Whether this channel REPLAYS the fills missed during a WS gap on resubscribe is still NOT established** — no probe, fixture or vendor doc; the only claim is prose in commit `2a703e9`, and the one adjacent documented behaviour points the other way (`kalshi-ws-snapshots-only-on-subscribe`).
- **Failure it causes:** a consumer must sum the deltas locally, so its total is what the process RECEIVED. A frame lost in a gap leaves contracts filled at the venue and unhedged, and no gauge sees it: no frame arrives, so nothing is unattributed and no obligation is minted.
- **What makes it reconcilable anyway:** `GET /portfolio/fills` returns the same `trade_id` per fill (`fill_id` equals it on every row of the live dump). So venue truth can be MERGED into the local dedupe set rather than overwriting the total — a fill the socket missed is new and counts, a fill it delivered is a duplicate and does not. That is safe whether or not the channel replays, which is why the replay question no longer gates the repair.
- **The assumption that remains, and how it is contained:** no raw WS fill frame has ever been captured, so "WS `trade_id` and REST `trade_id` are the same id space" rests on them being the same field name from the same account. If they are not, every venue row looks new and a partially-filled order gets its contracts hedged twice. It is checkable for free: the reconciliation window starts at process start, so the venue's rows for it are a superset of what the socket delivered *if* the ids share a space — project the merge (deduping fill ids within the page set first, or a row the venue repeats across pages loosens the check by its own size) and refuse any order where it would exceed the venue's own total. Under a mismatch an order the process has already counted a fill on is then refused rather than double-hedged; an order it has not is merged, and merged correctly, because there is nothing local to double against. Refusals cannot be told from ordinary REST lag by COUNT — both scale with the fill rate — only by whether the same order id keeps reappearing.
- **Current handling:** `rust/bins/arb-trader/src/fills.rs` — one `claim` for both sources, `reconcile` spawned on every reconnect (window = process start), gauges `kalshi_fills_recovered` / `kalshi_reconcile_failures` / `kalshi_reconcile_rejected`. Python never subscribed to this channel.
- **Port requirement:** Never overwrite a locally-summed fill total with a venue COUNT — merge by fill id, or a replay hedges the recovered contracts twice. Keep the emitted count a floor on venue truth, and check that floor against the venue's own per-order total before merging. Do not assume the history endpoint's sort order, and never let a truncated or empty page read as "nothing to reconcile". Boot is not reconcilable this way (the window would contain orders the process never registered); cover it with the startup sweep and persisted undischarged-hedge state.

### `kalshi-count-fp-is-fractional`
- **Venue:** kalshi
- **What the API does:** `count_fp` is a plain contract count (see `kalshi-fill-count-fp-plain-count`) but **it is not always an integer.** 22 of the 217 live fills in `data/venue/kalshi_fills.json` are fractional, and they pair up inside one order: `2.13` + `1.87` on a 4-lot, `0.98` + `4.02` on a 5-lot, `0.01` + `4.99` on another 5-lot. Kalshi splits a fill across price levels and the pieces sum to the order's size. Exactly two decimals on all 217 rows. An order can also END fractional — four in the history do, one having filled `0.41` and nothing else — so a fractional POSITION is real, not just a fractional print.
- **Failure it caused:** reading the field as `as f64 as i64` truncates each piece independently — 2.13 -> 2 and 1.87 -> 1 is three contracts on an order the venue filled four. A piece below 1.00 truncated to 0, hit the non-positive arm and was SKIPPED, bumping a gauge documented "must stay 0" while its contracts went to the venue unhedged. Replaying the live history through that parser loses **11.27 contracts** (9 of them whole and recoverable; the remaining 2.27 is sub-contract dust) and skips 5 frames, across **~6.6% of orders**. No gap, no reconnect and no alarm involved.
- **Current handling:** `rust/bins/arb-trader/src/fills.rs::count_fp_hundredths` — exact string arithmetic to hundredths (never f64; a third decimal truncates DOWN), accumulated per order, floored to whole contracts once at emission. Banked-but-not-whole fractions are reported by `kalshi_fill_dust_hundredths`.
- **Port requirement:** Accumulate `count_fp` in fixed-point and round DOWN once, at the end. Never truncate a single fill, never treat a sub-contract piece as a malformed payload, and keep the running total a FLOOR on venue truth — a count above what the venue filled is the one direction that mints a hedge for a contract that does not exist.

---

## Polymarket US (QCX) — order API

### `pmus-create-omits-fill-data`
- **Venue:** polymarket_us
- **What the API does:** Order CREATE responses carry `{"id", "executions": []}` — no `avgPx`, no `cumQuantity`, no fees. Fills report asynchronously; even an IMMEDIATE re-fetch of the order can still show `cumQuantity=0`/`avgPx=0`.
- **Failure it caused:** 2026-07-21/22 — async fills recorded as `avg_price=0`/`fees=0` in the ledger, understating cost on Kalshi-maker fills; a suppressed first-attempt 429 during the re-fetch left `avg_price=0`/`order_id=null` and a -$1 phantom loss on the dashboard (2026-07-22).
- **Current handling:** `src/arbbot/exec/main.py:60-90` (re-fetch up to 8x with 0.6s sleeps, per-attempt try/except, until `cumQuantity>=1` and `avgPx.value` present; falls back to the placement book price flagged `price_estimated`, never a fabricated 0), `src/arbbot/exec/polymarket_us_gateway.py:139-146` (`get_order` is the authoritative read).
- **Port requirement:** Never read execution economics from a create response; poll the order record with retries until the async fill lands, and record an explicit estimated-price flag if the venue stays unreachable.

### `pmus-commission-is-total`
- **Venue:** polymarket_us
- **What the API does:** `commissionNotionalTotalCollected.value` on the order record is the TOTAL commission for the whole order, not per contract.
- **Failure it prevents:** treating it as per-contract multiplies the fee by the fill quantity (4x on the recorded Mamdani fixture).
- **Current handling:** `src/arbbot/exec/main.py:82-84` (pre-divides by filled qty before the generic `* filled`).
- **Port requirement:** Treat PM commission as an order-level total; divide by quantity before any per-contract math.

### `pmus-ioc-fill-report-lag`
- **Venue:** polymarket_us
- **What the API does:** Fill reporting for IOC orders lags the execution by seconds — occasionally MINUTES. A single immediate `filled_qty` read returns 0 on orders that DID fill.
- **Failure it caused:** 2026-07-22 — "unfilled" verdicts on filled IOCs left naked PM shorts; an unguarded 2s refire loop stacked 130 shorts from a 2-lot hedge (the refire runaway); 2026-07-23 — a fill landing after cancel produced the naked -5 incident.
- **Current handling:** poll-before-concluding everywhere: `src/arbbot/exec/main.py:425-437` (6x0.5s), `scripts/unwind_positions.py:56-61`, `scripts/hedge_naked_legs.py:195-202`, `scripts/sports_arb.py:239-253` (`_confirm_ioc_fill`: 10x0.5s + authoritative `get_order.cumQuantity` confirm), `scripts/pmus_maker_probe.py:468-476` (post-cancel delayed re-checks at +3/+10/+30s).
- **Port requirement:** Never act on a single immediate fill read; poll with a deadline, confirm against the order record, and re-check after cancels for late-landing fills.

### `pmus-unfilled-counts-against-cap`
- **Venue:** polymarket_us
- **What the API does:** (consequence of the fill lag) an order reported "unfilled" may actually be filled.
- **Failure it caused:** 2026-07-22 — a wrong "unfilled" abort left naked PM shorts invisible to a Kalshi-only concentration cap, which then kept re-firing the same trade.
- **Current handling:** `src/arbbot/exec/main.py:433-437` (attempted size counted against the cap until reconciliation verifies), `:357-360` (cap also reads PM-side net positions, not just Kalshi).
- **Port requirement:** Charge attempted size against risk caps on any ambiguous outcome and only release it after independent position reconciliation; caps must observe BOTH venues' positions.

### `pmus-cancel-requires-market-slug`
- **Venue:** polymarket_us
- **What the API does:** `POST /v1/order/{id}/cancel` requires the order's `marketSlug` in the request body.
- **Failure it caused:** self-resolving the slug via `open_orders` on every reprice hammered the API into 429 storms; a silently-failed cancel accumulated stray live orders.
- **Current handling:** `src/arbbot/exec/polymarket_us_gateway.py:110-127` (caller MUST pass the slug — deliberately NO `open_orders` fallback; `raise_for_status` so a failed cancel is never mistaken for success).
- **Port requirement:** Thread the market slug through every order-tracking structure so cancel never needs a lookup call; raise on cancel failure.

### `pmus-create-bare-body-preview-wrapped`
- **Venue:** polymarket_us
- **What the API does:** `POST /v1/orders` (create) takes the BARE order body; only `POST /v1/order/preview` wraps it in `{"request": ...}`. A mismatched wrapper fails with a misleading "Market not found".
- **Failure it prevents:** hours lost debugging a "missing market" that is actually a body-shape error (verified against the official SDK).
- **Current handling:** `src/arbbot/exec/polymarket_us_gateway.py:101-106` (create, bare), `:69-77` (preview, wrapped).
- **Port requirement:** Encode the two body shapes separately; do not share a serializer between create and preview.

### `pmus-cancel-unknown-id-succeeds`
- **Venue:** polymarket_us
- **What the API does:** `POST /v1/order/{id}/cancel` answers <300 for an `id` the venue has never issued. There is no "no such order" error to detect.
- **Failure it caused:** every per-order cancel the trader sent carried OUR `m…` client id, and all of them were logged as `[exec] PolymarketUs cancelled` — 11 of them on 2026-07-28 — while every quote went on resting. `exec_failed` stayed 0 the whole time, so the one counter that would have shown it read healthy.
- **Current handling:** `rust/crates/arb-venue/src/gateway/pmus.rs` (`cancel` REFUSES a `CancelBy::ClientId` target locally and never puts it on the wire); `rust/bins/arb-trader/src/engine/cancel.rs` (a cancel is parked until the venue's own id is known, rather than addressed with ours).
- **Port requirement:** Never send our id to this endpoint. A 2xx from it is not evidence that anything was cancelled unless the id came from the venue; the resting-order list is the only proof.

### `pmus-no-client-order-id`
- **Venue:** polymarket_us
- **What the API does:** The create body carries NO client-order-id field of any kind (`POST /v1/orders` takes marketSlug/type/price/quantity/tif/intent and nothing of ours), so the venue never learns a tag we could look ourselves up by. Kalshi's `client_order_id` has no counterpart here.
- **Failure it caused:** a place whose RESPONSE is lost (the 15s HTTP timeout, or a body that will not parse) may still have REACHED the venue, and the order then rests under an id we never learned: no per-order cancel can address it, the client-id escalation is refused locally and forever, and a fill on it arrives under an id the engine cannot attribute (`fills_unattributed`, no hedge minted, the leg naked).
- **Current handling:** `rust/crates/arb-venue/src/gateway/pmus.rs` (`recover_place`: one `GET /v1/orders/open`, matched on marketSlug + quantity, excluding ids this process has already claimed, and refusing outright when more than one candidate matches); `rust/bins/arb-trader/src/exec.rs` (the recovered order is adopted with a real `order_ack` and counted in `exec_recovered`). The Python this ports had no recovery at all.
- **Port requirement:** Never send our id in a PM-US order body or cancel path (it will be answered <300 for an order the venue never issued). Recover a lost create ONLY from the open-orders row, scoped to orders this process owns and refused when ambiguous — the account is shared, and an adopted order gets cancelled later.

### `pmus-order-id-field-is-id`
- **Venue:** polymarket_us
- **What the API does:** The order id field is `id` (Kalshi uses `order_id`, sometimes nested under `order`).
- **Failure it prevents:** a missed id makes the quoter track a phantom None-id quote that can never be cancelled or hedged.
- **Current handling:** `src/arbbot/exec/quoter.py:265-280` (extraction chain `order.order_id | order_id | id`; None => treat as place-failure, don't track), `src/arbbot/exec/kalshi_gateway.py:138` (Kalshi nesting).
- **Port requirement:** Model per-venue response shapes explicitly and refuse to track an order without a confirmed id.

### `pmus-intent-buy-short`
- **Venue:** polymarket_us
- **What the API does:** There is no sell-to-open. Opening a NO position is `ORDER_INTENT_BUY_SHORT` at the YES-axis price (hitting a resting YES bid at p holds NO at cost 1-p, fully collateralized). `ORDER_INTENT_SELL_LONG` only closes a held long. Post-only is `participateDontInitiate`.
- **Failure it prevents:** using SELL_LONG to open a short is rejected/wrong-position; prices sent on the NO axis are wrong by 1-p.
- **Current handling:** `src/arbbot/exec/polymarket_us_gateway.py:51-67` (body/intent mapping), `:79-93` (`place_short` vs `place_yes`).
- **Port requirement:** Keep the intent enum mapping (bid->BUY_LONG, open-short->BUY_SHORT, close-long->SELL_LONG) and all prices on the YES axis.

### `pmus-ed25519-signing`
- **Venue:** polymarket_us
- **What the API does:** Auth = Ed25519 signature over `"{ts_ms}{METHOD}{path}"`, headers `X-PM-Access-Key/-Timestamp/-Signature` (base64). The key file is base64; the SEED is the first 32 bytes of the decoded blob. Timestamp must be within ~30s of server time. Same scheme signs WS upgrade handshakes.
- **Failure it prevents:** signing with the full decoded blob (not the 32-byte seed) or a stale timestamp yields opaque 401s.
- **Current handling:** `src/arbbot/venues/pmus.py` (`load_ed25519` + `sign_headers`, the single implementation); `src/arbbot/exec/polymarket_us_gateway.py:_headers` and `src/arbbot/record/polymarket_us.py:ws_auth_headers` delegate to it.
- **Port requirement:** Derive the Ed25519 key from `base64decode(file)[:32]` and sign `{ts}{METHOD}{path}` with millisecond timestamps refreshed per request.

## Polymarket US (QCX) — positions API

### `pmus-positions-empty-glitch`
- **Venue:** polymarket_us
- **What the API does:** `GET /v1/portfolio/positions` transiently returns an EMPTY positions map while positions are actually held (seen 4x on 2026-07-22).
- **Failure it caused:** a glitched-empty read freed phantom cap headroom and let a capped relationship re-fire; it also painted false NAKED alerts.
- **Current handling:** `src/arbbot/exec/main.py:365-374` (keep previous state on empty read), `:336-341` (fired-qty ratchet seeded from the ledger, never trusts a fresh-session empty read), `src/arbbot/venues/pmus.py` (`PmusSession.get_positions`: empty read raises unless `allow_empty` — `scripts/hedge_naked_legs.py` and `scripts/reconcile_positions.py` retry through it).
- **Port requirement:** An empty positions read while prior state says we hold positions is a glitch: keep last-known state, retry, and never release risk headroom from it.

### `pmus-positions-partial-stale-sticky`
- **Venue:** polymarket_us
- **What the API does:** During platform incidents the positions endpoint serves PARTIAL sets (1-4 of ~10 held) and hours-STALE snapshots; the staleness is STICKY (a server-side cache survives back-to-back reads).
- **Failure it caused:** 2026-07-22 false NAKED alarms and the Mamdani ghost-position pages; a same-run confirm fetch is NOT sufficient because the cache survives it.
- **Current handling:** `scripts/reconcile_positions.py:85-126` (last-known-good count ratchet + ledger-derived expected-slug set: any missing expected leg => DEGRADED, skip naked evaluation), `:215-235` (imbalances alert only when seen on two consecutive RUNS ~60s apart, and a degraded confirm read aborts), `:243-248` (dashboard snapshot kept if PM count collapsed), `scripts/hedge_naked_legs.py:79-91` (act only on two consecutive reads that agree exactly).
- **Port requirement:** Treat PM positions as an unreliable witness: require cross-read consensus and expected-set completeness before acting, and separate DEGRADED (platform) from NAKED (us).

### `pmus-positions-dict-keyed-by-slug`
- **Venue:** polymarket_us
- **What the API does:** Positions come back as a DICT keyed by market slug with STRING fields (`netPosition: "-100"`, `avgPx.value`, `costPerShare.value` — invented value, see the card below); `/v1/positions` 404s (the path is `/v1/portfolio/positions`).
- **Failure it prevents:** list-shaped parsers or numeric-typed decoders fail on real payloads.
- **Current handling:** `scripts/hedge_naked_legs.py:60-77` (+ basis math `:173-182`), `src/arbbot/exec/main.py:362-365`.
- **Port requirement:** Parse the slug-keyed dict with string decimals.
- **What `costPerShare` MEANS is a separate quirk, and this card used to get it wrong** — see `pmus-cost-per-share-is-base-cost-over-net` below.

### `pmus-cost-per-share-is-base-cost-over-net`
- **Venue:** polymarket_us
- **What the API does:** `costPerShare` is **exactly `baseCost / |netPosition|`** — a RESIDUAL cost basis divided by the CURRENT net. It is not a per-share entry price. The two coincide only for a position opened once and never traded back; a non-zero `qtyBought` alongside a short net is ordinary, not exotic, and for those the ratio is not a price at all. Verified on 2026-07-29 against every open PM-US position in one signed read-only `GET /v1/portfolio/positions`: the identity held to the reported precision in every case. **Every number in this card is invented** — a residual `baseCost` of 60.00 over a net of 100 reports `costPerShare 0.6000`. Nothing here is derived from a position we hold, and nothing here should be: this file is public.
- **This card previously asserted "`costPerShare` for a short is the NO cost (1 - sold YES price)". That is WRONG.** It is a coincidence of the never-traded-back case. Do not port it, and do not reintroduce it from the Python that still applies it.
- **It degenerates as `|net| -> 0`, which is exactly the naked-leg condition a hedger reads it under.** The denominator is the current net, so a position traded back to a net of ±1 reports its whole residual basis as the per-share cost — buy 50, sell 51, and a residual `baseCost` of 6.00 over a net of 1 reports `costPerShare 6.0000`, nonsense as a probability. The field is least trustworthy precisely where it is most consulted.
- **The error is asymmetric, and only one direction is loud.** A hedge-completion limit is `1 − basis − fee − lock`. A basis reading HIGH drives the limit negative, no ask ever clears it, and nothing fills — it surfaces as "waiting for a better book", which is safe and visible. A basis reading LOW yields a too-generous limit, an IOC that fills at the ask, and a locked LOSS booked as a gain. A quiet wrong answer is the one to design against.
- **Failure it prevents:** pricing a hedge-completion limit off a number that is not a price.
- **Current handling:** the misreading is live in frozen Python, in two places. `scripts/hedge_naked_legs.py:70` (parse) and `:179-187`, where `pm_basis` is set from `cost_per_share` under the comment `# NO cost/ct = 1 - sold YES px` and then fed straight into the order limit — this is the one that can place a bad order. `scripts/reconcile_positions.py:122` and `:356`, where the same field becomes `pm_basis_ct` in `locked_ct = 1 - k_basis_ct - pm_basis_ct` — that one only misreports a locked profit. Cited as WHERE the bug is, not as a pattern to reproduce.
- **Port requirement:** Do not price a hedge off `costPerShare`. The basis for completing a half-filled basket is OUR OWN recorded fill — `rust/bins/arb-trader/src/orphan.rs` already carries `maker_order_id`, `hedge_market` and `anchor_price` per undischarged mint. Treat `costPerShare` only as `baseCost/|net|`: a reconciliation input, never a per-share price, and never at small `|net|`.

## Polymarket US (QCX) — accounting / cash

### `pmus-no-history-endpoints`
- **Venue:** polymarket_us
- **What the API does:** PM-US exposes **only** `/v1/account/balances` and `/v1/portfolio/positions`. There is no fill history, no settlement history, and no deposit/transfer history. Probed and 404 on 2026-07-27: `/v1/portfolio/fills`, `/v1/portfolio/trades`, `/v1/portfolio/settlements`, `/v1/account/transfers`, `/v1/account/transactions`, `/v1/account/activity`, `/v1/account/deposits`, `/v1/portfolio/activity`.
- **Failure it prevents:** assuming a Kalshi-style rebuild is possible on PM-US. It is not — the account cannot be reconstructed from the venue.
- **Port requirement:** PM-US books open from a human-supplied funding total plus a reconciling entry against `currentBalance`, and record fills going forward from our own observation. Only Kalshi gets penny-exact provenance. Re-probe periodically in case PM-US ships history endpoints.

### `pmus-margin-is-one-dollar-per-short`
- **Venue:** polymarket_us
- **What the API does:** `/v1/account/balances` returns `currentBalance` (total cash), `marginRequirement`, and `buyingPower`, where `buyingPower = currentBalance − marginRequirement` and **`marginRequirement` is exactly $1.00 × total short contracts**. Verified: 7 short positions summing 323.86 contracts ⇒ `marginRequirement` 323.86.
- **Failure it prevents:** treating `buyingPower` as total cash understates the account by the margin (here $323.86 of $653.16). Conversely, treating `currentBalance` as spendable overstates it.
- **Current handling:** `scripts/capital_snapshot.py:59-62` reads `buyingPower` — correct for its "idle capital" purpose, and it should stay.
- **Port requirement:** Books carry `currentBalance` as `cash:pmus` with the `marginRequirement` portion encumbered; use `buyingPower` only for "what can we deploy." Same $1/contract mechanic as `kalshi-short-collateral-one-dollar-per-contract`.

### `pmus-positions-carry-fees-and-stale-marks`
- **Venue:** polymarket_us
- **What the API does:** Each position carries `fees` (cumulative, per position), `baseCost`, `costPerShare`, `avgPx`, and `cashValue` — but also an `updateTime` that can be **days stale** (observed 07-22 on a position read 07-27), so `cashValue` is not a live mark.
- **Failure it prevents:** (a) believing PM-US reports no fees — it does, and they totalled $1.30 on open positions against $0.12 booked; (b) using `cashValue` as a current mark and reporting a stale portfolio value as live.
- **Port requirement:** Book `fees` as a real expense. For marks, price positions off the live book, not `cashValue`; if `cashValue` is used, surface `updateTime` alongside it.

## Polymarket US (QCX) — market data / WS

### `pmus-ws-full-book-snapshots-offers`
- **Venue:** polymarket_us
- **What the API does:** The markets WS pushes a FULL book (`marketData.bids/offers`) on every change — no deltas, no seq numbers; the ask side is named `offers` (not `asks`). Same shape as REST `/book`.
- **Failure it prevents:** reading `asks` yields permanently one-sided books; building delta machinery adds gap risk that does not exist.
- **Current handling:** `src/arbbot/record/polymarket_us.py:79-88`, `:110-132`, `scripts/sports_arb.py:501-504`.
- **Port requirement:** Treat every MARKET_DATA frame as a self-contained snapshot and read the ask side from `offers`.

### `pmus-ws-trade-quantity-is-money-wrapped-contracts`
- **Venue:** polymarket_us
- **What the API does:** A WS TRADE frame wraps BOTH legs of the print in the venue's generic money type: `price` is `{"currency":"USD","value":"0.2800"}` and `quantity` is `{"currency":"USD","value":"5.0000"}`. **The `currency: USD` label belongs to the wrapper, not to the meaning — `quantity` is a CONTRACT COUNT, not USD notional.** The same feed's book levels spell the same two things differently again: `px` is money-wrapped, `qty` is a bare decimal string. The order API is a third spelling: `POST /v1/orders` takes a wrapped `price` and a BARE integer `quantity` (`rust/crates/arb-venue/src/wire.rs::pmus_order_body`), and the portfolio API wraps every money field (`cost`, `avgPx`, `fees`) while leaving every quantity bare (`netPosition`, `qtyAvailable`).
- **How the units were settled — against the book, in the same feed:** take a print on `ewc-pres-bra-2026-10-04-flabol` at `price 0.2800` carrying `quantity.value 5.0000`. Across that print the resting bid at 0.2800 goes 240.0000 → 235.0000: the book is consumed by **exactly the printed `value`**. Book sizes are contracts — `qty` is the bare decimal the venue also accepts as a bare integer `quantity` on `POST /v1/orders` — so the printed `value` is contracts too. Had it been USD notional the same print would have had to remove 5.0000/0.28 = 17.86 from that level, and it does not. (Independently cross-checked against a locally recorded order whose contract count was known ahead of the print; it agreed. Note also that a non-integral `value` is NOT evidence against contracts — PM-US positions are genuinely fractional, see `pmus-positions-*`, and 49.2% of live prints are.)
- **Failure it caused:** the recorder read `quantity` without unwrapping and `dec_string`'s `to_string()` fallthrough stringified the whole object into `TapeEvent::Trade.size` (a `String`, which accepts it). 11,541 of 11,541 PM-US trade lines on the Rust tape — 100%, every day it has run — carry a size like `"{\"currency\":\"USD\",\"value\":\"4.0000\"}"`. Nothing went red: `serde_json` round-trips it, and so does `arb-recorder --parse-check`, which asks only whether a line re-serializes byte-identically — and a string that was already a string when it arrived always does. The Python recorder never reached the bug from the other side either: `Decimal(str(t["quantity"]))` raises on a dict, so the Python tape has ZERO PM-US trades and there was nothing to diff against.
- **Current handling:** `rust/bins/arb-recorder/src/pmus.rs::money_value` (one helper for all three wrapped fields; a bare scalar is accepted as-is, since that is the venue's own other spelling), and `rust/bins/arb-recorder/src/core.rs::dec_string`, which returns `None` for any non-scalar so a shape nobody has seen drops the field/level/event instead of being laundered into a decimal-shaped string.
- **Port requirement:** Unwrap `.value` on every money-typed field and never read `currency` as the unit. Never render an unexpected JSON shape into a numeric field with `to_string()` — a byte-identity re-serialize gate cannot tell that apart from a real decimal.

### `pmus-ws-auth-required-chunked-subs`
- **Venue:** polymarket_us
- **What the API does:** The market-data WS 401s unauthenticated (only trade-capable keys exist). Subscription frames carry `marketSlugs`; the venue-side cap per subscribe request is unknown/undocumented — oversized lists risk silent partial coverage.
- **Failure it prevents:** silently missing books on wide universes (no error is returned).
- **Current handling:** `src/arbbot/record/recorder.py:363-373` (chunks of 150 with distinct requestIds), `scripts/sports_arb.py:483-485` (chunks of 20), `src/arbbot/record/polymarket_us.py:10-12` (REST-poll fallback when credential-free, `src/arbbot/record/recorder.py:391-414`).
- **Port requirement:** Chunk WS subscriptions with distinct request ids and verify coverage; keep a credential-free REST poll path.

### `pmus-metadata-tag-route-only`
- **Venue:** polymarket_us
- **What the API does:** There is no by-slug metadata endpoint — `gateway.polymarket.us/v1/markets/{slug}` 404s. Metadata comes only from `GET /events?tag_slug=<tag>` (nested markets). `feeCoefficient` on the market is the venue-reported taker coefficient.
- **Failure it prevents:** a leg whose tag isn't polled has NO metadata (tick, fees) — the runner warns and quoting on it would use wrong ticks.
- **Current handling:** `src/arbbot/record/polymarket_us.py:5-9`, `:142-158`, `src/arbbot/exec/main.py:167-173` (missing-metadata warning tied to `polymarket_us_tags`).
- **Port requirement:** Resolve PM US metadata through the tag route and fail loudly when a traded slug has no metadata.

### `pmus-books-freeze`
- **Venue:** polymarket_us
- **What the API does:** Books sometimes FREEZE: quotes stay displayed but nothing is matchable — marketable IOCs through the touch expire with zero fill, and the WS stops ticking (seen 2026-07-22, Babel). Ghost fills against frozen books left PM-short excess.
- **Failure it caused:** 2026-07-22 Babel/Jones naked legs — executing against a display that isn't ticking.
- **Current handling:** `scripts/sports_arb.py:112-221` (per-slug blacklist with escalating TTL, adaptive tick-freshness EWMA, resubscribe heal probe, venue-wide circuit breaker: >=5 dead books/10min => 30min halt), `:306-309` (zero-fill IOC at displayed touch => mark dead), `scripts/hedge_naked_legs.py:150-152` (profit-only completion of the resulting shorts).
- **Port requirement:** Gate execution on book LIVENESS (recent ticks), treat a zero-fill marketable IOC at the displayed touch as a frozen book, and stop trusting that market for a cooling period.

### `pmus-private-order-ws`
- **Venue:** polymarket_us
- **What the API does:** `wss://api.polymarket.us/v1/ws/private` with `SUBSCRIPTION_TYPE_ORDER` pushes order executions (`orderSubscriptionUpdate.execution`, types `EXECUTION_TYPE_FILL`/`_PARTIAL_FILL`, order carries `cumQuantity`) sub-second — vs ~2s REST polling.
- **Failure it prevents:** slow fill detection widens the naked-exposure window on maker fills; double-processing a fill seen by both WS and poll double-hedges.
- **Current handling:** `src/arbbot/exec/main.py:465-509` (private WS listener), `:303-330` (shared idempotent `_process_fill` keyed on cumulative qty per order id — WS and 2s poll fallback `:565-578` dedupe through it).
- **Port requirement:** Use the private ORDER WS as the primary fill signal with REST polling as fallback, and make fill processing idempotent on (order_id, cumulative_qty) so both paths can run concurrently.

---

## Polymarket international (CLOB, data-only)

### `pm-gamma-json-encoded-fields`
- **Venue:** polymarket_intl
- **What the API does:** Gamma market fields `clobTokenIds` and `outcomes` are JSON-encoded STRINGS inside the JSON response; outcome order is not fixed, so the YES token must be found by outcome label.
- **Failure it prevents:** assuming index 0 == YES silently swaps YES/NO on some markets.
- **Current handling:** `src/arbbot/record/polymarket.py:42-47`.
- **Port requirement:** Double-decode these fields and map tokens by outcome label, never by position.

### `pm-gamma-repeated-params`
- **Venue:** polymarket_intl
- **What the API does:** Gamma `/markets` expects REPEATED `clob_token_ids` query params; comma-joined values 422.
- **Current handling:** `src/arbbot/record/polymarket.py:173-177` (repeated params, batches of 20).
- **Port requirement:** Encode `clob_token_ids` as repeated query parameters.

### `pm-clob-book-reverse-ordering`
- **Venue:** polymarket_intl
- **What the API does:** Live CLOB REST `/book` returns asks high->low and bids low->high — best price at the END of each array.
- **Failure it prevents:** taking `[0]` as best silently reads the WORST price.
- **Current handling:** `src/arbbot/record/polymarket.py:6-8`, `:72-78` (always sort, never assume).
- **Port requirement:** Sort book levels on ingestion; never trust venue array order.

### `pm-ws-no-seq-numbers`
- **Venue:** polymarket_intl
- **What the API does:** The market WS feed has NO sequence numbers; `price_change` carries the NEW TOTAL size per level (0 removes) — opposite of Kalshi's delta-of-change; keepalive is a literal `"PING"` text frame (~10s) answered by `"PONG"`.
- **Failure it prevents:** without synthesized seq + periodic REST re-snapshots, silent event loss is undetectable; unhandled `"PONG"` frames break JSON parsing.
- **Current handling:** `src/arbbot/record/polymarket.py:128-136` (`SeqCounter`), `:94-112` (new-total deltas), `:142-143` (PONG ignored), `src/arbbot/record/recorder.py:186-215` (PING every 10s + 300s integrity re-snapshot + gap -> REST re-snapshot).
- **Port requirement:** Synthesize per-market seq, re-snapshot periodically for integrity, apply `price_change` as new totals, and speak the PING/PONG text protocol.

### `pm-taker-base-fee-zero-override`
- **Venue:** polymarket_intl
- **What the API does:** CLOB `/markets/{condition_id}` reports `taker_base_fee`; 0 means the venue declares the market FEE-FREE. Nonzero values have an unverified unit mapping.
- **Failure it prevents:** charging schedule fees on fee-free markets kills real edges (and trusting unverified nonzero units would understate fees).
- **Current handling:** `src/arbbot/record/polymarket.py:195-217` (override to 0 only when venue says 0; keep the schedule on any doubt).
- **Port requirement:** Honor the venue's explicit fee-free declaration; on any ambiguity, use the conservative published schedule.

---

## Cross-venue / general

### `xv-429-at-rehearsal-proves-auth`
- **Venue:** both
- **What the API does:** Both venues rate-limit (429) under our own restart churn and the shared API budget with the research probes.
- **Failure it caused:** 2026-07-23 — persistent 429s at startup kept the live trader down even though the order path was fine.
- **Current handling:** `src/arbbot/exec/main.py:264-289` (rehearsal retries with backoff; a 429 PROVES the signed path works — authenticated, understood, throttled — so the trader proceeds), `scripts/pmus_maker_probe.py:269-292` (recon 429 -> jittered backoff), `:478-481` (poll sparingly, >=30s — shared budget contract).
- **Port requirement:** Treat 429 as proof-of-auth during startup validation, and give every background poll loop 429-aware backoff; venue throttling must never be startup-fatal.

### `xv-shared-api-budget`
- **Venue:** both (PM-US metered today)
- **What the API does:** One venue API budget is shared by the live trader, the systemd timers, and the research probes; uncoordinated callers 429-storm each other (incident 2026-07-23 — ~9 scripts each hand-rolling PM-US requests with no shared limiter).
- **Current handling:** `src/arbbot/venues/pmus.py` (`PmusSession` + `consume_budget`: cross-process per-minute token bucket persisted in `data/exec/trading.db` table `rate_budget`; background callers block until the window rolls, `priority="critical"` — order place/cancel/hedge — bypasses the budget and never opens the DB).
- **Port requirement:** All background venue reads must draw from one cross-process budget; the order/hedge path must bypass it and never wait.
- **Port corollary (2026-07-29):** "the order path" includes the READS an order-path call cannot complete without — Kalshi's `all_orders` (a client-id cancel resolves through it, and `cancel_all_open` pages it before it can cancel anything) and both venues' resting-order list, which is the only evidence `cancel_all_and_verify` accepts. Metering those leaves the kill sweep refusable by a token bucket. The cost of exempting them is that a sweep is now locally unmetered end to end: Kalshi's `cancel_all_open` is 1 list + N DELETEs, `SweepPolicy::default()` runs 4 rounds of that plus 3 proof polls each, and the STARTUP sweep runs before `spawn_executors` (`main.rs:971` vs `:988`) so it does not pass the executor's 8/s shaper either — worst case ~4×(1 + N + 3) unshaped requests against the resting book, N being however many orders a previous run left (small in practice; PM-US has no fan-out, its cancel-all is one call). That is the accepted trade: a self-inflicted 429 mid-sweep fails SAFE (arming refuses, exit 10, "the book could not be proven clean"), whereas a sweep the local budget refuses fails the same way for no reason at all.

### `xv-cold-tls-adds-hedge-latency`
- **Venue:** both
- **What the API does:** A cold TCP+TLS handshake adds 40-70ms to the first request (measured 2026-07-22) — more than the entire warm hedge round-trip.
- **Failure it prevents:** hedge latency (the naked-exposure window) doubles when the HTTP pool has gone idle.
- **Current handling:** `src/arbbot/exec/main.py:598-613` (connection warmer: cheap authenticated GET per gateway every 25s holds keep-alive open).
- **Port requirement:** Keep order-path connections warm (keep-alive pings or pooled pre-established TLS); never pay a handshake on the hedge path.

### `xv-zombie-socket-on-reconnect`
- **Venue:** both (recorder Unix socket)
- **What the API does:** (internal, but a port hazard) Leaking the old Unix-socket connection on reconnect leaves a zombie the recorder keeps writing into — Send-Q grows until the recorder's slow-subscriber guard drops the client.
- **Current handling:** `src/arbbot/exec/main.py:587-596` (close before reconnect), `src/arbbot/record/recorder.py:123-133` (recorder disconnects subscribers whose write buffer exceeds 1MB rather than blocking its write path).
- **Port requirement:** Close the old feed connection before reconnecting; the data-plane publisher must never block on a slow reader.

### `xv-graceful-shutdown-cancels-orders`
- **Venue:** both
- **What the API does:** GTC maker orders survive process death; systemd restarts/recycles send SIGTERM.
- **Failure it prevents:** a restart orphans live untracked resting orders (naked-fill risk); a blanket cancel-all from one workstream kills another workstream's orders on the SHARED Kalshi account.
- **Current handling:** `src/arbbot/exec/main.py:644-647` (SIGTERM -> KeyboardInterrupt so the finally-block runs), `:621-633` (cancel every resting quote + venue-wide sweep on shutdown), `scripts/sports_arb.py:1051-1058`+`:1062-1066` (probe cancels ONLY its own order ids), `:684-693` (startup self-heal sweeps only sports-prefixed tickers).
- **Port requirement:** Convert termination signals into an orderly cancel of OWN resting orders before exit; scope any sweep to orders this process owns (shared-account contract).

### `xv-append-only-ledger-with-corrections`
- **Venue:** both
- **What the API does:** (consequence of venue unreliability) recorded values can be wrong after the fact — e.g. a hedge response lost to a 429 recorded `avg_price=0`.
- **Current handling:** `src/arbbot/exec/ledger.py:26-52` (append-only correction records `{"status":"correction","corrects_ts",...}` shallow-merged by every consumer; unwind netting `:55-89`).
- **Port requirement:** Keep the trade ledger append-only with correction/unwind records folded at read time — never rewrite history to fix a venue glitch.

### `xv-settlement-skew`
- **Venue:** both
- **What the API does:** The two venues finalize the same real-world event at different times, so a hedged basket looks imbalanced while one side has settled.
- **Failure it prevents:** false NAKED pages and pointless "hedges" against a market that has already resolved.
- **Current handling:** `scripts/reconcile_positions.py:337-351` (suppress imbalances whose Kalshi market is not active — settlement sweeper's domain), `:255-263` (rows marked `settling`), `scripts/settle_baskets.py` (realizes finalized baskets at $1/contract by construction).
- **Port requirement:** Before flagging or acting on an imbalance, check market status on both venues; finalized-on-one-venue is a settlement event, not directional risk.

### `xv-tick-floored-limits`
- **Venue:** both
- **What the API does:** Limit prices computed from arithmetic (basis + fee + margin) are generally not tick-aligned, and venues reject or round sub-tick prices.
- **Current handling:** `scripts/hedge_naked_legs.py:180-182` (profit-guaranteeing limit floored to the market tick before sending), `src/arbbot/exec/quoter.py:152-154`/`:172-175` (quantize down/up toward the profitable side).
- **Port requirement:** Quantize every computed limit to the venue tick, rounding toward the profitable side, before it leaves the process.
