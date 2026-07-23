# Venue Quirk Registry — Test-Gap Inventory

One row per entry in `docs/venue-quirks.md`: which regression test currently
pins the behavior, or **GAP** with a one-line suggested test. "Partial" means
the core behavior is pinned but a load-bearing edge is not.

## Kalshi — order API

- `kalshi-create-endpoint-410` — **DONE** (card 7fab301e): `tests/test_venue_contract_gaps.py::test_k6_create_posts_events_orders_endpoint`, `::test_k6_cancel_deletes_events_orders_id`, `::test_k6_list_and_get_stay_on_portfolio_orders`.
- `kalshi-orders-list-history-paginated` — pinned: `tests/test_venue_contracts.py:62` (K1: full history across cursor pages, resting filtered client-side).
- `kalshi-signature-path-only` — pinned: `tests/test_venue_contracts.py:83` (K2).
- `kalshi-fill-count-fp-plain-count` — pinned: `tests/test_venue_contracts.py:96` (K3), `:102` (K3b: 0 on missing/dry).
- `kalshi-cancel-404-is-success` — pinned: `tests/test_venue_contracts.py:108` (K4: 404 success, 500 raises).
- `kalshi-postonly-cross-rejected-400` — pinned: `tests/test_venue_contracts.py:116` (K5: the 400 raising), `tests/test_quoter.py` (`test_one_tick_book_joins_best_bid_never_crosses`: 1-tick-book clamp; `test_failed_place_backs_off_min_requote_s`: failed-place backoff).
- `kalshi-v2-yes-axis` — pinned: `tests/test_kalshi_gateway.py:16` (body shape), `:27` (ask on YES axis), `:35` (IOC taker), `:50` (4-dp fixed-point).
- `kalshi-deci-cent-ticks` — **DONE** (card 7fab301e): `tests/test_hedge_naked_legs.py::test_kalshi_ask_parses_deci_cent_step`, `::test_kalshi_ask_defaults_to_penny_without_price_ranges`, `::test_kalshi_ask_no_ask_returns_none`, `::test_hedge_limit_floored_to_deci_cent_tick` (same limit floors to 0.388 on a 0.001 market vs 0.38 on a penny market).
- `kalshi-positions-and-settlement-fields` — **DONE** (card 7fab301e): `tests/test_hedge_naked_legs.py::test_kalshi_positions_position_fp_is_signed_count`, `tests/test_settle_baskets.py::test_settles_only_finalized_with_yes_no_result` (finalized+void and active both refuse to settle).

## Kalshi — market data

- `kalshi-both-ladders-are-bids` — pinned: `tests/test_venue_adapters.py:17` (NO bid -> YES ask), `:30` (sort + zero filter), `tests/test_kalshi_ws.py:22` (WS snapshot path).
- `kalshi-ws-delta-is-change-not-total` — pinned: `tests/test_kalshi_ws.py:22` (change -> total, level removal), `:47` (new level from zero).
- `kalshi-ws-seq-per-subscription` — pinned: `tests/test_kalshi_ws.py:55` (regression for the 2026-07-20 11:45 incident).
- `kalshi-trade-needs-own-seq-stream` — pinned: `tests/test_kalshi_ws.py:102` (regression for the traded-markets-die P1).
- `kalshi-ws-snapshots-only-on-subscribe` — partial: `tests/test_kalshi_ws.py:143` pins gap -> `get_snapshot` request. **GAP**: no test of the REST-snapshot resync fallback (`recorder.py:298-305`) or the welcome/30s-rebroadcast heal.
- `kalshi-market-data-auth-split` — partial: the fake-WS tests exercise the signed handshake incidentally. **GAP**: no test asserting the recorder chooses WS-with-recorder-key vs credential-free REST poll.

## Polymarket US — order API

- `pmus-create-omits-fill-data` — pinned: `tests/test_venue_contracts.py:143` (P1: create carries no fill data), `tests/test_record_fill.py:49` (retries past the async-fill race).
- `pmus-commission-is-total` — pinned: `tests/test_venue_contracts.py:151` (P2), `tests/test_record_fill.py:49` (fees recorded as TOTAL, not x qty).
- `pmus-ioc-fill-report-lag` — partial -> mostly pinned (card 7fab301e): `tests/test_record_fill.py:49` (recorder-path retry) plus `tests/test_sports_arb_quirks.py::test_fill_reported_after_lag_is_found_by_polling`, `::test_partial_fill_confirmed_via_cumquantity_after_polls`, `::test_all_reads_failing_returns_zero_not_crash` (`_confirm_ioc_fill` lag + cumQuantity confirm). Remaining edge: the post-cancel delayed re-checks.
- `pmus-unfilled-counts-against-cap` — **DONE** (card 7fab301e): `tests/test_exec_runner_quirks.py::test_take_take_unfilled_pm_leg_still_counts_against_cap` (real `run()` + fake gateways: unfilled PM leg increments `tt["fired"]`, blocks the refire with cooldown disabled, never places the Kalshi leg).
- `pmus-cancel-requires-market-slug` — **DONE** (card 7fab301e): `tests/test_venue_contract_gaps.py::test_p6_cancel_requires_market_slug_in_body`, `::test_p6_cancel_without_slug_raises_never_hits_wire`, `::test_p6_cancel_non_2xx_raises`.
- `pmus-create-bare-body-preview-wrapped` — **DONE** (card 7fab301e): `tests/test_venue_contract_gaps.py::test_p7_create_bare_body_preview_wrapped`.
- `pmus-order-id-field-is-id` — pinned: `tests/test_venue_contracts.py:167` (P4: extraction chain), `tests/test_quoter.py` (`test_no_order_id_in_response_is_place_failure`: no-id response => place-failure, no phantom RestingQuote).
- `pmus-intent-buy-short` — pinned: `tests/test_polymarket_us_gateway.py:38` (ask -> BUY_SHORT), `:45` (place_short IOC), `:58` (post-only rest), `:67` (taker not post-only).
- `pmus-ed25519-signing` — pinned: `tests/test_polymarket_us_gateway.py:83` (headers present/signed), `tests/test_polymarket_us.py:77` (WS handshake headers). Note: no fixture with a >32-byte key file pinning the seed-truncation (`[:32]`).

## Polymarket US — positions API

- `pmus-positions-empty-glitch` — **DONE** (card 7fab301e), recon path: `tests/test_reconcile_positions.py::test_empty_pm_read_is_glitch_retried_then_recon_error` (3 retries then RECON error, never NAKED), `::test_empty_pm_read_recovers_on_retry`. Remaining edge: the runner-side keep-previous guard in `_tt_refresh` (closure; its cap-safety consequence is pinned by the fired-ratchet test instead).
- `pmus-positions-partial-stale-sticky` — **DONE** (card 7fab301e): `tests/test_reconcile_positions.py::test_partial_pm_read_missing_ledger_leg_is_degraded_not_naked` (DEGRADED, no NAKED, even across consecutive runs), `::test_real_naked_alerts_only_on_second_consecutive_run` (two-consecutive-runs alert rule).
- `pmus-positions-dict-keyed-by-slug` — pinned: `tests/test_venue_contracts.py:175` (P5).

## Polymarket US — market data / WS

- `pmus-ws-full-book-snapshots-offers` — pinned: `tests/test_polymarket_us.py:31` (offers = asks, sorted), `:51` (MARKET_DATA -> snapshot).
- `pmus-ws-auth-required-chunked-subs` — **GAP**. Suggested: fake-WS test with >150 slugs asserting multiple subscribe frames with distinct requestIds and full slug coverage.
- `pmus-metadata-tag-route-only` — partial: `tests/test_polymarket_us.py:12` pins normalization of tag-route market shape. **GAP**: no MockTransport test of `markets_by_tags` (dedupe across tags, nested events->markets).
- `pmus-books-freeze` — **GAP**. Suggested: unit tests for `pm_fresh`/`mark_pm_dead`/circuit-breaker (`scripts/sports_arb.py:112-221`): EWMA staleness, heal timeout -> blacklist, 5 deaths/10min -> halt.
- `pmus-private-order-ws` — **DONE** (card 7fab301e): `tests/test_exec_runner_quirks.py::test_ws_fill_hedged_once_when_poll_reports_same_cum` (real `run()` with a fake `websockets` module + local unix-socket recorder: WS FILL cum=3 hedges once; the 2s poll re-reporting cum=3 hedges nothing; WS cum=5 hedges only the +2 increment then pops the quote; ledger records 3 then 2).

## Polymarket international

- `pm-gamma-json-encoded-fields` — pinned: `tests/test_venue_adapters.py:68` (double-decoded fields), `:89` (outcome order not assumed).
- `pm-gamma-repeated-params` — **GAP**. Suggested: MockTransport test asserting `clob_token_ids` is sent as repeated params (never comma-joined).
- `pm-clob-book-reverse-ordering` — **DONE** (card 7fab301e): `tests/test_venue_contract_gaps.py::test_p8_clob_book_best_at_end_normalized_best_first` (`ClobRest.book` on a best-at-END fixture: asks high->low, bids low->high in; best-first out).
- `pm-ws-no-seq-numbers` — pinned: `tests/test_venue_adapters.py:101` (new-total deltas + synthesized seq), `:119` (PONG ignored, array frames), `tests/test_recorder.py:72` (task-level record from fake server). PING-send cadence itself not asserted.
- `pm-taker-base-fee-zero-override` — partial: `tests/test_fees.py:85`/`:92` pin the override semantics in the fee engine. **GAP**: no test of `ClobRest.taker_fee_overrides` mapping (0 -> override 0; nonzero/error -> keep schedule).

## Cross-venue / general

- `xv-429-at-rehearsal-proves-auth` — **DONE** (card 7fab301e): `tests/test_exec_runner_quirks.py::test_rehearsal_429_proves_auth_and_proceeds` (3x 429 -> proceeds into the socket loop), `::test_rehearsal_non_429_failure_aborts`.
- `xv-cold-tls-adds-hedge-latency` — **GAP** (timing behavior). Suggested: test that `conn_warmer` issues one authenticated GET per gateway per cycle (mocked sleep/clock).
- `xv-zombie-socket-on-reconnect` — partial: `tests/test_recorder.py:60` pins the recorder-side slow-subscriber drop. **GAP**: no test that the trader closes the old socket before reconnecting.
- `xv-graceful-shutdown-cancels-orders` — partial: `tests/test_quoter.py:186` pins kill-switch -> `cancel_all`. **GAP**: no test of SIGTERM -> finally-block cancel, nor of the sports probe's own-ids-only sweep.
- `xv-append-only-ledger-with-corrections` — pinned: `tests/test_ledger.py:49` (correction folds+hides), `:60` (open_baskets applies corrections), `tests/test_ledger_import_parity.py:49` (429-lost-value correction fixture, parity gate).
- `xv-settlement-skew` — **DONE** (card 7fab301e): `tests/test_reconcile_positions.py::test_settlement_skew_finalized_kalshi_market_suppresses_naked` (NAKED suppressed on two consecutive runs; positions.json row marked `settling`).
- `xv-tick-floored-limits` — pinned (card 7fab301e closed the gap): `tests/test_quoter.py:142` (jittered prices on the tick grid) + `tests/test_hedge_naked_legs.py::test_hedge_limit_floored_to_deci_cent_tick` (hedge-limit floor on a 0.001-step market, and the same limit on a penny market floors to 0.38).

## Totals

- Entries: 44 — Kalshi 15 (9 order + 6 market data), Polymarket US 17 (9 order + 3 positions + 5 market data), Polymarket intl 5, cross-venue 7.
- After card 7fab301e (2026-07-23): fully pinned 31, partial 9, full GAP 4
  (`pmus-ws-auth-required-chunked-subs`, `pmus-books-freeze`,
  `pm-gamma-repeated-params`, `xv-cold-tls-adds-hedge-latency`).
- Original inventory: fully pinned 18, partial 10, full GAP 16. (Every partial
  carries at least one untested load-bearing edge listed above.)
