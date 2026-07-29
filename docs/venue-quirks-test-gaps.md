# Venue Quirk Registry — Test-Gap Inventory

One row per entry in `docs/venue-quirks.md`: which regression test currently
pins the behavior, or **GAP** with a one-line suggested test. "Partial" means
the core behavior is pinned but a load-bearing edge is not.

## Kalshi — order API

- `kalshi-create-endpoint-410` — **GAP**. Suggested: MockTransport test asserting create POSTs `/portfolio/events/orders`, cancel DELETEs `/portfolio/events/orders/{id}`, list/get GET `/portfolio/orders`.
- `kalshi-orders-list-history-paginated` — pinned: `tests/test_venue_contracts.py:62` (K1: full history across cursor pages, resting filtered client-side).
- `kalshi-signature-path-only` — pinned: `tests/test_venue_contracts.py:83` (K2).
- `kalshi-fill-count-fp-plain-count` — pinned: `tests/test_venue_contracts.py:96` (K3), `:102` (K3b: 0 on missing/dry).
- `kalshi-cancel-404-is-success` — pinned: `tests/test_venue_contracts.py:108` (K4: 404 success, 500 raises).
- `kalshi-postonly-cross-rejected-400` — pinned: `tests/test_venue_contracts.py:116` (K5: the 400 raising), `tests/test_quoter.py` (`test_one_tick_book_joins_best_bid_never_crosses`: 1-tick-book clamp; `test_failed_place_backs_off_min_requote_s`: failed-place backoff).
- `kalshi-v2-yes-axis` — pinned: `tests/test_kalshi_gateway.py:16` (body shape), `:27` (ask on YES axis), `:35` (IOC taker), `:50` (4-dp fixed-point).
- `kalshi-deci-cent-ticks` — **GAP**. Suggested: unit test of `kalshi_ask()` parsing `price_ranges[].step` (0.001 market) plus quoter `_tick`/quantize sending only tick-aligned prices.
- `kalshi-positions-and-settlement-fields` — **GAP**. Suggested: fixture test parsing `/portfolio/positions` (`position_fp` -> signed count) and a `settle_baskets` gate test (only `finalized` + `result in {yes,no}` settles).

## Kalshi — market data

- `kalshi-both-ladders-are-bids` — pinned: `tests/test_venue_adapters.py:17` (NO bid -> YES ask), `:30` (sort + zero filter), `tests/test_kalshi_ws.py:22` (WS snapshot path).
- `kalshi-ws-delta-is-change-not-total` — pinned: `tests/test_kalshi_ws.py:22` (change -> total, level removal), `:47` (new level from zero).
- `kalshi-ws-seq-per-subscription` — pinned: `tests/test_kalshi_ws.py:55` (regression for the 2026-07-20 11:45 incident).
- `kalshi-trade-needs-own-seq-stream` — pinned: `tests/test_kalshi_ws.py:102` (regression for the traded-markets-die P1).
- `kalshi-ws-snapshots-only-on-subscribe` — partial: `tests/test_kalshi_ws.py:143` pins gap -> `get_snapshot` request, which pins the frozen PYTHON stack only, and pins behaviour the production Rust recorder deliberately does NOT have (#32 deleted the request — the venue never answered it). Rust side: `kalshi::tests::a_wire_gap_does_not_discard_the_next_tickers_delta` pins what the gap branch does instead. **GAP**: no test of the REST-snapshot resync fallback (`recorder.py:298-305`) or the welcome/30s-rebroadcast heal.
- `kalshi-market-data-auth-split` — partial: the fake-WS tests exercise the signed handshake incidentally. **GAP**: no test asserting the recorder chooses WS-with-recorder-key vs credential-free REST poll.
- `kalshi-count-fp-is-fractional` — pinned: `fills::kalshi_tests::a_fractional_fill_pair_sums_to_the_venues_whole_contract` (the real `2.13`+`1.87` 4-lot), `:a_sub_contract_piece_is_banked_not_called_unreadable` (the real `0.98`+`4.02` 5-lot), `:fractional_pieces_floor_the_same_total_in_either_order` (arrival order cannot change the total), `:banked_dust_is_visible_and_falls_when_its_sibling_lands`, `:count_fp_is_parsed_as_exact_hundredths` and `:a_numeric_count_fp_floors_rather_than_rounds` (no f64, third decimal truncates down on both branches).

## Polymarket US — order API

- `pmus-create-omits-fill-data` — pinned: `tests/test_venue_contracts.py:143` (P1: create carries no fill data), `tests/test_record_fill.py:49` (retries past the async-fill race).
- `pmus-commission-is-total` — pinned: `tests/test_venue_contracts.py:151` (P2), `tests/test_record_fill.py:49` (fees recorded as TOTAL, not x qty).
- `pmus-ioc-fill-report-lag` — partial: `tests/test_record_fill.py:49` pins the recorder-path retry only. **GAP**: no test of `_confirm_ioc_fill` (`scripts/sports_arb.py:239`) against a gateway whose `filled_qty` lags N calls, nor of the post-cancel delayed re-checks.
- `pmus-unfilled-counts-against-cap` — **GAP**. Suggested: unit test that a reported-unfilled PM leg still increments the fired/cap ratchet (`main.py:433-437`) and blocks the next fire.
- `pmus-cancel-requires-market-slug` — **GAP** (only dry-run cancel is tested, `tests/test_polymarket_us_gateway.py:75`). Suggested: MockTransport test asserting live cancel body contains `marketSlug`, `ValueError` without it, and non-2xx raises.
- `pmus-create-bare-body-preview-wrapped` — **GAP**. Suggested: MockTransport test asserting create posts the BARE body and preview posts `{"request": ...}`.
- `pmus-order-id-field-is-id` — pinned: `tests/test_venue_contracts.py:167` (P4: extraction chain), `tests/test_quoter.py` (`test_no_order_id_in_response_is_place_failure`: no-id response => place-failure, no phantom RestingQuote).
- `pmus-intent-buy-short` — pinned: `tests/test_polymarket_us_gateway.py:38` (ask -> BUY_SHORT), `:45` (place_short IOC), `:58` (post-only rest), `:67` (taker not post-only).
- `pmus-ed25519-signing` — pinned: `tests/test_polymarket_us_gateway.py:83` (headers present/signed), `tests/test_polymarket_us.py:77` (WS handshake headers). Note: no fixture with a >32-byte key file pinning the seed-truncation (`[:32]`).

## Polymarket US — positions API

- `pmus-positions-empty-glitch` — **GAP**. Suggested: unit test that an empty positions read with prior held state keeps previous state (`main.py:365-374`) / raises for retry (`reconcile_positions.py:71-75`).
- `pmus-positions-partial-stale-sticky` — **GAP**. Suggested: recon test feeding a partial positions fixture (missing a ledger-expected slug) and asserting DEGRADED + no NAKED output; plus the two-consecutive-runs alert rule.
- `pmus-positions-dict-keyed-by-slug` — pinned: `tests/test_venue_contracts.py:175` (P5).

## Polymarket US — market data / WS

- `pmus-ws-full-book-snapshots-offers` — pinned: `tests/test_polymarket_us.py:31` (offers = asks, sorted), `:51` (MARKET_DATA -> snapshot).
- `pmus-ws-auth-required-chunked-subs` — **GAP**. Suggested: fake-WS test with >150 slugs asserting multiple subscribe frames with distinct requestIds and full slug coverage.
- `pmus-metadata-tag-route-only` — partial: `tests/test_polymarket_us.py:12` pins normalization of tag-route market shape. **GAP**: no MockTransport test of `markets_by_tags` (dedupe across tags, nested events->markets).
- `pmus-books-freeze` — **GAP**. Suggested: unit tests for `pm_fresh`/`mark_pm_dead`/circuit-breaker (`scripts/sports_arb.py:112-221`): EWMA staleness, heal timeout -> blacklist, 5 deaths/10min -> halt.
- `pmus-private-order-ws` — **GAP**. Suggested: fake private-WS server pushing `EXECUTION_TYPE_FILL` and asserting `_process_fill` hedges the increment exactly once when the 2s poll also reports the same fill (idempotence on cumQuantity).

## Polymarket international

- `pm-gamma-json-encoded-fields` — pinned: `tests/test_venue_adapters.py:68` (double-decoded fields), `:89` (outcome order not assumed).
- `pm-gamma-repeated-params` — **GAP**. Suggested: MockTransport test asserting `clob_token_ids` is sent as repeated params (never comma-joined).
- `pm-clob-book-reverse-ordering` — **GAP** (sorting exercised only on trivial fixtures). Suggested: `/book` fixture with best-at-END arrays asserting best-first normalized output.
- `pm-ws-no-seq-numbers` — pinned: `tests/test_venue_adapters.py:101` (new-total deltas + synthesized seq), `:119` (PONG ignored, array frames), `tests/test_recorder.py:72` (task-level record from fake server). PING-send cadence itself not asserted.
- `pm-taker-base-fee-zero-override` — partial: `tests/test_fees.py:85`/`:92` pin the override semantics in the fee engine. **GAP**: no test of `ClobRest.taker_fee_overrides` mapping (0 -> override 0; nonzero/error -> keep schedule).

## Cross-venue / general

- `xv-429-at-rehearsal-proves-auth` — **GAP**. Suggested: unit test of the rehearsal loop with a gateway raising 429 three times asserting the runner proceeds (and aborts on non-429 exhaustion).
- `xv-cold-tls-adds-hedge-latency` — **GAP** (timing behavior). Suggested: test that `conn_warmer` issues one authenticated GET per gateway per cycle (mocked sleep/clock).
- `xv-zombie-socket-on-reconnect` — partial: `tests/test_recorder.py:60` pins the recorder-side slow-subscriber drop. **GAP**: no test that the trader closes the old socket before reconnecting.
- `xv-graceful-shutdown-cancels-orders` — partial: `tests/test_quoter.py:186` pins kill-switch -> `cancel_all`. **GAP**: no test of SIGTERM -> finally-block cancel, nor of the sports probe's own-ids-only sweep.
- `xv-append-only-ledger-with-corrections` — pinned: `tests/test_ledger.py:49` (correction folds+hides), `:60` (open_baskets applies corrections), `tests/test_ledger_import_parity.py:49` (429-lost-value correction fixture, parity gate).
- `xv-settlement-skew` — **GAP**. Suggested: recon test where an imbalanced pair's Kalshi market is `finalized` asserting the NAKED alert is suppressed and the row marked settling.
- `xv-tick-floored-limits` — partial: `tests/test_quoter.py:142` bounds jittered prices to the tick grid. **GAP**: no test of the hedge-limit floor on a 0.001-step market (`hedge_naked_legs.py:180-182`).

## Totals

- Entries: 45 — Kalshi 16 (9 order + 7 market data), Polymarket US 17 (9 order + 3 positions + 5 market data), Polymarket intl 5, cross-venue 7.
- Fully pinned: 19. Partial: 10. Full GAP: 16. (Every partial carries at least one untested load-bearing edge listed above.)
