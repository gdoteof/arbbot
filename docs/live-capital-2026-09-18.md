# Live capital and original funding

Original external contributions are $439.78 Kalshi and $500 PM-US. Geoff confirmed these are the only deposits; goodwill credits count as profit. `config/funding.yaml` is read only by dashboard reporting, never by the trading risk gate.

The armed trader reads both venues every 60 seconds and atomically publishes `data/exec/capital.json`. Risk and the dashboard share these observations. The unarmed shadow follows this file without credentials. New entries wait for the first successful poll and refuse cash observations older than 180 seconds. Failed reads preserve the last successful timestamp, rather than falling back to declared balances or a fixed bankroll.

Capital is available cash plus reserved cash plus position value. Kalshi supplies balance and portfolio value (the latter is cents unless a dollar field is present). PM-US available cash is buying power; reserved cash is current balance minus margin requirement minus buying power. Its collateral is already represented in position value and is not added again. Fractional signed positions are retained. Fresh local book bids mark longs; one minus asks marks shorts. Missing fresh quotes retain venue cashValue, with timestamps and coverage shown explicitly. These are estimated values before liquidation fees, not guaranteed proceeds.

The root dashboard now shows live capital, profit including unrealized value and goodwill, valuation sources, freshness, and risk policies. No manual report is required. Historical imported books remain at `/api/books/history`.

Removed the production fixed $980 bankroll and hand-entered cash seeds, including the installed armed override. Existing intentional policies remain: 85% deployment, 150-contract relationship ceiling, 2% tail allocation, and topic budgets. Exposure sizing still uses the existing contract-based risk measure; this change does not redefine that policy.

Validation: 942 relevant tests passed; Clippy, inline JavaScript syntax, and service-unit checks passed. The final removal of the obsolete startup cash prerequisite passed all 13 precondition tests. Built and restarted armed trader, shadow, and dashboard; verified running processes, automatic timestamp advances, $939.78 funding, and fresh book marks for all 20 PM-US positions. No connected browser was available for visual inspection.

## Kalshi position detail

The capital poll now walks all Kalshi position pages and retains signed fractional quantities. Existing fresh engine books independently mark each holding: YES at bid, NO at one minus ask. Missing quotes produce null values, never zero. Dashboard detail includes both venues and reports Kalshi marked coverage, subtotal, venue total, and the difference when coverage is complete. Kalshi reported equity remains authoritative for risk and profit reporting; position marks are a comparison, not an override. This adds the paginated position read each minute, using the existing shared API budget, without per-market REST quote requests.

Validation: all 945 relevant tests passed, including pagination, incomplete reads, fractional long/short valuation, missing quotes, and reconciliation without changing venue equity. Clippy and dashboard JavaScript syntax passed.

Live verification after deployment: all three services active; `/api/books` returned 20 nonzero Kalshi holdings, 19 with fresh book marks. `KXTIME-26-AI` lacked a usable fresh two-sided quote and remained explicitly unvalued. Its value remains included in Kalshi's account-level portfolio total. The dashboard correctly withheld a full reconciliation difference while that mark was missing.
