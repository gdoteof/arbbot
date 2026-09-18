# Recent exit accounting audit — 2026-09-18

Entry cost includes the entry's fees. Net = gross exit proceeds minus all-in entry
cost minus exit fees. Realized APR is simple annualization:
`net / entry_cost * 365.25 / held_days * 100`. It is not the return earned during
the holding period and is not compounded.

Checked the live `/api/trades` output against the ledger: all 21 September 18
exits (103 contracts) join to opening lots, have no excess quantity allocated to
those lots, and reproduce the displayed net and APR calculations independently.

Matched 19 exits (93 contracts) to exact venue order IDs in the trader journal.
Authenticated Kalshi fill history and PM-US order responses confirm the recorded
quantities and prices. Their actual fees differed slightly from the modeled
fees. Added 19 exit-fee corrections, retaining venue order IDs and fee provenance.
Verified the three highlighted Kalshi entry orders (client ID where recorded,
otherwise the unique market/price/size/time match) and added three entry-fee
corrections. Original rows, quantities, prices, timestamps and lot links remain
unchanged; the append-only ledger contains 22 auditable correction records.

## Three highlighted round trips, after reconciliation

| Lot entered UTC | Contracts | Entry cost incl. fees | Gross proceeds | Exit fees | Net | Held days | Annualized APR |
|---|---:|---:|---:|---:|---:|---:|---:|
| Fed cut, Sep 14 01:06 | 3 | $2.864100 | $2.967000 | $0.019800 | $0.083100 | 4.670990 | 226.8789% |
| BTC 130k, Sep 2 03:04 | 5 | $4.722800 | $4.950000 | $0.039800 | $0.187400 | 16.556898 | 87.5349% |
| BTC 130k, Sep 2 01:38 | 5 | $4.772800 | $4.950000 | $0.039800 | $0.137400 | 16.617060 | 63.2775% |

For the Fed example, principal is `3 × (0.089 + 0.86) = $2.847`.
The Kalshi entry fee is $0.0171, giving $2.8641 entry cost including that fee.
The PM-US maker entry fee remains modeled at zero. Exit fees are the reported
$0.0098 Kalshi plus $0.0100 PM-US; net is $0.0831. No entry fee is omitted or
subtracted twice.

## Reporting fixes and remaining qualifications

The dashboard previously marked fees settled if *any* leg had a fee. It now
requires every leg to report a fee, including an explicit zero. Entry costs, net,
and APR carry estimate markers while entry fees remain modeled; exit fees have
an independent marker. Derived-P&L tooltips now explain that reported fees are
preferred and missing fees estimated.

The three PM-US entry fees have not been independently fetched; they remain
modeled at zero, and their round trips are correctly marked estimated. The two
earlier September 18 exits outside the exact-order reconciliation remain modeled.
This audit does not make future exits automatically fee-settled: the writer still
uses modeled fees unless venue-reported fee records are supplied.

Holding periods use the ledger's basket-opening timestamp. In particular, the
Sep 2 01:38 BTC entry is a naked-hedge reconstruction carrying PM-US basis from
an earlier lot (`naked_hedge_lot_ts`); its timestamp is the hedge completion, not
independent proof of when that PM-US exposure first began. Its APR is a return
against the recorded basket basis/time, not a certified account cash-flow IRR.
No arbitrary timestamp or basis migration was made.

The ledger-wide two historical orphan-unwind warnings remain; none of the 21
recent exits audited here is an orphan. There are no malformed ledger lines,
unmatched corrections, unusable unwind quantities or unresolved swapped-leg
warnings in the dashboard output.

Validation: all 153 dashboard tests, Clippy with warnings denied, JavaScript
syntax check and diff whitespace check passed. Rebuilt/restarted only the
dashboard and verified the corrected amounts and fee-status flags through the
live API. The trader continued running.
