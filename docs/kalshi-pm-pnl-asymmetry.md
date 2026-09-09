# Why the P&L lands on PM-US and the losses land on Kalshi

Research sprint, 2026-09-09. Question: realised P&L runs about **−$0.19/day on
Kalshi and +$2.03/day on PM-US**. Is money leaking from one venue to the other,
are we filled reliably in one direction, and is there alpha in it?

Short answers: **no leak** (it is one hedged basket booked across two venues),
**yes we are one-directional** (by construction, not by adverse selection), and
**yes there is alpha** — Kalshi is the price leader and PM-US does ~85–90% of
every basis convergence.

## 1. The split is real, and it is not a leak

FIFO reconstruction of `data/exec/trades.jsonl` (659 legs, zero unmatched
sells), realised P&L only:

| venue | legs | realised | settled fees | per active day |
|---|---:|---:|---:|---:|
| kalshi | 331 | **−$3.17** | $3.52 | −$0.19 |
| polymarket_us | 328 | **+$34.45** | $0.12 | +$2.03 |
| net | | **+$31.28** | | +$1.84 |

Over 17 days with any realisation. All of it sits in `maker-exit` (+$20.49) and
`unwind` (+$10.91) — it only appears when a basket is *closed*.

Decomposing the 72 `maker-exit` closes (258 contracts), separating fee drag
from actual price movement:

| component | $ | c/ct |
|---|---:|---:|
| Kalshi price move (ex-fee) | −2.70 | −1.05 |
| Kalshi entry fee (carried in `maker_exit_k_basis`) | −2.23 | −0.87 |
| **= Kalshi leg as booked** | **−4.94** | **−1.91** |
| PM-US price move (ex-fee) | +24.70 | +9.57 |
| PM-US entry fee | −1.30 | −0.50 |
| **= PM-US leg as booked** | **+23.40** | **+9.07** |
| **total** | **+18.46** | **+7.16** |

The basis fields were verified against the entry records they close: all 72
matched bit-exactly on `closes_ts`, and `k_basis − entry_price` is +0.88c mean
— the Kalshi entry fee, correctly capitalised. This is *not* the
`costPerShare` residual trap, and not a leg inversion.

**The Kalshi leg is a hedge. It costs fees plus spread and earns approximately
nothing, which is exactly what a hedge should do.** Its median price move over
the hold is **−0.05c** — indistinguishable from zero. The −1.05c mean is a tail:
three closes in two names (Sudan ERR, Pope Leo) that genuinely repriced 12–14c
over 12–32 days carry $1.68 of the $2.70. Excluding them the mean is −0.63c,
about one Kalshi tick — i.e. the spread we cross, not a market move against us.

## 2. Yes, we are filled in one direction — by construction

- **195 open entries: 167 are Kalshi-YES + PM-NO** ("short the rich PM"), 28 the
  other way.
- **70 of 72 exits cross as Kalshi taker** (`--maker-exit-take`).
- Fees to date: Kalshi **taker $11.47**, Kalshi **maker $0.04**, PM-US taker $1.30.

There is **no adverse-selection signature on Kalshi fills**. If we were being
picked off, Kalshi would drift against us immediately after each fill; instead
the post-entry Kalshi move is 36 down / 30 up / 6 flat with a median of −0.05c.

## 3. The alpha: Kalshi leads, PM-US follows

Unconditional ToB study — 83 mapped pairs, 18 days (2026-07-26 … 08-22),
forward-filled book (top-of-book is a step function; stale quotes dropped after
12h). **The basis is read one bar before the return window opens**, so bid-ask
bounce in the mid cannot manufacture the reversion being looked for — this
matters because PM-US's median spread is 3.0c against Kalshi's 1.0c, and the
noisier mid would otherwise fake it.

24h forward move, by basis state:

| basis at t₀ | n | ΔKalshi | ΔPM-US | PM share of convergence |
|---|---:|---:|---:|---:|
| PM-US rich (> +2c) | 3,236 | +0.26 ±0.04 | **−2.10 ±0.11** | 89% |
| tight (\|b\| < 2c) | — | ~0 | ~0 | — |
| Kalshi rich (< −2c) | 1,491 | −0.52 ±0.10 | **+2.67 ±0.17** | 84% |

### Controls

- **Not longshot decay.** Unconditional 24h drift with no basis filter is
  ΔKalshi −0.02 ±0.02c, ΔPM-US −0.20 ±0.05c. Essentially zero on both venues.
  The PM move is entirely *conditional on the basis*.
- **Not a price-level artifact.** The pattern holds inside every band —
  <10c, 10–25c, 25–50c, >50c — with PM share 77–108% throughout.
- **Not a permanent level offset.** Median basis is +1.0c (PM slightly rich),
  so a ±2c excursion is a real signal, not the resting state. |basis| > 2c
  occurs 50% of the time (PM rich 34%, Kalshi rich 16%).
- **Not overlapping-window inflation.** Hourly windows overlap 24×, so the
  standard errors above are optimistic. Re-run non-overlapping and clustered by
  pair (each pair contributes one mean):

  | basis state | pairs | ΔKalshi | ΔPM-US | ratio |
  |---|---:|---:|---:|---:|
  | PM rich > +2c | 32 | −0.22 ±0.24 | **−2.36 ±0.57** | **10.8×** |
  | Kalshi rich < −2c | 14 | −0.88 ±0.59 | **+3.50 ±1.03** | 4.0× |

  PM-US still moves an order of magnitude more than Kalshi, at 4.1σ, with
  Kalshi indistinguishable from zero.

**Conclusion: the basis is a forecast of the PM-US price, not a symmetric
convergence.** Kalshi is the anchor.

### Caveat worth respecting

Split by vetting tier, the **PM-rich direction holds in both** (human-vetted 89%
PM share at 24h, agent-vetted 89%). The **Kalshi-rich direction only holds on
agent-vetted pairs** — on human-vetted pairs PM is inert there (+0.06 ±0.03c)
and Kalshi does the converging. Agent-vetted mappings are the less trustworthy
ones, and a large "basis" on a mismapped pair is a mapping error rather than a
mispricing. Do not lean on the Kalshi-rich cell.

## 4. What this is worth

**Hold time is by far the biggest lever.** Realised APR on our own 72 exits:

| hold | n | contracts | edge c/ct | APR |
|---|---:|---:|---:|---:|
| <1d | 3 | 11 | 6.46 | 6763% |
| 1–3d | 25 | 86 | 6.05 | **1138%** |
| 3–7d | 12 | 37 | 5.12 | 412% |
| 7–21d | 17 | 74 | 13.01 | 392% |
| >21d | 15 | 50 | 2.05 | **25%** |

Edge per contract is roughly flat at 5–6c regardless of hold — long holds earn
the same cents over far more days. The tape agrees: of the 72h convergence,
~60% lands in the first 24h (+2.36c of +3.86c). Median hold today is 5.36 days.

Ranked, and none of these are applied — they are all engine-behaviour changes:

1. **Take a smaller lock earlier.** The exit trigger currently waits for a lock
   that a median 5.4-day hold pays for. Tightening it toward a 24–72h target
   should dominate every other change here.
2. **Kalshi taker fees are 300× maker fees** ($11.47 vs $0.04) and are ~0.9–1.4
   c/ct — roughly the entire Kalshi leg "loss". Any Kalshi leg shifted from
   taker to maker is close to free money, subject to queue position.
3. **Keep the directional skew.** The 86/14 tilt toward short-the-rich-PM is
   empirically the correct tilt on the pairs we trust. Do not "balance" it.
4. **The basis can time the PM exit** — rest the PM leg while the basis is still
   wide, cross once it has collapsed, rather than choosing shape on spread alone.

## 5. Limitations

- The tape study covers 2026-07-26 … 08-22; live trading continued to 09-08.
  The ToB rollup has no coverage after 08-22, so the most recent three weeks of
  trading are outside the tape window.
- 258 contracts across 72 closes is a small realised sample; the tape study is
  the load-bearing evidence, not the realised P&L.
- `data/venue/*` snapshots are from 2026-07-27, so `arb-books` and `/api/books`
  are stale and were not used here.
