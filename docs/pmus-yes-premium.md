# The PM-US YES premium, and why ~40% of our booked edge cannot converge

Third note in the cross-venue series, after
[kalshi-pm-pnl-asymmetry.md](kalshi-pm-pnl-asymmetry.md) (the venue split is one
hedged basket) and [xv-alpha-search.md](xv-alpha-search.md) (the lead-lag is real
but untradeable).

Both of those answered "is the PM-US leg's profit a leak?" with *no, it is the
hedge working*. Neither answered the question actually being asked: **if PM-US
is reliably the leg in the green, what is the exploitable content of that?**

It has one, it is not breadth, and it is not hold time.

## PM-US YES is structurally rich, not randomly rich

Unconditional, over every mapped pair and every bar where both books are tight
(spread <=5c so the mid means something): 829,446 bars, 80 pairs, 48 days.

```
basis = PM-US mid - Kalshi mid, both quoted on YES

  mean   +1.239c        median +1.000c
  share of bars with PM-US RICH: 62.1%

     PM cheap by >2c    12.4%
     PM cheap 0.5-2c    14.0%
     flat +/-0.5c       14.9%
     PM rich 0.5-2c     23.7%
     PM RICH by >2c     34.9%
```

58 of 80 pairs have a positive mean basis, and the tails are asymmetric 2.8x
(34.9% rich by >2c against 12.4% cheap by >2c). This is not a symmetric basis
that we happen to lean on — PM-US carries a persistent YES premium.

The ledger is the mirror image. Of 220 priceable hedged entries, priced by each
leg's *recorded* direction rather than by assuming the usual shape:

| entry direction | n | mean edge | median |
|---|---|---|---|
| Kalshi YES + PM-US NO (short the premium) | 184 | +8.09c | +5.70c |
| Kalshi NO + PM-US YES (long the premium) | 36 | +1.78c | +4.00c |

So 84% of the book is short the PM-US YES premium, at roughly 4.5x the edge of
the reverse. **That is why the PM-US leg is the green one.** It is not a fill
asymmetry and not a leak: we are systematically short a one-way flow premium,
the premium decays, and the PM-US leg books the decay while the Kalshi leg —
inert, median post-entry move -0.05c — books the fee.

## The exploitable part: ~40% of the premium never converges

A premium that fully mean-reverted would be pure profit and nothing would need
changing. It does not.

Method: split each pair's bars in half. The FIRST half estimates that pair's
premium; the SECOND half is tested, so the mean is never fitted on the bars it
predicts. Non-overlapping 24h windows, basis lagged one bar off the return
window so the two never share an endpoint. 942 windows, 76 pairs.

The premium is a real, estimable quantity — out-of-sample correlation between a
pair's first-half and second-half mean basis is **+0.566** across 76 pairs.

Regressing the 24h PM-US move on the raw basis and the pair's own premium,
**clustered by family** (13 families — `bestai-*`, `time-poty-*`,
`france-pres-27-*` share underlyings, so windows within one are not
independent):

| term | coef | clustered se | t |
|---|---|---|---|
| intercept | +0.014 | 0.128 | +0.11 |
| raw basis | **-0.489** | 0.048 | **-10.09** |
| pair premium | **+0.198** | 0.045 | **+4.42** |

A negative raw coefficient means a wide basis converges — about 49% of it
within 24h. A **positive premium coefficient means a high standing premium does
not converge**; it pushes back against the convergence the raw basis predicts.

Leave-one-family-out moves the premium coefficient only between +0.167 and
+0.230. No family drives it.

The ratio is the correction: `0.198 / 0.489 = 0.41`.

**Subtract 0.41x the pair's own standing premium from the basis before reading
it as edge** -- but see [Deployability](#deployability-the-rule-does-not-survive-a-real-time-estimator)
below, which is the section that decides whether this can be acted on. It
largely cannot, yet.

Note this is a *partial* correction, and fully demeaning would overshoot. An
earlier cut of this bucketed on demeaned basis and appeared to double the
convergence (-1.06c to -2.18c per window), but that bucket spanned 2c to 50c of
raw basis, so it was mostly selecting a bigger raw basis. Holding raw basis in
narrow bands and regressing is what isolates the premium's own contribution, and
it is 0.41, not 1.0.

## Deployability: the rule does not survive a real-time estimator

The +0.198 coefficient uses each pair's FIRST-HALF MEAN as its premium. That is
a long, clean estimate computed with hindsight, and no live engine can have it.
A shippable estimator has to be trailing and causal, and it is much noisier:
predicting the next day's mean basis, a 7d trailing mean gets MAE 2.20c against
a population mean premium of only 1.24c.

Noise in a regressor attenuates its coefficient, so the same design was re-run
with the premium computed strictly from prior days:

| premium estimator | n | raw basis | premium coef | clustered se | t |
|---|---|---|---|---|---|
| first-half mean (hindsight) | 942 | -0.489 | **+0.198** | 0.045 | **+4.42** |
| 7d trailing mean | 216 | -0.463 | +0.103 | 0.153 | +0.68 |
| expanding mean | 742 | -0.428 | +0.073 | 0.074 | +0.99 |
| expanding + shrink to global | 1409 | -0.359 | +0.138 | 0.098 | +1.40 |
| pooled across family | 2007 | -0.322 | -0.004 | 0.069 | -0.06 |
| EWMA, 7d half-life | 742 | -0.435 | +0.082 | 0.082 | +1.01 |

**No causal estimator clears significance.** The best is shrinkage at t=+1.40,
and leave-one-family-out on the 7d version spans 0.00 to 0.41 — it contains
zero. The premium correction is therefore NOT deployable as it stands, and the
0.41 figure should not be shipped.

Two things are learned rather than lost:

- **The premium is pair-specific, not family-level.** Pooling across a family
  destroys the effect outright (t=-0.06) while nearly tripling n, so a family
  does NOT share a premium and pooling is the wrong way to buy precision.
- **Shrinkage is the promising direction.** It is the only variant that both
  raises t and nearly doubles usable n, because it can price a pair with a short
  history instead of discarding it. Its `K0` was set to 5.0 by hand and has not
  been tuned.

**The raw-basis coefficient, by contrast, is robust to all of this** — it sits
between -0.32 and -0.49 across every estimator and every clustering, at t=-4.84
to -10.09. About 46% of a basis converges within 24h regardless of how the
premium is measured. That number is usable today; the premium correction is not.

The bottleneck is estimator precision, and it is a data problem before it is a
modelling one: requiring a few prior days with >=200 tight-book bars each is
what cut n from 942 to 216. More tape per pair is the cheapest way to buy the
significance back.

## What it would have changed on the book we actually put on

Joining the tape premium to the ledger by `relationship_id`, 198 of 220 entries
match a pair with enough tape:

| relationship | n | premium | raw edge | corrected | haircut |
|---|---|---|---|---|---|
| `xvus-fedcut-26-usfed-2026-cut` | 33 | +2.10c | +7.10c | +6.24c | +0.86c |
| `xvus-time-poty-26-zohranmamdani` | 25 | +5.94c | +8.00c | **+5.56c** | **+2.44c** |
| `xvus-btcmax-26-31-2026-130k` | 25 | +4.23c | +5.00c | **+3.26c** | **+1.74c** |
| `xvus-brazil-pres-26-flaviobolsonaro` | 24 | -1.23c | +5.00c | +5.00c | +0.50c |
| `xvus-nobel-peace-26-sudansemergencyresponser` | 17 | +1.96c | +5.00c | +4.20c | +0.80c |
| `xvus-france-pres-27-brunoretailleau` | 7 | +3.68c | +5.20c | **+3.69c** | **+1.51c** |
| `xvus-nobel-peace-26-doctorswithoutborders` | 8 | -1.57c | +3.00c | +2.36c | +0.64c |

Book-wide the mean edge falls from +7.76c to +6.77c and the median from +5.20c
to +4.50c. Across 1,185 contracts, **$12.29 of booked edge is premium-attributable
— structurally unable to converge.** On a book making ~$1.84/day that is about a
week of P&L sitting in baskets with nothing to give.

The re-ranking is where the value is, not the haircut:

```
entries clearing a 3c bar: raw 182 -> corrected 159   (13% would not have been taken)
entries clearing a 4c bar: raw 163 -> corrected 119   (27% would not have been taken)
entries clearing a 5c bar: raw 129 -> corrected  92   (29% would not have been taken)
```

At a 4-5c entry bar, **27-29% of what the engine currently reads as an
opportunity is fair value wearing a costume** — and those are exactly the
baskets that then sit for days, because there was never anything there to
converge. Meanwhile `brazil-pres-26-flaviobolsonaro` (premium -1.23c) and
`nobel-peace-26-doctorswithoutborders` (-1.57c) are being *under*-credited: a 3c
basis on a pair that normally sits 1.5c cheap is a genuine 4c dislocation.

This also dissolves the hold-time puzzle from the first note — edge flat at
5-6c/ct regardless of hold, APR collapsing from 1138% to 25% purely on the
denominator. Hold time was never an independent dial. It is an *outcome* of
entry selection: a basket entered on standing premium has nothing to converge,
so it sits until something unrelated moves it. Fix the entry signal and the
hold-time distribution follows, without ever scheduling an unwind.

## Dead end recorded: the fee shape

Worth writing down because it looks compelling and is not.

`maker_leg_indices` returns `vec![0, 1]` for cross-venue relationships, so the
engine quotes maker on BOTH legs and crosses whichever hedge is needed. Which
leg we end up crossing is therefore a market outcome, and it is lopsided:

| venue | role | legs | contracts | fees | per contract |
|---|---|---|---|---|---|
| kalshi | maker | 61 | 340 | $0.00 | 0.000c |
| kalshi | taker | 159 | 891 | $3.52 | **0.395c** |
| polymarket_us | maker | 81 | 427 | $0.00 | 0.000c |
| polymarket_us | taker | 139 | 804 | $0.12 | **0.015c** |

Crossing Kalshi costs **26x** what crossing PM-US costs, and we cross Kalshi on
159 of 220 entries — the expensive fee, paid on the leg that contributes no
convergence. That sounds like free money.

It is not: 0.395c against a +6.00c median edge is 6.6% of it. And the shape is
not assignable anyway — baskets where we rested on Kalshi carry +3.87c mean edge
against +8.28c for those where we crossed it, because we cross the leg that is
running away. Chasing the cheap shape would forfeit more edge than the fee costs.

## Caveats

- The premium is estimated from tight-book mids. A pair needs enough two-sided
  tape before its premium is worth trusting; 0.566 is the correlation across
  pairs that had >=500 bars per half.
- A large persistent premium is ambiguous with a MISMAPPED pair, where the two
  contracts are not the same thing and the "premium" is a permanent offset
  ([[war-contract-vetting]]'s `israel-pm-26` case). The 0.41 coefficient says
  the population is ~60% convergent, so these are not pure traps — but the
  correction is protective either way, since it demands MORE edge on exactly the
  high-premium pairs that trap risk concentrates in.
- 942 windows over 13 family clusters. The clustered t of +4.42 is the honest
  one; the naive t was +4.26, so clustering did not flatter it here.
- This is an entry-pricing change and therefore an engine-behaviour change. It
  is not deployed and should not be without an explicit decision and a digest
  re-pin -- and on the evidence in Deployability it should not be deployed at
  all yet, because the coefficient does not survive a real-time estimator.
- Endogeneity was checked and cleared: we are short the premium on 184 of 220
  baskets, so our own flow pushes PM-US YES down, but the premium is +0.92c and
  positive on 77% of the 26 pairs we have NEVER traded. It is not our footprint.
