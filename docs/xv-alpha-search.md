# What else is exploitable, other than hold time

Follow-up to [kalshi-pm-pnl-asymmetry.md](kalshi-pm-pnl-asymmetry.md), which
established that the per-venue P&L split is one hedged basket rather than a
leak, and that PM-US delivers ~85-90% of every basis convergence.

That note's recommendation was to hold for less time. That is not actionable —
unwinding early is opportunistic, not scheduled. So this asks the other
question: with hold time off the table, what is left?

Three candidates. Two are real effects that cannot be traded. The third is the
binding constraint.

Tape: 48 days, both venues, 5s grid, rebuilt from Parquet after #108 (which is
what made the PM-US side of it readable at all).

## 1. The Kalshi -> PM-US lead is real, robust, and an order of magnitude too small

Setup: Kalshi mid moves >=1c in 60s, PM-US has NOT moved (<0.5c, so its quote
is stale), and both books are tight at signal time (PM-US spread <=5c, Kalshi
<=3c). The liquidity filter is not optional — the tape includes a stretch with
hundreds of near-dead PM-US markets whose median spread is 49c, and any average
over that population is noise. Filtered, the median PM-US spread at signal time
is 2c. 2,450 events across 41 pairs.

| horizon | n | PM-US mid follows | taker keeps, net of spread |
|---|---|---|---|
| 60s | 2,339 | +0.083 ± 0.014 | **-2.119 ± 0.026** |
| 120s | 2,211 | +0.120 ± 0.020 | **-2.161 ± 0.031** |
| 300s | 1,977 | +0.193 ± 0.028 | **-2.178 ± 0.038** |
| 600s | 1,654 | +0.305 ± 0.046 | **-2.339 ± 0.061** |
| 1800s | 821 | +1.102 ± 0.113 | **-1.805 ± 0.118** |

The effect is genuine — monotone in horizon, 6-10 sigma from zero — and it is
not one market. Leave-one-pair-out over all 41 pairs moves the 60s figure
between +0.07c and +0.09c and the 600s figure between +0.20c and +0.34c. There
is no pair whose removal breaks it.

It is simply far too small. To collect +0.08c at 60s a taker must cross a 2c
spread. The taker column is negative at every horizon, and it does not
approach zero even at 30 minutes.

(An earlier cut of this on 20 days found the signal collapsing when one pair
was dropped. That was a small-sample artifact; at n=2,339 it does not
reproduce.)

## 2. We are not adversely selected, so quote-skew has nothing to defend

If we cannot take, the fallback is to defend: skew resting PM-US quotes when
Kalshi moves, so we are not lifted at a stale price. That presumes we are being
run over.

For a resting PM-US quote on the side Kalshi just moved toward, how far the mid
travels *past* our price:

| horizon | n | mid travels past our quote by |
|---|---|---|
| 60s | 2,339 | -0.982 ± 0.018 |
| 300s | 1,977 | -0.895 ± 0.031 |
| 600s | 1,654 | -0.813 ± 0.049 |
| 1800s | 821 | -0.037 ± 0.114 |

Negative means the mid stays *inside* our quote. At every horizon, including 30
minutes, the market never comes through us.

This is the same fact as §1 seen from the other side: the follow-through is
smaller than the spread, so a stale quote is not actually stale enough to be
picked off. The two results are consistent, and together they close off both
the aggressive and the defensive version of the lead-lag trade.

## 3. Breadth is the binding constraint

The runner hard-gates on `vetted_by: human`. 19 of the 83 mapped `xvus-` pairs
with data on both venues clear it. Counting *episodes* — a contiguous run of
bars where an executable riskless lock is open,
`1 - (k_ask + (1 - pm_bid) + kalshi_fee)`, so one hour-long opportunity counts
once, not twelve:

| threshold | human tier | agent tier | ratio |
|---|---|---|---|
| >=2c | 45 | 693 | 15.4x |
| >=4c | 13 | 327 | 25.2x |
| >=6c | 11 | 152 | 13.8x |

Independently reproduced on the live 60s tape (17x / 45x / 23x on the subset of
days it covers). Not one human-vetted pair appears in the top 20 by episode
count.

### The episode count is an upper bound, and must be discounted

A "riskless lock" of 25c is not riskless. It means the two contracts are not
the same contract — precisely the failure vetting exists to catch, as with
`israel-pm-26`, where a persistent 5c crossing turned out to be a
repeat-election divergence. The signature is a pair locked a large fraction of
its life: a real dislocation is transient, a mismapping is permanent.
`time-poty-26-zohranmamdani` is locked 54% of its life,
`france-pres-27-jordanbardella` 43%, `bestai-26dec-xai` 38%. Those are traps,
not inventory.

### In dollars, at the clip we actually send

Depth-aware, pricing each episode at its best bar as
`min(kalshi ask depth, PM-US bid depth) x lock`, capped at the engine's real
clip of 25 contracts, over 48 days:

| tier | episodes | total | per day |
|---|---|---|---|
| human | 45 | $31 | **$1** |
| agent | 693 | $480 | **$10** |

So the 15-25x in episode *count* is roughly a **10x in money**: about $10/day of
riskless crossings sits behind the gate against $1/day in front of it, on a book
currently making ~$1.84/day. Median dollars per episode is ~$1; the total is
carried by a handful of fat episodes, and thin depth at the crossing price is
what keeps the number small.

Restricting to the plausible subset — pairs locked <20% of their life with >=5
episodes — leaves 22 pairs worth ~$5/day at clip 25.

This measures the take-take channel only. It says the take-take channel is
exhausted *on the pairs we may trade*, not in general — which is the missing
half of the "absorption, not the cap" finding. Vetting also widens the
maker-hedge channel, where most realized P&L actually comes from; that upside is
real but is not measured here and should not be assumed proportional.

## Vetting queue

Ranked by dollars, restricted to transient locks (<20% of life locked, >=5
episodes). These are candidates for **human** vetting — `vetted_by: human` must
never be stamped programmatically, and nothing in this note touches the
registry.

| pair | episodes | % life locked | median lock | $ (clip 25) |
|---|---|---|---|---|
| `xvus-bestai-26dec-openai` | 43 | 12.8% | 5.4c | $33 |
| `xvus-gpt6-ladder-2026-09-30` | 26 | 8.2% | 4.3c | $26 |
| `xvus-france-pres-27-brunoretailleau` | 39 | 14.8% | 2.8c | $20 |
| `xvus-fedcut-26-usfed-2026-cut` | 24 | 19.8% | 2.9c | $19 |
| `xvus-brazil-pres-26-renansantos` | 14 | 11.4% | 2.8c | $18 |
| `xvus-btcmax-26-31-2026-100k` | 24 | 8.5% | 3.8c | $15 |
| `xvus-aliens-26-12-31-2026` | 14 | 8.3% | 2.5c | $13 |
| `xvus-gpt6-ladder-2026-12-31` | 21 | 10.9% | 4.0c | $12 |
| `xvus-france-pres-27-gabrielattal` | 17 | 10.8% | 5.3c | $11 |
| `xvus-time-poty-26-taylorswift` | 13 | 15.2% | 3.7c | $11 |
| `xvus-france-pres-27-jeanlucmelenchon` | 10 | 11.4% | 4.0c | $9 |
| `xvus-brazil-pres-26-flaviobolsonaro` | 13 | 7.1% | 2.6c | $9 |

The `gpt6-ladder`, `btcmax` and `aliens` entries are the cleanest — 8% of life
locked means the crossing genuinely opens and closes rather than standing there.
`bestai-26dec-*` is the largest family but sits at 13%, near enough the trap
band to want a careful read of both contracts' terms; `fedcut` at 19.8% is right
on the line.

## Caveats

- Locks are computed from top-of-book prices and depths. A resting order is not
  a fill, and the take-take path must still win the race.
- `% life locked` is a heuristic for mismapping, not a proof. Each queued pair
  needs its actual contract terms read before it is vetted.
- The lead-lag test conditions on both books being tight; that is the regime we
  would trade in, but it is a minority of tape.
