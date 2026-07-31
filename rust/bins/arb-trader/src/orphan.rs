//! Hedge obligations this engine minted and never discharged — recovered
//! across a restart from the engine's OWN record, never from a venue snapshot.
//!
//! THE INCIDENT (2026-07-29 00:53:50, arbbot-trader-m3, armed). A take-take
//! filled leg 1 — sold 1x `cbpac-usfed-2026-cut` @0.17 on PM-US — and the
//! Kalshi hedge did not fill: the ask it was anchored to was pulled 150ms
//! later, and 0.1300 stayed outside the 1c slip budget on a 0.1080 anchor. The
//! obligation lived in `pending_hedges` with its naked alarm ticking. At 01:34
//! the process restarted and came back with `hedges_pending 0`, having
//! forgotten it owed a Kalshi buy while the PM-US short was still real at the
//! venue.
//!
//! `book_basket` writes `data/exec/trades.jsonl` only when the hedge FILLS, so
//! an obligation that never filled leaves no trace there — which is why
//! `seed_exposure_from_ledger` cannot see it. But the obligation is not
//! actually unrecorded: `Intent::HedgeNeeded` is emitted the instant it is
//! minted and goes to the `--out` stream, which is opened APPEND-ONLY and
//! therefore survives the restart. And `book_basket` stamps leg 1 with
//! `order_id` = the maker order the obligation was minted from. So the two
//! files join on an IDENTITY:
//!
//! ```text
//!   owed(order)   = sum of HedgeNeeded.qty for that maker order   (--out)
//!   booked(order) = sum of basket qty whose leg 1 names it        (ledger)
//!   undischarged  = owed - booked   > 0
//! ```
//!
//! ...with ONE addition, because the join has a blind spot with a live writer
//! in it. `book_basket` always stamps leg 1 with the maker order id, but the
//! process that actually completes these obligations is not this one:
//! `scripts/hedge_naked_legs.py` (arbbot-hedge.timer, every 5 minutes) writes
//! legs carrying no `order_id` at all. Its baskets are the discharge and the
//! join cannot see them. So a basket naming no order on any leg is credited
//! against the obligation's HEDGE MARKET instead, bounded by its own `qty` and
//! by the obligation being OLDER than it — see `draw_down`, which also explains
//! why leaving this out was not the conservative choice it looks like.
//!
//! That join is why this module reads no venue at all, and it is worth being
//! explicit about what that buys, because the alternative — differencing venue
//! positions against the ledger — fails on all four counts:
//!
//!   * FALSE ADOPTION. The join is an identity on an order id, not a difference
//!     of two noisy snapshots. `positions()` on both gateways reads ONE PAGE and
//!     ignores the cursor, and the PM-US endpoint intermittently serves empty or
//!     drops positions outright (2026-07-22, which is why the Python reader
//!     takes two agreeing reads). A dropped Kalshi long over a correctly-read
//!     PM short reads exactly like a naked short.
//!   * ATTRIBUTION. A bare venue position knows no relationship and no anchor.
//!     Here the anchor comes back EXACTLY — the price at which the basket was
//!     proven profitable, captured at place time precisely because the burst
//!     that fills you is the burst that gaps your book (`hedge_anchor`). It is
//!     not recoverable from a position: PM-US reports one average price over
//!     the whole holding, and on the incident's own market that was 74 hedged
//!     contracts averaged with the 1 that was not.
//!   * NO HISTORY API. PM-US exposes current positions only, so a venue diff is
//!     a snapshot difference that cannot tell a three-hour-old orphan from a leg
//!     that filled 200ms ago whose partner is still on the wire. This join has a
//!     timestamp per obligation.
//!   * API BUDGET. Zero venue calls. The positions endpoints share a rate budget
//!     with the order path, where 429s are a live concern.
//!
//! WHAT THIS DELIBERATELY DOES NOT DO: hedge. `arbbot-hedge.timer` already
//! completes naked legs from venue truth every 5 minutes, profitable-only, and
//! it is enabled and firing — it has been logging this very obligation as
//! `xvus-fedcut-26-usfed-2026-cut naked x1` since 01:15. A second hedger on the
//! same Kalshi key is not a backup, it is a DOUBLE HEDGE: both IOCs fill, we
//! end up long 2 against a short 1, and that is directional exposure the
//! opposite way. `hedges_overfilled` would not even catch it, because the
//! Python's fill is credited to no obligation of ours — it arrives as an
//! unattributed fill on an account-wide channel. Coordination would need the
//! other party to take a lock, and the Python stack is frozen; a lock only one
//! side takes is not a lock. So this reports, loudly and with the full
//! attribution the venue-truth owner cannot produce, and leaves acting to the
//! single owner that already has it.
//!
//! UPDATE, and it does not change the paragraph above so much as give it a
//! second subject. `--positions-recon-act` is now a venue-truth completer inside
//! THIS binary, so "the single owner that already has it" is a choice between
//! two rather than a fact about one — and the double-hedge argument applies
//! unchanged to running both. What this module does is still the same: it
//! reports. It does not hedge, and it could not: an undischarged obligation is
//! reconstructed from the INTENT stream, and the completer prices from the
//! LEDGER, so an obligation that never booked a basket has no cost basis for
//! either of them to act on. See `naked_act`, which refuses exactly that case by
//! name.

use serde_json::Value;
use std::collections::BTreeMap;

/// One maker order whose hedge obligations were not all booked.
///
/// Aggregated per maker order rather than per obligation because the LEDGER
/// side can only be aggregated: a basket record names the maker order, not
/// which of its obligations it discharged. That loses nothing — an anchor is
/// captured once per ORDER (`drain_intents` registers it on the place, and
/// every obligation minted from that order's fills carries the same one), so
/// the anchor below is exact however many obligations the order minted.
#[derive(Debug, PartialEq)]
pub struct Undischarged {
    /// The maker (or take-take leg 1) order the obligations were minted from.
    pub maker_order_id: String,
    /// The market the hedge is owed ON — the other leg.
    pub hedge_market: String,
    /// Where the hedge leg stood when the basket was proven profitable.
    pub anchor_price: String,
    pub owed: i64,
    pub booked: i64,
    /// Tape time of the EARLIEST obligation on this order — how long the leg
    /// has been naked.
    pub first_ts: f64,
}

impl Undischarged {
    pub fn missing(&self) -> i64 {
        self.owed - self.booked
    }
}

/// Obligations minted and not booked, oldest first.
///
/// `intents` is the raw `--out` stream (one intent per line, append-only across
/// restarts). `ledger` is `crate::ledger::read`'s output.
///
/// A line that will not parse is SKIPPED here, unlike in the ledger reader. The
/// two files carry different things: an unreadable ledger line is a basket of
/// unknown size on unknown markets, so it refuses to arm, while the intent
/// stream is a decision log whose every entry is also present in the WAL. A
/// torn intent line therefore costs at most one obligation's visibility, and
/// refusing to start over it would take the engine down for a cosmetic file.
pub fn undischarged(intents: &str, ledger: Vec<Value>) -> Vec<Undischarged> {
    // Corrections first, for the same reason `open_exposure` applies them: a
    // correction that restates a basket's `qty` restates how much of the
    // obligation was really booked, and reading the superseded number would
    // invent an orphan.
    let ledger = crate::ledger::apply_corrections(ledger);

    // An `unwound` record does NOT reduce `booked`. Unwinding closes a
    // position that was hedged; the hedge still happened. Netting it out here
    // would resurrect every closed basket as an orphan.
    let mut booked: BTreeMap<String, i64> = BTreeMap::new();
    let mut unjoined: Vec<Unjoined> = Vec::new();
    for r in &ledger {
        if r.get("status").and_then(|v| v.as_str()) != Some("open") {
            continue;
        }
        // Read as f64 and truncate, because `ledger::open_exposure` reads this
        // same field with `f64_of` — and `as_i64()` answers None to a JSON
        // `2.0`. A float `qty` would then be seeded by the ledger while
        // producing neither a `booked` credit nor an `unjoined` entry here,
        // which is the double count this whole path exists to close, reopened
        // by a type.
        //
        // The field is NOT integer-typed in this file, whatever the records
        // that happen to exist today look like: `settle_baskets.py` writes
        // `float(t.get("qty", 0))` and its records are already there. They are
        // `status: "unwound"`, so they reach neither this collection nor
        // `open_exposure`'s open sum, and no writer emits a float `qty` on an
        // OPEN basket right now — so this is latent rather than live. What is
        // not latent is the divergence: two readers of one field disagreeing
        // about its type is a defect whoever writes the next record, and the
        // ledger's own reader is the one to match.
        //
        // Truncation errs toward a smaller credit, i.e. toward reporting
        // exposure rather than hiding it — the same FLOOR rule #42 established
        // for Kalshi's fractional fill counts, and for the same reason: a total
        // that can only understate what was discharged can only over-report
        // what is still naked.
        let qty = r.get("qty").and_then(|v| v.as_f64()).unwrap_or(0.0) as i64;
        let legs: Vec<&Value> =
            r.get("legs").and_then(|v| v.as_array()).into_iter().flatten().collect();
        let mut names_an_order = false;
        for leg in &legs {
            if let Some(oid) = leg.get("order_id").and_then(|v| v.as_str()) {
                *booked.entry(oid.to_string()).or_default() += qty;
                names_an_order = true;
            }
        }
        if !names_an_order && qty > 0 {
            unjoined.push(Unjoined {
                ts: r.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0),
                markets: legs
                    .iter()
                    .filter_map(|l| l.get("market_id").and_then(|v| v.as_str()))
                    .map(str::to_string)
                    .collect(),
                left: qty,
            });
        }
    }
    // Oldest first, so the drawdown below is a deterministic function of the
    // file rather than of its ordering.
    unjoined.sort_by(|a, b| a.ts.total_cmp(&b.ts));

    let mut owed: BTreeMap<String, Undischarged> = BTreeMap::new();
    for l in intents.lines() {
        let Ok(v) = serde_json::from_str::<Value>(l) else { continue };
        // `Intent` is `#[serde(untagged)]`, so the discriminator is the field.
        let Some(market) = v.get("hedge_needed").and_then(|x| x.as_str()) else { continue };
        let (Some(oid), Some(qty)) = (
            v.get("order_id").and_then(|x| x.as_str()),
            v.get("qty").and_then(|x| x.as_i64()),
        ) else {
            continue;
        };
        let anchor = v.get("anchor_price").and_then(|x| x.as_str()).unwrap_or_default();
        // UNDATED IS `INFINITY`, NOT ZERO, AND THE SIGN OF THAT DEFAULT IS THE
        // WHOLE SAFETY RULE. `draw_down` refuses a basket OLDER than the
        // obligation (`b.ts < first_ts`), so on the basket side `unwrap_or(0.0)`
        // fails closed — an undated basket is never eligible. The identical
        // default here INVERTS it: `first_ts = 0.0` makes every real basket
        // newer, so every unjoined basket on that market becomes eligible, the
        // obligation is fully credited, and `retain` below drops it — silently
        // adopting a naked leg with no report line at all. `serde_json`
        // serialises a non-finite `f64` as `null`, which is one way in, so the
        // filter is on usability and not just presence.
        let ts = v
            .get("ts")
            .and_then(|x| x.as_f64())
            .filter(|f| f.is_finite())
            .unwrap_or(f64::INFINITY);
        let e = owed.entry(oid.to_string()).or_insert_with(|| Undischarged {
            maker_order_id: oid.to_string(),
            hedge_market: market.to_string(),
            anchor_price: anchor.to_string(),
            owed: 0,
            booked: 0,
            first_ts: ts,
        });
        e.owed += qty;
        e.first_ts = e.first_ts.min(ts);
    }

    let mut out: Vec<Undischarged> = owed
        .into_values()
        .map(|mut u| {
            u.booked = booked.get(&u.maker_order_id).copied().unwrap_or(0);
            u
        })
        .collect();
    // Oldest first: the longest-naked leg is the one to read about first. The
    // map is already ordered by id and `sort_by` is stable, so ties are still
    // deterministic — which the drawdown below depends on.
    out.sort_by(|a, b| a.first_ts.total_cmp(&b.first_ts));
    for u in out.iter_mut() {
        u.booked += draw_down(&mut unjoined, &u.hedge_market, u.first_ts, u.missing());
    }
    out.retain(|u| u.missing() > 0);
    out
}

/// An open basket that names NO maker order on any leg, and so cannot be joined
/// by the identity this module is built on.
///
/// There is a live writer of exactly these, and it is the one that DISCHARGES
/// the obligations counted here: `scripts/hedge_naked_legs.py` completes naked
/// legs from venue truth every 5 minutes (`arbbot-hedge.timer`, enabled) and
/// emits legs carrying only venue/market_id/side/role/qty/yes_price. The Rust
/// `book_basket` always stamps leg 1 with its maker order id, so "no leg names
/// an order" is precisely "this engine did not book it".
struct Unjoined {
    ts: f64,
    /// Both legs' markets. The obligation is owed on ONE of them and the record
    /// does not say which, so either may match — but `left` is shared, so one
    /// basket can never discharge more than its own `qty`.
    markets: Vec<String>,
    left: i64,
}

/// Contracts an unjoined basket can be said to have discharged for an
/// obligation on `market` minted at `first_ts`, up to `need`.
///
/// WHY THIS EXISTS. `main.rs` seeds the census into the risk view, and
/// `seed_exposure_from_ledger` seeds `ledger::open_exposure` — which keys on
/// `relationship_id` (ledger.rs) while `booked` above keys on a LEG's
/// `order_id`. For a basket carrying no leg `order_id` the two disagree: the
/// ledger seed counts it, `booked` does not, so `missing()` stays at the full
/// amount and the SAME contracts are seeded twice, under the same relationship
/// and the same class. `--out` is append-only across restarts, so that phantom
/// never self-heals and repeats on every startup — a monotone overstatement
/// against a class cap that is already close to its ceiling.
///
/// WHY THE TIME BOUND IS THE DISCRIMINATOR. A basket written BEFORE an
/// obligation was minted cannot have discharged it — that is physics, not a
/// heuristic — and it is what keeps this from crediting the historical Python
/// baskets against a Rust obligation on the same market. They are about half of
/// today's open records, counted AFTER `apply_corrections` because that is the
/// only population this function ever sees; only a small minority of them are
/// naked-leg-hedge records, and the rest predate the Rust engine's obligations
/// on the same markets. (Counts omitted: the ledger lives under the gitignored
/// `data/` and this repository is public.)
///
/// Simulated against the live intent tape and ledger, this credits NOTHING
/// today — no unjoined basket even names an obligation's hedge market — so the
/// bound is not currently load-bearing. It exists for the writer that will.
///
/// Going forward the population is narrower still. Of the enabled timers,
/// `arbbot-hedge.timer` is the only writer of a NEW unjoined `open` basket, and
/// completing these obligations is its entire job. `arbbot-settle.timer` is
/// also enabled and also writes this file, but `scripts/settle_baskets.py`
/// emits `unwound` closing records, which the `status == "open"` filter above
/// never collects. `arbbot-taketake.timer`, `arbbot-unwind.timer` and every
/// Python trader unit are disabled.
///
/// A basket later UNWOUND is still offered here, deliberately and on the same
/// rule as `booked`: unwinding closes a position that was hedged, and the hedge
/// still happened. It costs nothing — `open_exposure` already nets that basket
/// to zero, so crediting the obligation against it leaves both seeds at zero,
/// which is the truth for a closed position.
///
/// The remaining error modes, both bounded:
///   * OVER-crediting — silencing a naked leg — needs an unjoined basket on the
///     same market, written after the mint, that is not the discharge.
///   * UNDER-crediting, from the oldest-first greedy: where two obligations on
///     one market are both eligible, the older takes the basket even if the
///     newer is what it discharged. The newer is then reported naked and
///     seeded. Deterministic and bounded by the basket's `qty`, and it errs
///     toward reporting exposure rather than hiding it.
///
/// Doing nothing was not the safe alternative it looks like: it reports a
/// completed basket as naked forever AND double counts its capital forever.
fn draw_down(unjoined: &mut [Unjoined], market: &str, first_ts: f64, need: i64) -> i64 {
    let mut taken = 0;
    for b in unjoined.iter_mut() {
        if taken >= need {
            break;
        }
        if b.left <= 0 || b.ts < first_ts || !b.markets.iter().any(|m| m == market) {
            continue;
        }
        let take = (need - taken).min(b.left);
        b.left -= take;
        taken += take;
    }
    taken
}

/// What to print. A pure function of the census so the wording — which is the
/// entire deliverable of a report-only feature — is pinned by a test.
///
/// `rel_of` maps a hedge market id to `(relationship id, venue)` for the
/// relationships THIS run quotes. A miss is reported rather than hidden: it
/// means the obligation is on a relationship outside this process's
/// `--rel-prefix`, so this engine could not discharge it even if it were
/// allowed to, and only the venue-truth owner can.
pub fn report(
    u: &[Undischarged],
    rel_of: &BTreeMap<String, (String, String)>,
    now: f64,
) -> Vec<String> {
    if u.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for o in u {
        let where_ = match rel_of.get(&o.hedge_market) {
            Some((rel, venue)) => format!("on {venue} ({rel})"),
            None => "on a relationship OUTSIDE this run's --rel-prefix, which this \
                     process could not hedge even if it were allowed to"
                .to_string(),
        };
        // An obligation whose `ts` was unusable carries `INFINITY` (see
        // `undischarged`), which is what keeps it out of the drawdown. Saying
        // "0 minutes ago" for it would read as "just now", which is the one
        // thing it definitely is not.
        let age = if o.first_ts.is_finite() {
            format!("{:.0} minutes ago", ((now - o.first_ts) / 60.0).max(0.0))
        } else {
            "at an unreadable time (no usable `ts` on the mint)".to_string()
        };
        out.push(format!(
            "[hedge] UNDISCHARGED FROM A PREVIOUS RUN: {}x {} {where_} — obligation(s) \
             minted by order {} (owed {}, booked {}) at anchor {}, {age}. \
             The other leg is REAL at the venue and this basket is NOT complete.",
            o.missing(),
            o.hedge_market,
            o.maker_order_id,
            o.owed,
            o.booked,
            o.anchor_price,
        ));
    }
    // Said once, after the list: the reason is the same for all of them, and a
    // repeated paragraph is a paragraph nobody reads.
    out.push(
        "[hedge] This engine's HEDGE RETRY will not touch these — the obligations belong to \
         a previous process and nothing here can re-mint them. Naked-leg completion from \
         venue truth belongs to exactly one owner, and there are now two spellings of it: \
         arbbot-hedge.timer (every 5 minutes, profitable-only) and this binary's own \
         --positions-recon-act. RUNNING BOTH IS A DOUBLE HEDGE, not a backup — both IOCs \
         fill and we end up long against our own short, and the other party's fill is \
         credited to no obligation here, so `hedges_overfilled` would still read 0. CHECK \
         WHICH ONE IS ARMED: systemctl --user is-active arbbot-hedge.timer, and this \
         process's own startup banner. NOTE that a venue-truth owner can only complete \
         what our ledger vouches for a cost basis on, and an obligation that never booked \
         a basket has none — so an orphan listed above may be one NEITHER owner will close."
            .to_string(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).expect("test fixture")
    }

    /// The obligation, as `Intent::HedgeNeeded` really serialises it — copied
    /// from the live m3 stream, line 228772.
    const MINT: &str = r#"{"anchor_price":"0.1080","hedge_needed":"KXRATECUT-26DEC31","order_id":"t1785282065001","qty":1,"ts":1785300830.6798358}"#;

    /// A basket `book_basket` really wrote, with leg 1 naming its maker order.
    fn basket(oid: &str, qty: i64) -> Value {
        v(&format!(
            r#"{{"ts":1.0,"relationship_id":"r1","qty":{qty},"status":"open","legs":[
                 {{"venue":"polymarket_us","market_id":"P","side":"ask","role":"taker",
                   "qty":{qty},"yes_price":"0.17","order_id":"{oid}"}},
                 {{"venue":"kalshi","market_id":"K","side":"bid","role":"taker",
                   "qty":{qty},"yes_price":"0.11"}}]}}"#
        ))
    }

    /// THE INCIDENT. An obligation was minted, the hedge never filled, so no
    /// basket was ever booked against it — and the restart must see it.
    ///
    /// This is the exact pair of records the live files hold: `hedges_pending`
    /// read 0 after the 01:34 restart because nothing looked here.
    #[test]
    fn an_obligation_whose_hedge_never_filled_survives_the_restart() {
        let got = undischarged(MINT, vec![]);
        assert_eq!(
            got,
            vec![Undischarged {
                maker_order_id: "t1785282065001".into(),
                hedge_market: "KXRATECUT-26DEC31".into(),
                anchor_price: "0.1080".into(),
                owed: 1,
                booked: 0,
                first_ts: 1785300830.6798358,
            }]
        );
        assert_eq!(got[0].missing(), 1);
    }

    /// ...and the anchor that comes back is the one the basket was PROVEN
    /// profitable at, not whatever the book is offering now. That is the whole
    /// reason this reads the intent stream instead of a venue position: 0.1080
    /// is unrecoverable from a PM-US holding of 75 contracts averaged together.
    #[test]
    fn the_recovered_anchor_is_the_original_one() {
        assert_eq!(undischarged(MINT, vec![])[0].anchor_price, "0.1080");
    }

    /// The other 5 obligations in the live stream, which all booked. A census
    /// that flagged a completed basket would be asking a human to hedge a leg
    /// that is already hedged — the exact false adoption this must never do.
    #[test]
    fn a_booked_obligation_is_not_an_orphan() {
        let mint = r#"{"anchor_price":"0.0400","hedge_needed":"KXNOBELPEACE-26-DJT","order_id":"t2","qty":5,"ts":1785257243.9}"#;
        assert!(undischarged(mint, vec![basket("t2", 5)]).is_empty());
    }

    /// A PARTIALLY booked obligation reports only what is still missing. The
    /// hedge filling 2 of 5 books 2, and 3 are still naked.
    #[test]
    fn a_partial_booking_leaves_only_the_remainder() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m9","qty":5,"ts":10.0}"#;
        let got = undischarged(mint, vec![basket("m9", 2)]);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].owed, got[0].booked, got[0].missing()), (5, 2, 3));
    }

    /// Two partial maker fills on ONE order are two obligations
    /// (`PendingHedge`'s I4), and they share one anchor because the anchor is
    /// registered on the ORDER. Both the owed side and the booked side
    /// aggregate, so 4 + 3 owed against 4 booked leaves 3.
    #[test]
    fn two_obligations_on_one_maker_order_aggregate() {
        let mints = concat!(
            r#"{"anchor_price":"0.40","hedge_needed":"K","order_id":"m1","qty":4,"ts":20.0}"#,
            "\n",
            r#"{"anchor_price":"0.40","hedge_needed":"K","order_id":"m1","qty":3,"ts":30.0}"#,
        );
        let got = undischarged(mints, vec![basket("m1", 4)]);
        assert_eq!((got[0].owed, got[0].booked, got[0].missing()), (7, 4, 3));
        assert_eq!(got[0].first_ts, 20.0, "the age is the EARLIEST obligation's");
    }

    /// An UNWOUND basket was hedged — closing a position is not un-hedging it.
    /// Netting unwinds out here would resurrect every basket ever closed as a
    /// naked leg, which is a census nobody would read twice.
    #[test]
    fn an_unwound_basket_is_still_a_discharged_obligation() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m5","qty":5,"ts":10.0}"#;
        let ledger = vec![
            basket("m5", 5),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":5}"#),
        ];
        assert!(undischarged(mint, ledger).is_empty());
    }

    /// A correction that restates the booked qty is honoured — the ledger's own
    /// rule, and reading the superseded number would invent an orphan.
    #[test]
    fn a_correction_to_the_booked_qty_is_applied() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m7","qty":5,"ts":10.0}"#;
        let ledger = vec![
            basket("m7", 2),
            v(r#"{"status":"correction","relationship_id":"r1","corrects_ts":1.0,"fields":{"qty":5}}"#),
        ];
        assert!(
            undischarged(mint, ledger).is_empty(),
            "the correction says all 5 were booked after all"
        );
    }

    /// Other intent kinds are not obligations. `Intent` is untagged, so the
    /// only thing separating a `HedgeNeeded` from a `Place` or a `Skip` is the
    /// field — and `Place` carries `order_id` and `qty`-shaped fields too.
    #[test]
    fn places_skips_and_cancels_are_not_obligations() {
        let stream = concat!(
            r#"{"count":5,"order_id":"m1","place":"K","price":"0.04","side":"bid","taker":true,"ts":1.0,"venue":"kalshi"}"#,
            "\n",
            r#"{"skip":["class cap: 341+5 > 343.00"],"ts":2.0}"#,
            "\n",
            "not json at all\n",
        );
        assert!(undischarged(stream, vec![]).is_empty());
    }

    /// The report names the relationship and venue when this run quotes it...
    #[test]
    fn the_report_names_the_relationship_and_says_who_owns_the_hedge() {
        let u = undischarged(MINT, vec![]);
        let mut rel = BTreeMap::new();
        rel.insert(
            "KXRATECUT-26DEC31".to_string(),
            ("xvus-fedcut-26-usfed-2026-cut".to_string(), "kalshi".to_string()),
        );
        let lines = report(&u, &rel, 1785300830.6798358 + 2460.0);
        assert_eq!(lines.len(), 2, "one per obligation, then the ownership note");
        assert!(lines[0].contains("1x KXRATECUT-26DEC31"), "{}", lines[0]);
        assert!(lines[0].contains("on kalshi (xvus-fedcut-26-usfed-2026-cut)"), "{}", lines[0]);
        assert!(lines[0].contains("anchor 0.1080"), "{}", lines[0]);
        assert!(lines[0].contains("41 minutes ago"), "{}", lines[0]);
        assert!(
            lines[1].contains("DOUBLE HEDGE") && lines[1].contains("arbbot-hedge.timer"),
            "the reason for NOT acting, and where to look instead: {}",
            lines[1]
        );
        assert!(
            lines[1].contains("--positions-recon-act"),
            "there are now TWO spellings of the venue-truth owner and the operator has to \
             know which is armed: {}",
            lines[1]
        );
    }

    /// ...and says so plainly when it does not, because that is the case where
    /// this process is not even a candidate to fix it.
    #[test]
    fn an_obligation_outside_this_runs_relationships_says_so() {
        let u = undischarged(MINT, vec![]);
        let lines = report(&u, &BTreeMap::new(), 1785300830.6798358);
        assert!(lines[0].contains("OUTSIDE this run's --rel-prefix"), "{}", lines[0]);
    }

    /// A clean book prints NOTHING. A census that speaks on every start is a
    /// census whose one real line gets scrolled past.
    #[test]
    fn a_clean_start_is_silent() {
        assert!(report(&[], &BTreeMap::new(), 0.0).is_empty());
    }

    // ---- the writer that discharges these obligations names no order id ----

    /// A basket exactly as `scripts/hedge_naked_legs.py` writes it: legs
    /// carrying venue/market_id/side/role/qty/yes_price and no `order_id`.
    fn python_hedge(ts: f64, qty: i64, kalshi: &str) -> Value {
        v(&format!(
            r#"{{"ts":{ts},"relationship_id":"r1","title":"r1 (naked-leg hedge)",
                 "qty":{qty},"strategy":"take-take","status":"open","legs":[
                 {{"venue":"kalshi","market_id":"{kalshi}","side":"yes","role":"taker",
                   "qty":{qty},"yes_price":"0.11"}},
                 {{"venue":"polymarket_us","market_id":"P","side":"no","role":"taker",
                   "qty":{qty},"yes_price":"0.86"}}]}}"#
        ))
    }

    /// THE DEFECT the `order_id` join could not see. `arbbot-hedge.timer` owns
    /// naked-leg completion — this module's own startup banner says so — and it
    /// writes no leg `order_id`, so `booked` stayed 0 while
    /// `ledger::open_exposure` (which keys on `relationship_id`) counted the
    /// basket. The obligation was reported naked forever, and `main.rs` seeded
    /// the same contracts a second time on every startup.
    #[test]
    fn a_basket_that_names_no_maker_order_still_discharges_the_obligation() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#;
        assert_eq!(undischarged(mint, vec![]).len(), 1, "naked while nothing has hedged it");
        assert!(
            undischarged(mint, vec![python_hedge(20.0, 5, "K")]).is_empty(),
            "the timer completed it; the ledger seed already carries those contracts"
        );
    }

    /// ...and only the REMAINDER survives a partial completion, exactly as a
    /// partial `order_id` booking does.
    #[test]
    fn a_partial_unjoined_booking_leaves_only_the_remainder() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#;
        let got = undischarged(mint, vec![python_hedge(20.0, 2, "K")]);
        assert_eq!((got[0].owed, got[0].booked, got[0].missing()), (5, 2, 3));
    }

    /// THE BOUND that keeps this from crediting history. A basket written
    /// BEFORE the obligation was minted cannot have discharged it — and open
    /// baskets with no leg `order_id` dominate the live ledger, only a small
    /// minority of them naked-leg-hedge records. The rest predate the Rust
    /// engine's obligations on the same markets. Crediting one would silence a
    /// genuinely naked leg, which is the false adoption this module's header
    /// exists to refuse.
    #[test]
    fn a_basket_written_before_the_obligation_cannot_have_discharged_it() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#;
        let got = undischarged(mint, vec![python_hedge(5.0, 5, "K")]);
        assert_eq!(got.len(), 1, "an older basket is somebody else's position");
        assert_eq!(got[0].missing(), 5);
    }

    /// ...AND THE DEFAULT FOR AN UNDATED OBLIGATION MUST FAIL THE SAME WAY.
    ///
    /// The basket side defaults an unreadable `ts` to 0.0, which fails CLOSED —
    /// `b.ts < first_ts` holds, so it is never eligible. The identical default
    /// on the OBLIGATION side inverts the rule: `first_ts = 0.0` makes every
    /// real basket newer, so every unjoined basket on that market becomes
    /// eligible, the obligation is fully credited, and `retain` drops it — a
    /// naked leg adopted in silence, with no report line at all. `INFINITY` is
    /// the default that keeps the sign of the comparison honest.
    #[test]
    fn an_obligation_with_no_usable_timestamp_is_never_credited_away() {
        for bad_ts in ["null", "\"nope\""] {
            let mint = format!(
                r#"{{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":{bad_ts}}}"#
            );
            let got = undischarged(&mint, vec![python_hedge(20.0, 5, "K")]);
            assert_eq!(got.len(), 1, "ts {bad_ts} silently adopted a naked leg");
            assert_eq!(got[0].missing(), 5);
            // ...and it says the age is unknown rather than claiming "0 minutes
            // ago", which would read as "this just happened".
            let lines = report(&got, &BTreeMap::new(), 1e9);
            assert!(lines[0].contains("unreadable time"), "{}", lines[0]);
        }
    }

    /// A basket whose `qty` is a JSON float is still a basket. `as_i64()`
    /// answers None to `2.0` while `ledger::open_exposure` reads the same field
    /// as an f64 — so the ledger would seed those contracts while this path
    /// credited nothing, which is the double count reopened by a type.
    /// `settle_baskets.py` already writes a float `qty`, so the field is not
    /// integer-typed in this file; its records are `unwound`, which is why no
    /// OPEN basket trips this today. Two readers of one field must agree about
    /// its type regardless of which records happen to exist.
    #[test]
    fn a_float_qty_basket_is_read_the_same_way_the_ledger_reads_it() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#;
        let float_qty = v(
            r#"{"ts":20.0,"relationship_id":"r1","qty":5.0,"status":"open","legs":[
                 {"venue":"kalshi","market_id":"K","qty":5.0},
                 {"venue":"polymarket_us","market_id":"P","qty":5.0}]}"#,
        );
        assert_eq!(
            crate::ledger::open_exposure(vec![float_qty.clone()]).get("r1"),
            Some(&5.0),
            "the ledger seed counts it"
        );
        assert!(undischarged(mint, vec![float_qty]).is_empty(), "so this must too");
    }

    /// ...and a basket on a DIFFERENT market never matches, however recent.
    #[test]
    fn an_unjoined_basket_on_another_market_is_not_this_obligations_hedge() {
        let mint = r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#;
        let got = undischarged(mint, vec![python_hedge(20.0, 5, "OTHER")]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].missing(), 5);
    }

    /// ONE basket discharges at most its own `qty`, however many obligations
    /// could match it. The record names BOTH legs' markets and does not say
    /// which one the hedge was owed on, so either may match — the shared budget
    /// is what stops a 5-lot basket from clearing 10 contracts of obligation.
    #[test]
    fn one_unjoined_basket_cannot_discharge_more_than_it_bought() {
        let mints = concat!(
            r#"{"anchor_price":"0.04","hedge_needed":"K","order_id":"m1","qty":5,"ts":10.0}"#,
            "\n",
            // the other leg of the same relationship, owed the other way
            r#"{"anchor_price":"0.86","hedge_needed":"P","order_id":"m2","qty":5,"ts":11.0}"#,
        );
        let got = undischarged(mints, vec![python_hedge(20.0, 5, "K")]);
        assert_eq!(got.len(), 1, "one of the two is discharged, not both");
        assert_eq!(
            (got[0].maker_order_id.as_str(), got[0].missing()),
            ("m2", 5),
            "and the OLDER obligation is credited first, deterministically"
        );
    }
}
