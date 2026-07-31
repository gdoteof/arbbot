//! The two things a naked-leg hedger must refuse to do, as pure predicates —
//! port of the guards in `scripts/hedge_naked_legs.py` (arbbot-hedge.timer,
//! every 5 minutes, live).
//!
//! Both are REFUSALS, and that is the whole reason they are here as separate,
//! individually-tested units rather than as two `if` statements inside a sweep.
//! A venue-truth sweep's happy path is "PM-US is short 3 and Kalshi is long 0,
//! so buy 3 Kalshi YES". Each rule below removes a case from that path, and in
//! each case the cost of losing the rule silently is an order against a
//! position that is not ours to complete:
//!
//!   * [`ProbeOwnership`] — the market belongs to a research probe.
//!   * [`imbalance_action`] — the missing leg is on PM-US, which this hedger
//!     has no way to price or place.
//!
//! Neither predicate reads a venue, a socket, or a file. The sweep does the
//! reading; these decide.

use serde_json::Value;
use std::collections::BTreeSet;

// ---------------------------------------------------------------------------
// Rule 1 — ownership exclusion
// ---------------------------------------------------------------------------

/// The `owner` prefix that marks a research-probe claim. `ledgerdb.owned_by`
/// selects on `owner LIKE 'probe-%'`; the prefix test lives here instead of in
/// the sweep's SQL so that it is pinned by a test rather than by a query
/// string. The distinction is load-bearing: `arbbot-trader` and `probe-leadlag`
/// both own positions, and only the second kind is excluded.
pub const PROBE_OWNER_PREFIX: &str = "probe-";

/// How far back in a probe's own log a claim is still honoured.
///
/// The Python reads `read_text().splitlines()[-800:]`, which makes probe
/// ownership a SLIDING RECENCY WINDOW over a file rather than a claim with a
/// lifetime — see the ownership caveats on [`ProbeOwnership`]. Ported verbatim,
/// because the number is the behaviour.
pub const PROBE_LOG_TAIL_LINES: usize = 800;

/// Probe log actions that do NOT establish ownership: the probe was reasoning
/// out loud, not trading. Every other action does — including the `skip:*`
/// actions, which is not obviously right (see [`ProbeOwnership`]).
const DRY_RUN_ACTIONS: [&str; 2] = ["dry_quote", "dry_run_would_trade"];

/// The set of PM-US slugs a research probe is working, and therefore the set
/// the hedger must keep its hands off.
///
/// THE CONTRACT. Trading and research run against the same two venue accounts.
/// Research manages its own positions end to end; the trading side must never
/// look at a research position, see one leg missing, and "complete" it. A probe
/// that is short PM-US on purpose does not want a Kalshi YES bought against it
/// — that turns its open experiment into somebody else's basket, and the probe
/// then flattens a leg it does not know is paired. `scripts/unwind_positions.py`
/// enforces the same contract from the other end, and the shape of the two
/// enforcements is worth noticing: the unwinder alerts a human and skips when a
/// basket's `strategy` tag is `pmus-maker-probe` or `ml-leadlag-probe`, i.e. it
/// filters on OUR OWN ledger tag. The hedger cannot, because a naked leg by
/// definition has no basket record to carry a tag. It has only a venue position
/// and a slug, so it must ask the question the other way round: is this slug
/// one a probe is working?
///
/// TWO SOURCES, UNIONED, AND BOTH ARE WEAKER THAN THEY LOOK. Build with
/// [`ProbeOwnership::claim_rows`] and [`ProbeOwnership::absorb_probe_log`].
///
///   * The `ownership` table is documented in the Python as the "authoritative
///     source". It is EMPTY (checked 2026-07-31 against `data/exec/trading.db`)
///     and `scripts/compile_strategies.py` calls it "the inert `ownership`
///     table", to be replaced by compiled `strategy_claim` rows. Nothing has
///     ever called `claim_ownership`. So the source labelled authoritative
///     contributes nothing, and the source labelled TRANSITIONAL — the grep
///     over the probes' own logs — is doing 100% of the work.
///   * That grep is a tail over a file. Ownership therefore expires by VOLUME,
///     not by time: a probe that filled 900 log lines ago has silently lost its
///     claim, and a chatty probe evicts its own older fills by quoting. It also
///     depends on the probe being the thing that writes the log, so a probe that
///     dies holding a position stops defending it the moment its last 800 lines
///     roll over.
///
/// The consequence of both: this predicate is FAIL-OPEN. An unreadable DB, a
/// missing log, a rolled-over window — each one silently converts a probe
/// position into a hedger position. The Python is explicit about choosing that
/// (`except Exception: pass  # coordination read must not block hedging`), and
/// the choice is defensible for a profit-gated hedger, but it means an empty
/// [`ProbeOwnership`] and "nothing is probe-owned" are indistinguishable here.
/// A sweep that wants the other posture must check its sources loaded, before
/// it asks.
#[derive(Debug, Default, Clone)]
pub struct ProbeOwnership {
    slugs: BTreeSet<String>,
}

impl ProbeOwnership {
    pub fn new() -> Self {
        ProbeOwnership::default()
    }

    /// Absorb rows of the `ownership` table as `(relationship_id, owner)`,
    /// keeping those whose owner carries [`PROBE_OWNER_PREFIX`].
    ///
    /// Note the type pun the Python inherits and this preserves: the column is
    /// named `relationship_id`, but the value is compared against a PM-US slug
    /// (`if ps in owned`). For probe claims the two are the same string; a
    /// claim registered under a registry relationship id would never match and
    /// would fail open.
    pub fn claim_rows<'a, I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        for (relationship_id, owner) in rows {
            if owner.starts_with(PROBE_OWNER_PREFIX) {
                self.slugs.insert(relationship_id.to_string());
            }
        }
    }

    /// Absorb one probe's JSONL log (whole file contents; the tail is taken
    /// here because [`PROBE_LOG_TAIL_LINES`] is part of the rule, not part of
    /// the reading).
    ///
    /// A record claims the slug under `pm` (maker probe) or `pair` (lead-lag
    /// probe) unless its `action` is a dry run. Unparseable lines are skipped
    /// exactly as the Python's `except ValueError: continue` does — a torn
    /// final line from a live writer is normal and must not cost the whole
    /// file's claims.
    pub fn absorb_probe_log(&mut self, contents: &str) {
        let total = contents.lines().count();
        for line in contents
            .lines()
            .skip(total.saturating_sub(PROBE_LOG_TAIL_LINES))
        {
            let Ok(rec) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            let slug = rec
                .get("pm")
                .and_then(Value::as_str)
                .or_else(|| rec.get("pair").and_then(Value::as_str));
            let Some(slug) = slug.filter(|s| !s.is_empty()) else {
                continue;
            };
            let action = rec.get("action").and_then(Value::as_str).unwrap_or("");
            if DRY_RUN_ACTIONS.contains(&action) {
                continue;
            }
            self.slugs.insert(slug.to_string());
        }
    }

    /// THE PREDICATE. May the hedger act on this PM-US slug's position?
    ///
    /// False means a research probe is working the market and the position is
    /// theirs to manage — surface it, never complete it.
    pub fn hedger_owns(&self, pm_slug: &str) -> bool {
        !self.slugs.contains(pm_slug)
    }
}

// ---------------------------------------------------------------------------
// Rule 2 — Kalshi-long imbalance policy
// ---------------------------------------------------------------------------

/// Below this imbalance (STRICTLY below — see [`imbalance_action`]) the hedger
/// buys the missing Kalshi YES. Half a contract, because the quantity is then
/// rounded to a whole Kalshi contract and anything shallower rounds to zero.
pub const HEDGE_IMBALANCE_THRESHOLD: f64 = -0.5;

/// At or above this imbalance the hedger tells a human and does nothing. A
/// whole contract, not half: this gates an ALERT, and `position_fp` /
/// `netPosition` are floats whose sub-contract dust is not news.
pub const KALSHI_LONG_ALERT_THRESHOLD: f64 = 1.0;

/// What a venue-truth sweep may do about one pair's imbalance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ImbalanceAction {
    /// PM-US is short more than Kalshi is long: the basket is missing its
    /// Kalshi YES leg. Buy `qty` — still subject to the profit gate, which is
    /// a separate decision this predicate does not make.
    HedgeKalshiYes { qty: u32 },
    /// Kalshi is long more than PM-US is short, by at least one whole
    /// contract. Report it; do not act. `excess` is the raw imbalance.
    AlertKalshiLong { excess: f64 },
    /// Inside the dead band, or balanced. Nothing to do and nothing to say.
    Hold,
}

/// The imbalance policy: `imb = kalshi_position + pmus_net`, negative means a
/// naked PM-US short.
///
/// # Why the policy is asymmetric
///
/// It hedges a naked PM-US short and merely ALERTS on a naked Kalshi long. That
/// looks like half a feature. It is not; there are three reasons in the Python,
/// and the second is the one that would still stand after somebody "finished"
/// the first.
///
/// 1. THE MISSING LEG IS ON THE VENUE THIS HEDGER CANNOT TRADE. Completing
///    `imb < 0` means buying Kalshi YES, and the script builds a
///    `KalshiOrderGateway` and places an IOC. Completing `imb > 0` means
///    acquiring PM-US NO. The script's only PM-US object is
///    `PmusSession(...).get_positions()` — read-only, used for the position
///    read and nothing else. There is no PM-US order path in this process at
///    all, so the branch could not be written as a one-line mirror of the
///    other.
///
/// 2. THE PROFIT GUARANTEE IS NOT COMPUTABLE IN THAT DIRECTION. This is the
///    real asymmetry, and it survives adding an order path. The rule the whole
///    script exists to honour is Geoff's, 2026-07-22: *"hedge only if
///    profitable; otherwise find a profitable hedge in the future"* — encoded
///    as a limit price that guarantees the COMPLETED basket costs under $1:
///
///    ```text
///      limit = 1 - pm_basis - kalshi_taker_fee(ask) - MIN_LOCK
///    ```
///
///    Every term but one is a quote. `pm_basis` is the STANDING leg's cost —
///    what we already paid for the half we hold — and without it there is no
///    such thing as a price that locks a profit. The two position reads are not
///    symmetric in exactly this field: `pmus_positions()` returns
///    `{net, cost_per_share}`, while `kalshi_positions()` returns
///    `{ticker: position_fp}` — a QUANTITY, with no cost attached. So when the
///    standing leg is the Kalshi long, this hedger knows how many it holds and
///    not what they cost, and can compute no limit price at which buying the
///    PM-US side is provably not a loss. Hedging anyway would mean guessing the
///    basis, which is the one thing the profit gate is there to forbid.
///
/// 3. THE OBSERVED FAILURE MODE IS ONE-SIDED. The naked legs this timer was
///    written to clean up arrive as PM-short excess — "frozen-book ghost fills
///    leave PM-short excess", the comment on the sports pairings. A Kalshi-long
///    excess is off-nominal for this shape and more likely to be somebody
///    else's deliberate position (maker inventory, a directional probe, a
///    take-take leg not yet booked) than half of our basket. Alerting is the
///    right response to a case whose CAUSE is unknown.
///
/// A note on what is NOT defensible, kept separate from the above because
/// implementing the existing behaviour is the job and changing live risk policy
/// is not: the two branches escalate very differently. A completed hedge fires
/// an ntfy push through `Alerter`; the Kalshi-long branch only `print`s to
/// stdout, i.e. into the timer's journal, where it reaches a human only if one
/// goes looking. That leg is real directional exposure sitting unhedged, and
/// "not auto-hedged (v1)" is a decision to hold it, not a decision to hide it.
/// [`ImbalanceAction::AlertKalshiLong`] is returned as a first-class variant so
/// a sweep can route it somewhere a human will see; how loudly it does so is
/// the sweep's call, not this predicate's.
///
/// # Boundaries
///
/// The Python guard is `if imb >= -0.5: continue`, so the hedge branch is
/// `imb < -0.5` STRICTLY — an imbalance of exactly -0.5 does nothing. Ported as
/// written. Between -0.5 and +1 there is a genuine dead band: no order, and no
/// alert either.
///
/// One deliberate divergence, in the safe direction: a NaN imbalance lands in
/// [`ImbalanceAction::Hold`] here (both comparisons are false), where the
/// Python falls through its negated guard into the hedge branch and raises out
/// of `int(round(nan))`. Both refuse to trade; this one refuses without taking
/// the process down.
pub fn imbalance_action(kalshi_position: f64, pmus_net: f64) -> ImbalanceAction {
    let imb = kalshi_position + pmus_net;
    if imb < HEDGE_IMBALANCE_THRESHOLD {
        // `int(round(-imb))`. Never zero: -imb > 0.5, so this rounds to 1 or
        // more, and the sweep never places an empty order.
        ImbalanceAction::HedgeKalshiYes {
            qty: round_half_to_even(-imb) as u32,
        }
    } else if imb >= KALSHI_LONG_ALERT_THRESHOLD {
        ImbalanceAction::AlertKalshiLong { excess: imb }
    } else {
        ImbalanceAction::Hold
    }
}

/// Python's `round()`: half-to-EVEN. `f64::round` is half-AWAY-FROM-ZERO, and
/// the two disagree on every other exact half — `round(2.5)` is 2 in Python and
/// 3 in Rust. Positions are floats (`position_fp`, `netPosition`), so an exact
/// -2.5 imbalance is reachable, and the naive port would buy a third Kalshi
/// contract against a two-contract hole: an over-hedge that leaves us naked
/// LONG, in the direction rule 2 above will then refuse to fix.
fn round_half_to_even(x: f64) -> f64 {
    let floor = x.floor();
    let frac = x - floor;
    let up = if frac > 0.5 {
        true
    } else if frac < 0.5 {
        false
    } else {
        // Exactly half: round to whichever neighbour is even.
        (floor as i64) % 2 != 0
    };
    if up {
        floor + 1.0
    } else {
        floor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Rule 1 -------------------------------------------------------------

    #[test]
    fn unclaimed_slug_is_the_hedgers() {
        let own = ProbeOwnership::new();
        assert!(own.hedger_owns("xvus-fedcut-26-usfed-2026-cut"));
    }

    #[test]
    fn ownership_table_claims_only_the_probe_prefix() {
        let mut own = ProbeOwnership::new();
        own.claim_rows([
            ("aec-itfme-olisan-joabia-2026-07-23", "probe-pmus-maker"),
            ("xvus-fedcut-26-usfed-2026-cut", "arbbot-trader"),
        ]);
        assert!(!own.hedger_owns("aec-itfme-olisan-joabia-2026-07-23"));
        // The prefix is the rule. A non-probe owner is still ours to hedge —
        // this is a coordination contract with research, not a lock table.
        assert!(own.hedger_owns("xvus-fedcut-26-usfed-2026-cut"));
    }

    #[test]
    fn probe_log_fill_claims_its_slug() {
        // Verbatim shape from data/exec/pmus_maker_probe.jsonl.
        let log = r#"{"ts":1784826559.0,"pm":"aec-itfme-mickro-javcos-2026-07-23","action":"fill","side":"sell","qty":5,"px":0.51}"#;
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(log);
        assert!(!own.hedger_owns("aec-itfme-mickro-javcos-2026-07-23"));
    }

    #[test]
    fn leadlag_probe_claims_under_pair_not_pm() {
        // Verbatim shape from data/exec/leadlag_probe.jsonl: the slug is under
        // `pair`, and `kalshi` is a ticker that must NOT be read as a slug.
        let log = r#"{"ts":1784797992.2,"pair":"aec-itfme-hercas-ludhed-2026-07-23","kalshi":"KXITFMATCH-26JUL23CASHED-CAS","action":"skip:p<0.65","side":"yes"}"#;
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(log);
        assert!(!own.hedger_owns("aec-itfme-hercas-ludhed-2026-07-23"));
        assert!(own.hedger_owns("KXITFMATCH-26JUL23CASHED-CAS"));
    }

    #[test]
    fn dry_run_records_claim_nothing() {
        // A probe that only ever considered a market has no position in it, so
        // it has nothing to protect and the hedger stays free to act.
        let log = concat!(
            r#"{"pm":"aec-mlb-sd-mia-2026-07-26","action":"dry_quote","px":0.17}"#,
            "\n",
            r#"{"pair":"aec-itfwo-simoge-geocra-2026-07-23","action":"dry_run_would_trade","side":"no"}"#,
        );
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(log);
        assert!(own.hedger_owns("aec-mlb-sd-mia-2026-07-26"));
        assert!(own.hedger_owns("aec-itfwo-simoge-geocra-2026-07-23"));
    }

    #[test]
    fn claims_expire_out_of_the_tail_window() {
        // Ownership is a tail over a file, so a claim old enough to be pushed
        // past PROBE_LOG_TAIL_LINES is silently gone and the hedger will act.
        let mut log = String::from(
            r#"{"pm":"aec-old-claim-2026-07-23","action":"fill","qty":5}"#,
        );
        for _ in 0..PROBE_LOG_TAIL_LINES {
            log.push('\n');
            log.push_str(r#"{"pm":"aec-noise-2026-07-26","action":"quote","qty":5}"#);
        }
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(&log);
        assert!(own.hedger_owns("aec-old-claim-2026-07-23"));
        assert!(!own.hedger_owns("aec-noise-2026-07-26"));
    }

    #[test]
    fn claim_survives_at_the_edge_of_the_tail_window() {
        let mut log = String::from(
            r#"{"pm":"aec-old-claim-2026-07-23","action":"fill","qty":5}"#,
        );
        for _ in 0..(PROBE_LOG_TAIL_LINES - 1) {
            log.push('\n');
            log.push_str(r#"{"pm":"aec-noise-2026-07-26","action":"quote","qty":5}"#);
        }
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(&log);
        assert!(!own.hedger_owns("aec-old-claim-2026-07-23"));
    }

    #[test]
    fn a_torn_line_does_not_cost_the_other_claims() {
        // A live writer's last line is routinely half-written.
        let log = concat!(
            r#"{"pm":"aec-good-2026-07-26","action":"fill","qty":5}"#,
            "\n",
            r#"{"pm":"aec-torn-2026-07-2"#,
        );
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(log);
        assert!(!own.hedger_owns("aec-good-2026-07-26"));
    }

    #[test]
    fn records_with_no_slug_claim_nothing() {
        // data/exec/toxicity_probe.jsonl keys every record by `ticker` and
        // carries neither `pm` nor `pair`, so the whole file contributes zero
        // claims — see the note in the report; this pins the behaviour, not
        // its desirability.
        let log = r#"{"ts":1784849313.8,"ticker":"KXPRESNOMD-28-GN","action":"fill","px":0.17,"qty":5}"#;
        let mut own = ProbeOwnership::new();
        own.absorb_probe_log(log);
        assert!(own.hedger_owns("KXPRESNOMD-28-GN"));
    }

    #[test]
    fn sources_union_rather_than_override() {
        let mut own = ProbeOwnership::new();
        own.claim_rows([("aec-from-table-2026-07-26", "probe-leadlag")]);
        own.absorb_probe_log(r#"{"pm":"aec-from-log-2026-07-26","action":"fill"}"#);
        assert!(!own.hedger_owns("aec-from-table-2026-07-26"));
        assert!(!own.hedger_owns("aec-from-log-2026-07-26"));
    }

    // -- Rule 2 -------------------------------------------------------------

    #[test]
    fn complete_basket_is_left_alone() {
        assert_eq!(imbalance_action(5.0, -5.0), ImbalanceAction::Hold);
        assert_eq!(imbalance_action(0.0, 0.0), ImbalanceAction::Hold);
    }

    #[test]
    fn naked_pm_short_buys_the_missing_kalshi_yes() {
        assert_eq!(
            imbalance_action(0.0, -3.0),
            ImbalanceAction::HedgeKalshiYes { qty: 3 }
        );
        // Partially hedged: only the hole is bought, not the whole PM leg.
        assert_eq!(
            imbalance_action(2.0, -5.0),
            ImbalanceAction::HedgeKalshiYes { qty: 3 }
        );
    }

    #[test]
    fn kalshi_long_is_alerted_and_never_hedged() {
        // THE ASYMMETRY. The missing leg is on PM-US: no order path here, and
        // no Kalshi cost basis with which to prove a completion is profitable.
        assert_eq!(
            imbalance_action(3.0, 0.0),
            ImbalanceAction::AlertKalshiLong { excess: 3.0 }
        );
        assert_eq!(
            imbalance_action(5.0, -2.0),
            ImbalanceAction::AlertKalshiLong { excess: 3.0 }
        );
        assert_eq!(
            imbalance_action(1.0, 0.0),
            ImbalanceAction::AlertKalshiLong { excess: 1.0 }
        );
    }

    #[test]
    fn dead_band_between_the_thresholds_is_silent() {
        // Not an order, and not an alert either: sub-contract dust on both
        // sides of flat.
        assert_eq!(imbalance_action(0.9, 0.0), ImbalanceAction::Hold);
        assert_eq!(imbalance_action(0.0, -0.4), ImbalanceAction::Hold);
        assert_eq!(imbalance_action(1.0, -0.5), ImbalanceAction::Hold);
    }

    #[test]
    fn the_hedge_boundary_is_strict() {
        // `if imb >= -0.5: continue` — exactly -0.5 does nothing at all.
        assert_eq!(imbalance_action(0.0, -0.5), ImbalanceAction::Hold);
        assert_eq!(
            imbalance_action(0.0, -0.51),
            ImbalanceAction::HedgeKalshiYes { qty: 1 }
        );
    }

    #[test]
    fn hedge_quantity_is_never_zero() {
        // A zero-lot order is a rejected order at best. The -0.5 boundary and
        // the rounding are what make this structural: just past the threshold
        // the quantity is already 1.
        for hundredths in 51..400 {
            let imb = -(f64::from(hundredths) / 100.0);
            match imbalance_action(0.0, imb) {
                ImbalanceAction::HedgeKalshiYes { qty } => assert!(qty >= 1, "imb {imb}"),
                other => panic!("imb {imb} should hedge, got {other:?}"),
            }
        }
    }

    #[test]
    fn fractional_imbalance_rounds_half_to_even_like_python() {
        // int(round(2.5)) is 2 in Python, and f64::round would say 3 — which
        // would buy one contract more than the hole and leave us naked long.
        assert_eq!(
            imbalance_action(0.0, -2.5),
            ImbalanceAction::HedgeKalshiYes { qty: 2 }
        );
        assert_eq!(
            imbalance_action(0.0, -4.5),
            ImbalanceAction::HedgeKalshiYes { qty: 4 }
        );
        // ...and the odd-floor halves round UP, where both agree.
        assert_eq!(
            imbalance_action(0.0, -1.5),
            ImbalanceAction::HedgeKalshiYes { qty: 2 }
        );
        assert_eq!(
            imbalance_action(0.0, -3.5),
            ImbalanceAction::HedgeKalshiYes { qty: 4 }
        );
        // Ordinary fractions are unaffected.
        assert_eq!(
            imbalance_action(0.0, -2.4),
            ImbalanceAction::HedgeKalshiYes { qty: 2 }
        );
        assert_eq!(
            imbalance_action(0.0, -2.6),
            ImbalanceAction::HedgeKalshiYes { qty: 3 }
        );
    }

    #[test]
    fn nan_imbalance_never_places_an_order() {
        assert_eq!(imbalance_action(f64::NAN, -3.0), ImbalanceAction::Hold);
        assert_eq!(imbalance_action(0.0, f64::NAN), ImbalanceAction::Hold);
    }
}
