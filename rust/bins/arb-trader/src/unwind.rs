//! Opportunistic unwind: which open baskets have stopped being the best use of
//! the capital they lock.
//!
//! THE ENTRY RULE IS "cross when the edge beats the APR bar". The symmetric
//! EXIT rule is "close when holding is worse than redeploying" — unwind a
//! basket whose remaining forward APR is below the maker hurdle that the freed
//! capital would itself have to clear. Both halves of that comparison already
//! existed and neither is invented here:
//!
//!   * the hurdle is `crate::apr_bar(utilization())`, the number
//!     `Engine::apr_tick` already installs on every quoter and reports as
//!     `maker_apr_bar`. The caller passes the bar IN FORCE, so the exit is
//!     measured against exactly the bar a fresh quote would be;
//!   * the forward APR is `forward_hold_apr` from `data/exec/marks.json`
//!     (`scripts/mark_positions.py`): remaining locked profit over the
//!     liquidation value the position is tying up, annualised to resolution.
//!
//! WHY THERE IS NO PLACING CODE HERE. This module DECIDES; nothing in this
//! workspace can yet act on the decision, and pretending otherwise would be the
//! dangerous half. `Intent` has four variants — place, cancel, hedge-needed,
//! skip — and an exit is not expressible as any of them today: a fill on a
//! `Tag::TakeTake` place mints a hedge that OPENS the other leg, and
//! `engine::fill::book_basket` appends `status: "open"`. An exit fill has to
//! CLOSE the other leg and append a `status: "unwound"` record netting against
//! one specific open record's `ts` (`ledger::open_exposure`). None of that
//! exists. Building it is a separate change to the money path; this one is the
//! decision, its gates, and the evidence.
//!
//! WHAT THE EXIT WOULD BE, WHEN IT EXISTS. A maker exit, not a taker one:
//! crossing the spread to get out burns the edge that made the position
//! profitable. `Exit::suppress_key` is the hand-off — the (market, side) the
//! unwinder owns and `Quoter::target` already yields on (`quoter.rs:350`, "side
//! owned by maker-unwind — cancel/stay out"). It is COMPUTED and logged here,
//! and deliberately not installed: installing it today would pull the entry
//! quote off that side and rest nothing in its place, which is strictly worse
//! than the status quo.
//!
//! THE DIRECTION IS FIXED BY THE INPUT. `maker_exit_ct` is priced by
//! `mark_positions.py` for the STANDARD basket only (long Kalshi YES + long PM
//! NO): rest a Kalshi ask one tick inside the competing ask, and close the PM
//! NO one tick through its ask when it fills. Inverted baskets (Kalshi NO + PM
//! YES) get `maker_exit_ct: null` there, so they are unpriceable here and are
//! never selected. That is the scope, not an oversight.

use crate::taketake::{bar_from_marks, today_iso, Bar};
use arb_core::model::BookSide;

/// Minimum per-contract profit a maker exit must lock before it is worth
/// resting. Port of `mark_positions.py`'s own eligibility floor
/// (`mx >= Decimal("0.005")`), and the guard that separates a profit-taker from
/// a liquidator.
///
/// It is doing the load-bearing work, not rounding out the APR test. Measured
/// against the live marks on 2026-07-29, the APR half alone selects most of the
/// open book and the large majority of THOSE price a maker exit at a
/// per-contract LOSS — a position that has not converged far enough for a
/// passive exit to beat its own entry basis plus both legs' fees. Unwinding
/// them to free capital would pay more to escape than the redeployed capital
/// could earn back.
const MIN_EXIT_CT: f64 = 0.005;

/// A basket that should be quoted out of: holding it is worse than redeploying
/// its capital, and the passive exit is profitable today.
#[derive(Debug, Clone, PartialEq)]
pub struct Exit {
    pub rel_id: String,
    /// The Kalshi ticker the exit ask would rest on.
    pub market_id: String,
    /// The `ts` of the `open` ledger record this closes. THE BASKET IDENTITY:
    /// one relationship holds SEVERAL independently-priced positions, entered
    /// at different prices on different days and converging at different rates,
    /// and `ledger::open_exposure` nets an unwind against exactly one of them
    /// by `(relationship_id, closes_ts)`. A candidate that named only the
    /// relationship would be unbookable.
    pub opened_ts: f64,
    pub qty: i64,
    /// Remaining forward APR of CONTINUING to hold, %/yr.
    pub fwd_apr: f64,
    /// Per-contract profit the maker exit locks, net of both legs' fees.
    pub exit_ct: f64,
}

impl Exit {
    /// The (market, side) an exit quote owns, for `Quoter::set_suppress`.
    ///
    /// ALWAYS THE KALSHI ASK. We are long Kalshi YES, so the passive exit rests
    /// an ask on that market — and that is also the side the ENTRY quoter uses
    /// to open an inverted basket (`maker_leg_indices` returns both legs and
    /// `target` is asked for both sides), so the two collide unless the entry
    /// quoter steps out. That collision is why `suppress` exists.
    pub fn suppress_key(&self) -> (String, BookSide) {
        (self.market_id.clone(), BookSide::Ask)
    }
}

/// Why a position is not a candidate. Reported rather than dropped: a basket
/// that ALMOST cleared is the interesting telemetry, and "nothing selected" has
/// four very different causes.
#[derive(Debug, Clone, PartialEq)]
pub enum Skip {
    /// No live book behind it, so no exit price and no forward APR. THIS IS
    /// ALSO WHAT A SETTLED MARKET LOOKS LIKE: `mark_positions.py` prices a
    /// position from top-of-book, and a market that has resolved has none, so
    /// its row carries nulls (or the position never reaches the file at all).
    /// A settled market is therefore unquotable-into by construction, not by a
    /// rule that has to remember to fire.
    NotPriceable,
    /// At or past its resolve date — settling or settled. The belt to
    /// `NotPriceable`'s braces: a book can linger after the event.
    Settled { resolves_by: String },
    /// A family whose exits this engine does not own. Sports baskets resolve in
    /// hours and `mark_positions.py` keeps every unwind signal dark for them
    /// ("hold to settlement — the sweeper realizes them"); contradicting the
    /// writer of our own input is not a decision this module gets to make.
    HeldToSettlement,
    /// Holding still beats redeploying.
    HoldIsBetter { fwd_apr: f64, hurdle: f64 },
    /// Holding is the worse trade, but getting out costs more than it frees.
    ExitUnprofitable { exit_ct: f64 },
}

/// One position's verdict. Pure: the caller supplies the hurdle and the day.
fn consider(p: &serde_json::Value, hurdle: f64, today: &str) -> Result<Exit, Skip> {
    let rel_id = p.get("relationship_id").and_then(|v| v.as_str()).unwrap_or_default();
    if rel_id.starts_with("sports-") {
        return Err(Skip::HeldToSettlement);
    }
    let resolves_by = p.get("resolves_by").and_then(|v| v.as_str()).ok_or(Skip::NotPriceable)?;
    // String compare, because both are `YYYY-MM-DD` and that ordering IS the
    // date ordering. `<=` and not `<`: a market resolving today is settling.
    if resolves_by <= today {
        return Err(Skip::Settled { resolves_by: resolves_by.to_string() });
    }
    let (Some(fwd_apr), Some(exit_ct), Some(market_id), Some(opened_ts), Some(qty)) = (
        p.get("forward_hold_apr").and_then(|v| v.as_f64()),
        p.get("maker_exit_ct").and_then(|v| v.as_f64()),
        p.get("kalshi_ticker").and_then(|v| v.as_str()),
        p.get("ts").and_then(|v| v.as_f64()),
        p.get("qty").and_then(|v| v.as_i64()),
    ) else {
        return Err(Skip::NotPriceable);
    };
    if qty < 1 {
        return Err(Skip::NotPriceable);
    }
    // THE RULE. Holding beats redeploying while the remaining forward APR still
    // clears the bar a fresh maker quote would have to clear.
    if fwd_apr >= hurdle {
        return Err(Skip::HoldIsBetter { fwd_apr, hurdle });
    }
    if exit_ct < MIN_EXIT_CT {
        return Err(Skip::ExitUnprofitable { exit_ct });
    }
    Ok(Exit {
        rel_id: rel_id.to_string(),
        market_id: market_id.to_string(),
        opened_ts,
        qty,
        fwd_apr,
        exit_ct,
    })
}

/// Every open position marks can price, against the hurdle in force.
///
/// `Err` is a REFUSAL to decide anything at all, not an empty answer: marks are
/// the only input, and a marks file that is stale, unageable or corrupt cannot
/// support a decision in either direction. The staleness rule is
/// `taketake::bar_from_marks`'s and not a second copy of it — one file, one
/// definition of "too old to act on".
///
/// A file that is simply ABSENT is a cold start (`Bar::NoPortfolio`), which
/// selects nothing and is not an error.
pub fn select(marks_json: &str, hurdle: f64, now: f64) -> Result<(Vec<Exit>, Vec<Skip>), String> {
    if let Bar::Untrusted { why } = bar_from_marks(marks_json, now) {
        return Err(why);
    }
    let today = today_iso(now);
    let mut exits = Vec::new();
    let mut skips = Vec::new();
    let doc: serde_json::Value = serde_json::from_str(marks_json).unwrap_or_default();
    let empty = Vec::new();
    for p in doc.get("positions").and_then(|v| v.as_array()).unwrap_or(&empty) {
        match consider(p, hurdle, &today) {
            Ok(e) => exits.push(e),
            Err(s) => skips.push(s),
        }
    }
    // Deepest lock first: an operator reading one line of this wants the
    // basket that frees the most capital, and a stable order makes two ticks
    // comparable. `total_cmp` because a NaN APR must not scramble the rest.
    exits.sort_by(|a, b| b.qty.cmp(&a.qty).then(a.fwd_apr.total_cmp(&b.fwd_apr)));
    Ok((exits, skips))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-07-29T20:15:04Z, the live marks file's own `generated_at`.
    const NOW: f64 = 1785356104.0;

    fn iso_z(t: f64) -> String {
        let s = (t as i64).rem_euclid(86_400);
        format!("{}T{:02}:{:02}:{:02}Z", today_iso(t), s / 3600, (s % 3600) / 60, s % 60)
    }

    /// A marks document around `positions`, stamped fresh at [`NOW`].
    fn marks(positions: &str) -> String {
        format!(r#"{{"generated_at":"{}","positions":[{positions}]}}"#, iso_z(NOW))
    }

    /// One position in the shape `mark_positions.py` writes.
    fn pos(rel: &str, qty: i64, resolves: &str, fwd: &str, mx: &str) -> String {
        format!(
            r#"{{"relationship_id":"{rel}","ts":1784646659.716,"kalshi_ticker":"KX-{rel}",
                 "qty":{qty},"cost_usd":10.0,"locked_profit_usd":1.0,
                 "resolves_by":"{resolves}","forward_hold_apr":{fwd},"maker_exit_ct":{mx}}}"#
        )
    }

    /// THE RULE, both directions, on the shape the live book carried when this
    /// was written.
    ///
    /// A long-dated basket sitting at forward 12.6%/yr with a maker exit
    /// locking 3.12c/ct, against a maker hurdle of `apr_bar(1.0)` = 16.0%/yr —
    /// which is the bar a book at its class cap asks for, and the book was at
    /// its cap. Holding earns 12.6%/yr on capital a fresh quote must make 16%
    /// on: redeploying is the better trade and the position is a candidate.
    /// Drop the hurdle under its forward APR and the same position is held.
    #[test]
    fn a_basket_is_exited_when_holding_earns_less_than_the_bar_the_freed_capital_faces() {
        let m = marks(&pos("longdated", 20, "2027-04-25", "12.6", "0.0312"));

        let (exits, _) = select(&m, 16.0, NOW).expect("fresh marks");
        assert_eq!(exits.len(), 1, "12.6%/yr does not clear a 16%/yr hurdle: {exits:?}");
        assert_eq!(exits[0].qty, 20);
        assert_eq!(exits[0].rel_id, "longdated");
        assert_eq!(exits[0].opened_ts, 1784646659.716, "the basket identity, not just the rel");

        let (held, skips) = select(&m, 12.0, NOW).expect("fresh marks");
        assert!(held.is_empty(), "12.6%/yr beats a 12%/yr hurdle — hold: {held:?}");
        assert!(
            matches!(skips.as_slice(), [Skip::HoldIsBetter { .. }]),
            "and it says so: {skips:?}"
        );

        // the boundary: equal is HOLD. An exit has to be strictly better.
        assert!(select(&m, 12.6, NOW).unwrap().0.is_empty(), "equal is not better");
    }

    /// AND THE EXIT MUST ITSELF PAY. The commonest shape in the live book on
    /// 2026-07-29: a long-dated basket at forward 11.0%/yr — under every live
    /// hurdle — whose maker exit priced at MINUS 3.3c/contract, because the
    /// passive exit does not recover the entry basis plus both legs' fees.
    /// Selecting on the APR test alone would quote out of it and PAY to free
    /// the capital.
    ///
    /// The large majority of that day's APR-eligible baskets were this shape,
    /// so this guard is the difference between the candidate set and the noise.
    #[test]
    fn a_maker_exit_that_loses_money_per_contract_is_not_an_exit() {
        let m = marks(&pos("unconverged", 20, "2027-04-25", "11.0", "-0.033"));
        let (exits, skips) = select(&m, 16.0, NOW).expect("fresh marks");
        assert!(exits.is_empty(), "a 3.3c/ct loss is not a profit-take: {exits:?}");
        assert!(matches!(skips.as_slice(), [Skip::ExitUnprofitable { .. }]), "{skips:?}");

        // the floor is half a cent, matching `mark_positions.py`'s own
        // eligibility test — a tenth of a cent is inside the noise of the
        // touch it was priced against.
        let thin = marks(&pos("thin", 10, "2027-04-25", "11.0", "0.001"));
        assert!(select(&thin, 16.0, NOW).unwrap().0.is_empty(), "0.1c/ct is not worth resting");
        let ok = marks(&pos("ok", 10, "2027-04-25", "11.0", "0.005"));
        assert_eq!(select(&ok, 16.0, NOW).unwrap().0.len(), 1, "the floor itself qualifies");
    }

    /// A MARKET THAT HAS RESOLVED IS NEVER QUOTED INTO.
    ///
    /// Three independent ways, because the ledger carries baskets whose markets
    /// are gone (task #50: settled sports positions still `status: open`) and an
    /// exit ask into a dead book is an order that can only sit there.
    #[test]
    fn a_settled_or_settling_market_is_never_a_candidate() {
        let today = today_iso(NOW);

        // 1. past its resolve date, however good the numbers look
        let past = marks(&pos("done", 10, "2026-07-01", "1.0", "0.30"));
        let (e, s) = select(&past, 16.0, NOW).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::Settled { .. }]), "{s:?}");

        // ...including one resolving TODAY, which is settling right now.
        let now_res = marks(&pos("today", 10, &today, "1.0", "0.30"));
        assert!(select(&now_res, 16.0, NOW).unwrap().0.is_empty());

        // 2. no live book, so no forward APR and no exit price. This is what a
        //    resolved market looks like to `mark_positions.py`.
        let dead = format!(
            r#"{{"generated_at":"{}","positions":[{{"relationship_id":"gone","ts":1.0,
                 "kalshi_ticker":"KX-gone","qty":10,"resolves_by":"2027-04-25",
                 "forward_hold_apr":null,"maker_exit_ct":null}}]}}"#,
            iso_z(NOW)
        );
        let (e, s) = select(&dead, 16.0, NOW).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::NotPriceable]), "{s:?}");

        // 3. a family whose exits this engine does not own at all.
        let sport = marks(&pos("sports-wta-A@B", 2, "2027-04-25", "1.0", "0.30"));
        let (e, s) = select(&sport, 16.0, NOW).unwrap();
        assert!(e.is_empty(), "{e:?}");
        assert!(matches!(s.as_slice(), [Skip::HeldToSettlement]), "{s:?}");
    }

    /// MARKS THAT CANNOT BE TRUSTED DECIDE NOTHING — not "nothing to exit".
    ///
    /// Same rule and same code as the take-take bar (`taketake::bar_from_marks`),
    /// because it is the same file and one of them being stale while the other
    /// is fresh is not a state that can exist. The numbers are the 2026-07-28
    /// incident's: marks stopped being written and the engine traded against
    /// the frozen file for four hours.
    #[test]
    fn stale_or_corrupt_marks_refuse_rather_than_selecting_nothing() {
        let m = marks(&pos("longdated", 20, "2027-04-25", "12.6", "0.0312"));
        assert_eq!(select(&m, 16.0, NOW).unwrap().0.len(), 1, "fresh: the candidate is there");

        assert!(select(&m, 16.0, NOW + 15210.0).is_err(), "15210s behind must refuse");
        assert!(select(&m, 16.0, NOW - 15210.0).is_err(), "a clock skewed forward too");
        assert!(select(r#"{"generated_at":"2026-07-2"#, 16.0, NOW).is_err(), "torn write");
        assert!(select(r#"{"positions":[]}"#, 16.0, NOW).is_err(), "unageable");

        // ...but an ABSENT file is a cold start, not a fault.
        assert_eq!(select("", 16.0, NOW).unwrap().0.len(), 0);
    }

    /// THE FEEDBACK LOOP, PINNED. An unwind frees capital, which lowers
    /// `utilization()`, which lowers `apr_bar` — so the very act of exiting
    /// moves the hurdle the exit was selected against.
    ///
    /// It cannot oscillate, and this is why: `apr_bar` is monotone
    /// non-decreasing in utilization and an exit can only REDUCE exposure, so
    /// the candidate set can only ever SHRINK. Freeing capital makes the next
    /// exit harder to justify, never easier — the loop is a ratchet, and it
    /// runs toward holding rather than toward liquidating.
    ///
    /// Shapes taken from the live book: a book AT its class cap asks the
    /// ceiling, 16.0%/yr. Unwinding enough to bring utilization to 0.933 asks
    /// 15.20%/yr instead — at which the 15.6%/yr basket that qualified a moment
    /// ago no longer does, while the 12.6%/yr one still does.
    #[test]
    fn freeing_capital_lowers_the_hurdle_so_the_candidate_set_can_only_shrink() {
        let before = crate::apr_bar(1.0);
        let after = crate::apr_bar(0.933);
        assert!(after < before, "freeing capital must lower the bar: {after} vs {before}");

        let m = marks(&format!(
            "{},{}",
            pos("longdated", 20, "2027-04-25", "12.6", "0.0312"),
            pos("neardated", 5, "2026-12-09", "15.6", "0.0144")
        ));
        let hi = select(&m, before, NOW).unwrap().0;
        let lo = select(&m, after, NOW).unwrap().0;
        assert_eq!(hi.len(), 2, "both clear a 16%/yr hurdle: {hi:?}");
        assert_eq!(lo.len(), 1, "only the deeper one clears 15.2%/yr: {lo:?}");
        assert!(
            lo.iter().all(|e| hi.contains(e)),
            "the lower hurdle's set must be a SUBSET, never a new basket: {lo:?} vs {hi:?}"
        );

        // ...and that is the general property, not one lucky pair: the rule is
        // a threshold on a value the hurdle does not touch, so every hurdle
        // between the two selects a subset of the higher one's answer.
        for step in 0..=20 {
            let h = after + (before - after) * f64::from(step) / 20.0;
            let mid = select(&m, h, NOW).unwrap().0;
            assert!(mid.iter().all(|e| hi.contains(e)), "at hurdle {h}: {mid:?}");
        }
    }

    /// THE HAND-OFF. A standard basket is long Kalshi YES, so its passive exit
    /// rests an ASK on the Kalshi market — the side `Quoter::target` already
    /// returns `None` for when it is suppressed, so the entry quoter cancels out
    /// and stays out rather than fighting its own unwind for the queue.
    #[test]
    fn the_side_an_exit_owns_is_the_kalshi_ask_the_entry_quoter_yields() {
        let m = marks(&pos("longdated", 20, "2027-04-25", "12.6", "0.0312"));
        let (exits, _) = select(&m, 16.0, NOW).unwrap();
        assert_eq!(exits[0].suppress_key(), ("KX-longdated".to_string(), BookSide::Ask));
    }

    /// WHY AN EXIT MUST NOT RESERVE, as arithmetic rather than as an opinion.
    ///
    /// `risk.rs` reserves a resting quote's contracts against the caps, and an
    /// exit quote rests. But `RiskGate::check` "gates OPENING risk only" and the
    /// reservation is that gate's committing half: the position's contracts are
    /// ALREADY in `by_rel`/`by_class` from `record_open`, so charging the exit
    /// for them again counts the same money twice — in the direction that
    /// forbids the trade that would give the money back.
    ///
    /// Concretely: the live book is already OVER its class budget, so the cap
    /// refuses every quote with a `class cap:` reason. An exit routed through
    /// that gate is refused for being too large while being the only thing
    /// that could make it smaller — a deadlock, not a safeguard.
    ///
    /// This pins EXISTING `risk.rs` behaviour, so it is a characterisation test
    /// and not a proof of anything new. Its job is to make the reserve decision
    /// fail loudly if a future change quietly routes exits through this gate.
    #[test]
    fn routing_an_exit_through_the_opening_cap_would_deadlock_the_cap() {
        // The live config, written out rather than read from `config/exec.yaml`
        // — a test binary's cwd is the crate dir, so that path does NOT exist
        // here, and `RiskView::load` answers a missing file with a $0 bankroll
        // that refuses everything. The assertion below would then have passed
        // for a reason that has nothing to do with the cap.
        let d = std::env::temp_dir().join(format!("arb-unwind-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let exec = d.join("exec.yaml");
        std::fs::write(&exec, "bankroll_usd: 980\nper_class_cap: 0.35\n").unwrap();
        let v = crate::risk::RiskView::load(
            &exec.to_string_lossy(),
            "/nonexistent/topics.yaml",
            vec![("kalshi".into(), "1000".into()), ("polymarket_us".into(), "1000".into())],
            std::collections::HashMap::new(),
        );
        let rel = arb_core::scan::Rel {
            id: "r1".into(),
            rtype: arb_core::scan::RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                arb_core::scan::RelLeg {
                    venue: arb_core::model::Venue::Kalshi,
                    market_id: "K".into(),
                },
                arb_core::scan::RelLeg {
                    venue: arb_core::model::Venue::PolymarketUs,
                    market_id: "P".into(),
                },
            ],
        };
        // ...and the gate is live: on an EMPTY book the same clip passes, so
        // the refusal below is the cap and not a broken fixture.
        let empty =
            arb_core::quoter::RiskGate::check(&v, &rel, arb_core::model::Venue::Kalshi, 20, None);
        assert!(empty.allowed, "fixture is not gating for the wrong reason: {:?}", empty.reasons);

        // ...and now the class budget is full, and then some, as it is live.
        v.record_open("already-open", "cross-venue-equivalent", 980.0 * 0.35 + 1.0);
        let d =
            arb_core::quoter::RiskGate::check(&v, &rel, arb_core::model::Venue::Kalshi, 20, None);
        assert!(!d.allowed, "the opening cap refuses the exit that would empty it");
        assert!(d.reasons.iter().any(|r| r.contains("class cap")), "{:?}", d.reasons);
    }
}
