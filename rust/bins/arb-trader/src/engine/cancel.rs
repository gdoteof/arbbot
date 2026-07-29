//! Cancelling an order at a venue that accepts only its OWN id for it.
//!
//! The engine names its orders; both venues rename them. A cancel therefore
//! cannot be sent until the `order_ack` carrying the venue's id comes back, and
//! sending ours instead is a no-op that BOTH venues report as success. So a
//! cancel the engine has decided on but cannot address is PARKED, escalated
//! once if the ack never arrives, and retired only when a real command was
//! queued for it.
//!
//! `intent_actions` is the other half: the effect commands one intent line
//! implies, in dispatch order — an amend is a cancel AND a place, cancel first.

use super::Engine;
use crate::exec::Action;
use arb_core::intent::Intent;
use arb_core::model::{BookSide, Venue};
use arb_venue::gateway::{CancelBy, CancelRequest, PlaceRequest, Side as VenueSide, Tif};
use std::collections::HashMap;

/// A cancel the engine has DECIDED on but cannot yet ADDRESS: the `order_ack`
/// carrying the venue's own order id for that order has not come back.
///
/// Parking it is the point. Both venues accept only THEIR id, so sending ours is
/// a no-op that both report as success (audit 2026-07-28: Kalshi 404 -> quirk K4
/// -> `Ok(())`, PM-US <300 for an id it never issued), and simply dropping the
/// cancel would leave the quote resting at a price the engine has already judged
/// wrong.
///
/// An entry is retired ONLY when a venue-addressed cancel has actually been
/// QUEUED for it. Not when the ack lands, not when it is escalated: a `try_send`
/// that hits a full channel loses the command, and retiring on a lost command
/// was a way to leave an unaddressable quote resting while the
/// `cancels_unresolved` gauge read healthy.
pub(super) struct ParkedCancel {
    pub(super) venue: Venue,
    pub(super) market: String,
    /// MONOTONIC time the cancel was decided.
    ///
    /// Not tape time: the feed-stale pull is one of the callers, and tape time
    /// stops advancing exactly when the feed dies. Not wall time either — an NTP
    /// step backwards would freeze every parked cancel and a step forwards would
    /// expire them all at once, which is the escalation storm this deadline is
    /// rate-limited to avoid. `Instant` satisfies both requirements.
    pub(super) since: std::time::Instant,
    /// Whether the client-id escalation has already gone out for this entry.
    /// One escalation per order: it costs a full paginated account read on
    /// Kalshi, and repeating it would be the 429 shape `PmusGateway::cancel`
    /// documents refusing.
    pub(super) escalated: bool,
}

/// How long a cancel waits for its order's `order_ack` before the engine stops
/// expecting to learn the venue's id and escalates to a client-id cancel.
///
/// The place's HTTP timeout is 15s (`main.rs`), so past that the ack has either
/// arrived or never will. Escalating early costs one background order-list read;
/// escalating late leaves a stale quote resting and fillable. The one thing that
/// can escalate early is an executor backlogged behind its token bucket for
/// longer than that, which is its own alarm (`exec_dropped`, `chan_high_water`).
const CANCEL_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

/// The one place a cancel command is built. `venue_oid` is the venue's OWN id.
fn cancel_by_venue_id(venue_oid: &str, market: &str) -> Action {
    Action::Cancel(CancelRequest {
        // PM-US REQUIRES the market slug in the cancel body and we refuse to
        // self-resolve it (that hammered the API into 429s); the intent carries
        // it, so it rides along here. Kalshi ignores it.
        market_slug: Some(market.to_string()),
        by: CancelBy::VenueId(venue_oid.to_string()),
    })
}

/// The venue-addressed cancel for OUR order id `oid`, or `None` when the venue's
/// id for it is not known yet — in which case the request is PARKED rather than
/// sent with an id the venue has never heard of.
///
/// `armed` false means no sink exists, so no place ever reaches a venue and no
/// `order_ack` ever comes back: parking would accumulate forever and prove
/// nothing. A dry run therefore addresses the cancel by our client id, which is
/// an honest statement of everything it knows, and the inert executor drops it.
fn resolve_cancel(
    oid: &str,
    market: &str,
    venue: Venue,
    armed: bool,
    oid_venue: &HashMap<String, String>,
    parked: &mut HashMap<String, ParkedCancel>,
    now: std::time::Instant,
) -> Option<Action> {
    if let Some(vid) = oid_venue.get(oid) {
        return Some(cancel_by_venue_id(vid, market));
    }
    if !armed {
        return Some(Action::Cancel(CancelRequest {
            by: CancelBy::ClientId(oid.to_string()),
            market_slug: Some(market.to_string()),
        }));
    }
    parked.entry(oid.to_string()).or_insert(ParkedCancel {
        venue,
        market: market.to_string(),
        since: now,
        escalated: false,
    });
    None
}

/// One item of work on the park, carrying everything the dispatch needs. Split
/// out so the policy is a pure function of (park, mappings, clock, killed) and
/// can be tested without a runtime.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CancelWork {
    /// The venue's id is known now, so send the real cancel. Retires the entry
    /// once it is queued.
    Send { oid: String, venue: Venue, market: String, venue_order_id: String },
    /// The ack never came. Escalate to a client-id cancel, which Kalshi resolves
    /// against its own order list and PM-US refuses locally. The entry STAYS
    /// parked: a late ack must still be able to send the real cancel.
    Escalate { oid: String, venue: Venue, market: String },
}

impl CancelWork {
    pub(super) fn venue(&self) -> Venue {
        match self {
            CancelWork::Send { venue, .. } | CancelWork::Escalate { venue, .. } => *venue,
        }
    }

    pub(super) fn action(&self) -> Action {
        match self {
            CancelWork::Send { venue_order_id, market, .. } => {
                cancel_by_venue_id(venue_order_id, market)
            }
            CancelWork::Escalate { oid, market, .. } => Action::Cancel(CancelRequest {
                by: CancelBy::ClientId(oid.clone()),
                market_slug: Some(market.clone()),
            }),
        }
    }
}

/// Record the outcome of DISPATCHING one item of cancel work.
///
/// `queued` is `dispatch!`'s return: a `try_send` into a full channel LOSES the
/// command. Every state change therefore hangs off it —
///   * a `Send` retires the park entry only when the cancel is really on its way.
///     Retiring on a lost command dropped `cancels_unresolved` back to 0 while an
///     unaddressable quote went on resting, i.e. the one gauge built to surface
///     this read healthy;
///   * an `Escalate` burns the order's single escalation only when it was really
///     sent, so a lost one is retried on the next tick.
pub(super) fn settle(parked: &mut HashMap<String, ParkedCancel>, work: &CancelWork, queued: bool) {
    if !queued {
        return;
    }
    match work {
        CancelWork::Send { oid, .. } => {
            parked.remove(oid);
        }
        CancelWork::Escalate { oid, .. } => {
            if let Some(p) = parked.get_mut(oid) {
                p.escalated = true;
            }
        }
    }
}

/// What the cancel deadline should do this tick, in dispatch order.
///
/// `Send` is unbounded — each is one targeted DELETE for a cancel we already owe,
/// and the parked set is bounded by the quotes whose cancel raced an ack.
/// `Escalate` is capped at ONE per tick, and skipped entirely while killed:
///   * a client-id cancel costs `all_orders()`, a paginated read of the FULL
///     order history, taken inside the venue executor's one-command-at-a-time
///     `spawn_blocking` — so a burst of them blocks every place and cancel for
///     that venue, and competes for the same Background budget as
///     `resting_order_ids`, which is the only evidence `cancel_all_and_verify`
///     accepts. Starving that turns a clean shutdown into "NOT CLEAN at exit".
///   * while killed, `Action::SweepAndVerify` is already queued and is a
///     strictly better remedy: it reaches orders we hold no id for at all, and
///     it proves the outcome.
fn cancel_work(
    parked: &HashMap<String, ParkedCancel>,
    oid_venue: &HashMap<String, String>,
    now: std::time::Instant,
    killed: bool,
) -> Vec<CancelWork> {
    let mut oids: Vec<&String> = parked.keys().collect();
    oids.sort(); // deterministic order for the log and the dispatch
    let mut out = Vec::new();
    let mut escalate: Option<CancelWork> = None;
    for oid in oids {
        let p = &parked[oid];
        if let Some(vid) = oid_venue.get(oid) {
            out.push(CancelWork::Send {
                oid: oid.clone(),
                venue: p.venue,
                market: p.market.clone(),
                venue_order_id: vid.clone(),
            });
            continue;
        }
        let expired = now.saturating_duration_since(p.since) >= CANCEL_ACK_GRACE;
        if expired && !p.escalated && !killed && escalate.is_none() {
            escalate = Some(CancelWork::Escalate {
                oid: oid.clone(),
                venue: p.venue,
                market: p.market.clone(),
            });
        }
    }
    out.extend(escalate);
    out
}

/// The effect commands one intent line implies, IN DISPATCH ORDER.
///
/// A place carrying `replaces` is an amend, and an amend is TWO effects: cancel
/// the order being replaced, then place the new one. The quoter emits it as one
/// intent because Python did the cancel inline through the gateway and used
/// `replaces` only to label the activity feed — so porting the intent stream
/// faithfully reproduced the RECORD of the amend and dropped its EFFECT. Every
/// superseded quote stayed live at a price the engine had already rejected
/// (audit 2026-07-28: 6785 of 8316 places in the 40-relationship shadow carry
/// `replaces`, against 850 cancels).
///
/// CANCEL FIRST, and the order is a deliberate choice. Both commands go into one
/// per-venue executor channel that is drained strictly sequentially, so
/// cancel-then-place leaves a window with NO quote of about one venue round
/// trip, while place-then-cancel leaves a window with TWO live quotes — both
/// fillable, one at a price we have already decided is wrong. The first costs a
/// missed fill; the second is exactly the adverse selection the reprice exists
/// to avoid. Python cancelled first for the same reason
/// (`src/arbbot/exec/quoter.py:311-313`).
///
/// A place whose cancel had to be PARKED still goes out. The quoter has already
/// moved its `resting` state and cannot be told otherwise (it is pure), so
/// withholding the place would leave it believing it rests a quote that does not
/// exist — the "market goes quietly dark" failure the order-id seeding comment
/// below describes. Two live quotes for the length of the parked cancel is the
/// lesser harm, and it is counted (`cancels_unresolved`) rather than silent.
pub(super) fn intent_actions(
    intent: &Intent,
    armed: bool,
    oid_venue: &HashMap<String, String>,
    parked: &mut HashMap<String, ParkedCancel>,
    now: std::time::Instant,
) -> Vec<Action> {
    let mut out: Vec<Action> = Vec::new();
    match intent {
        Intent::Place(p) => {
            if let Some(roid) = &p.replaces {
                if let Some(a) =
                    resolve_cancel(roid, &p.place, p.venue, armed, oid_venue, parked, now)
                {
                    out.push(a);
                }
            }
            // The quoter only ever rests post-only GTC makers, so tif/post_only
            // are fixed here; a taker hedge carries its own.
            // `client_order_id` is our own order id, which is what makes a
            // retried place idempotent at the venue.
            out.push(Action::Place(PlaceRequest {
                market: p.place.clone(),
                // THE line this whole typing pass existed for. It was
                // `if p.side == "ask" { Ask } else { Bid }`, so every value
                // that was not exactly `"ask"` — a typo, a case difference, a
                // venue's own spelling — became a BID, which is a BUY, at a
                // price chosen for the other side of the book. There is no
                // else here any more: two variants in, two arms out, and the
                // compiler refuses a third.
                side: match p.side {
                    BookSide::Bid => VenueSide::Bid,
                    BookSide::Ask => VenueSide::Ask,
                },
                price: p.price.clone(),
                qty: p.count,
                tif: if p.taker { Tif::Ioc } else { Tif::Gtc },
                post_only: !p.taker,
                client_order_id: p.order_id.clone(),
            }));
        }
        Intent::Cancel(c) => {
            if let Some(a) =
                resolve_cancel(&c.order_id, &c.cancel, c.venue, armed, oid_venue, parked, now)
            {
                out.push(a);
            }
        }
        // Records, not orders: no venue command follows from either.
        Intent::HedgeNeeded(_) | Intent::Skip(_) => {}
    }
    out
}

impl Engine {
    /// Dispatch one tick's worth of parked-cancel work.
    pub(super) fn cancel_tick(&mut self) {
        for w in cancel_work(
            &self.parked_cancels,
            &self.oid_venue,
            std::time::Instant::now(),
            self.killed,
        ) {
            let queued = self.dispatch(w.venue(), w.action());
            settle(&mut self.parked_cancels, &w, queued);
            if let (CancelWork::Escalate { oid, venue, market }, true) = (&w, queued) {
                self.n_cancel_escalated += 1;
                // Deliberately not shouted. The commonest cause is a
                // place the venue REJECTED (post-only would cross, rate
                // limited, 409 duplicate id): the quoter still believes
                // it rests, so its next reprice parks a cancel for an
                // order that never existed and nothing is wrong at the
                // venue. A replay of one 3.7-day shadow produced ~150 of
                // those on PM-US alone, and a line that cries wolf 150
                // times is a line operators stop reading.
                eprintln!(
                    "[engine] cancel {oid} ({market} on {}) has had no order_ack for \
                     {}s — escalating to a client-id cancel (Kalshi resolves it against \
                     its order list; PM-US cannot and will refuse). A place the venue \
                     rejected looks exactly like this.",
                    venue.as_str(),
                    CANCEL_ACK_GRACE.as_secs()
                );
            }
        }
    }
}

/// The cancel path: a cancel must be addressed with the VENUE's order id, and a
/// reprice must cancel the order it replaces.
///
/// Before 2026-07-28 neither was true. Every per-order cancel carried our own
/// `m…` id, which Kalshi 404s (mapped to success by quirk K4) and PM-US answers
/// <300 for; and a reprice emitted a single place intent carrying `replaces`,
/// whose only effect was FillLedger bookkeeping. The two compose into an engine
/// that believes it holds one quote per market while the venue holds several at
/// stale prices, all live and fillable.
#[cfg(test)]
mod cancel_addressing_tests {
    use super::*;

    /// Action has no Debug (it lives in the exec module and carries venue
    /// request types), so describe it for assertions.
    fn describe(a: &Action) -> String {
        match a {
            Action::Place(p) => format!("place {} @{} as {}", p.market, p.price, p.client_order_id),
            Action::Cancel(c) => format!("cancel {:?}", c.by),
            Action::SweepAndVerify => "sweep".to_string(),
        }
    }

    fn describe_all(v: &[Action]) -> Vec<String> {
        v.iter().map(describe).collect()
    }

    /// The one reprice the armed M3 run made, verbatim from
    /// `data/trader-rs/m3-intents.jsonl` — the line that left two Kalshi asks
    /// resting (5 @ 0.12 and 5 @ 0.13) where the engine believed it held one.
    const LIVE_REPRICE: &str = r#"{"count":5,"old_price":"0.12","order_id":"m1785257819053","place":"KXNOBELPEACE-27-STC","price":"0.13","replaces":"m1785257819045","side":"ask","ts":1785257814.0,"venue":"kalshi"}"#;

    fn intent(s: &str) -> Intent {
        Intent::from_line(s).expect("test fixture json")
    }

    /// A cancel the engine has decided on. Built rather than written as JSON,
    /// because the venue used to be handed to `intent_actions` ALONGSIDE the
    /// line and could contradict it; it now rides on the intent, so there is
    /// only one of it to get wrong.
    fn cancel_of(market: &str, oid: &str, venue: Venue) -> Intent {
        Intent::Cancel(arb_core::intent::Cancel {
            cancel: market.into(),
            order_id: oid.into(),
            price: "0.12".into(),
            side: BookSide::Ask,
            ts: 1.0,
            venue,
        })
    }

    /// A fixed monotonic origin. `Instant` cannot be constructed from a number,
    /// so the tests take one `now` and add to it — which is also the only way to
    /// exercise the deadline without sleeping.
    fn t0() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn after(t: std::time::Instant, secs: f64) -> std::time::Instant {
        t + std::time::Duration::from_secs_f64(secs)
    }

    /// A cancel goes out addressed with the venue's own id, learned from the ack.
    #[test]
    fn a_cancel_carries_the_venues_order_id_not_ours() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1".to_string(), "66e1c799-507b".to_string());
        let mut parked = HashMap::new();
        let line = intent(
            r#"{"cancel":"KXNOBELPEACE-27-STC","order_id":"m1","price":"0.12","side":"ask","ts":1.0,"venue":"kalshi"}"#,
        );

        let acts = intent_actions(&line, true, &oid_venue, &mut parked, t0());
        assert_eq!(
            describe_all(&acts),
            vec![r#"cancel VenueId("66e1c799-507b")"#],
            "the cancel must address the venue's id"
        );
        assert!(parked.is_empty(), "nothing to park once the ack has landed");
        let Action::Cancel(c) = &acts[0] else { panic!("expected a cancel") };
        assert_eq!(
            c.market_slug.as_deref(),
            Some("KXNOBELPEACE-27-STC"),
            "PM-US needs the slug in the body; it rides on every cancel"
        );
    }

    /// No ack yet => the cancel is PARKED, never sent with our id. Sending it
    /// would be the phantom cancel both venues report as success.
    #[test]
    fn a_cancel_with_no_venue_id_yet_is_parked_and_nothing_is_sent() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let line = cancel_of("KXNOBELPEACE-27-STC", "m1", Venue::Kalshi);

        let t = t0();
        let acts = intent_actions(&line, true, &oid_venue, &mut parked, t);
        assert!(acts.is_empty(), "nothing may be sent: {:?}", describe_all(&acts));
        let p = parked.get("m1").expect("the cancel must be parked, not dropped");
        assert_eq!(p.venue, Venue::Kalshi);
        assert_eq!(p.market, "KXNOBELPEACE-27-STC");
        assert_eq!(p.since, t);
        assert!(!p.escalated);
    }

    /// A repeated cancel intent for the same order keeps the ORIGINAL park time,
    /// so the escalation deadline cannot be pushed out forever.
    #[test]
    fn re_parking_the_same_cancel_does_not_reset_its_clock() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("K", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);
        intent_actions(&line, true, &oid_venue, &mut parked, after(t, 8.0));
        assert_eq!(parked["m1"].since, t);
        assert_eq!(cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false).len(), 1);
    }

    /// The escalation: a parked cancel whose ack never arrived is re-addressed by
    /// OUR client id — which Kalshi resolves against its own order list and PM-US
    /// refuses locally. It must not expire early, and it must be offered once.
    #[test]
    fn an_unresolved_cancel_escalates_by_client_id_only_after_the_grace() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("KXTEST", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);

        assert!(
            cancel_work(&parked, &oid_venue, after(t, 14.9), false).is_empty(),
            "must not give up on the ack early"
        );
        assert_eq!(
            cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false),
            vec![CancelWork::Escalate {
                oid: "m1".into(),
                venue: Venue::Kalshi,
                market: "KXTEST".into()
            }]
        );
        // ...and once the caller has actually queued it, never again.
        parked.get_mut("m1").unwrap().escalated = true;
        assert!(
            cancel_work(&parked, &oid_venue, after(t, 1e6), false).is_empty(),
            "one escalation per order — it costs a full paginated account read"
        );
    }

    /// A backward clock step must not freeze the deadline, and a forward one must
    /// not expire everything at once. `Instant` is monotonic, so neither is
    /// reachable — this pins the CHOICE of clock, which a switch back to
    /// `SystemTime` would silently undo.
    #[test]
    fn the_park_deadline_is_immune_to_a_clock_step() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("K", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);
        // a clock reading from BEFORE the park (what an NTP step back looks like
        // to a wall clock) must not panic and must not escalate
        assert!(cancel_work(&parked, &oid_venue, t - std::time::Duration::from_secs(3600), false)
            .is_empty());
        assert!(!parked.is_empty(), "and the entry survives");
    }

    /// The escalation is capped at ONE per tick: it costs `all_orders()`, a
    /// paginated read of the whole account history, taken inside the venue
    /// executor's one-at-a-time blocking slot — and it competes for the same
    /// budget as the sweep's only proof that the book is clean.
    #[test]
    fn at_most_one_escalation_is_offered_per_tick() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        for oid in ["m1", "m2", "m3"] {
            let line = cancel_of("K", oid, Venue::Kalshi);
            intent_actions(&line, true, &oid_venue, &mut parked, t);
        }
        let work = cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false);
        assert_eq!(work.len(), 1, "a burst drains at one per second, not all at once");
        assert_eq!(
            work[0],
            CancelWork::Escalate { oid: "m1".into(), venue: Venue::Kalshi, market: "K".into() },
            "the oldest by id order, deterministically"
        );
    }

    /// While killed, `SweepAndVerify` is already queued and is a strictly better
    /// remedy — it reaches orders we hold no id for at all and it PROVES the
    /// outcome. Escalating on top of it would put paginated reads on the same
    /// budget as the sweep's proof-of-clean, which is how a clean shutdown gets
    /// reported "NOT CLEAN at exit".
    #[test]
    fn a_kill_suppresses_escalation_because_the_sweep_is_the_better_remedy() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("K", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);
        assert!(cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, true).is_empty());
        assert!(!parked.is_empty(), "still owed, so still counted");
    }

    /// A late ack must still be able to send the REAL cancel, even after the
    /// escalation has been and gone. The concrete case: a place sits behind an
    /// executor backlog, the escalation fires at +15s and PM-US refuses it by
    /// design, then at +20s the place lands and its ack carries the venue id —
    /// the cancel is now perfectly addressable and somebody has to send it.
    #[test]
    fn a_late_ack_still_sends_the_real_cancel_after_an_escalation() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("slug", "m1", Venue::PolymarketUs);
        intent_actions(&line, true, &oid_venue, &mut parked, t);

        // +15s: the escalation goes out and PM-US refuses it, by design.
        let esc = cancel_work(&parked, &oid_venue, t + CANCEL_ACK_GRACE, false);
        assert!(matches!(esc[0], CancelWork::Escalate { .. }));
        settle(&mut parked, &esc[0], true);
        assert!(
            parked.contains_key("m1"),
            "an escalation must not retire the entry — the cancel is still owed"
        );

        // +20s: the backlogged place finally lands and its ack carries the id
        oid_venue.insert("m1".to_string(), "BH8H83AY09NG".to_string());
        assert_eq!(
            cancel_work(&parked, &oid_venue, after(t, 20.0), false),
            vec![CancelWork::Send {
                oid: "m1".into(),
                venue: Venue::PolymarketUs,
                market: "slug".into(),
                venue_order_id: "BH8H83AY09NG".into(),
            }],
            "an escalated entry is not a retired entry"
        );
    }

    /// THE rule that keeps the gauge honest: a command that was never QUEUED
    /// changes nothing. `dispatch!` returns false when the executor channel is
    /// full and the command is lost; retiring the park entry on that dropped
    /// `cancels_unresolved` back to 0 while the order went on resting.
    #[test]
    fn a_cancel_that_was_never_queued_stays_owed() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("K", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);
        oid_venue.insert("m1".to_string(), "v-1".to_string());

        let w = &cancel_work(&parked, &oid_venue, t, false)[0];
        settle(&mut parked, w, false); // channel full: nothing went out
        assert!(parked.contains_key("m1"), "a lost cancel is still owed");
        assert_eq!(
            cancel_work(&parked, &oid_venue, t, false).len(),
            1,
            "and the next tick retries it"
        );
        settle(&mut parked, w, true); // this one really went out
        assert!(parked.is_empty(), "retired only once the command was queued");
    }

    /// Same rule for the escalation: a lost one must not burn the order's single
    /// escalation, or the one venue-side resolution attempt is spent on nothing.
    #[test]
    fn an_escalation_that_was_never_queued_is_not_spent() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        let line = cancel_of("K", "m1", Venue::Kalshi);
        intent_actions(&line, true, &oid_venue, &mut parked, t);
        let due = t + CANCEL_ACK_GRACE;

        let w = &cancel_work(&parked, &oid_venue, due, false)[0];
        settle(&mut parked, w, false);
        assert!(!parked["m1"].escalated, "a lost escalation did not happen");
        assert_eq!(cancel_work(&parked, &oid_venue, due, false).len(), 1, "retried next tick");
        settle(&mut parked, w, true);
        assert!(parked["m1"].escalated);
        assert!(
            cancel_work(&parked, &oid_venue, due, false).is_empty(),
            "and now it is spent"
        );
    }

    /// The command each item of work actually sends. A `Send` must carry the
    /// venue's id and an `Escalate` must carry ours — swapping them is the whole
    /// original defect.
    #[test]
    fn work_items_address_the_id_space_they_name() {
        let send = CancelWork::Send {
            oid: "m1".into(),
            venue: Venue::Kalshi,
            market: "K".into(),
            venue_order_id: "66e1c799".into(),
        };
        assert_eq!(describe(&send.action()), r#"cancel VenueId("66e1c799")"#);
        let esc =
            CancelWork::Escalate { oid: "m1".into(), venue: Venue::Kalshi, market: "K".into() };
        assert_eq!(describe(&esc.action()), r#"cancel ClientId("m1")"#);
    }

    /// Every parked cancel whose venue id is known is retried — a `Send` is one
    /// targeted DELETE for a cancel we already owe, so it is not rate-capped the
    /// way an escalation is.
    #[test]
    fn resolvable_cancels_are_all_retried_not_capped() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        for oid in ["m1", "m2"] {
            let line = cancel_of("K", oid, Venue::Kalshi);
            intent_actions(&line, true, &oid_venue, &mut parked, t);
            oid_venue.insert(oid.to_string(), format!("v-{oid}"));
        }
        let work = cancel_work(&parked, &oid_venue, t, false);
        assert_eq!(work.len(), 2, "{work:?}");
        assert!(matches!(work[0], CancelWork::Send { .. }));
    }

    /// THE reprice fix: an amend is a cancel AND a place, cancel first.
    #[test]
    fn a_reprice_cancels_the_replaced_order_before_placing_the_new_one() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1785257819045".to_string(), "d56aa591-4b72".to_string());
        let mut parked = HashMap::new();

        let acts = intent_actions(
            &intent(LIVE_REPRICE),
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(
            describe_all(&acts),
            vec![
                r#"cancel VenueId("d56aa591-4b72")"#,
                "place KXNOBELPEACE-27-STC @0.13 as m1785257819053",
            ],
            "the replaced order must be cancelled, and cancelled FIRST"
        );
    }

    /// ...and the cancel names the order being REPLACED, never the new one.
    #[test]
    fn the_reprice_cancel_names_the_old_order_not_the_new_one() {
        let mut oid_venue = HashMap::new();
        oid_venue.insert("m1785257819045".to_string(), "old-venue-id".to_string());
        oid_venue.insert("m1785257819053".to_string(), "new-venue-id".to_string());
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &intent(LIVE_REPRICE),
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        let Action::Cancel(c) = &acts[0] else { panic!("expected the cancel first") };
        assert_eq!(c.by, CancelBy::VenueId("old-venue-id".into()));
    }

    /// A reprice whose replaced order has no ack yet still PLACES — the quoter
    /// has already moved its resting state and cannot be told otherwise, so
    /// withholding the place would make the market go quietly dark. The cancel
    /// is parked and counted instead of being sent to a phantom.
    #[test]
    fn a_reprice_with_an_unacked_old_order_places_and_parks_the_cancel() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &intent(LIVE_REPRICE),
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(
            describe_all(&acts),
            vec!["place KXNOBELPEACE-27-STC @0.13 as m1785257819053"]
        );
        assert!(parked.contains_key("m1785257819045"), "the cancel is owed, not forgotten");
    }

    /// A plain place is still exactly one command, and a hedge is still an IOC.
    #[test]
    fn a_place_without_replaces_emits_one_command() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &intent(
                r#"{"count":25,"order_id":"m9","place":"KXTEST","price":"0.31","side":"bid","ts":1.0,"venue":"kalshi"}"#,
            ),
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(acts.len(), 1);
        let Action::Place(p) = &acts[0] else { panic!("expected a place") };
        assert_eq!(p.qty, 25);
        assert!(p.post_only, "a maker quote rests");
        assert!(matches!(p.tif, Tif::Gtc));

        let hedge = intent_actions(
            &intent(
                r#"{"count":25,"order_id":"h1","place":"KXTEST","price":"0.30","side":"ask","tag":"hedge","taker":true,"ts":1.0,"venue":"kalshi"}"#,
            ),
            true,
            &oid_venue,
            &mut parked,
            t0(),
        );
        let Action::Place(p) = &hedge[0] else { panic!("expected a place") };
        assert!(!p.post_only, "a hedge crosses");
        assert!(matches!(p.tif, Tif::Ioc));
    }

    /// The side an intent decided is the side the venue is asked for, both ways
    /// round — the assertion this file did not have while the mapping was
    /// `if p.side == "ask" { Ask } else { Bid }`.
    ///
    /// That form had no way to be wrong about `"ask"` and no way to be right
    /// about anything else: every other value — a typo, a case difference, a
    /// venue's own spelling — took the else and became a BUY. It cannot arise
    /// now, because a place carrying one does not parse (`arb_core::intent`
    /// pins that) and would not compile if it were built by hand.
    #[test]
    fn a_place_asks_the_venue_for_the_side_the_intent_decided() {
        for (side, want) in [("bid", VenueSide::Bid), ("ask", VenueSide::Ask)] {
            let mut parked = HashMap::new();
            let acts = intent_actions(
                &intent(&format!(
                    r#"{{"count":5,"order_id":"m1","place":"KXTEST","price":"0.31","side":"{side}","ts":1.0,"venue":"kalshi"}}"#
                )),
                true,
                &HashMap::new(),
                &mut parked,
                t0(),
            );
            let Action::Place(p) = &acts[0] else { panic!("expected a place") };
            assert_eq!(p.side, want, "a {side} intent must place on the {side}");
        }
    }

    /// An UNARMED engine never receives an ack (the executor drops the command
    /// before any venue call), so parking would accumulate forever and prove
    /// nothing. It addresses the cancel by our client id — an honest statement
    /// of all it knows — and the inert sink drops it.
    #[test]
    fn a_dry_run_addresses_a_cancel_by_our_client_id_and_never_parks() {
        let oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let acts = intent_actions(
            &cancel_of("KXTEST", "m1", Venue::Kalshi),
            false,
            &oid_venue,
            &mut parked,
            t0(),
        );
        assert_eq!(describe_all(&acts), vec![r#"cancel ClientId("m1")"#]);
        assert!(parked.is_empty(), "an unarmed engine has nothing to wait for");
    }
}
