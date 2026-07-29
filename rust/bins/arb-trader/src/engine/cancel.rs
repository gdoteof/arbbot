//! Cancelling an order at a venue that accepts only its OWN id for it.
//!
//! The engine names its orders; both venues rename them. A cancel therefore
//! cannot be sent until the `order_ack` carrying the venue's id comes back, and
//! sending ours instead is a no-op that BOTH venues report as success. So a
//! cancel the engine has decided on is PARKED — addressable or not — escalated
//! once if the ack never arrives, and retired only when the VENUE says the
//! order is gone. Not when a command was queued for it: an executor channel
//! with room in it is not a venue that took the cancel, and while those two
//! were the same statement a refused cancel left an order resting that nothing
//! in this process still knew it owed a cancel for.
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
/// A cancel is parked once it is DECIDED, addressable or not — that is the
/// record that it is owed, and the only reason a refused one can be sent again.
///
/// An entry is retired when the VENUE says so (a `cancel_result` naming it) and
/// at no other moment: not when the ack lands, not when it is escalated, and —
/// the 2026-07-29 defect — not when the executor's channel merely accepted the
/// command. `try_send` returning true says the command was QUEUED; the venue can
/// still answer 502, and by then the quoter has already dropped its own record
/// of the order, so nothing in the process would ever try to cancel it again.
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
    /// Venue-addressed cancels DISPATCHED for this order. Bounded by
    /// [`MAX_CANCEL_TRIES`]: a retry that never gives up against a venue that
    /// keeps refusing is a self-inflicted 429, and the sweep is the remedy that
    /// does not need this order's id at all.
    pub(super) tries: u32,
    /// When the last one went, or `None` when none is outstanding — either
    /// because none has been sent yet or because the venue has already REFUSED
    /// the last one, which re-arms it for the next tick.
    pub(super) last_try: Option<std::time::Instant>,
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

/// How long a DISPATCHED cancel waits for the venue's answer before the engine
/// stops expecting one and offers the cancel again.
///
/// Strictly LONGER than the transport's own 15s timeout (`main.rs`), for the
/// reason `exec::QUIESCE_BUDGET` is: a cancel still on the wire at 15s will
/// answer by itself, and sending it again before then is a duplicate we
/// inflicted on ourselves. This deadline is the backstop for an answer that
/// never comes at all (an executor that died, a dry-run channel) — the normal
/// path is the `cancel_result` the executor emits either way.
const CANCEL_RESULT_GRACE: std::time::Duration = std::time::Duration::from_secs(20);

/// How many times ONE order's cancel may be put on the wire before the engine
/// stops. A venue that has refused three targeted cancels is not going to take
/// the fourth, and an unbounded retry there is a self-inflicted 429 against the
/// same budget the hedge path needs. What is left after that is the sweep, which
/// reaches the order without needing its id and PROVES the outcome — and the
/// entry stays parked, so `cancels_unresolved` keeps saying it is owed.
const MAX_CANCEL_TRIES: u32 = 3;

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
/// EITHER WAY the cancel is parked, which is the 2026-07-29 change. An
/// addressable cancel used to return here and be forgotten in the same
/// statement, so a venue that refused it — 502, connection reset, a 429 the
/// limiter did not shape — left an order resting that nothing in the process
/// still knew it owed a cancel for: the quoter has already dropped it
/// (`Quoter::on_book` removes the resting entry as it emits the amend) and the
/// park had never held it. The park is now the record of the obligation, and
/// `on_venue_answer` is the only thing that discharges it.
///
/// `armed` false means no sink exists, so no place ever reaches a venue and no
/// `order_ack` — and no `cancel_result` — ever comes back: parking would
/// accumulate forever and prove nothing. A dry run therefore addresses the
/// cancel by our client id, which is an honest statement of everything it knows,
/// and the inert executor drops it.
fn resolve_cancel(
    oid: &str,
    market: &str,
    venue: Venue,
    armed: bool,
    oid_venue: &HashMap<String, String>,
    parked: &mut HashMap<String, ParkedCancel>,
    now: std::time::Instant,
) -> Option<Action> {
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
        tries: 0,
        last_try: None,
    });
    oid_venue.get(oid).map(|vid| cancel_by_venue_id(vid, market))
}

/// The order whose cancel this intent implies, so the caller that DISPATCHES
/// that cancel can record the attempt against the right park entry. An amend's
/// cancel names the order being replaced, never the new one.
pub(super) fn cancel_target(intent: &Intent) -> Option<&str> {
    match intent {
        Intent::Place(p) => p.replaces.as_deref(),
        Intent::Cancel(c) => Some(&c.order_id),
        Intent::HedgeNeeded(_) | Intent::Skip(_) => None,
    }
}

/// One item of work on the park, carrying everything the dispatch needs. Split
/// out so the policy is a pure function of (park, mappings, clock, killed) and
/// can be tested without a runtime.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum CancelWork {
    /// The venue's id is known now, so send the real cancel. The entry STAYS
    /// parked until the venue answers — see [`on_venue_answer`].
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
///   * a `Send` counts as an ATTEMPT only when the cancel is really on its way.
///     It does not retire anything: the executor's channel accepting a command
///     is not the venue accepting a cancel, and treating the two as one answer
///     is how a refused cancel became an order nothing would ever try again;
///   * an `Escalate` burns the order's single escalation only when it was really
///     sent, so a lost one is retried on the next tick.
pub(super) fn settle(parked: &mut HashMap<String, ParkedCancel>, work: &CancelWork, queued: bool) {
    if !queued {
        return;
    }
    match work {
        CancelWork::Send { oid, .. } => mark_sent(parked, oid, true),
        CancelWork::Escalate { oid, .. } => {
            if let Some(p) = parked.get_mut(oid) {
                p.escalated = true;
            }
        }
    }
}

/// Record that a venue-addressed cancel for `oid` really went out.
///
/// Not a discharge — an ATTEMPT. The entry stops being offered while the answer
/// is outstanding (so the retry is not a duplicate), and it is spent against
/// [`MAX_CANCEL_TRIES`]. A cancel the channel LOST (`queued` false) never
/// happened and costs nothing.
pub(super) fn mark_sent(parked: &mut HashMap<String, ParkedCancel>, oid: &str, queued: bool) {
    if !queued {
        return;
    }
    if let Some(p) = parked.get_mut(oid) {
        p.tries += 1;
        p.last_try = Some(std::time::Instant::now());
    }
}

/// What the VENUE said about a cancel we sent — the only thing that retires a
/// cancel the engine owes.
///
/// Success is a discharge: the order is gone (both gateways answer `Ok` only for
/// that, Kalshi's 404-means-already-gone included). A REFUSAL re-arms the entry
/// so the next tick sends it again, which is the whole of the 2026-07-29 defect:
/// a 502 on a cancel used to be one `exec_failed` counter and one log line, with
/// every record that the order existed already retired.
///
/// Returns true when this refusal was the one that spent the order's last try —
/// the single case a human has to be told about, because from then on only the
/// sweep can reach that order.
pub(super) fn on_venue_answer(
    parked: &mut HashMap<String, ParkedCancel>,
    oid: &str,
    ok: bool,
) -> bool {
    if ok {
        parked.remove(oid);
        return false;
    }
    match parked.get_mut(oid) {
        Some(p) => {
            // No longer waiting on an answer: it came, and it was no.
            p.last_try = None;
            p.tries >= MAX_CANCEL_TRIES
        }
        None => false,
    }
}

/// What the cancel deadline should do this tick, in dispatch order.
///
/// `Send` is offered for every cancel we owe that is addressable, is not already
/// waiting on the venue's answer, and has tries left. It is not rate-capped the
/// way an escalation is — each is one targeted DELETE for a cancel we already
/// owe — but it IS bounded per order ([`MAX_CANCEL_TRIES`]).
/// `Escalate` is capped at ONE per tick, and skipped entirely while killed:
///   * a client-id cancel costs `all_orders()`, a paginated read of the FULL
///     order history, taken inside the venue executor's one-command-at-a-time
///     `spawn_blocking` — so a burst of them blocks every place and cancel for
///     that venue, including `resting_order_ids`, which is the only evidence
///     `cancel_all_and_verify` accepts. Delaying that turns a clean shutdown
///     into "NOT CLEAN at exit". (It no longer STARVES it: since 2026-07-29
///     that read is order-path priority and draws from no budget — but the
///     serialisation this cap exists for is unchanged.)
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
            // A cancel already on the wire is not a cancel to send again; only
            // a refusal (which clears `last_try`) or the grace expiring makes
            // it due. Saturating, so a `now` from before the dispatch — the
            // clock-step test's shape — reads as "still waiting", never as a
            // licence to duplicate.
            let waiting = p
                .last_try
                .is_some_and(|t| now.saturating_duration_since(t) < CANCEL_RESULT_GRACE);
            if !waiting && p.tries < MAX_CANCEL_TRIES {
                out.push(CancelWork::Send {
                    oid: oid.clone(),
                    venue: p.venue,
                    market: p.market.clone(),
                    venue_order_id: vid.clone(),
                });
            }
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

    /// The venue's answer to a cancel this engine sent (`exec::cancel_result`).
    ///
    /// This is the event the executor never emitted. Its absence is why a
    /// refused cancel was indistinguishable from a discharged one: the only
    /// thing that ever retired the obligation was `try_send` returning true,
    /// which says the executor's channel had room and nothing whatever about
    /// the venue.
    pub(super) fn on_cancel_result(&mut self, v: &serde_json::Value, t_read: std::time::Instant) {
        let ok = v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false);
        // Ours when the cancel was addressed by our id (an escalation);
        // otherwise the venue's, mapped back through the ack that taught us the
        // pair. An id we cannot map names no obligation, so there is nothing to
        // settle.
        let oid = match v.get("order_id").and_then(|x| x.as_str()) {
            Some(ours) => Some(ours.to_string()),
            None => v
                .get("venue_order_id")
                .and_then(|x| x.as_str())
                .and_then(|theirs| self.venue_oid.get(theirs).cloned()),
        };
        if let Some(oid) = oid {
            if on_venue_answer(&mut self.parked_cancels, &oid, ok) {
                let p = &self.parked_cancels[&oid];
                eprintln!(
                    "[engine] cancel {oid} ({} on {}) has now been REFUSED by the venue \
                     {MAX_CANCEL_TRIES} times ({}) — this engine will not send it again. \
                     If that order is still RESTING it is live at a price this engine has \
                     already rejected, and only a sweep reaches it now. CHECK THE VENUE.",
                    p.market,
                    p.venue.as_str(),
                    v.get("error").and_then(|x| x.as_str()).unwrap_or("no reason given"),
                );
            }
        }
        self.decision.record(t_read.elapsed().as_nanos() as u64);
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
        assert!(
            parked.contains_key("m1"),
            "...and it stays OWED until the venue says the order is gone. It used to be \
             forgotten in the same statement that sent it, so a venue that refused it left \
             an order resting that nothing still knew it owed a cancel for"
        );
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

    /// THE rule that keeps the gauge honest, and the 2026-07-29 correction to
    /// it. A command that was never QUEUED changes nothing — `dispatch!` returns
    /// false when the executor channel is full and the command is lost. But a
    /// command that WAS queued is not an outcome either: `try_send` returning
    /// true says the executor's channel had room, which is a claim about a
    /// bounded mpsc, not about a venue. Retiring the obligation there is what
    /// made a refused cancel indistinguishable from a completed one.
    #[test]
    fn a_queued_cancel_is_still_owed_until_the_venue_confirms() {
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
        assert_eq!(parked["m1"].tries, 0, "a command that was lost cost no attempt");

        settle(&mut parked, w, true); // this one really went out
        assert!(
            parked.contains_key("m1"),
            "STILL owed: the executor took the command, the VENUE has not answered"
        );
        assert_eq!(parked["m1"].tries, 1);
        assert!(
            cancel_work(&parked, &oid_venue, t, false).is_empty(),
            "and it is not sent twice while that answer is outstanding"
        );

        // ...and now the venue says the order is gone. Only this retires it.
        assert!(!on_venue_answer(&mut parked, "m1", true), "a success is not an exhaustion");
        assert!(parked.is_empty(), "retired by the VENUE, and by nothing else");
    }

    /// **DEFECT 1.** A cancel the venue REFUSES stays owed and is sent again —
    /// and the retry is BOUNDED.
    ///
    /// Before this, `Ok(Err(e))` in the executor was the whole handling: one
    /// counter, one log line, no channel back to the engine. By then the quoter
    /// had already dropped its record of the order (`Quoter::on_book` removes
    /// the resting entry as it emits the amend) and the park had never held an
    /// addressable cancel at all, so nothing in the process would ever try
    /// again: the superseded quote rested beside its replacement, both fillable,
    /// one at a price the engine had already decided was wrong — for as long as
    /// the market lasted.
    #[test]
    fn a_cancel_the_venue_refuses_is_retried_and_is_bounded() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        intent_actions(&cancel_of("K", "m1", Venue::Kalshi), true, &oid_venue, &mut parked, t);
        oid_venue.insert("m1".to_string(), "v-1".to_string());

        for n in 1..=MAX_CANCEL_TRIES {
            let work = cancel_work(&parked, &oid_venue, t, false);
            assert_eq!(work.len(), 1, "try {n}: the cancel is still owed, so it goes again");
            settle(&mut parked, &work[0], true);
            // 502 / reset / refusal: the venue did NOT cancel it.
            let exhausted = on_venue_answer(&mut parked, "m1", false);
            assert_eq!(exhausted, n == MAX_CANCEL_TRIES, "only the last one is the alarm");
            assert!(parked.contains_key("m1"), "a refused cancel is not a discharged one");
        }
        assert!(
            cancel_work(&parked, &oid_venue, after(t, 1e6), false).is_empty(),
            "BOUNDED: an unbounded retry against a venue that keeps refusing is a \
             self-inflicted 429, and the sweep is what reaches the order after this"
        );
        assert_eq!(parked["m1"].tries, MAX_CANCEL_TRIES);
    }

    /// The backstop for an answer that never arrives at all — an executor that
    /// died, or a `cancel_result` lost with the channel. The entry must not
    /// wedge waiting forever; it becomes due again after the grace, still inside
    /// the same try budget.
    ///
    /// The grace is longer than the transport's own 15s timeout on purpose: a
    /// cancel still on the wire will answer by itself, and re-sending it before
    /// then is a duplicate we inflicted on ourselves.
    #[test]
    fn a_cancel_the_venue_never_answers_is_sent_again_after_the_grace() {
        let mut oid_venue = HashMap::new();
        let mut parked = HashMap::new();
        let t = t0();
        intent_actions(&cancel_of("K", "m1", Venue::Kalshi), true, &oid_venue, &mut parked, t);
        oid_venue.insert("m1".to_string(), "v-1".to_string());
        let w = cancel_work(&parked, &oid_venue, t, false);
        settle(&mut parked, &w[0], true);

        assert!(
            cancel_work(&parked, &oid_venue, t + CANCEL_RESULT_GRACE / 2, false).is_empty(),
            "a cancel on the wire is not a cancel to send again"
        );
        assert!(
            CANCEL_RESULT_GRACE > std::time::Duration::from_secs(15),
            "the grace must outlast the transport timeout, or it duplicates a live call"
        );
        assert_eq!(
            cancel_work(&parked, &oid_venue, t + CANCEL_RESULT_GRACE * 2, false).len(),
            1,
            "silence past the grace is not an answer; the cancel is still owed"
        );
    }

    /// The whole chain, through the real engine and the executor's OWN line
    /// builder: a cancel is decided, dispatched, and the venue refuses it.
    ///
    /// This is the producer/consumer agreement as well as the state machine —
    /// the executor writes `cancel_result` and `on_feed` routes it, and a
    /// disagreement about either would leave the refusal unread, which is
    /// exactly the silence the defect was made of.
    #[test]
    fn the_engine_re_arms_a_cancel_the_venue_refused_and_retires_one_it_took() {
        use crate::engine::{test_cfg, test_engine, RunCfg};
        use arb_venue::gateway::CancelBy;

        let mut e = test_engine(RunCfg { armed: true, ..test_cfg() });
        // The ack that made the cancel addressable in the first place.
        e.on_order_ack(
            &serde_json::json!({"order_id": "m1", "venue_order_id": "BH8H83AY09NG"}),
            1_000_000_000,
            std::time::Instant::now(),
        );
        e.intents.push(cancel_of("slug", "m1", Venue::PolymarketUs));
        e.drain_intents(Option::<&arb_core::scan::Rel>::None);
        assert!(
            e.parked_cancels.contains_key("m1"),
            "owed from the moment it was DECIDED, addressable or not"
        );

        // 502. The order is still resting; the quoter has already forgotten it.
        let refused = crate::exec::cancel_result(
            Venue::PolymarketUs,
            &CancelBy::VenueId("BH8H83AY09NG".into()),
            "slug",
            Some("pmus cancel: HTTP 502: bad gateway"),
        );
        e.on_feed(
            crate::feed::FeedMsg {
                line: refused.to_string(),
                t_read: std::time::Instant::now(),
            },
            0,
            &mut [],
            &HashMap::new(),
        );
        assert!(
            e.parked_cancels.contains_key("m1"),
            "a cancel the venue refused is a cancel this engine still owes"
        );
        assert_eq!(e.summary()["cancels_unresolved"], serde_json::json!(1), "and it says so");

        // ...and the retry lands.
        let took = crate::exec::cancel_result(
            Venue::PolymarketUs,
            &CancelBy::VenueId("BH8H83AY09NG".into()),
            "slug",
            None,
        );
        e.on_feed(
            crate::feed::FeedMsg { line: took.to_string(), t_read: std::time::Instant::now() },
            0,
            &mut [],
            &HashMap::new(),
        );
        assert!(e.parked_cancels.is_empty(), "the VENUE discharged it, and only the venue can");
    }

    /// A refusal for an order we owe NOTHING for changes nothing — and cannot
    /// panic. The engine cancels orders it never parked in a dry run, and a
    /// venue id it has no mapping for names no obligation at all.
    #[test]
    fn a_venue_answer_for_an_unowed_cancel_is_a_no_op() {
        let mut parked: HashMap<String, ParkedCancel> = HashMap::new();
        assert!(!on_venue_answer(&mut parked, "m-nobody", false));
        assert!(!on_venue_answer(&mut parked, "m-nobody", true));
        assert!(parked.is_empty());
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
