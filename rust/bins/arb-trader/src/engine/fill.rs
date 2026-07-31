//! What one fill frame MEANS, and what the engine owes because of it.
//!
//! `Engine::attribute_fill` is the whole of that question: a frame is a hedge
//! discharging an obligation, a maker fill minting one, a report we have
//! already seen, or money we cannot yet explain. It has two callers — the
//! `fill` arm and the `order_ack` arm, which replays a fill that beat its own
//! ack — and they must never disagree about what a fill means, which is why it
//! is ONE function and not two copies of one.
//!
//! `book_basket` is the other end of the same story: the accounting record a
//! discharged obligation writes, which the next startup seeds its exposure
//! from.

use super::cancel::{settle, CancelWork};
use super::hedge::{first_attempt_acceptable, hedge_credit, taking_side, HedgeOrder, PendingHedge};
use super::Engine;
use arb_core::intent::{self, Intent, Tag};
use arb_core::model::{BookSide, Venue};
use arb_core::scan::Rel;
use serde_json::json;
use std::collections::HashMap;

/// The maker order behind a basket: everything the ledger record needs about
/// the leg we rested.
pub(super) struct MakerOrder {
    pub(super) rel_id: String,
    pub(super) class: &'static str,
    pub(super) venue: String,
    pub(super) market_id: String,
    /// Serialises into the ledger leg as `"bid"`/`"ask"` — the same bytes it
    /// wrote as a `String`, because `BookSide` renames to the wire spelling.
    pub(super) side: BookSide,
    pub(super) price: String,
    /// Which strategy opened this leg — `maker-hedge` or `take-take`.
    ///
    /// Take-take reuses the maker fill -> hedge pipeline, which is the right
    /// call mechanically but made the ACCOUNTING lie: every take-take basket
    /// booked as `strategy: maker-hedge` with its leg 1 tagged `role: maker`,
    /// when leg 1 was a marketable IOC. P&L could not be attributed between
    /// the two strategies, and Python's auto_take_take.py writes `take-take`,
    /// so the same trade had two names depending on which stack made it.
    pub(super) strategy: &'static str,
}

impl MakerOrder {
    /// Leg 1's true role. Take-take crosses the book; the maker path rests.
    fn role(&self) -> &'static str {
        if self.strategy == "take-take" {
            "taker"
        } else {
            "maker"
        }
    }

    /// Did this order REST at the venue, and so reserve capital in the risk
    /// view? Only the maker path does.
    ///
    /// This is not cosmetic. The reservation key is `(rel_id, market, side)`
    /// and take-take leg 1 SHARES one with the maker: a Kalshi-lead crossing is
    /// a BID on the Kalshi market (`Candidate::leg1`), the maker quotes both
    /// legs of the same relationship (`maker_leg_indices`), and take-take is
    /// placed through `drain_intents(Some(rel))`, so `order_rel` carries the
    /// identical triple. A take-take fill calling `consume` therefore deletes
    /// the MAKER's reservation for a quote that is still resting at the venue —
    /// `record_open` books the crossing's contracts and the same call frees the
    /// maker's, so the gate's total does not move while real committed capital
    /// rose. If the maker's target price has not changed, `Quoter::on_book`
    /// takes the hysteresis `continue` and never re-reserves.
    ///
    /// It does not RESERVE either, and must not: reserving would overwrite the
    /// maker's slot with the same collision, and an IOC that does not fill dies
    /// at the venue with no cancel to release it.
    fn rested(&self) -> bool {
        self.strategy != "take-take"
    }
}

/// Book a completed basket: the maker leg filled and its hedge filled, so the
/// position is real and the next restart must see it.
///
/// Deliberately NOT fee-complete. The engine knows the prices it traded at, but
/// venue fees arrive on the fill reports the reconciler reads, so writing a
/// `cost_usd` here would be a guess in the accounting record. `fees_pending`
/// says so out loud rather than shipping a confident wrong number.
/// `hedge_order_id` is OUR id for the attempt that filled — the one leg 2 is
/// stamped with, so a basket in the file names both of the orders that made it
/// and not just the maker's.
#[allow(clippy::too_many_arguments)]
pub(super) fn book_basket(
    path: &str,
    maker: &MakerOrder,
    hedge: &HedgeOrder,
    hedge_order_id: &str,
    qty: i64,
    ts: f64,
) {
    let rec = json!({
        "ts": ts,
        "relationship_id": maker.rel_id,
        "title": format!("{} (rust {})", maker.rel_id, maker.strategy),
        "qty": qty,
        "strategy": maker.strategy,
        "status": "open",
        "source": crate::ledger::SOURCE,
        "fees_pending": true,
        "legs": [
            {"venue": maker.venue, "market_id": maker.market_id, "side": maker.side,
             "role": maker.role(), "qty": qty, "yes_price": maker.price,
             "order_id": hedge.maker_order_id},
            {"venue": hedge.venue, "market_id": hedge.market_id, "side": hedge.side,
             "role": "taker", "qty": qty, "yes_price": hedge.price,
             "order_id": hedge_order_id},
        ],
    });
    match crate::ledger::append_basket(path, rec.clone()) {
        Ok(crate::ledger::Booking::Booked) => {}
        Ok(crate::ledger::Booking::AlreadyBooked) => eprintln!(
            "[ledger] ALREADY BOOKED {} at ts {ts} — a record with that exact key is \
             already in {path} and this one was NOT written. (relationship_id, ts) is \
             what every correction and unwind addresses a basket by, so two of them \
             would leave both ambiguous.",
            maker.rel_id
        ),
        Ok(crate::ledger::Booking::Contested(others)) => {
            for other in others {
                eprintln!(
                    "[ledger] CONTESTED {} — another writer already booked an OPEN basket \
                     on {} + {} at ts {other}, and this engine has just booked its own \
                     fill at ts {ts}. BOTH are in the ledger and BOTH count as exposure, \
                     because content cannot tell one position booked twice from two real \
                     fills seconds apart. If a human confirms it was ONE position, void \
                     the other by appending: {{\"ts\":<now>,\"relationship_id\":\"{}\",\
                     \"status\":\"correction\",\"corrects_ts\":{other},\"reason\":\"<why>\",\
                     \"fields\":{{\"status\":\"superseded\"}}}}. If it was TWO fills, the \
                     account is over-hedged on {} — RECONCILE BY HAND.",
                    maker.rel_id, maker.market_id, hedge.market_id, maker.rel_id,
                    hedge.market_id
                );
            }
        }
        Err(e) => {
            // The priority used to be inverted: a WAL write failure crash-stops the
            // process (wal.rs), while a LEDGER write failure was one stderr line and
            // trading carried on. The ledger is the more important file — an
            // unbooked basket is exposure no restart can see, so the caps free
            // themselves against it and nothing ever unwinds it — so it now stops
            // trading on the same terms, through the same clean halt: the book is
            // cancelled and PROVEN empty before the exit.
            eprintln!("[ledger] WRITE FAILED ({e}) — basket {} is UNBOOKED", maker.rel_id);
            eprintln!("[ledger] RECOVER BY HAND — append this line to {path}: {rec}");
            crate::exec::spawn_halt_and_exit(70, format!("ledger write failed: {e}"));
        }
    }
}

/// Which arm of the attribution one fill frame took.
///
/// Two gauges hang off this and they are NOT the same question: `fills` counts
/// frames belonging to a maker order of ours, while tape time advances for any
/// frame that is not a hedge's (which is what it did before the fill arm was
/// rewritten — a hedge fill has never advanced it). Returning a bool conflated
/// them and made `fills` over-report by every foreign fill on the account.
enum FillArm {
    /// Discharged (part of) a hedge obligation.
    Hedge,
    /// Belongs to a maker order of ours — a new fill or a replayed duplicate.
    Maker,
    /// Belongs to no order we know; held for its `order_ack`.
    Unattributed,
}

/// A fill the engine cannot attribute YET.
///
/// It is HELD, not dropped. A fill can beat its own `order_ack`: the ack is
/// emitted by the executor once the place's HTTP response returns (`exec.rs`),
/// while the fill arrives on the venue's private socket, and Kalshi's fill
/// channel carries only Kalshi's own order id — so until the ack lands there is
/// nothing to map it to. Dropping the frame there meant the basket was never
/// booked, nothing was credited, and the 5-second retry bought the hedge a
/// SECOND time; on take-take leg 1 no hedge was minted at all and `record_open`
/// never fired, so the concentration cap never bound.
///
/// What is still unclaimed past the grace is money that moved in our account
/// that we cannot explain, and it ALARMS. It is deliberately not fatal: the
/// Kalshi fill channel subscribes account-wide on purpose (`fills.rs`), so a
/// fill from a hand trade or another tool in this account is expected, and
/// halting on one would be a self-inflicted outage. It is also not credited to
/// any obligation — attributing money by guesswork is the one thing worse than
/// saying we cannot.
pub(super) struct UnclaimedFill {
    pub(super) venue: Venue,
    pub(super) market_id: String,
    /// Highest cumulative count reported. Cumulative semantics make keeping the
    /// maximum equivalent to replaying every frame.
    pub(super) cum: i64,
    /// MONOTONIC first-seen time, for the same reason `ParkedCancel::since` is:
    /// tape time stops advancing exactly when the feed dies, and this deadline
    /// must still fire. Re-parking keeps the ORIGINAL time.
    pub(super) since: std::time::Instant,
}

/// How long a fill waits for the `order_ack` that would make it attributable
/// before the engine calls it unexplained money.
///
/// Same bound as `CANCEL_ACK_GRACE` and for the same reason: the place's HTTP
/// timeout is 15s (`main.rs`), so past that the ack has either arrived or never
/// will. It is longer than the 5s hedge-retry interval on purpose — the retry
/// holds while a fill on its market is unclaimed, so the grace is exactly how
/// long the engine will wait for proof before it risks hedging twice.
pub(super) const FILL_ACK_GRACE: std::time::Duration = std::time::Duration::from_secs(15);

/// Held fills that have waited past the grace, in a deterministic order.
pub(super) fn unclaimed_expired(
    park: &HashMap<String, UnclaimedFill>,
    now: std::time::Instant,
) -> Vec<String> {
    let mut out: Vec<String> = park
        .iter()
        .filter(|(_, u)| now.saturating_duration_since(u.since) >= FILL_ACK_GRACE)
        .map(|(k, _)| k.clone())
        .collect();
    out.sort();
    out
}

impl Engine {
    /// Attribute ONE fill frame to the order it belongs to, and act on it.
    ///
    /// Returns which arm it took (`FillArm`), because the caller's two gauges ask
    /// different questions: `fills` counts maker frames only, and tape time
    /// advances for anything that is not a hedge frame.
    ///
    /// Called from the `fill` arm and from the `order_ack` arm, which replays a
    /// held fill the moment its ack makes it addressable. `since` is the
    /// MONOTONIC time the frame was first seen, so a frame going back on the
    /// hold keeps its original deadline rather than restarting it.
    ///
    /// This is one function rather than two copies because the two callers must
    /// never disagree about what a fill means.
    fn attribute_fill(
        &mut self,
        oid: &str,
        cum: i64,
        venue: Venue,
        market: &str,
        now: f64,
        since: std::time::Instant,
    ) -> FillArm {
        // A hedge fill discharges (part of) an obligation. Hedges are never
        // in the FillLedger — registering one would let its own fill mint
        // another hedge, forever — so they are recognised here.
        match self.hedge_orders.get(oid).cloned() {
            Some(h) => {
                let ob = self.pending_hedges.get(&h.chain_id).map(|p| (p.owed, p.filled));
                let c = hedge_credit(cum, h.qty, h.cum_filled, ob);
                if c.delta > 0 {
                    // I1/I2: credit the ATTEMPT (so its own later frames are
                    // deltas) and the OBLIGATION (so the retry knows what is
                    // left). Retiring the attempt on its first frame is what
                    // booked a 10-lot as a 4-lot and lost the rest.
                    if let Some(o) = self.hedge_orders.get_mut(oid) {
                        o.cum_filled += c.delta;
                    }
                    if let Some(p) = self.pending_hedges.get_mut(&h.chain_id) {
                        p.filled += c.delta;
                    }
                    if c.book > 0 {
                        match self.order_rel.get(&h.maker_order_id) {
                            Some(mo) => {
                                if let Some(lp) = self.cfg.ledger_path.as_deref() {
                                    book_basket(lp, mo, &h, oid, c.book, now);
                                }
                                eprintln!(
                                    "[ledger] booked {} x{} ({} maker / {} hedge)",
                                    mo.rel_id, c.book, mo.market_id, h.market_id
                                );
                            }
                            // Without the maker order there is no relationship
                            // id, so there is no honest ledger record to write
                            // — and an unbooked basket is exposure no restart
                            // can see. `order_rel` is never pruned, so this is
                            // a bug in the engine rather than a venue event.
                            None => eprintln!(
                                "[ledger] CANNOT BOOK {}x {} ({} hedge {oid}): maker order \
                                 {} is unknown to this engine — RECOVER BY HAND.",
                                c.book, h.market_id, h.venue.as_str(), h.maker_order_id
                            ),
                        }
                    }
                    if c.over > 0 {
                        // Contracts with no maker leg to pair them with:
                        // the obligation was already covered (a superseded
                        // IOC filled late). Booking them as a basket would
                        // invent a maker fill that never happened, so they
                        // are alarmed instead.
                        self.n_overhedge += 1;
                        eprintln!(
                            "[hedge] OVER-HEDGED: {} extra contract(s) filled on {} \
                             ({} {}) beyond what obligation {} owed. This is directional \
                             exposure the OPPOSITE way and it is NOT booked as a basket \
                             — RECONCILE BY HAND.",
                            c.over, oid, h.venue.as_str(), h.market_id, h.chain_id
                        );
                    }
                    if c.done {
                        // I3: retired only now that it is really covered.
                        // Its attempts stay in `hedge_orders` so a further
                        // frame on one is recognised as an over-fill rather
                        // than as money we cannot explain.
                        self.pending_hedges.remove(&h.chain_id);
                    }
                }
                FillArm::Hedge
            }
            None => match self.fills.observe_cum_fill(oid, cum) {
                arb_core::fill::FillOutcome::Minted(ob) => {
                    // Book the new exposure BEFORE the hedge intent, so
                    // the next quote sees capital this fill just spent.
                    if let (Some(rv), Some(mo)) = (self.cfg.risk.as_ref(), self.order_rel.get(oid))
                    {
                        rv.record_open(&mo.rel_id, mo.class, ob.qty() as f64);
                        // ...and if the order RESTED, those contracts are no
                        // longer capital merely COMMITTED, so the reservation
                        // gives up exactly what filled. Counting it in both
                        // places would refuse the same dollars twice; releasing
                        // the whole slot would free the part still resting.
                        //
                        // Only a maker order, though. Take-take leg 1 reserved
                        // nothing and SHARES a slot key with the maker quote on
                        // the same leg — see `MakerOrder::rested`.
                        //
                        // KNOWN, BOUNDED, AND NOT GATED: a SUPERSEDED order's
                        // fill consumes the slot its replacement now owns. The
                        // key is `(rel, market, side)`, and an amend rewrites
                        // that slot for the new order before the old one is
                        // cancelled — `drain_intents` says in its own words
                        // that a fill can still race the cancel. So Q1 rests
                        // holding 5, the book moves, Q2's check overwrites the
                        // slot, Q1 then fills: `record_open` books 5 and this
                        // zeroes the slot while Q2 rests unreserved.
                        //
                        // Left alone on purpose, and the reason rules out one
                        // DESIGN rather than the problem: gating it by passing
                        // the order id to `check` is impossible, because the
                        // quoter allocates no id until after the gate allows
                        // (a refused quote must not consume one). It is NOT
                        // ungatable — a `claim(rel, market, side, order_id)`
                        // where `resting.insert` already runs unconditionally
                        // would stamp the owning id into the reservation, and
                        // this call has the filling `oid` in hand to compare.
                        // That is one trait method and one write on a path that
                        // already writes. It is not done here because the
                        // residual needs a race, is bounded by one clip,
                        // self-heals on the next reprice or cancel of that
                        // slot, and is PERMISSIVE — strictly better than a gate
                        // that reserves nothing at all, which is what this
                        // replaces. Whoever needs it tighter has the shape.
                        if mo.rested() {
                            rv.consume(&mo.rel_id, &mo.market_id, mo.side, ob.qty() as f64);
                        }
                    }
                    // No anchor => no hedge target. The obligation is
                    // deliberately left unconsumed so the ledger's
                    // dropped_unconsumed() alarm surfaces it instead of
                    // an exposed leg vanishing silently. A crossed hedge
                    // book lands here too (see `hedge_anchor`).
                    if let Some(a) = ob.anchor().cloned() {
                        let (f_oid, _order_market, qty, _) = ob.into_parts();
                        self.n_hedge += 1;
                        self.intents.push(Intent::HedgeNeeded(intent::HedgeNeeded {
                            anchor_price: a.price.clone(),
                            hedge_needed: a.market_id.clone(),
                            order_id: f_oid.clone(),
                            qty,
                            ts: now,
                        }));
                        // The obligation says WHAT to hedge; the order
                        // below is the one that does it. Marketable IOC,
                        // not a resting quote: an unhedged leg is unbounded
                        // directional risk until resolution, so the hedge
                        // crosses rather than waits.
                        //
                        // `a.side` is the hedge-leg BOOK side we take.
                        let order_side = taking_side(a.side);
                        // Price at the CURRENT touch so it actually fills.
                        // The anchor is the fallback: it was captured at
                        // place time precisely because the burst that fills
                        // you is the burst that gaps your book, so a missing
                        // level here is exactly when it is needed.
                        let px = self
                            .books
                            .get(a.venue, &a.market_id)
                            .and_then(|b| match a.side {
                                BookSide::Bid => b.bids.first(),
                                BookSide::Ask => b.asks.first(),
                            })
                            .map(|l| l.price.clone())
                            .unwrap_or_else(|| a.price.clone());
                        self.next_hedge_oid += 1;
                        let hoid = format!("h{}", self.next_hedge_oid);
                        // The obligation, named by its FIRST attempt. It is
                        // recorded BEFORE the placement decision, so a
                        // refusal below still leaves a tracked, alarmed
                        // obligation rather than a leg nobody owns.
                        // MONOTONIC, not the fill's tape time: the fill feed
                        // is a separate socket from the market feed, so tape
                        // time can be frozen at this very moment (see
                        // `PendingHedge::first_at`).
                        let at = std::time::Instant::now();
                        self.pending_hedges.insert(
                            hoid.clone(),
                            PendingHedge {
                                maker_order_id: f_oid.clone(),
                                owed: qty,
                                filled: 0,
                                anchor: a.clone(),
                                first_at: at,
                                last_try_at: at,
                                latest_attempt: None,
                                tries: 0,
                                alarmed: false,
                                hold_logged: false,
                            },
                        );
                        // I5, first attempt included — see
                        // `first_attempt_acceptable` for why it is gated at
                        // all and why bench/replay is the one exception.
                        let acceptable = first_attempt_acceptable(
                            &mut self.cx,
                            self.cfg.hedge_retry.as_ref(),
                            a.side,
                            &px,
                            &a.price,
                        );
                        if acceptable {
                            if let Some(p) = self.pending_hedges.get_mut(&hoid) {
                                p.tries = 1;
                                p.latest_attempt = Some(hoid.clone());
                            }
                            self.hedge_orders.insert(
                                hoid.clone(),
                                HedgeOrder {
                                    maker_order_id: f_oid,
                                    chain_id: hoid.clone(),
                                    market_id: a.market_id.clone(),
                                    venue: a.venue,
                                    side: order_side,
                                    price: px.clone(),
                                    qty,
                                    cum_filled: 0,
                                    // The first attempt of a chain supersedes
                                    // nothing, so there is nothing to verify.
                                    supersedes: None,
                                },
                            );
                            self.intents.push(Intent::Place(intent::Place {
                                count: qty,
                                old_price: None,
                                order_id: hoid.clone(),
                                place: a.market_id.clone(),
                                price: px.clone(),
                                replaces: None,
                                // The FIRST attempt carries no `retry`, which
                                // is what makes the retries visible in the tape.
                                retry: None,
                                side: order_side,
                                tag: Some(Tag::Hedge),
                                taker: true,
                                ts: now,
                                venue: a.venue,
                            }));
                        } else {
                            let (slip, alarm_s) = self
                                .cfg
                                .hedge_retry
                                .as_ref()
                                .map(|p| (p.max_slip.as_str(), p.alarm_after_s))
                                .unwrap_or(("0", 0.0));
                            eprintln!(
                                "[hedge] WAIT {qty}x {} on {} — the touch {px} is worse \
                                 than the anchor {} by more than {slip} ({} side). \
                                 Obligation {hoid} is live and retrying; NOTHING is hedged \
                                 yet, and it alarms as NAKED if it is still open in \
                                 {alarm_s:.0}s.",
                                a.market_id,
                                a.venue.as_str(),
                                a.price,
                                a.side.as_str()
                            );
                        }
                        self.drain_intents(Option::<&Rel>::None);
                    }
                    FillArm::Maker
                }
                // Duplicate / stale / replayed report for an order we know:
                // idempotent by construction, nothing to do.
                arb_core::fill::FillOutcome::Seen => FillArm::Maker,
                arb_core::fill::FillOutcome::Unknown => {
                    // HELD, not dropped. See `UnclaimedFill` — this is the
                    // fill the engine used to count in `fills` and then
                    // throw away with no alarm at all.
                    if !self.unclaimed_fills.contains_key(oid) {
                        eprintln!(
                            "[fill] UNATTRIBUTED {cum}x on {} {} reported as order {oid} \
                             — no order of ours maps to that id. Holding it for its \
                             order_ack; it alarms in {}s if none comes.",
                            venue.as_str(),
                            market,
                            FILL_ACK_GRACE.as_secs()
                        );
                    }
                    let e = self.unclaimed_fills.entry(oid.to_string()).or_insert(UnclaimedFill {
                        venue,
                        market_id: market.to_string(),
                        cum,
                        since,
                    });
                    e.cum = e.cum.max(cum);
                    // NOT a maker frame. It was counted as one, so the `fills`
                    // gauge over-reported by every foreign fill on the account —
                    // and a frame replayed out of the hold would have been
                    // counted a second time. `fills_unclaimed` and
                    // `fills_unattributed` are where this frame is visible.
                    FillArm::Unattributed
                }
            },
        }
    }

    /// The venue's own id for an order of ours.
    ///
    /// The ledger already registered the order at place time (ids are ours), so
    /// an ack changes no decision state and emits no intent: digest-invisible.
    ///
    /// It carries ONE thing the engine cannot know otherwise: the venue's id for
    /// our order. Fills arrive under that id, so without this mapping a fill on
    /// a live order would match nothing and the hedge would never fire — and a
    /// CANCEL cannot be addressed at all, because both venues accept only their
    /// own id.
    pub(super) fn on_order_ack(
        &mut self,
        v: &serde_json::Value,
        ts_local_ns: i64,
    ) {
        if let (Some(ours), Some(theirs)) = (
            v.get("order_id").and_then(|x| x.as_str()),
            v.get("venue_order_id").and_then(|x| x.as_str()),
        ) {
            self.venue_oid.insert(theirs.to_string(), ours.to_string());
            self.oid_venue.insert(ours.to_string(), theirs.to_string());
            // ...and THE moment a fill that beat this ack becomes
            // attributable. A hedge is an IOC, so it fills in the
            // instant it is accepted and its fill frame can
            // overtake the ack that names it (observed margin
            // 48 ms). Dropping that frame left the basket
            // unbooked, the obligation credited 0, and the
            // 5-second retry bought the hedge a second time —
            // 10 Kalshi long against 5 PM short. Replay it here,
            // before any deadline can act on the wrong state.
            if let Some(u) = self.unclaimed_fills.remove(theirs) {
                eprintln!(
                    "[fill] {theirs} is {ours} — replaying the held fill of {} \
                     that arrived before its ack",
                    u.cum
                );
                let ts = ts_local_ns as f64 / 1e9;
                let _ = self.attribute_fill(ours, u.cum, u.venue, &u.market_id, ts, u.since);
            }
            // THE moment a cancel decided before this ack became
            // addressable. It was parked rather than sent with
            // our id (a no-op both venues report as success), so
            // this is where it actually goes out.
            //
            // `settle` records the ATTEMPT only if the command was
            // actually QUEUED — never before the dispatch. A full
            // channel loses the command, and logging a send that
            // never happened while the gauge dropped to 0 was how an
            // unaddressable quote could rest with every number
            // reading healthy. The entry itself is retired only by
            // the venue's own answer.
            let w = self.parked_cancels.get(ours).map(|p| CancelWork::Send {
                oid: ours.to_string(),
                venue: p.venue,
                market: p.market.clone(),
                venue_order_id: theirs.to_string(),
                attempt: p.sent + 1,
            });
            if let Some(w) = w {
                let queued = self.dispatch(w.venue(), w.action());
                settle(&mut self.parked_cancels, &w, queued);
                if queued {
                    eprintln!(
                        "[engine] cancel {ours} was waiting on its ack — sent to \
                         {} as {theirs}",
                        w.venue().as_str()
                    );
                } else {
                    eprintln!(
                        "[engine] cancel {ours} is addressable now ({theirs}) but \
                         the {} executor is FULL — NOT sent; the order is still \
                         resting and the cancel stays owed",
                        w.venue().as_str()
                    );
                }
            }
        }
        self.n_ack += 1;
        self.last_now = ts_local_ns as f64 / 1e9;
    }

    /// One fill frame off the private feed.
    pub(super) fn on_fill(
        &mut self,
        v: &serde_json::Value,
        venue: Venue,
        market_id: &str,
        ts_local_ns: i64,
    ) {
        let (Some(reported), Some(cum)) = (
            v.get("order_id").and_then(|x| x.as_str()),
            v.get("cum").and_then(|x| x.as_i64()),
        ) else {
            return;
        };
        // A venue reports its own id; the ledger knows ours.
        // Fall through to the reported id when it is already
        // ours (the dry-run/replay case, and the poll path
        // which looks orders up by our id).
        let oid: String =
            self.venue_oid.get(reported).cloned().unwrap_or_else(|| reported.to_string());
        let now = ts_local_ns as f64 / 1e9;
        let arm =
            self.attribute_fill(&oid, cum, venue, market_id, now, std::time::Instant::now());
        if matches!(arm, FillArm::Maker) {
            self.n_fill += 1;
        }
        if !matches!(arm, FillArm::Hedge) {
            self.last_now = now;
        }
    }

    /// Fills held for an `order_ack` that has not come.
    pub(super) fn unclaimed_tick(&mut self) {
        for id in unclaimed_expired(&self.unclaimed_fills, std::time::Instant::now()) {
            let Some(u) = self.unclaimed_fills.remove(&id) else { continue };
            self.n_unattributed += 1;
            eprintln!(
                "[fill] UNEXPLAINED {}x on {} {} reported as order {id} — no order_ack \
                 claimed it within {}s. Either a fill on an order of ours whose ack was \
                 lost (a place can return a parse error and still rest) or a fill from \
                 outside this engine — the Kalshi fill channel is account-wide by \
                 design. It is NOT credited to any hedge: attributing money by guess is \
                 worse than saying we cannot. RECONCILE BY HAND.",
                u.cum,
                u.venue.as_str(),
                u.market_id,
                FILL_ACK_GRACE.as_secs()
            );
        }
    }
}

#[cfg(test)]
mod ledger_write_tests {
    use super::*;

    /// The round trip that matters: a basket this engine books must be read
    /// back as OPEN exposure by the same seeding path used at startup. If these
    /// two disagree, an armed engine forgets its own positions on restart.
    #[test]
    fn a_booked_basket_seeds_exposure_on_the_next_startup() {
        let dir = std::env::temp_dir().join(format!("arb-book-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();

        let maker = MakerOrder {
            rel_id: "xvus-demo".into(),
            class: "cross-venue-equivalent",
            venue: "kalshi".into(),
            market_id: "KXDEMO".into(),
            side: BookSide::Bid,
            price: "0.31".into(),
            strategy: "maker-hedge",
        };
        let hedge = HedgeOrder {
            maker_order_id: "m1".into(),
            chain_id: "h1".into(),
            market_id: "demo-slug".into(),
            venue: Venue::PolymarketUs,
            side: BookSide::Ask,
            price: "0.40".into(),
            qty: 5,
            cum_filled: 0,
            supersedes: None,
        };
        book_basket(p, &maker, &hedge, "h1", 5, 1_700_000_000.0);
        book_basket(p, &maker, &hedge, "h1", 3, 1_700_000_100.0);

        let recs = crate::ledger::read(p).unwrap();
        let open = crate::ledger::open_exposure(recs);
        assert_eq!(
            open.get("xvus-demo"),
            Some(&8.0),
            "both baskets must read back as open exposure"
        );

        let first: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(p).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(first["status"], "open");
        assert_eq!(first["qty"], 5);
        assert_eq!(first["legs"][0]["venue"], "kalshi");
        assert_eq!(first["legs"][0]["role"], "maker");
        assert_eq!(first["legs"][1]["venue"], "polymarket_us");
        assert_eq!(first["legs"][1]["role"], "taker");
        assert_eq!(first["strategy"], "maker-hedge");
        assert_eq!(
            first["fees_pending"], true,
            "the engine does not know venue fees; the record must not pretend otherwise"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A take-take basket must NOT book as maker-hedge. It reuses the same
    /// fill -> hedge pipeline, so before this was fixed every crossing landed
    /// in the ledger as `strategy: maker-hedge` with leg 1 tagged `maker`,
    /// which is two lies in the accounting record: P&L could not be attributed
    /// between the strategies, and the same trade had a different name
    /// depending on whether Python or Rust made it (auto_take_take.py writes
    /// `take-take`).
    #[test]
    fn a_take_take_basket_books_as_take_take_with_a_taker_leg() {
        let dir = std::env::temp_dir().join(format!("arb-book-tt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();

        let maker = MakerOrder {
            rel_id: "xvus-nobel-peace-26-donaldtrump".into(),
            class: "cross-venue-equivalent",
            venue: "polymarket_us".into(),
            market_id: "tac-nobel-peace-2026-10-09-dontru".into(),
            side: BookSide::Ask,
            price: "0.0800".into(),
            strategy: "take-take",
        };
        let hedge = HedgeOrder {
            maker_order_id: "t1".into(),
            chain_id: "h1".into(),
            market_id: "KXNOBELPEACE-26-DJT".into(),
            venue: Venue::Kalshi,
            side: BookSide::Bid,
            price: "0.0400".into(),
            qty: 5,
            cum_filled: 0,
            supersedes: None,
        };
        book_basket(p, &maker, &hedge, "h1", 5, 1_700_000_000.0);

        let rec: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(p).unwrap().lines().next().unwrap())
                .unwrap();
        assert_eq!(rec["strategy"], "take-take");
        assert_eq!(rec["title"], "xvus-nobel-peace-26-donaldtrump (rust take-take)");
        assert_eq!(rec["legs"][0]["role"], "taker", "leg 1 crossed the book, it did not rest");
        assert_eq!(rec["legs"][1]["role"], "taker", "leg 2 is the crossing hedge");
        // and it must still seed exposure like any other basket
        let open = crate::ledger::open_exposure(crate::ledger::read(p).unwrap());
        assert_eq!(open.get("xvus-nobel-peace-26-donaldtrump"), Some(&5.0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2026-07-30, end to end. Kalshi was `409 trading_is_paused` for 31
    /// minutes with this take-take's PM-US leg already one short. This engine
    /// retried its hedge 336 times; the frozen Python hedger, which reads VENUE
    /// POSITIONS and completes any uncovered PM-US short, retried the same
    /// hedge every 5 minutes. Both got through 600ms apart when the venue
    /// reopened, and the engine booked a second complete basket over the top of
    /// the Python hedger's with nothing said.
    ///
    /// What is pinned: the engine NOTICES. The record is still written — both
    /// contracts are real and suppressing one would delete exposure — and it
    /// names the booking it may be a duplicate of, so the operator has the
    /// pair rather than a silent 30 -> 32.
    #[test]
    fn a_basket_another_writer_already_booked_is_flagged_not_silently_doubled() {
        let dir = std::env::temp_dir().join(format!("arb-book-contest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("trades.jsonl");
        let p = path.to_str().unwrap();

        // The Python hedger's line, off the live ledger: position sides, Kalshi
        // leg first, no `source`.
        crate::ledger::append(
            p,
            &serde_json::from_str(
                r#"{"ts":1785402005.539014,"relationship_id":"xvus-nobel-peace-26-mykolakuleba",
                    "title":"xvus-nobel-peace-26-mykolakuleba (naked-leg hedge)","qty":1,
                    "strategy":"take-take","status":"open","cost_usd":0.72,"profit_usd":0.28,
                    "legs":[{"venue":"kalshi","market_id":"KXNOBELPEACE-27-MKUL","side":"yes",
                             "role":"taker","qty":1,"yes_price":"0.0800"},
                            {"venue":"polymarket_us",
                             "market_id":"tac-nobel-peace-2026-10-09-mykkul","side":"no",
                             "role":"taker","qty":1,"yes_price":"0.36"}]}"#,
            )
            .unwrap(),
        )
        .unwrap();

        let maker = MakerOrder {
            rel_id: "xvus-nobel-peace-26-mykolakuleba".into(),
            class: "cross-venue-equivalent",
            venue: "polymarket_us".into(),
            market_id: "tac-nobel-peace-2026-10-09-mykkul".into(),
            side: BookSide::Ask,
            price: "0.3600".into(),
            strategy: "take-take",
        };
        let hedge = HedgeOrder {
            maker_order_id: "t1785351000001".into(),
            chain_id: "h1785351000001".into(),
            market_id: "KXNOBELPEACE-27-MKUL".into(),
            venue: Venue::Kalshi,
            side: BookSide::Bid,
            price: "0.0800".into(),
            qty: 1,
            cum_filled: 0,
            supersedes: None,
        };
        book_basket(p, &maker, &hedge, "h1785351000336", 1, 1785402003.8191998);

        let recs = crate::ledger::read(p).unwrap();
        assert_eq!(recs.len(), 2, "the basket is written — both contracts are real");
        let ours = recs.iter().find(|r| r["source"] == "arb-trader").expect("ours");
        assert_eq!(
            ours["contested_with_ts"],
            serde_json::json!([1785402005.539014]),
            "the engine's record must name the booking it collides with"
        );
        assert_eq!(
            ours["legs"][1]["order_id"], "h1785351000336",
            "leg 2 names the hedge attempt that filled it"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// `attribute_fill` itself, driven directly.
///
/// It was `attribute_fill!` — a macro body, so the only way to reach it was to
/// spawn `run()` over a real channel and infer what had happened from the
/// summary. Every arithmetic rule underneath it was pinned (`hedge_credit`,
/// `unclaimed_expired`); the WIRING between them was not, and the wiring is
/// where the audit's defects actually lived: an attempt retired on its first
/// frame, a held fill dropped instead of replayed, a foreign fill counted as
/// ours.
#[cfg(test)]
mod attribute_fill_tests {
    use super::*;
    use crate::engine::{test_cfg, test_engine};
    use arb_core::fill::HedgeAnchor;
    use std::time::Instant;

    /// The hedge leg: PM-US's bid, i.e. the maker leg filled long and the hedge
    /// sells into it.
    fn anchor() -> HedgeAnchor {
        HedgeAnchor {
            venue: Venue::PolymarketUs,
            market_id: "P".into(),
            side: BookSide::Bid,
            price: "0.40".into(),
        }
    }

    fn maker_order() -> MakerOrder {
        MakerOrder {
            rel_id: "synth-attribution-rel".into(),
            class: "cross-venue-equivalent",
            venue: "kalshi".into(),
            market_id: "K".into(),
            side: BookSide::Bid,
            price: "0.31".into(),
            strategy: "maker-hedge",
        }
    }

    fn hedge_order(qty: i64) -> HedgeOrder {
        HedgeOrder {
            maker_order_id: "m1".into(),
            chain_id: "h1".into(),
            market_id: "P".into(),
            venue: Venue::PolymarketUs,
            side: BookSide::Ask,
            price: "0.40".into(),
            qty,
            cum_filled: 0,
            supersedes: None,
        }
    }

    fn pending(owed: i64) -> PendingHedge {
        let at = Instant::now();
        PendingHedge {
            maker_order_id: "m1".into(),
            owed,
            filled: 0,
            anchor: anchor(),
            first_at: at,
            last_try_at: at,
            latest_attempt: Some("h1".into()),
            tries: 1,
            alarmed: false,
            hold_logged: false,
        }
    }

    /// THE reason this is one function and not two: both of its callers, in the
    /// order the ack race actually produces them.
    ///
    /// The `fill` arm cannot name the order — the venue reports its own id and
    /// no ack has mapped it yet — so the frame is HELD. The `order_ack` arm
    /// then learns the mapping and replays the SAME frame through the SAME
    /// function, which is the only way the two can be guaranteed to agree about
    /// what it means. Dropping it there left the basket unbooked, the
    /// obligation credited 0, and the 5-second retry bought the hedge twice.
    #[test]
    fn a_fill_that_beat_its_ack_is_held_and_the_ack_replays_it() {
        let mut e = test_engine(test_cfg());
        e.fills.register_order("m1", "K", 5, Some(anchor()));
        e.order_rel.insert("m1".into(), maker_order());

        let arm = e.attribute_fill("BH8H83AY09NG", 5, Venue::Kalshi, "K", 1.0, Instant::now());
        assert!(
            matches!(arm, FillArm::Unattributed),
            "a frame we cannot name is NOT a maker frame — counting it is what made \
             `fills` over-report by every foreign fill on the account"
        );
        assert_eq!(e.unclaimed_fills.len(), 1, "held, not dropped");
        assert!(
            e.pending_hedges.is_empty(),
            "and nothing may be hedged off a frame we cannot attribute"
        );

        // ...and now the ack that names it lands.
        e.on_order_ack(
            &json!({"order_id": "m1", "venue_order_id": "BH8H83AY09NG"}),
            2_000_000_000,
        );
        assert!(e.unclaimed_fills.is_empty(), "the held frame was claimed by its ack");
        assert_eq!(e.pending_hedges.len(), 1, "and minted the obligation it always owed");
        let p = e.pending_hedges.values().next().expect("the obligation");
        assert_eq!((p.owed, p.filled), (5, 0));
        assert_eq!(p.maker_order_id, "m1", "credited to the maker order, not to the venue id");
        assert_eq!(e.venue_oid.get("BH8H83AY09NG").map(String::as_str), Some("m1"));
    }

    /// I1/I2 through the engine rather than through the arithmetic. A 10-lot
    /// hedge filling 4 then 10 credits the ATTEMPT (so its own later frames are
    /// deltas) and the OBLIGATION (so the retry knows what is left), separately,
    /// and retires the obligation only once it is really covered.
    ///
    /// `hedge_credit` has pinned the numbers since the audit. The defect was in
    /// the wiring above it: `hedge_orders.remove(oid)` on the FIRST frame, so
    /// frame two matched no hedge at all, fell through to the maker path, found
    /// nothing in the FillLedger and was dropped — 10 lots booked as 4.
    #[test]
    fn a_hedge_filling_four_then_ten_credits_both_and_retires_once() {
        let mut e = test_engine(test_cfg());
        e.order_rel.insert("m1".into(), maker_order());
        e.hedge_orders.insert("h1".into(), hedge_order(10));
        e.pending_hedges.insert("h1".into(), pending(10));

        let arm = e.attribute_fill("h1", 4, Venue::PolymarketUs, "P", 1.0, Instant::now());
        assert!(matches!(arm, FillArm::Hedge));
        assert_eq!(e.hedge_orders["h1"].cum_filled, 4, "the attempt");
        assert_eq!(e.pending_hedges["h1"].filled, 4, "and the obligation, separately");

        e.attribute_fill("h1", 10, Venue::PolymarketUs, "P", 2.0, Instant::now());
        assert_eq!(e.hedge_orders["h1"].cum_filled, 10, "cumulative, so the delta was 6");
        assert!(!e.pending_hedges.contains_key("h1"), "covered now, so retired now");
        assert!(
            e.hedge_orders.contains_key("h1"),
            "...but the ATTEMPT stays, so a further frame on it is an over-fill rather \
             than money we cannot explain"
        );
        assert_eq!(e.n_overhedge, 0, "nothing was filled beyond what was owed");
    }

    /// I2's deliberate edge: contracts past what the obligation owed are filled
    /// and NOT booked. There is no maker fill to pair them with, so a basket
    /// record would invent one — they are alarmed for a human instead.
    #[test]
    fn a_fill_past_a_retired_obligation_is_alarmed_not_booked() {
        let mut e = test_engine(test_cfg());
        e.order_rel.insert("m1".into(), maker_order());
        // the attempt is still known; its obligation is already retired
        e.hedge_orders.insert("h1".into(), hedge_order(10));

        let arm = e.attribute_fill("h1", 6, Venue::PolymarketUs, "P", 1.0, Instant::now());
        assert!(matches!(arm, FillArm::Hedge), "a superseded attempt is still ours");
        assert_eq!(e.n_overhedge, 1);
        assert_eq!(
            e.summary()["hedges_overfilled"],
            serde_json::json!(1),
            "and it must reach the gauge a human watches"
        );
    }

    /// The two gauges the `FillArm` return exists for, which ask different
    /// questions: `fills` counts maker frames ONLY, and tape time advances for
    /// anything that is not a hedge frame. Returning a bool conflated them.
    #[test]
    fn a_hedge_frame_moves_neither_the_fills_gauge_nor_tape_time() {
        let mut e = test_engine(test_cfg());
        e.order_rel.insert("m1".into(), maker_order());
        e.hedge_orders.insert("h1".into(), hedge_order(5));
        e.pending_hedges.insert("h1".into(), pending(5));

        e.on_fill(
            &json!({"order_id": "h1", "cum": 5}),
            Venue::PolymarketUs,
            "P",
            9_000_000_000,
        );
        assert_eq!(e.n_fill, 0, "a hedge fill is not a maker fill");
        assert_eq!(e.last_now, 0.0, "and a hedge frame has never advanced tape time");

        // a maker frame moves both
        e.fills.register_order("m2", "K", 5, Some(anchor()));
        e.order_rel.insert("m2".into(), maker_order());
        e.on_fill(
            &json!({"order_id": "m2", "cum": 5}),
            Venue::Kalshi,
            "K",
            9_000_000_000,
        );
        assert_eq!(e.n_fill, 1);
        assert_eq!(e.last_now, 9.0);

        // ...and a frame we cannot attribute moves tape time but is NOT counted
        e.on_fill(
            &json!({"order_id": "nobody-we-know", "cum": 3}),
            Venue::Kalshi,
            "K",
            10_000_000_000,
        );
        assert_eq!(e.n_fill, 1, "a foreign fill is not ours to count");
        assert_eq!(e.last_now, 10.0);
        assert_eq!(e.unclaimed_fills.len(), 1, "it is held for the ack that might name it");
    }

    /// RELEASE ON FILL, through the engine rather than through `RiskView`.
    ///
    /// A resting quote holds its clip against the caps from the moment the gate
    /// allows it. When it fills, those contracts become real exposure — so the
    /// fill path owes BOTH calls: `record_open` for the new exposure and
    /// `consume` for the reservation it replaces. Only `record_open` would
    /// refuse the same dollars twice, in a gate that refuses on the total;
    /// only `consume` would lose them.
    #[test]
    fn a_maker_fill_books_its_exposure_and_gives_up_the_capital_it_reserved() {
        use arb_core::model::Venue as V;
        use arb_core::quoter::RiskGate;
        use arb_core::scan::{RelLeg, RelType};

        let dir = std::env::temp_dir().join(format!("arb-fill-risk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let exec = dir.join("exec.yaml");
        std::fs::write(&exec, "bankroll_usd: 980\nper_class_cap: 0.35\n").unwrap();
        let rv = std::sync::Arc::new(crate::risk::RiskView::load(
            exec.to_str().unwrap(),
            "/nonexistent/topics.yaml",
            vec![
                ("kalshi".to_string(), "1000".to_string()),
                ("polymarket_us".to_string(), "1000".to_string()),
            ],
            HashMap::from([("synth-attribution-rel".to_string(), "low".to_string())]),
        ));
        // The relationship `maker_order()` names, quoting Kalshi market "K".
        let rel = Rel {
            id: "synth-attribution-rel".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: V::Kalshi, market_id: "K".into() },
                RelLeg { venue: V::PolymarketUs, market_id: "P".into() },
            ],
        };
        assert!(rv.check(&rel, V::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        assert_eq!(rv.reserved_ct(), 5.0, "the quote rests holding its clip");

        let mut cfg = test_cfg();
        cfg.risk = Some(rv.clone());
        let mut e = test_engine(cfg);
        e.fills.register_order("m1", "K", 5, Some(anchor()));
        e.order_rel.insert("m1".into(), maker_order());

        e.attribute_fill("m1", 3, Venue::Kalshi, "K", 1.0, Instant::now());
        assert_eq!(
            (rv.open_ct("synth-attribution-rel"), rv.reserved_ct()),
            (3.0, 2.0),
            "3 of 5 filled: 3 are exposure, 2 are still resting"
        );

        e.attribute_fill("m1", 5, Venue::Kalshi, "K", 2.0, Instant::now());
        assert_eq!(
            (rv.open_ct("synth-attribution-rel"), rv.reserved_ct()),
            (5.0, 0.0),
            "and the whole clip has moved from committed to spent, once"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **A TAKE-TAKE FILL MUST NOT RELEASE THE MAKER'S RESERVATION.**
    ///
    /// The two share a slot key. A Kalshi-lead crossing places leg 1 as a BID
    /// on the Kalshi market (`Candidate::leg1`); the maker quoter for the same
    /// relationship quotes BOTH legs (`maker_leg_indices`), so it reserves
    /// `(rel, kalshi_market, Bid)`; and take-take is placed through
    /// `drain_intents(Some(rel))`, so `order_rel` carries the identical triple
    /// — unlike a hedge, which goes through `drain_intents(None)` and is never
    /// registered at all.
    ///
    /// So an unguarded `consume` books the crossing's contracts with
    /// `record_open` and frees the maker's whole reservation in the same
    /// breath: the gate's total does not move while real committed capital rose
    /// by a clip, and a 5-lot is still resting at the venue. That is the
    /// N-quotes-one-headroom defect this whole change exists to close,
    /// reintroduced on the armed take-take path.
    #[test]
    fn a_take_take_fill_does_not_free_the_maker_quote_resting_on_the_same_leg() {
        use arb_core::model::Venue as V;
        use arb_core::quoter::RiskGate;
        use arb_core::scan::{RelLeg, RelType};

        let dir = std::env::temp_dir().join(format!("arb-tt-risk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let exec = dir.join("exec.yaml");
        std::fs::write(&exec, "bankroll_usd: 980\nper_class_cap: 0.35\n").unwrap();
        let rv = std::sync::Arc::new(crate::risk::RiskView::load(
            exec.to_str().unwrap(),
            "/nonexistent/topics.yaml",
            vec![
                ("kalshi".to_string(), "1000".to_string()),
                ("polymarket_us".to_string(), "1000".to_string()),
            ],
            HashMap::from([("synth-attribution-rel".to_string(), "low".to_string())]),
        ));
        let rel = Rel {
            id: "synth-attribution-rel".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: V::Kalshi, market_id: "K".into() },
                RelLeg { venue: V::PolymarketUs, market_id: "P".into() },
            ],
        };
        // The MAKER rests a Kalshi bid on this relationship.
        assert!(rv.check(&rel, V::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        assert_eq!(rv.reserved_ct(), 5.0);

        let mut cfg = test_cfg();
        cfg.risk = Some(rv.clone());
        let mut e = test_engine(cfg);
        // ...and take-take fires leg 1 on the SAME market and side.
        let mut tt = maker_order();
        tt.strategy = "take-take";
        e.fills.register_order("t1", "K", 5, Some(anchor()));
        e.order_rel.insert("t1".into(), tt);

        e.attribute_fill("t1", 5, Venue::Kalshi, "K", 1.0, Instant::now());
        assert_eq!(
            rv.open_ct("synth-attribution-rel"),
            5.0,
            "the crossing's contracts are real exposure"
        );
        assert_eq!(
            rv.reserved_ct(),
            5.0,
            "and the maker's quote is STILL resting, so its capital is still committed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
