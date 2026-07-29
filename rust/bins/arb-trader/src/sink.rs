//! The order sink — where an effect stops being a record and becomes a wire.
//!
//! `None` (dry-run) is the default everywhere: the executor counts the command
//! and drops it. A `Some` sink is only ever constructed by `--enable-orders`,
//! and only after the preconditions in `main::order_preconditions` are met.
//!
//! The gateways are blocking (one-shot-tool shaped, see arb-venue), so calls go
//! through `spawn_blocking` — a blocking venue call on a tokio worker would
//! stall unrelated tasks, which is precisely the reader-never-stalls property
//! the shell exists to preserve.

use arb_venue::gateway::{CancelRequest, PlaceRequest, VenueGateway};
use arb_venue::VenueError;

/// How hard a sweep tries before it gives up and says so.
///
/// `rounds > 1` is the fix for the 2026-07-28 15:40 shutdown leak. The old
/// sweep issued cancel-all EXACTLY ONCE and then polled the resting list ten
/// times: structurally an observer. A queued maker place landed one second into
/// that sweep, and all ten polls saw the survivor and could do nothing about
/// it. The process then exited 0 with a real order resting on Kalshi.
#[derive(Clone, Debug)]
pub struct SweepPolicy {
    /// How many times cancel-all may be RE-ISSUED. A late arrival needs a
    /// second cancel; nothing else can clear it.
    pub rounds: u32,
    /// Resting-list reads per round. Both venues' lists lag a write, so a
    /// single immediate read cries wolf — that was the OTHER 2026-07-28 sweep
    /// bug, and it is why one poll is never enough either.
    pub polls_per_round: u32,
    pub poll_delay: std::time::Duration,
    /// Consecutive EMPTY reads required before the book is called clean.
    ///
    /// This is the same lag, read the other way round. `polls_per_round` exists
    /// because a non-empty list must not be believed on one read; believing an
    /// EMPTY one on a single read was the identical mistake pointing at a worse
    /// outcome. Trace, no wedged venue needed: a place dispatched 50 ms before
    /// SIGTERM returns 250 ms into the halt, the round-1 cancel-all does not see
    /// it yet because the venue's own order list has not caught up, and the
    /// first poll reads empty and calls it PROOF. Exit 0, order resting — the
    /// 15:40 incident again, through the new code. Two confirmations cost one
    /// `poll_delay` against a 20 s budget.
    pub confirm_empty_reads: u32,
    /// Hard wall-clock ceiling on the whole sweep. A venue that answers slowly
    /// must not be able to spend the entire systemd stop timeout by itself —
    /// the shutdown path sweeps venues concurrently precisely so one dead venue
    /// cannot cost the other its cancel, and that only holds if each is bounded.
    pub budget: std::time::Duration,
}

impl Default for SweepPolicy {
    fn default() -> Self {
        // 4 rounds x 3 polls x 400ms ~= 4.8s of polling plus round-trips,
        // hard-capped at 20s. Two venues in parallel therefore fit inside the
        // 40s shutdown deadline with room for an in-flight call to settle
        // first (`TimeoutStopUSec=1min 30s` for arbbot-trader).
        Self {
            rounds: 4,
            polls_per_round: 3,
            poll_delay: std::time::Duration::from_millis(400),
            confirm_empty_reads: 2,
            budget: std::time::Duration::from_secs(20),
        }
    }
}

/// Cancel everything resting on a venue and PROVE it is gone — blocking, so the
/// abnormal-exit paths (panic hook, WAL crash-stop) can run it without a tokio
/// runtime of their own.
///
/// The ONE implementation behind every sweep — startup, shutdown, and the kill
/// switch. They were three separate code paths and all four bugs on 2026-07-28
/// came from the differences between them:
///
///   * startup polled, and was correct;
///   * shutdown read the resting list once and reported an order as surviving
///     that the next sweep found already cancelled — both venues' lists lag a
///     write, so a single immediate read cries wolf;
///   * the kill switch cancelled by order id and never checked at all, logging
///     "cancelled" for an order that was still resting 35 minutes later;
///   * and once unified, the survivor kept surviving: one cancel-all followed
///     by ten observations cannot clear an order that appears after the cancel.
///
/// A response code is not proof. The resting list is, it has to be polled, and
/// when it comes back non-empty the cancel has to be ISSUED AGAIN. A cancel-all
/// that the venue refuses is no longer fatal on the spot either: the rate
/// limiter errors instead of waiting, and abandoning a kill sweep on the first
/// refusal is how a halt ends with orders resting. The refusal is remembered and
/// reported only if the book cannot be proven clean.
///
/// And "clean" itself needs confirming: see `confirm_empty_reads`. A single
/// empty read is exactly as untrustworthy as a single non-empty one, and it is
/// the direction that exits 0.
pub fn cancel_all_and_verify_blocking(
    sink: &dyn OrderSink,
    pol: &SweepPolicy,
) -> Result<(), String> {
    let t0 = std::time::Instant::now();
    let mut left: Vec<String> = Vec::new();
    let mut last_err: Option<String> = None;
    let mut rounds_done = 0u32;
    let need_empty = pol.confirm_empty_reads.max(1);
    // Consecutive empty reads so far. Deliberately NOT reset per round: a
    // cancel-all can only ever remove orders, so it cannot invalidate an
    // emptiness observation that precedes it.
    let mut empties = 0u32;
    for _ in 0..pol.rounds.max(1) {
        // Do not START a fresh cancel-all on a spent budget: Kalshi's
        // `cancel_all_open` is 1+N HTTP requests internally and checks no clock
        // of its own, so beginning one here is how the aggregate deadline gets
        // overshot by minutes on a wide book.
        if rounds_done > 0 && t0.elapsed() >= pol.budget {
            break;
        }
        rounds_done += 1;
        match sink.cancel_all_open() {
            Ok(()) => {}
            Err(e) => last_err = Some(format!("cancel_all_open: {e}")),
        }
        for _ in 0..pol.polls_per_round.max(1) {
            if !pol.poll_delay.is_zero() {
                std::thread::sleep(pol.poll_delay);
            }
            match sink.resting_order_ids() {
                Ok(ids) => {
                    if ids.is_empty() {
                        empties += 1;
                        if empties >= need_empty {
                            return Ok(()); // proven twice over, not assumed once
                        }
                    } else {
                        empties = 0; // the lag caught up; start over
                        left = ids;
                    }
                }
                // An unreadable list is not an empty one. Break the run of
                // confirmations rather than letting an error stand in for proof.
                Err(e) => {
                    empties = 0;
                    last_err = Some(format!("cannot list resting orders: {e}"));
                }
            }
            if t0.elapsed() >= pol.budget {
                break;
            }
        }
        if t0.elapsed() >= pol.budget {
            break;
        }
    }
    let mut msg = if left.is_empty() {
        // Never say "0 orders survived": we never got a readable list, so what
        // is resting is UNKNOWN, which is the worse of the two outcomes.
        format!("book could NOT be proven clean after {rounds_done} cancel-all round(s)")
    } else {
        format!(
            "{} order(s) SURVIVED {rounds_done} cancel-all round(s): {}",
            left.len(),
            left.join(" ")
        )
    };
    if t0.elapsed() >= pol.budget {
        msg.push_str(&format!(" [gave up at the {:.0}s budget]", pol.budget.as_secs_f64()));
    }
    if let Some(e) = last_err {
        msg.push_str(&format!(" (last venue error: {e})"));
    }
    Err(msg)
}

/// The async face of [`cancel_all_and_verify_blocking`] with the default policy.
/// Signature unchanged: the engine's kill path and the startup sweep call this.
pub async fn cancel_all_and_verify(
    sink: std::sync::Arc<dyn OrderSink>,
) -> Result<(), String> {
    cancel_all_and_verify_with(sink, SweepPolicy::default()).await
}

/// As [`cancel_all_and_verify`], with an explicit policy — the shutdown path
/// bounds itself, and the tests drive it with no sleeps.
pub async fn cancel_all_and_verify_with(
    sink: std::sync::Arc<dyn OrderSink>,
    pol: SweepPolicy,
) -> Result<(), String> {
    // The whole sweep, sleeps included, runs on ONE blocking thread rather than
    // hopping back to the runtime between polls: the gateways are blocking, and
    // a tokio worker must never be the thread that waits on a venue.
    tokio::task::spawn_blocking(move || cancel_all_and_verify_blocking(sink.as_ref(), &pol))
        .await
        .map_err(|e| format!("sweep task panicked: {e}"))?
}

/// What an executor may do to a venue. Deliberately only two verbs: the engine
/// amends by cancel+place, and nothing here can read the account.
pub trait OrderSink: Send + Sync {
    /// Returns the venue's order id.
    fn place(&self, req: &PlaceRequest) -> Result<String, VenueError>;
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError>;
    /// Cancel every resting order on the account (the startup sweep and the
    /// kill-switch sweep are the same operation).
    fn cancel_all_open(&self) -> Result<(), VenueError>;
    /// Ids still resting. A 200 from `cancel_all_open` only says the venue
    /// accepted the request; this is the evidence that it worked.
    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError>;
    /// The venue's id for a place we could not read the answer for — see
    /// [`VenueGateway::recover_place`]. `Ok(None)` means nothing matching it is
    /// resting, and the default is no recovery at all.
    fn recover_place(
        &self,
        _req: &PlaceRequest,
        _claimed: &std::collections::HashSet<String>,
    ) -> Result<Option<String>, VenueError> {
        Ok(None)
    }
}

/// Every gateway is a sink. The adapter is the same four delegations for both
/// venues, and the one place they used to differ — Kalshi's response spells the
/// id `order_id`, PM-US spells it `id` — is what `VenueGateway::order_id`
/// already exists to hide, so a per-venue impl was reaching around the trait to
/// re-answer a question the trait answers.
///
/// Generic over the transport, not pinned to HTTP: a gateway carrying the
/// `NotWired` transport is still a valid (inert) sink, and tests drive a mock.
impl<G> OrderSink for G
where
    G: VenueGateway + Send + Sync,
{
    fn place(&self, req: &PlaceRequest) -> Result<String, VenueError> {
        VenueGateway::place(self, req).map(|o| G::order_id(&o))
    }
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        VenueGateway::cancel(self, req)
    }
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        VenueGateway::cancel_all_open(self)
    }
    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
        VenueGateway::resting_order_ids(self)
    }
    fn recover_place(
        &self,
        req: &PlaceRequest,
        claimed: &std::collections::HashSet<String>,
    ) -> Result<Option<String>, VenueError> {
        VenueGateway::recover_place(self, req, claimed)
    }
}

/// The sweep loop itself, whose doc comment above names four separate sweep
/// bugs from 2026-07-28 and which — until now — had no tests at all. That is why
/// the fourth one shipped.
///
/// A local sink double rather than `arb-venue`'s `MockTransport`, which cannot
/// return `Err`; a sweep that abandons the book on a venue refusal is exactly
/// the failure mode worth pinning.
#[cfg(test)]
mod sweep_tests {
    use super::*;
    use std::sync::Mutex;

    /// A scripted book. `on_cancel[i]` is what is resting after the i-th
    /// cancel-all; `cancel_errs[i]` optionally makes that cancel fail.
    struct Scripted {
        resting: Mutex<Vec<String>>,
        on_cancel: Vec<Vec<String>>,
        cancel_errs: Vec<bool>,
        list_errs: Mutex<u32>,
        cancels: Mutex<usize>,
    }

    impl Scripted {
        fn new(initial: &[&str], on_cancel: &[&[&str]]) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Scripted {
                resting: Mutex::new(initial.iter().map(|s| s.to_string()).collect()),
                on_cancel: on_cancel
                    .iter()
                    .map(|r| r.iter().map(|s| s.to_string()).collect())
                    .collect(),
                cancel_errs: Vec::new(),
                list_errs: Mutex::new(0),
                cancels: Mutex::new(0),
            })
        }
        fn cancels(&self) -> usize {
            *self.cancels.lock().unwrap()
        }
    }

    impl OrderSink for Scripted {
        fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
            unreachable!("a sweep never places")
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            unreachable!("a sweep uses cancel_all_open")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            let mut n = self.cancels.lock().unwrap();
            let i = *n;
            *n += 1;
            if self.cancel_errs.get(i).copied().unwrap_or(false) {
                return Err(VenueError::Transport("429 rate limited".into()));
            }
            if let Some(next) = self.on_cancel.get(i) {
                *self.resting.lock().unwrap() = next.clone();
            }
            Ok(())
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            let mut e = self.list_errs.lock().unwrap();
            if *e > 0 {
                *e -= 1;
                return Err(VenueError::Transport("503".into()));
            }
            Ok(self.resting.lock().unwrap().clone())
        }
    }

    fn fast() -> SweepPolicy {
        SweepPolicy { poll_delay: std::time::Duration::ZERO, ..SweepPolicy::default() }
    }

    /// A venue whose resting list LAGS a write: the first `lag_reads` reads come
    /// back empty, then the truth appears. Ported from the adversarial review.
    struct LaggingList {
        reads: Mutex<u32>,
        lag_reads: u32,
        truth: Vec<String>,
        cancels: Mutex<usize>,
    }
    impl OrderSink for LaggingList {
        fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
            unreachable!("a sweep never places")
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            unreachable!("a sweep uses cancel_all_open")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            *self.cancels.lock().unwrap() += 1;
            Ok(()) // the venue has not registered the order yet, so this misses it
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            let mut n = self.reads.lock().unwrap();
            *n += 1;
            Ok(if *n <= self.lag_reads { Vec::new() } else { self.truth.clone() })
        }
    }

    /// **D1 — one empty read is not proof.** Ported from the adversarial review,
    /// where it passed.
    ///
    /// The sweep's whole thesis is that these lists lag a write, which is why one
    /// NON-empty read must not be believed. The same lag in the other direction
    /// was believed completely, on the first read — and that is the direction
    /// that exits 0.
    #[test]
    fn a_resting_list_that_lags_a_place_is_not_believed_on_one_empty_read() {
        let s = LaggingList {
            reads: Mutex::new(0),
            lag_reads: 1,
            truth: vec!["66e1c799-507b-4a59-89aa-ec23ad14b990".into()],
            cancels: Mutex::new(0),
        };
        let err = cancel_all_and_verify_blocking(&s, &fast())
            .expect_err("one empty read while an order is resting is NOT clean");
        assert!(err.contains("66e1c799"), "it found the order the lag hid: {err}");
        assert!(*s.reads.lock().unwrap() > 1, "it looked more than once");
        assert!(*s.cancels.lock().unwrap() > 1, "and re-cancelled once it saw it");
    }

    /// The other side of the same policy: a genuinely empty book still returns
    /// Ok — it just has to say so twice. This is the cost (one poll_delay) and it
    /// must not turn into "never clean".
    #[test]
    fn a_genuinely_empty_book_is_still_clean_after_confirmation() {
        let s = LaggingList {
            reads: Mutex::new(0),
            lag_reads: u32::MAX, // always empty
            truth: Vec::new(),
            cancels: Mutex::new(0),
        };
        cancel_all_and_verify_blocking(&s, &fast()).expect("empty twice over is clean");
        assert_eq!(*s.reads.lock().unwrap(), 2, "exactly the confirmations asked for");
    }

    /// An unreadable list is not an empty one, so it cannot stand in for one of
    /// the confirmations.
    #[test]
    fn an_error_does_not_count_as_an_empty_confirmation() {
        // empty, then error, then empty: never two CONSECUTIVE empties.
        struct Alternating {
            n: Mutex<u32>,
        }
        impl OrderSink for Alternating {
            fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
                unreachable!()
            }
            fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
                unreachable!()
            }
            fn cancel_all_open(&self) -> Result<(), VenueError> {
                Ok(())
            }
            fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
                let mut n = self.n.lock().unwrap();
                *n += 1;
                if n.is_multiple_of(2) {
                    Err(VenueError::Transport("503".into()))
                } else {
                    Ok(Vec::new())
                }
            }
        }
        let err = cancel_all_and_verify_blocking(&Alternating { n: Mutex::new(0) }, &fast())
            .expect_err("an error between two empties breaks the run");
        assert!(err.contains("could NOT be proven clean"), "{err}");
    }

    /// THE 15:40 leak, in one test. A queued place lands one second into the
    /// sweep, so the book is non-empty AFTER the first cancel-all. The old code
    /// cancelled once and then polled ten times: it could only watch. This must
    /// re-issue the cancel and converge.
    #[test]
    fn a_late_arrival_is_re_cancelled_and_the_sweep_converges() {
        // cancel #1 clears the book but a place lands right behind it;
        // cancel #2 takes that one off.
        let s = Scripted::new(&["m1"], &[&["66e1c799-507b-4a59-89aa-ec23ad14b990"], &[]]);
        cancel_all_and_verify_blocking(s.as_ref(), &fast()).expect("must converge");
        assert_eq!(s.cancels(), 2, "the second cancel-all is the whole fix");
    }

    /// The give-up condition: bounded rounds, then a NAMED failure. Silence and
    /// "probably fine" are both forbidden here.
    #[test]
    fn a_venue_that_never_goes_clean_is_a_named_error() {
        let s = Scripted::new(&["m1"], &[&["m1"], &["m1"], &["m1"], &["m1"], &["m1"]]);
        let pol = fast();
        let err = cancel_all_and_verify_blocking(s.as_ref(), &pol).unwrap_err();
        assert!(err.contains("SURVIVED"), "{err}");
        assert!(err.contains("m1"), "names the order left behind: {err}");
        assert_eq!(s.cancels(), pol.rounds as usize, "it tried every round it was given");
    }

    /// A refused cancel-all is not the end of the sweep. The rate limiter errors
    /// instead of waiting, and the old code `?`-aborted on the first refusal —
    /// which on a full book is how a kill sweep ends with orders resting.
    #[test]
    fn a_refused_cancel_all_is_retried_not_fatal() {
        let mut s = Scripted::new(&["m1"], &[&["m1"], &[]]);
        std::sync::Arc::get_mut(&mut s).unwrap().cancel_errs = vec![true, false];
        cancel_all_and_verify_blocking(s.as_ref(), &fast()).expect("the retry clears it");
        assert_eq!(s.cancels(), 2);
    }

    /// A book we cannot READ is never reported as clean, and never as
    /// "0 orders survived" either — what is resting is unknown, which is worse.
    #[test]
    fn an_unreadable_resting_list_is_an_unproven_book_not_an_empty_one() {
        let s = Scripted::new(&[], &[]);
        *s.list_errs.lock().unwrap() = 1000;
        let err = cancel_all_and_verify_blocking(s.as_ref(), &fast()).unwrap_err();
        assert!(err.contains("could NOT be proven clean"), "{err}");
        assert!(err.contains("503"), "carries the venue's own error: {err}");
    }

    /// An already-empty book is proven inside the first round and costs one
    /// cancel-all — two confirming reads, not two rounds.
    #[test]
    fn an_empty_book_is_proven_inside_one_round() {
        let s = Scripted::new(&[], &[]);
        cancel_all_and_verify_blocking(s.as_ref(), &fast()).unwrap();
        assert_eq!(s.cancels(), 1, "confirmation costs a poll, not a second cancel-all");
    }

    /// The wall-clock ceiling holds even when the rounds have not run out — a
    /// slow venue must not be able to spend the whole systemd stop timeout.
    #[test]
    fn the_budget_bounds_a_venue_that_answers_forever() {
        let s = Scripted::new(&["m1"], &[]);
        let pol = SweepPolicy {
            rounds: 1000,
            polls_per_round: 1,
            poll_delay: std::time::Duration::from_millis(5),
            confirm_empty_reads: 2,
            budget: std::time::Duration::from_millis(80),
        };
        let t0 = std::time::Instant::now();
        let err = cancel_all_and_verify_blocking(s.as_ref(), &pol).unwrap_err();
        assert!(t0.elapsed() < std::time::Duration::from_secs(3), "took {:?}", t0.elapsed());
        assert!(err.contains("gave up at the"), "{err}");
        assert!(s.cancels() < 1000, "it stopped short of the round limit");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_venue::gateway::{KalshiGateway, Side, Tif};
    use arb_venue::ratelimit::RateLimiter;
    use arb_venue::transport::{Response, Transport};
    use arb_venue::KalshiSigner;
    use serde_json::Value;
    use std::sync::Mutex;

    struct Mock {
        replies: Mutex<Vec<(u16, String)>>,
        sent: Mutex<Vec<(String, String, Option<Value>)>>,
    }
    impl Transport for Mock {
        fn send(
            &self,
            method: &str,
            path: &str,
            _q: Option<&str>,
            _h: &[(&'static str, String)],
            body: Option<&Value>,
        ) -> Result<Response, VenueError> {
            self.sent.lock().unwrap().push((
                method.to_string(),
                path.to_string(),
                body.cloned(),
            ));
            let (status, body) = self.replies.lock().unwrap().remove(0);
            Ok(Response { status, body })
        }
    }

    fn signer() -> KalshiSigner {
        let v: Value = serde_json::from_str(include_str!(
            "../../../crates/arb-venue/tests/fixtures/venue/sigs.json"
        ))
        .unwrap();
        KalshiSigner::from_pkcs8_pem(
            v["kalshi"]["api_key_id"].as_str().unwrap(),
            v["kalshi"]["private_key_pkcs8_pem"].as_str().unwrap(),
        )
        .unwrap()
    }

    /// The engine's PlaceRequest reaches the venue intact — this is the whole
    /// point of making ExecCmd order-shaped.
    #[test]
    fn a_place_command_reaches_the_venue_with_the_engines_order() {
        let mock = Mock {
            replies: Mutex::new(vec![(
                201,
                r#"{"order_id":"srv-1","client_order_id":"m42"}"#.to_string(),
            )]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 0),
            mock,
        );
        let oid = OrderSink::place(
            &gw,
            &PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Ask,
                price: "0.4200".into(),
                qty: 5,
                tif: Tif::Gtc,
                post_only: true,
                client_order_id: "m42".into(),
            },
        )
        .unwrap();
        assert_eq!(oid, "srv-1", "the venue's id, not ours");

        let sent = gw.transport.sent.lock().unwrap();
        let (method, path, body) = &sent[0];
        assert_eq!(method, "POST");
        assert_eq!(path, "/trade-api/v2/portfolio/events/orders");
        let b = body.as_ref().unwrap();
        assert_eq!(b["ticker"], "KXTEST");
        assert_eq!(b["side"], "ask");
        assert_eq!(b["price"], "0.4200");
        assert_eq!(b["count"], "5.00");
        assert_eq!(b["post_only"], true);
        assert_eq!(
            b["client_order_id"], "m42",
            "our order id rides along — that is what makes a retry idempotent"
        );
    }

    #[test]
    fn a_cancel_command_reaches_the_venue() {
        let mock = Mock {
            replies: Mutex::new(vec![(200, "{}".to_string())]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 0),
            mock,
        );
        OrderSink::cancel(
            &gw,
            &CancelRequest {
                by: arb_venue::gateway::CancelBy::VenueId("srv-1".into()),
                market_slug: Some("KXTEST".into()),
            },
        )
        .unwrap();
        let sent = gw.transport.sent.lock().unwrap();
        assert_eq!(sent[0].0, "DELETE");
        assert_eq!(sent[0].1, "/trade-api/v2/portfolio/events/orders/srv-1");
    }

    /// The whole chain, end to end: a venue answering 200 with a body that has
    /// no `orders` key must come out of the sweep as UNPROVEN.
    ///
    /// `sweep_tests` above proves the loop refuses to let a read ERROR stand in
    /// for a confirmation. This proves the venue layer actually hands it one —
    /// which, until `KalshiOrdersPage::orders` became required, it did not: the
    /// body deserialized to an empty page, `cancel_all_open` cancelled nothing
    /// and returned `Ok(())`, both confirming reads came back empty, and this
    /// call returned `Ok(())`. That `Ok` is what `exec::ShutdownOutcome` prints
    /// as "book PROVEN clean at exit" before exiting 0.
    #[test]
    fn a_book_the_venue_never_showed_us_is_not_proven_clean() {
        let mock = Mock {
            // one cancel-all round + one confirming poll, both list reads
            replies: Mutex::new(vec![
                (200, r#"{"error":"internal"}"#.to_string()),
                (200, r#"{"error":"internal"}"#.to_string()),
            ]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 0),
            mock,
        );
        let pol = SweepPolicy {
            rounds: 1,
            polls_per_round: 1,
            poll_delay: std::time::Duration::ZERO,
            ..SweepPolicy::default()
        };
        let err = cancel_all_and_verify_blocking(&gw, &pol)
            .expect_err("a book we never read is not a clean book");
        assert!(err.contains("could NOT be proven clean"), "{err}");
        assert!(err.contains("missing required field `orders`"), "names the cause: {err}");
    }

    /// A venue rejection surfaces as an error rather than being counted as a
    /// successful place.
    #[test]
    fn a_rejected_place_is_an_error() {
        let mock = Mock {
            replies: Mutex::new(vec![(400, r#"{"error":"post_only_would_cross"}"#.to_string())]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 0),
            mock,
        );
        assert!(OrderSink::place(
            &gw,
            &PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Bid,
                price: "0.4200".into(),
                qty: 5,
                tif: Tif::Gtc,
                post_only: true,
                client_order_id: "m1".into(),
            },
        )
        .is_err());
    }
}
