//! Per-venue executor tasks — the effects boundary of the P3 shell.
//!
//! DRY-RUN BY DEFAULT: an executor with no [`OrderSink`] counts its command and
//! drops it, loading no credentials (arb-recorder's posture). A sink is only
//! ever supplied by `--enable-orders`, and only once its preconditions pass.
//!
//! Each executor owns its venue's rate budget (token bucket) and records the
//! engine->executor hop latency — the point where the order goes to the wire. A
//! slow venue therefore backs up ITS executor channel only; the engine never
//! blocks on venue I/O (the P1 postmortem's gap mechanism).

use crate::hist::Hist;
use crate::sink::{OrderSink, SweepPolicy};
use arb_venue::gateway::{CancelBy, CancelRequest, PlaceRequest};
use arb_core::clock::now_ns;
use arb_core::model::Venue;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// The effect, carrying the ORDER — not just its shape.
///
/// This used to be a bare `Place`/`Cancel` with no market, price, quantity or
/// side: enough to measure the engine->executor hop, not enough to put an order
/// on a wire. The requests are arb-venue's venue-neutral types, so the same
/// value the executor counts today is the value a gateway will send.
pub enum Action {
    Place(PlaceRequest),
    /// A cancel, plus the engine's ATTEMPT NUMBER for the order it names.
    ///
    /// The number never reaches a wire — it is echoed back in `cancel_result` so
    /// the engine can tell the answer to the attempt currently outstanding from
    /// a LATE answer to a superseded one. Without it, an answer to attempt 1
    /// arriving after attempt 2 had gone re-armed the retry and put attempt 3 on
    /// the wire alongside 2. `0` means unnumbered: an escalation, or a dry run,
    /// whose answer must never be matched to a numbered attempt.
    Cancel { req: CancelRequest, attempt: u32 },
    /// Cancel EVERYTHING resting on this venue and verify it is gone.
    ///
    /// The kill switch's per-quote cancels only reach orders the engine still
    /// has ids for; this reaches the rest, and unlike a cancel it proves the
    /// outcome. Halting is the one moment where "probably cancelled" is not
    /// good enough.
    SweepAndVerify,
}

pub struct ExecCmd {
    pub t_read: Instant,
    pub action: Action,
}

pub struct ExecStats {
    pub hop: Hist,
    pub placed: AtomicU64,
    pub cancelled: AtomicU64,
    pub dropped: AtomicU64, // engine try_send failures (executor backlogged)
    /// Commands that actually reached a venue (0 in dry-run).
    pub sent: AtomicU64,
    /// Venue rejections/errors. Counted separately from `sent` so a venue that
    /// refuses everything cannot read as a working order path.
    pub failed: AtomicU64,
    /// Places whose RESPONSE was lost and whose order was then FOUND resting at
    /// the venue. Each one was live under an id this process had not learned:
    /// unaddressable by any cancel, and a fill on it would have arrived under an
    /// id nothing could attribute. Never routine; must never be silent.
    pub recovered: AtomicU64,
}

// -------------------------------------------------------------- the halt ---

/// Exit code that means EXACTLY one thing: **this process may have left an
/// order resting on a venue.** Go look.
///
/// It exists because of 2026-07-28 15:40. The shutdown sweep logged
/// `Kalshi: NOT CLEAN at exit — 1 order(s) SURVIVED the sweep` and then fell
/// into the same unconditional `std::process::exit(0)` as the success path, so
/// systemd recorded `Stopped arbbot-trader-m3.service.`, `systemctl stop`
/// returned 0, and a real unattended maker order rested on Kalshi until it was
/// cancelled by hand. A halt that cannot fail loudly is not a halt.
pub const EXIT_ORDERS_LEFT_RESTING: i32 = 17;

/// Exit code that means: **the cancel went in, nothing was seen resting, and
/// the book could not be CONFIRMED.** Weaker than 17 and still not zero.
///
/// It is not zero because of how this unit is actually watched.
/// `arbbot-trader-m3` is `Restart=no` with no `OnFailure=`, and
/// `scripts/freshness_check.sh` pages on `systemctl is-failed` and nothing
/// else — its own comment says "is-failed is therefore the only RUNTIME
/// condition that means something is wrong". So a zero exit here would leave
/// the unit `inactive/Result=success`, byte-identical to a deliberate disarm,
/// with the evidence only in a journal nobody is instructed to read. The one
/// automated alarm this system has would be silent for the one outcome nobody
/// has ever observed.
///
/// The asymmetry with the ARMING path is deliberate and is the whole reason
/// these are two different decisions. Refusing to start risks a real outage —
/// an engine that can never come up over an unobserved response body — and a
/// human is present to see it. On the way OUT the process exits either way:
/// the code changes nothing about whether orders rest, only whether the alarm
/// fires. Fail-closed here has zero outage risk. Noisy, never an outage — and
/// that noise is precisely how the missing observation finally gets made.
///
/// This constant exists because the first cut of it did not, and the result was
/// BACKWARDS: it PAGED WHEN IT REFUSED and stayed SILENT WHEN IT PROCEEDED
/// UNPROVEN. The refusal path already exits 10 and trips `is-failed`, so the
/// outcome a human was told about was the one where nothing had been left
/// resting, while the outcome where the book was genuinely unknown looked like
/// a clean stop.
///
/// SO, FOR EVERY NEW "we could not tell" STATE ADDED HERE — name BOTH outcomes
/// it splits into, give each one an exit code, and confirm the one that
/// PROCEEDS is not quieter than the one that REFUSES. It is a procedure and not
/// a question because both paths feel conservative from the inside, and the
/// asymmetry is only visible once the two codes are written down next to each
/// other.
///
/// It has already caught one: `Unproven::cancel_accepted` was a latch, so a
/// sweep whose FIRST round succeeded and whose later rounds were cut short came
/// out as merely unconfirmed — proceeding, quietly, over a real resting order on
/// a page nobody read. Running the procedure finds it; asking "which way round
/// does this page" does not.
pub const EXIT_BOOK_UNCONFIRMED: i32 = 18;

/// Wall-clock budget for in-flight PLACES to settle before we verify.
///
/// Strictly LONGER than `HttpTransport`'s own 15s timeout, on purpose. A place
/// still outstanding at this point makes the exit non-zero (its resting list read
/// is not evidence), so the budget has to outlast the slowest call that will
/// resolve by itself — otherwise a place that was merely about to time out gets
/// reported as wedged and manufactures a false `EXIT_ORDERS_LEFT_RESTING`. 5s of
/// slack past the transport timeout.
const QUIESCE_BUDGET: Duration = Duration::from_secs(20);
/// Aggregate budget for sweeping EVERY venue, which happens concurrently.
/// `TimeoutStopUSec=1min 30s`: 20 + 40 leaves ~30s of headroom, so SIGKILL can
/// never arrive mid-sweep and leave everything resting.
const SWEEP_BUDGET: Duration = Duration::from_secs(40);
/// How long a halt that LOST the claim waits for the winner to exit the process.
/// Must stay well inside `TimeoutStopUSec=1min 30s`: waiting longer than the
/// stop timeout hands the arbitration to SIGKILL, which is the bug this whole
/// file exists to close.
const HALT_LOSER_WAIT: Duration = Duration::from_secs(30);
/// How long the panic path's private runtime waits for a still-running blocking
/// venue call before abandoning it. See `sweep_on_private_runtime`.
const RUNTIME_ABANDON_AFTER: Duration = Duration::from_secs(1);

/// The shutdown latch, enforced at the effects boundary.
///
/// Once [`Halt::begin`] has been called, no `Place` may reach a venue from this
/// process again: every queued one is DISCARDED and counted. `Cancel` and
/// `SweepAndVerify` still go through — they are how the book gets clean.
///
/// 2026-07-28 15:40:13: SIGTERM started the shutdown sweep by calling
/// `cancel_all_and_verify` directly on the sink, while three per-venue executor
/// tasks were still alive draining up to 1024 queued commands each behind a
/// token bucket. One of them placed a maker order at 15:40:14 — one second INTO
/// the sweep — and it was still resting when the process exited. Nothing had
/// told the executors to stop placing. This is that thing.
///
/// It lives at the executor rather than in the engine's decision loop on
/// purpose: the executor is the last code between this process and a live order,
/// so a latch here holds no matter which decision path emitted the place, and
/// holds for paths that never consult the engine at all.
#[derive(Default)]
pub struct Halt {
    halted: AtomicBool,
    /// PLACES dispatched to a venue and not yet returned.
    ///
    /// Places only, deliberately. The verify has to wait for these or it races
    /// the very order it is looking for — that is the second half of the 15:40
    /// leak. Counting cancels here too would be worse than useless: the engine
    /// keeps emitting cancels throughout a halt (that is how the book gets
    /// clean), so quiescence would routinely burn its whole 15 s budget and the
    /// one signal that matters would arrive as an every-shutdown warning. A
    /// cancel racing the verify is harmless anyway — it can only REMOVE orders,
    /// so it cannot make a dirty book read clean.
    inflight_places: AtomicU64,
    /// Places refused because the latch was already on.
    discarded: AtomicU64,
    /// Of those, the ones that could leave a leg NAKED — see [`is_taker`].
    discarded_takers: AtomicU64,
    /// Our ids for the first few of them, for the exit banner. Bounded: this is
    /// for the human reading `systemctl status`, not an audit log.
    naked_risk_ids: std::sync::Mutex<Vec<String>>,
}

/// A place that cannot rest is one leg of something already half-done.
///
/// The quoter only ever rests post-only GTC makers; the two taker paths are the
/// hedge that makes a filled maker leg riskless (`engine.rs` intent_actions) and
/// a take-take entry (`taker: true`). Discarding a *maker* at shutdown is pure
/// safety — nothing rests, nothing is naked. Discarding a *taker* may mean a
/// filled leg with no other side, which is a real position and must never exit 0.
///
/// This deliberately does not try to tell a hedge from a take-take leg for the
/// exit code: take-take's two legs go to two different executors, so the latch
/// can catch the second one after the first is already on the wire. Both are
/// naked-leg risks. The `h`/`t`/`m` id prefixes (`engine.rs`, the same
/// convention that keeps ids unique across restarts) are used only to LABEL
/// them for the human.
fn is_taker(req: &PlaceRequest) -> bool {
    !req.post_only || matches!(req.tif, arb_venue::gateway::Tif::Ioc)
}

/// Held for exactly as long as a blocking venue call is in flight.
pub struct InFlight {
    halt: Arc<Halt>,
    /// Only a place is counted, so only a place is decremented.
    counted: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self.counted {
            self.halt.inflight_places.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Halt {
    /// Latch it. Returns `true` if this call was the one that flipped it.
    pub fn begin(&self) -> bool {
        !self.halted.swap(true, Ordering::SeqCst)
    }

    pub fn is_on(&self) -> bool {
        self.halted.load(Ordering::SeqCst)
    }

    pub fn discarded(&self) -> u64 {
        self.discarded.load(Ordering::SeqCst)
    }

    pub fn discarded_takers(&self) -> u64 {
        self.discarded_takers.load(Ordering::SeqCst)
    }

    pub fn naked_risk_ids(&self) -> Vec<String> {
        self.lock_ids().clone()
    }

    /// Poison-recovering: this is reachable from a panic hook, and a second
    /// panic there aborts the process with orders resting.
    fn lock_ids(&self) -> std::sync::MutexGuard<'_, Vec<String>> {
        self.naked_risk_ids.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Count a place the executor refused before it ever dispatched.
    pub fn count_discard(&self, req: &PlaceRequest) -> u64 {
        let n = self.discarded.fetch_add(1, Ordering::SeqCst) + 1;
        if is_taker(req) {
            self.discarded_takers.fetch_add(1, Ordering::SeqCst);
            let mut ids = self.lock_ids();
            if ids.len() < 10 {
                ids.push(req.client_order_id.clone());
            }
        }
        n
    }

    /// Claim an in-flight slot for a blocking venue call. `None` means the latch
    /// won the race and this order must never reach the wire.
    ///
    /// `place` is `None` for a cancel, which needs no interlock at all: a cancel
    /// can only ever remove an order, so letting one race the verify cannot make
    /// a dirty book look clean. It still gets a guard so the call site is
    /// uniform.
    ///
    /// For a place the increment happens BEFORE the latch is read, and
    /// [`Halt::begin`] stores before `await_quiescent` reads `inflight_places`.
    /// That ordering is what makes the two mutually exclusive: either the halter
    /// sees this call and waits for it, or this call sees the latch and discards
    /// itself. There is no interleaving in which a place slips out unseen.
    pub fn enter(self: &Arc<Self>, place: Option<&PlaceRequest>) -> Option<InFlight> {
        let Some(req) = place else {
            return Some(InFlight { halt: self.clone(), counted: false });
        };
        self.inflight_places.fetch_add(1, Ordering::SeqCst);
        if self.halted.load(Ordering::SeqCst) {
            self.inflight_places.fetch_sub(1, Ordering::SeqCst);
            self.count_discard(req);
            return None;
        }
        Some(InFlight { halt: self.clone(), counted: true })
    }

    /// Wait for every in-flight PLACE to return. Yields the number still on the
    /// wire when the budget ran out — non-zero means the verify below it is
    /// racing an order the venue may already have accepted, so the sweep's
    /// answer is not evidence and the exit code must say so.
    pub async fn await_quiescent(&self, budget: Duration) -> u64 {
        let t0 = Instant::now();
        loop {
            let n = self.inflight_places.load(Ordering::SeqCst);
            if n == 0 {
                return 0;
            }
            if t0.elapsed() >= budget {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The process's one latch. Global rather than threaded through
/// `spawn_executors` because shutdown IS process-wide, and because the abnormal
/// exits that most need it (a WAL crash-stop, a panic) have no handle on the
/// engine's plumbing. Tests drive [`run_executor`] with their own [`Halt`].
static HALT: std::sync::LazyLock<Arc<Halt>> =
    std::sync::LazyLock::new(|| Arc::new(Halt::default()));

pub fn halt() -> &'static Arc<Halt> {
    &HALT
}

/// The armed sinks, so an ABNORMAL exit can still cancel.
static SINKS: std::sync::OnceLock<HashMap<Venue, Arc<dyn OrderSink>>> = std::sync::OnceLock::new();

/// Hand the armed sinks to the halt path. Without this, `Wal::append`'s
/// crash-stop and any panic call `std::process::exit` with quotes resting and
/// `Restart=no` — the sweep only ever listened for SIGTERM.
pub fn register_sinks(sinks: HashMap<Venue, Arc<dyn OrderSink>>) {
    let _ = SINKS.set(sinks);
}

/// `None` means the order path was NEVER armed, which is the only honest reason
/// for a halt to sweep nothing. An armed-but-empty map is a different thing and
/// must not read as clean — see `halt_and_sweep`.
fn registered_sinks() -> Option<HashMap<Venue, Arc<dyn OrderSink>>> {
    SINKS.get().cloned()
}

/// Only one halt may own the exit. Every entry point ends in
/// `std::process::exit`, so the loser must not start a second concurrent sweep
/// (duplicate cancel-alls against a rate limiter that errors instead of
/// waiting) — it waits for the winner to kill the process.
static HALTING: AtomicBool = AtomicBool::new(false);

pub fn halting() -> bool {
    HALTING.load(Ordering::SeqCst)
}

fn claim_halt() -> bool {
    !HALTING.swap(true, Ordering::SeqCst)
}

/// What the halt proved, separated from the `exit()` so it can be tested
/// without ending the test process.
#[derive(Debug, Default)]
pub struct ShutdownOutcome {
    pub clean: Vec<Venue>,
    pub unclean: Vec<(Venue, String)>,
    /// Swept, cancel-all ACCEPTED, and nothing ever observed resting — but the
    /// confirmation could not be READ. Absence of evidence, not evidence of a
    /// leak, and deliberately NOT part of `exit_code`.
    ///
    /// It is its own bucket because the alternative is a self-inflicted outage:
    /// nobody has captured what PM-US sends on an EMPTY book, so if that shape
    /// is one `open_orders` cannot parse, counting it as unclean would make
    /// every shutdown exit 17 and every start refuse, for ever, over an empty
    /// book. The line is unmissable and carries the raw body, which is how the
    /// observation finally gets made.
    pub unconfirmed: Vec<(Venue, String)>,
    /// Places the latch refused after shutdown began. Each one is an order that
    /// would otherwise have been placed by a process on its way out. Makers
    /// among these are pure safety.
    pub discarded_places: u64,
    /// Of those, the ones that could leave a leg naked (hedges, take-take legs).
    pub discarded_takers: u64,
    /// Our ids for the first few of those, for the banner.
    pub naked_risk_ids: Vec<String>,
    /// PLACES still on the wire when we gave up waiting and verified anyway.
    /// The sweep's answer is not evidence when this is non-zero: the venue may
    /// have accepted an order the resting list had not caught up with.
    pub places_inflight_at_verify: u64,
    /// Another halt already owned the exit, so this one did nothing. The caller
    /// must NOT exit on this — the winner's verdict is the real one.
    pub already_halting: bool,
}

impl ShutdownOutcome {
    /// Non-zero unless the book is provably clean AND nothing was on the wire
    /// while we proved it AND no half-done position was abandoned.
    ///
    /// The middle condition is the one 15:40 needed: a place in flight at verify
    /// time means the resting list we read could not yet have shown it, so an
    /// empty list is silence, not proof. It was a WARNING line and had no effect
    /// on the code — which is the same shape as the original bug, one level up.
    pub fn exit_code(&self) -> i32 {
        let provable = self.unclean.is_empty()
            && self.places_inflight_at_verify == 0
            && self.discarded_takers == 0;
        if !provable {
            return EXIT_ORDERS_LEFT_RESTING;
        }
        // Nothing is known to be resting, but a venue could not confirm it.
        // Distinct from 17 so a human never has to disambiguate the two, and
        // non-zero so the one alarm this deployment has actually fires.
        if !self.unconfirmed.is_empty() {
            return EXIT_BOOK_UNCONFIRMED;
        }
        0
    }

    /// Unmissable in `systemctl status`, which shows the last log lines and the
    /// exit code and nothing else.
    pub fn report(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.already_halting {
            out.push(
                "[exec] another halt already owns the exit; this one swept nothing \
                 (its verdict stands, not ours)"
                    .into(),
            );
            return out;
        }
        // A venue whose sweep came back clean has NOT been proven clean if a
        // place was on the wire while we read it — the venue may have accepted
        // an order the resting list could not yet show. Do not print the word
        // "PROVEN" over the top of the banner that says exactly the opposite.
        let proven = self.places_inflight_at_verify == 0;
        for v in &self.clean {
            if proven {
                out.push(format!("[exec] {v:?}: book PROVEN clean at exit"));
            } else {
                out.push(format!(
                    "[exec] {v:?}: resting list read EMPTY — but see below, this is not proof"
                ));
            }
        }
        let makers = self.discarded_places.saturating_sub(self.discarded_takers);
        if makers > 0 {
            out.push(format!(
                "[exec] halt discarded {makers} queued maker place(s) — none reached a venue"
            ));
        }
        if self.exit_code() == 0 {
            return out;
        }
        let code = self.exit_code();
        out.push("[exec] ###########################################################".into());
        if code == EXIT_BOOK_UNCONFIRMED {
            // Weaker claim, deliberately: nothing was SEEN resting. Saying
            // "orders may still be resting" here would cry wolf on every
            // shutdown if this turns out to be what an empty PM-US book looks
            // like, and an alarm that cries wolf is the one nobody reads.
            out.push("[exec] ### BOOK NOT CONFIRMED — the cancel went in, nothing was ###".into());
            out.push("[exec] ### seen resting, and no venue could PROVE it empty.    ###".into());
        } else {
            out.push("[exec] ### ORDERS MAY STILL BE RESTING ON A VENUE — NOT CLEAN  ###".into());
        }
        out.push("[exec] ###########################################################".into());
        for (v, e) in &self.unclean {
            out.push(format!("[exec] ### {v:?}: {e}"));
        }
        for (v, e) in &self.unconfirmed {
            out.push(format!("[exec] ### {v:?}: {e}"));
            out.push(
                "[exec] ###   ^ an UNREAD list, not a proven-empty one. The body above is \
                 a response shape this repo has never captured — PIN IT and tighten the check"
                    .into(),
            );
        }
        if self.places_inflight_at_verify > 0 {
            out.push(format!(
                "[exec] ### {} PLACE(S) STILL ON THE WIRE when the book was read — an \
                 empty resting list here is silence, NOT proof",
                self.places_inflight_at_verify
            ));
        }
        if self.discarded_takers > 0 {
            // A maker discard is safety. A taker discard is a leg that was
            // supposed to close something: a hedge for a fill that already
            // happened, or the second half of a take-take crossing. That is a
            // real position with nothing on the other side of it.
            out.push(format!(
                "[exec] ### {} TAKER place(s) DISCARDED — a filled leg may be NAKED: {}",
                self.discarded_takers,
                if self.naked_risk_ids.is_empty() {
                    "(ids unavailable)".to_string()
                } else {
                    self.naked_risk_ids.join(" ")
                }
            ));
            out.push(
                "[exec] ###   ids starting h = hedge (a maker leg filled and is unhedged), \
                 t = take-take leg"
                    .into(),
            );
            out.push(
                "[exec] ###   CHECK POSITIONS BY HAND: arbbot-hedge.timer only \
                 reconciles PROFITABLE naked legs"
                    .into(),
            );
        }
        out.push(
            "[exec] ### CANCEL BY HAND NOW: arb-trader --sweep-only \
             --cred-suffix <as armed>"
                .into(),
        );
        out.push(format!(
            "[exec] ### exiting {code} so systemd records a FAILURE, not a clean stop"
        ));
        out
    }
}

/// How an abnormal stop's own exit code composes with what the sweep proved.
///
/// The stop's code survives a CLEAN book — a WAL hole is still a WAL hole, and a
/// panic is still a panic — but a book that is not clean overrides it, because
/// what the sweep found is the one thing a human must never have to
/// disambiguate. Split out from the `exit()` so it can be asserted on.
///
/// "Not clean" means BOTH non-zero verdicts, and that is deliberate:
/// `EXIT_BOOK_UNCONFIRMED` (18) overrides a WAL stop's 70 and a panic's 101 the
/// same way `EXIT_ORDERS_LEFT_RESTING` (17) does. A panic whose sweep could not
/// read the book is a panic AND an unknown book, and the unknown book is the
/// half that needs a human at the venue. 17 still beats 18 — that ordering is
/// in `ShutdownOutcome::exit_code`, not here.
pub fn halt_exit_code(clean_code: i32, out: &ShutdownOutcome) -> i32 {
    if out.exit_code() == 0 { clean_code } else { out.exit_code() }
}

/// Sweep every venue CONCURRENTLY under one aggregate deadline.
///
/// Sequentially — as the shutdown path did — a venue that hangs burns the whole
/// `TimeoutStopSec` budget and SIGKILL then leaves the OTHER venue's orders
/// resting too. A venue that does not report inside the deadline is `unclean`:
/// silence is not proof of an empty book.
pub async fn shutdown_sweep(
    sinks: HashMap<Venue, Arc<dyn OrderSink>>,
    pol: SweepPolicy,
    aggregate: Duration,
) -> ShutdownOutcome {
    let deadline = tokio::time::Instant::now() + aggregate;
    let mut pending: HashSet<Venue> = sinks.keys().copied().collect();
    let mut set = tokio::task::JoinSet::new();
    for (v, s) in sinks {
        let p = pol.clone();
        set.spawn(async move { (v, crate::sink::cancel_all_and_verify_with(s, p).await) });
    }
    let mut out = ShutdownOutcome::default();
    while !pending.is_empty() {
        match tokio::time::timeout_at(deadline, set.join_next()).await {
            Ok(Some(Ok((v, Ok(()))))) => {
                pending.remove(&v);
                out.clean.push(v);
            }
            Ok(Some(Ok((v, Err(e))))) => {
                pending.remove(&v);
                if e.is_only_unconfirmed() {
                    out.unconfirmed.push((v, e.msg));
                } else {
                    out.unclean.push((v, e.msg));
                }
            }
            // A panicked sweep proves nothing; its venue stays `pending` and is
            // reported below rather than quietly counted clean.
            Ok(Some(Err(e))) => eprintln!("[exec] a shutdown sweep task panicked: {e}"),
            Ok(None) => break,
            Err(_) => break, // aggregate deadline
        }
    }
    let mut left: Vec<Venue> = pending.into_iter().collect();
    left.sort();
    for v in left {
        out.unclean.push((
            v,
            format!(
                "sweep did not finish inside the {:.0}s shutdown deadline — \
                 book NOT proven clean",
                aggregate.as_secs_f64()
            ),
        ));
    }
    out.clean.sort();
    out
}

/// The one halt path: latch the effects boundary, let in-flight venue calls
/// settle, then prove every venue's book empty.
///
/// Order matters. Latching first is what makes the verify meaningful: sweeping a
/// venue whose executor is still draining queued places is how 15:40 happened.
pub async fn halt_and_sweep(reason: &str) -> ShutdownOutcome {
    let h = halt();
    h.begin();
    eprintln!("[exec] HALT ({reason}): no further order can reach a venue from this process");
    // THE claim, for every in-runtime halt including SIGTERM. It used to live
    // only in `spawn_halt_and_exit`, so a SIGTERM sweep left `HALTING` false for
    // its whole 55s — which made `wal.rs`'s dedup inert and let a WAL crash-stop
    // or a `book_basket` ledger failure start a SECOND concurrent sweep: two
    // sets of cancel-alls against a limiter that errors rather than waits, and
    // two racing `process::exit`s, so an unproven-book 17 could be overwritten
    // by a "clean" 70 — or self-inflicted 429s could manufacture a false 17.
    if !claim_halt() {
        eprintln!("[exec] halt already in progress — not starting a second sweep");
        return ShutdownOutcome { already_halting: true, ..Default::default() };
    }
    let stuck = h.await_quiescent(QUIESCE_BUDGET).await;
    if stuck > 0 {
        eprintln!("[exec] halt: {stuck} place(s) did not settle in {QUIESCE_BUDGET:?}");
    }
    eprintln!("[exec] halt: cancelling and VERIFYING every venue (concurrently)");
    let mut out = match registered_sinks() {
        Some(sinks) if sinks.is_empty() => {
            // Armed, yet no venue to sweep. Unreachable today (arming builds
            // both sinks or refuses), and cheap insurance against the day it is
            // not: "clean" must never be indistinguishable from "never looked".
            let mut o = ShutdownOutcome::default();
            o.unclean.push((
                Venue::Kalshi,
                "the order path was armed but no venue sink was registered — \
                 nothing could be swept, so nothing is proven"
                    .to_string(),
            ));
            o
        }
        Some(sinks) => shutdown_sweep(sinks, SweepPolicy::default(), SWEEP_BUDGET).await,
        None => {
            eprintln!("[exec] halt: the order path was never armed — nothing to sweep");
            ShutdownOutcome::default()
        }
    };
    out.discarded_places = h.discarded();
    out.discarded_takers = h.discarded_takers();
    out.naked_risk_ids = h.naked_risk_ids();
    out.places_inflight_at_verify = stuck;
    out
}

/// Emergency halt from a SYNC context that is already inside the runtime — the
/// WAL crash-stop. Returns immediately; the sweep runs as a task and exits the
/// process when it is done.
///
/// The caller's thread carries on, which is safe *because* the latch is already
/// on: anything the engine emits from here on is discarded at the boundary
/// instead of reaching a venue. That is what preserves the crash-stop's intent
/// (stop trading NOW) while fixing its bug (stop trading without cleaning up).
pub fn spawn_halt_and_exit(clean_code: i32, reason: String) {
    // Latch SYNCHRONOUSLY, before this returns. That is the load-bearing half:
    // the caller (`Wal::append`, `book_basket`) carries on running, and what
    // makes that safe is that nothing it emits from here can reach a venue.
    halt().begin();
    // The claim itself belongs to `halt_and_sweep` (so SIGTERM claims too), but
    // check it here as well: these callers fire once per failed write, and
    // without this a disk-full engine spawns a task per event.
    if halting() {
        return;
    }
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            rt.spawn(async move {
                let out = halt_and_sweep(&reason).await;
                for l in out.report() {
                    eprintln!("{l}");
                }
                if out.already_halting {
                    return; // the winner exits; racing it would overwrite its verdict
                }
                std::process::exit(halt_exit_code(clean_code, &out));
            });
        }
        Err(_) => {
            // No runtime (bench/replay, or a test): there is nothing to sweep
            // with and nothing armed, so the bare exit is the whole story.
            eprintln!("[exec] halt ({reason}) with no runtime — exiting {clean_code}");
            std::process::exit(clean_code);
        }
    }
}

/// Sweep on a PRIVATE runtime and shut that runtime down under a bound.
///
/// Split out of `halt_and_exit_blocking` so the bound can be tested: the rest of
/// that function ends in `std::process::exit`, which no in-process test survives.
/// Must be called on a thread with no ambient runtime — `block_on` inside one
/// panics.
///
/// The bound is the whole point. DROPPING a tokio runtime blocks until every
/// blocking task that has begun has finished, and an aborted `JoinSet` cannot
/// stop a blocking venue call, so a plain `drop(rt)` put an unbounded tail on the
/// panic halt — outside `SWEEP_BUDGET`, outside the 90 s stop timeout, and
/// inherited by the `join()` above it. We are about to exit; give a fast call a
/// moment to land and abandon the rest. A cancel still on the wire can only help:
/// it is idempotent and can only remove orders.
fn sweep_on_private_runtime(
    sinks: Option<HashMap<Venue, Arc<dyn OrderSink>>>,
    pol: SweepPolicy,
    aggregate: Duration,
) -> Result<ShutdownOutcome, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("halt runtime: {e}"))?;
    let out = rt.block_on(async move {
        // Short, and it cannot be QUIESCE_BUDGET: on the panic path the
        // panicking thread may itself hold the `InFlight` guard, which does not
        // drop until the unwind we are preventing.
        let stuck = halt().await_quiescent(Duration::from_secs(2)).await;
        let mut out = match sinks {
            Some(s) => shutdown_sweep(s, pol, aggregate).await,
            None => ShutdownOutcome::default(),
        };
        out.discarded_places = halt().discarded();
        out.discarded_takers = halt().discarded_takers();
        out.naked_risk_ids = halt().naked_risk_ids();
        out.places_inflight_at_verify = stuck;
        out
    });
    rt.shutdown_timeout(RUNTIME_ABANDON_AFTER);
    Ok(out)
}

/// Emergency halt that must NOT return — the panic hook.
///
/// The sweep runs on a fresh OS thread with its own current-thread runtime,
/// because `block_on` inside an existing runtime panics and the panicking thread
/// here is usually a tokio worker. We then block until it finishes, so the
/// unwind cannot outrun the cancel.
///
/// The quiescence wait is much shorter here than on the SIGTERM path, and it
/// cannot be `QUIESCE_BUDGET`: the panicking thread may itself be the
/// blocking-pool thread holding an [`InFlight`] guard, which does not drop until
/// the unwind this function is preventing. Waiting the full 15s for a guard we
/// are holding would be 15s of nothing. The multi-round sweep is what actually
/// covers a place already on the wire.
fn halt_and_exit_blocking(clean_code: i32, reason: &str) -> ! {
    halt().begin();
    eprintln!("[exec] HALT ({reason}): no further order can reach a venue from this process");
    if !claim_halt() {
        // Do NOT return: returning resumes the unwind, and if this is the main
        // task that tears the runtime down under the sweep that is already
        // running. Wait for the winner to kill the process.
        //
        // 30s, not 120s: `TimeoutStopUSec=1min 30s`, so a 120s wait handed the
        // arbitration to SIGKILL mid-sweep — the original bug. And reaching the
        // end of this wait MEANS the winner never exited, i.e. the halt failed,
        // so the code must be the one that says orders may be resting. Reachable
        // for real: a panic inside a sweep task during a panic-initiated halt
        // lands this sleep on the halt runtime's only worker while the winner
        // blocks in `h.join()` below.
        eprintln!("[exec] halt already in progress; waiting for it to exit the process");
        std::thread::sleep(HALT_LOSER_WAIT);
        eprintln!(
            "[exec] ### the halt that owned the exit never finished ({HALT_LOSER_WAIT:?}) \
             — ORDERS MAY BE RESTING"
        );
        std::process::exit(EXIT_ORDERS_LEFT_RESTING);
    }
    let sinks = registered_sinks();
    let joined = std::thread::Builder::new()
        .name("halt-sweep".into())
        .spawn(move || sweep_on_private_runtime(sinks, SweepPolicy::default(), SWEEP_BUDGET))
        .map_err(|e| format!("spawn halt thread: {e}"))
        .and_then(|h| h.join().map_err(|_| "halt sweep thread panicked".to_string()))
        .and_then(|r| r);
    let code = match joined {
        Ok(out) => {
            for l in out.report() {
                eprintln!("{l}");
            }
            halt_exit_code(clean_code, &out)
        }
        Err(e) => {
            eprintln!("[exec] ### HALT SWEEP COULD NOT RUN ({e}) — ORDERS MAY BE RESTING");
            EXIT_ORDERS_LEFT_RESTING
        }
    };
    std::process::exit(code);
}

/// A panic must not be a way to leave an order resting.
///
/// The sweep only ever awaited SIGTERM/ctrl-c, so every panic bypassed it: the
/// `--out` write/flush `expect`s (the armed drop-in passes `--out` and flushes
/// every intent, so ENOSPC panics it), the WAL writer thread's `expect`s, and
/// `panic!("unknown oracle_risk")` on the FIRST quote decision from an
/// unvalidated registry string. Any of those reached the 15:40 orphan scenario
/// with no signal at all.
///
/// **Limits, stated honestly.** This covers every *unwinding* panic on every
/// thread, including the engine future that `main` awaits inline, because a
/// panic hook runs before the unwind starts. It does NOT cover:
///   * `panic = "abort"` or `-C panic=abort` (the hook still runs, but only if
///     the runtime got that far — the workspace release profile unwinds today,
///     and this code depends on that);
///   * a panic *inside* the hook, a stack overflow, or an allocation failure —
///     all of those abort;
///   * SIGKILL, and therefore an OOM kill under the unit's `MemoryMax=1G`;
///   * `libc::abort` from a C dependency.
///
/// For those the startup sweep of the NEXT run remains the only backstop. A
/// `catch_unwind` around the engine would not add anything here — the hook
/// already runs earlier, on every thread, and the engine is not the only thread
/// that can panic. Full coverage of the abort cases is not reachable in-process.
pub fn install_armed_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev(info); // keep the normal panic message; it is the only diagnosis
        eprintln!("[exec] PANIC in an ARMED process — halting and sweeping before exit");
        // 101 is what an unhandled panic exits with; keep that when the book is
        // provably clean, so only 17 ever means "orders may be resting".
        halt_and_exit_blocking(101, "panic");
    }));
}

/// Tell the engine what a venue call decided.
///
/// `acks` is the SAME channel the feed writes to, so a venue reply is an event
/// like any other: it enters the one ordered channel, lands in the WAL and
/// replays with everything else. `None` is dry-run, where nothing reached a
/// venue and there is nothing to report.
async fn tell_engine(acks: &Option<mpsc::Sender<crate::feed::FeedMsg>>, line: serde_json::Value) {
    if let Some(tx) = acks {
        let _ = tx
            .send(crate::feed::FeedMsg { line: line.to_string(), t_read: Instant::now() })
            .await;
    }
}

/// The venue's ANSWER to one cancel, as an event.
///
/// A cancel is the one command whose outcome the engine cannot infer from
/// anything else it sees: a place answers with an `order_ack`, a fill answers
/// with a fill, and a cancel the venue REFUSED used to answer with nothing at
/// all. The engine had already retired every record that the order existed, so
/// nothing would ever try again and the quote went on resting beside its
/// replacement, at a price the engine had decided was wrong.
///
/// The id is reported in the SPACE it was addressed in — that is the whole
/// point of [`CancelBy`] — and the engine maps a venue id back through the ack
/// that taught it the pair.
pub(crate) fn cancel_result(
    venue: Venue,
    by: &CancelBy,
    market: &str,
    attempt: u32,
    err: Option<&str>,
) -> serde_json::Value {
    let (field, id) = match by {
        CancelBy::VenueId(v) => ("venue_order_id", v),
        CancelBy::ClientId(c) => ("order_id", c),
    };
    let mut v = serde_json::json!({
        "kind": "cancel_result",
        "venue": venue.as_str(),
        "market_id": market,
        // Echoed, not interpreted: it is the engine's own attempt number, and it
        // is what lets a late answer be told from the current one.
        "attempt": attempt,
        "ok": err.is_none(),
        "error": err,
        "ts_local_ns": now_ns(),
    });
    v[field] = serde_json::json!(id);
    v
}

/// The venue's ANSWER to a halt sweep, as an event.
///
/// The engine discharges a `sweeps_owed` entry on PROOF, and this is the proof:
/// it is the executor that awaits `cancel_all_and_verify`, so it is the only
/// thing in the process that knows whether the venue answered. Without it the
/// engine retired the obligation on `try_send` — and a sweep that was queued,
/// ran, and came back `KILL SWEEP FAILED` left a log line and no state, which is
/// exactly what happened on both venues inside the four-minute outage on
/// 2026-07-29.
///
/// It carries no market: a sweep is the account-wide command, and the engine
/// reads it above its own market guard for that reason.
pub(crate) fn sweep_result(venue: Venue, err: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "kind": "sweep_result",
        "venue": venue.as_str(),
        "ok": err.is_none(),
        "error": err,
        "ts_local_ns": now_ns(),
    })
}

/// Whether a failed place left us UNABLE TO SAY if the venue took the order.
///
/// This is the entire scope of the lost-response recovery, and it is a much
/// narrower question than "did the place fail". Both gateways return
/// [`VenueError::Status`] for any `status >= 300`, so a post-only that would
/// cross (400), a 403, a 422 are all DEFINITIVE: the venue did not take it,
/// nothing of ours rests, and there is nothing to recover. Those are also
/// routine — one 3.7-day shadow replay produced ~150 rejected places on PM-US
/// alone — and PM-US's recovery can only match on market and size, so running it
/// on a rejection would sooner or later adopt an order belonging to somebody
/// else on this shared account, cancel it, and book its fills as ours.
///
/// What is left is exactly the errors that can only happen AFTER the request
/// left this process:
///   * [`VenueError::Transport`] — the 15s reqwest timeout, a reset, a closed
///     connection. The venue may have processed it and we never saw the answer;
///   * [`VenueError::Parse`] / [`VenueError::MissingField`] — a 2xx whose body we
///     could not read. The order EXISTS; we just cannot name it. This is the
///     pair `KalshiGateway::rehearse` already recovers from, and the 2026-07-27
///     incident is the reason it does.
///
/// `NotWired`, `Sign` and `RateLimited` all fail before a byte is sent.
fn place_answer_was_lost(e: &arb_venue::VenueError) -> bool {
    use arb_venue::VenueError as E;
    match e {
        E::Transport(_) | E::Parse { .. } | E::MissingField { .. } => true,
        E::Status { .. } | E::NotWired | E::Sign(_) | E::RateLimited { .. } => false,
    }
}

/// `acks` is the SAME channel the feed writes to. A venue reply is an event
/// like any other: it enters the one ordered channel, so it lands in the WAL
/// and replays with everything else.
pub fn spawn_executors(
    rate_per_s: f64,
    mut sinks: HashMap<Venue, Arc<dyn OrderSink>>,
    acks: Option<mpsc::Sender<crate::feed::FeedMsg>>,
) -> (HashMap<Venue, mpsc::Sender<ExecCmd>>, Arc<ExecStats>) {
    let stats = Arc::new(ExecStats {
        hop: Hist::new(),
        placed: AtomicU64::new(0),
        cancelled: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        sent: AtomicU64::new(0),
        failed: AtomicU64::new(0),
        recovered: AtomicU64::new(0),
    });
    let mut txs = HashMap::new();
    for venue in [Venue::Kalshi, Venue::Polymarket, Venue::PolymarketUs] {
        let sink = sinks.remove(&venue);
        let acks = acks.clone();
        let (tx, rx) = mpsc::channel::<ExecCmd>(1024);
        let st = stats.clone();
        let h = halt().clone();
        tokio::spawn(run_executor(venue, rate_per_s, sink, rx, st, h, acks));
        txs.insert(venue, tx);
    }
    (txs, stats)
}

/// One venue's executor loop. A free function rather than an inline closure so
/// the tests can drive it with their own [`Halt`] and their own sink — the audit
/// found this file had ZERO tests, which is why finding #0 shipped.
async fn run_executor(
    venue: Venue,
    rate_per_s: f64,
    sink: Option<Arc<dyn OrderSink>>,
    mut rx: mpsc::Receiver<ExecCmd>,
    st: Arc<ExecStats>,
    halt: Arc<Halt>,
    acks: Option<mpsc::Sender<crate::feed::FeedMsg>>,
) {
    let mut tokens = rate_per_s.max(0.0);
    let mut last = Instant::now();
    // Venue ids this executor has taken responsibility for. It is the scope of
    // the lost-response recovery below: an id in here is an order we already
    // know about, so it can never be handed back as a NEW one — which is what
    // stops the recovery re-adopting our own order off a resting list that lags
    // a write. Grows with places made, never pruned: forgetting an id we once
    // claimed is the direction that adopts the wrong order.
    let mut claimed: HashSet<String> = HashSet::new();
    while let Some(cmd) = rx.recv().await {
        st.hop.record(cmd.t_read.elapsed().as_nanos() as u64);
        // `placed`/`cancelled` are dequeue counters — `would_place` has always
        // counted commands that never reached a venue (that is what dry-run IS),
        // so a discard belongs here too and `exec_sent` remains the count that
        // actually touched a wire. Incremented before the token bucket rather
        // than after it; totals are identical either way.
        match &cmd.action {
            Action::Place(_) => st.placed.fetch_add(1, Ordering::Relaxed),
            Action::Cancel { .. } | Action::SweepAndVerify => {
                st.cancelled.fetch_add(1, Ordering::Relaxed)
            }
        };
        // THE QUIESCENCE GATE. Checked before the token bucket, so a thousand
        // queued places are dropped instantly instead of trickling out at the
        // rate limit while the sweep tries to verify an empty book behind them.
        if halt.is_on() {
            if let Action::Place(p) = &cmd.action {
                let n = halt.count_discard(p);
                // A discarded taker is a leg that was meant to CLOSE something,
                // so it always gets a line however many there are; makers are
                // noise past the first few.
                if n <= 3 || is_taker(p) {
                    eprintln!(
                        "[exec] {venue:?}: HALTED — discarding queued {} {} ({} @{})",
                        if is_taker(p) { "TAKER place (possible naked leg!)" } else { "place" },
                        p.client_order_id,
                        p.market,
                        p.price
                    );
                }
                continue;
            }
        }
        if rate_per_s > 0.0 {
            // token bucket: this venue's API budget, owned here
            loop {
                let now = Instant::now();
                tokens =
                    (tokens + now.duration_since(last).as_secs_f64() * rate_per_s).min(rate_per_s);
                last = now;
                if tokens >= 1.0 {
                    tokens -= 1.0;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let Some(sink) = sink.clone() else {
            // dry-run: counted, dropped. A SWEEP still answers. A venue this
            // process holds no sink for is one it has never placed an order at,
            // so there is nothing of ours resting there to prove gone — and
            // saying nothing is no longer free, now that the engine holds the
            // obligation until something proves it. `spawn_executors` makes a
            // channel for all three venues however few are armed, so without
            // this every ARMED session would owe an INTL sweep for ever.
            //
            // ARMED is the whole of its reach, and it is worth being exact
            // about why: `main` passes `acks: None` when there is no sink at
            // ALL (`sinks.is_empty()`), so in a genuinely unarmed run this
            // answer is built and dropped, and the engine's obligation stands
            // for the life of the process. That is not the omission it looks
            // like — an unarmed process has no order path to prove anything
            // with, and its `sweeps_owed` reads as the standing "this run
            // swept nothing" it truthfully is. The case this branch exists for
            // is the mixed one, which is every armed session: Kalshi and PM-US
            // hold sinks, INTL never does.
            if matches!(cmd.action, Action::SweepAndVerify) {
                tell_engine(&acks, sweep_result(venue, None)).await;
            }
            continue;
        };
        // Not a per-order verb: it owns its own blocking + polling, so
        // it is handled before the place/cancel dispatch below.
        if matches!(cmd.action, Action::SweepAndVerify) {
            let failure = match crate::sink::cancel_all_and_verify(sink).await {
                Ok(()) => {
                    eprintln!("[exec] {venue:?}: kill sweep verified clean");
                    None
                }
                Err(e) => {
                    st.failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[exec] {venue:?}: KILL SWEEP FAILED — {e}");
                    Some(e.to_string())
                }
            };
            // Both outcomes, not just the good one: the failure is the whole
            // reason this channel exists (`sweep_result`), and a sweep that
            // reports nothing is one the engine has to assume the worst of.
            tell_engine(&acks, sweep_result(venue, failure.as_deref())).await;
            continue;
        }
        // The gateways block; running one on this worker would stall
        // every other task on it.
        let st2 = st.clone();
        let sink2 = sink.clone();
        // The order this command carries, kept OUT of the blocking closure: the
        // ack needs our id and its market (a fill arrives under the VENUE's id,
        // and this is the only place both are in hand), and a place whose
        // response is lost needs the whole request to find the order by.
        let placing = match &cmd.action {
            Action::Place(p) => Some(p.clone()),
            Action::Cancel { .. } | Action::SweepAndVerify => None,
        };
        // ...and what a cancel was addressed to, and as which attempt, for the
        // same reason: the engine cannot learn a cancel's outcome any other way.
        let cancelling = match &cmd.action {
            Action::Cancel { req, attempt } => {
                Some((req.by.clone(), req.market_slug.clone().unwrap_or_default(), *attempt))
            }
            Action::Place(_) | Action::SweepAndVerify => None,
        };
        // The latch can flip between the check above and here. `enter` closes
        // that window and counts the discard; the guard rides INTO the blocking
        // closure so `await_quiescent` cannot return until the venue has
        // actually answered.
        let Some(guard) = halt.enter(match &cmd.action {
            Action::Place(p) => Some(p),
            Action::Cancel { .. } | Action::SweepAndVerify => None,
        }) else {
            eprintln!("[exec] {venue:?}: HALTED mid-dispatch — place discarded, not sent");
            continue;
        };
        let res = tokio::task::spawn_blocking(move || {
            let _g = guard;
            match &cmd.action {
                Action::Place(p) => sink.place(p).map(Some),
                Action::Cancel { req, .. } => sink.cancel(req).map(|_| None),
                // handled above, before this dispatch
                Action::SweepAndVerify => Ok(None),
            }
        })
        .await;
        // Whatever the venue did, the engine is TOLD. Both arms below report,
        // and the failure arm is the one that used to end here.
        //
        // `unreadable` is the narrower question the recovery below turns on: did
        // this failure leave us UNABLE TO SAY whether the venue took the order?
        // Not the same as "it failed" — see `place_answer_was_lost`.
        let mut unreadable = false;
        let failure = match res {
            Ok(Ok(venue_oid)) => {
                st2.sent.fetch_add(1, Ordering::Relaxed);
                match (venue_oid, &placing) {
                    (Some(vid), Some(p)) => {
                        eprintln!("[exec] {venue:?} placed {} -> {vid}", p.client_order_id);
                        claimed.insert(vid.clone());
                        tell_engine(
                            &acks,
                            serde_json::json!({
                                "kind": "order_ack",
                                "venue": venue.as_str(),
                                "market_id": p.market,
                                "order_id": p.client_order_id,
                                "venue_order_id": vid,
                                "ts_local_ns": now_ns(),
                            }),
                        )
                        .await;
                    }
                    _ => eprintln!("[exec] {venue:?} cancelled"),
                }
                None
            }
            Ok(Err(e)) => {
                st2.failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("[exec] {venue:?} FAILED: {e}");
                unreadable = place_answer_was_lost(&e);
                Some(e.to_string())
            }
            Err(e) => {
                st2.failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("[exec] {venue:?} task panicked: {e}");
                // A panic is our bug, not a venue answer. It says nothing about
                // whether the order landed, so it is not licence to go looking
                // for one.
                Some(format!("executor task panicked: {e}"))
            }
        };
        if let Some((by, market, attempt)) = &cancelling {
            tell_engine(&acks, cancel_result(venue, by, market, *attempt, failure.as_deref()))
                .await;
        }
        // A place whose answer was LOST may still have reached the venue. Kalshi
        // has recovered from this since the first live smoke left an order
        // resting (2026-07-27, `gateway/kalshi.rs`); PM-US could not, because it
        // carries no client_order_id to look itself up by. `recover_place` is
        // that lookup by the only other means the venue offers — and it is why
        // the gate is `unreadable` and NOT "the place failed". A REJECTED place
        // is routine (a replay of one 3.7-day shadow produced ~150 on PM-US
        // alone) and definitive: nothing of ours rests, so a search that matches
        // on market and size could only find somebody ELSE's order, on a shared
        // account, and adopt it.
        //
        // Skipped while halting, for the reason the cancel escalation stands
        // down while killed: the sweep is already cancelling EVERYTHING and
        // proving it, which reaches this order without needing its id, and the
        // read would compete with the only evidence the halt accepts.
        let (Some(p), true, false) = (&placing, unreadable, halt.is_on()) else { continue };
        let (req, mine) = (p.clone(), claimed.clone());
        match tokio::task::spawn_blocking(move || sink2.recover_place(&req, &mine)).await {
            Ok(Ok(Some(vid))) => {
                st2.recovered.fetch_add(1, Ordering::Relaxed);
                claimed.insert(vid.clone());
                // NOT "is RESTING". Kalshi's recovery searches its whole order
                // history, so an adopted order may already be EXECUTED — which
                // is the case an operator reading this line most needs to know
                // about, because it is the one where a duplicate hedge may be on
                // the book. What every recovery does establish is that the venue
                // HAS the order; whether it is still live is a separate read.
                eprintln!(
                    "[exec] {venue:?}: the place of {} ({} @{}) FAILED but the venue HAS \
                     order {vid} — it took the place and we could not read the answer. \
                     Adopting it, so it can be cancelled if it is still resting and its \
                     fills attributed if it is not.",
                    p.client_order_id, p.market, p.price
                );
                tell_engine(
                    &acks,
                    serde_json::json!({
                        "kind": "order_ack",
                        "venue": venue.as_str(),
                        "market_id": p.market,
                        "order_id": p.client_order_id,
                        "venue_order_id": vid,
                        "ts_local_ns": now_ns(),
                    }),
                )
                .await;
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => eprintln!(
                "[exec] {venue:?}: {} FAILED and the recovery read could not settle whether \
                 the venue took it ({e}) — if it did, this process cannot address that \
                 order. CHECK THE VENUE.",
                p.client_order_id
            ),
            Err(e) => eprintln!("[exec] {venue:?}: recovery task panicked: {e}"),
        }
    }
}

/// The effects boundary had ZERO tests. That is why the 15:40 leak shipped: the
/// bug was not in a decision, it was in what the last mile did with one.
///
/// Local sink doubles rather than `arb-venue`'s `MockTransport`, which cannot
/// return `Err` and cannot block.
#[cfg(test)]
mod tests {
    use super::*;
    use arb_venue::gateway::{CancelBy, Side, Tif};
    use arb_venue::VenueError;
    use std::sync::Mutex;

    /// Records every call. `resting` is what the venue would report; a place
    /// adds to it, so "a place slipped out" is observable as a dirty book.
    #[derive(Default)]
    struct Recorder {
        placed: Mutex<Vec<String>>,
        cancelled: Mutex<Vec<String>>,
        sweeps: Mutex<u32>,
        resting: Mutex<Vec<String>>,
        /// Never goes clean, whatever we do to it.
        wedged: bool,
        /// Blocking delay on every resting-list read.
        stall: Option<Duration>,
        /// The venue REFUSES every cancel — a 502, a reset, a rate limit the
        /// shaper did not catch.
        refuse_cancels: bool,
        /// Places refused from the Nth on, 1-based (0 = never), with this
        /// error. The default is the lost-response shape (a reqwest timeout);
        /// a `Status` is the venue REFUSING, which is a different thing
        /// entirely. `recover` is what a resting-order read then finds.
        refuse_places_from: usize,
        place_err: Option<VenueError>,
        recover: Option<String>,
        /// Latched from INSIDE the place — SIGTERM arriving while this very
        /// call is on the wire.
        latch: Option<Arc<Halt>>,
    }

    impl OrderSink for Recorder {
        fn place(&self, r: &PlaceRequest) -> Result<String, VenueError> {
            let n = {
                let mut p = self.placed.lock().unwrap();
                p.push(r.client_order_id.clone());
                p.len()
            };
            if self.refuse_places_from > 0 && n >= self.refuse_places_from {
                if let Some(h) = &self.latch {
                    h.begin();
                }
                return Err(self.place_err.clone().unwrap_or(VenueError::Transport(
                    "connection closed before message completed".into(),
                )));
            }
            self.resting.lock().unwrap().push(r.client_order_id.clone());
            Ok(format!("venue-{}", r.client_order_id))
        }
        fn cancel(&self, r: &CancelRequest) -> Result<(), VenueError> {
            // Matched locally rather than via an accessor: `CancelBy` exists to
            // keep OUR id space and the venue's apart, and a helper that returns
            // the bare string erases exactly that distinction (B1 removed it).
            let id = match &r.by {
                CancelBy::VenueId(s) | CancelBy::ClientId(s) => s.clone(),
            };
            self.cancelled.lock().unwrap().push(id);
            if self.refuse_cancels {
                return Err(VenueError::Status {
                    endpoint: "test cancel",
                    status: 502,
                    body: "bad gateway".into(),
                });
            }
            Ok(())
        }
        /// Whatever the lost response left behind — unless this process has
        /// already claimed it, which is the scope rule the real PM-US
        /// implementation enforces (`PmusGateway::recover_place`).
        fn recover_place(
            &self,
            _req: &PlaceRequest,
            claimed: &HashSet<String>,
        ) -> Result<Option<String>, VenueError> {
            Ok(self.recover.clone().filter(|v| !claimed.contains(v)))
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            *self.sweeps.lock().unwrap() += 1;
            if !self.wedged {
                self.resting.lock().unwrap().clear();
            }
            Ok(())
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            if let Some(d) = self.stall {
                std::thread::sleep(d);
            }
            Ok(self.resting.lock().unwrap().clone())
        }
    }

    fn place(oid: &str) -> ExecCmd {
        ExecCmd {
            t_read: Instant::now(),
            action: Action::Place(PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Bid,
                price: "0.4200".into(),
                qty: 5,
                tif: Tif::Gtc,
                post_only: true,
                client_order_id: oid.into(),
            }),
        }
    }

    /// A hedge / take-take leg: IOC, not post-only. It cannot rest, and it is
    /// the leg that closes something already half-done.
    fn taker_place(oid: &str) -> ExecCmd {
        ExecCmd {
            t_read: Instant::now(),
            action: Action::Place(PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Ask,
                price: "0.3000".into(),
                qty: 5,
                tif: Tif::Ioc,
                post_only: false,
                client_order_id: oid.into(),
            }),
        }
    }

    /// A venue where a place takes time to LAND, as a real one does: the HTTP
    /// call is on the wire for `land_after`, and only then is the order resting
    /// and visible to the resting list. (Ported from the review probe.)
    struct LateLander {
        land_after: Duration,
        resting: Mutex<Vec<String>>,
    }

    impl OrderSink for LateLander {
        fn place(&self, r: &PlaceRequest) -> Result<String, VenueError> {
            std::thread::sleep(self.land_after);
            self.resting.lock().unwrap().push(r.client_order_id.clone());
            Ok(format!("venue-{}", r.client_order_id))
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            Ok(())
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            self.resting.lock().unwrap().clear();
            Ok(())
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            Ok(self.resting.lock().unwrap().clone())
        }
    }

    fn cancel(oid: &str) -> ExecCmd {
        ExecCmd {
            t_read: Instant::now(),
            action: Action::Cancel {
                req: CancelRequest {
                    by: CancelBy::VenueId(oid.into()),
                    market_slug: Some("KXTEST".into()),
                },
                attempt: 1,
            },
        }
    }

    fn stats() -> Arc<ExecStats> {
        Arc::new(ExecStats {
            hop: Hist::new(),
            placed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            recovered: AtomicU64::new(0),
        })
    }

    /// Drain a queue of commands through one executor to completion. `rate` 0
    /// disables the token bucket, as bench does.
    async fn drain(sink: Arc<Recorder>, halt: Arc<Halt>, cmds: Vec<ExecCmd>) -> Arc<ExecStats> {
        let (tx, rx) = mpsc::channel::<ExecCmd>(1024);
        for c in cmds {
            tx.try_send(c).expect("test queue fits");
        }
        drop(tx); // the loop ends when the channel closes
        let st = stats();
        run_executor(Venue::Kalshi, 0.0, Some(sink), rx, st.clone(), halt, None).await;
        st
    }

    /// Drain commands and collect everything the executor told the ENGINE.
    ///
    /// That channel is the fix: until 2026-07-29 the executor's only
    /// engine-bound output was an `order_ack` on a SUCCESSFUL place, so every
    /// other outcome — a refused cancel, a place whose answer was lost — was
    /// counted, logged and dropped.
    async fn drain_reporting(
        venue: Venue,
        sink: Arc<Recorder>,
        cmds: Vec<ExecCmd>,
    ) -> (Arc<ExecStats>, Vec<serde_json::Value>) {
        let (tx, rx) = mpsc::channel::<ExecCmd>(1024);
        for c in cmds {
            tx.try_send(c).expect("test queue fits");
        }
        drop(tx);
        let (atx, mut arx) = mpsc::channel::<crate::feed::FeedMsg>(64);
        let st = stats();
        run_executor(venue, 0.0, Some(sink), rx, st.clone(), Arc::new(Halt::default()), Some(atx))
            .await;
        let mut told = Vec::new();
        while let Ok(m) = arx.try_recv() {
            told.push(serde_json::from_str(&m.line).expect("the engine parses these"));
        }
        (st, told)
    }

    /// FINDING #0, first half. At 15:40:13 the sweep began; at 15:40:14 an
    /// executor that nothing had told to stop placed a real maker order from its
    /// queue. A place queued when the halt latches must never reach the venue.
    #[tokio::test]
    async fn a_place_queued_at_shutdown_is_discarded_not_sent() {
        let sink = Arc::new(Recorder::default());
        let halt = Arc::new(Halt::default());
        halt.begin();
        let st = drain(sink.clone(), halt.clone(), vec![place("m1"), place("m2"), place("m3")])
            .await;

        assert!(sink.placed.lock().unwrap().is_empty(), "NOTHING may reach the venue");
        assert!(sink.resting.lock().unwrap().is_empty(), "and the book stays clean");
        assert_eq!(halt.discarded(), 3, "every discard is counted, not silent");
        assert_eq!(st.sent.load(Ordering::Relaxed), 0);
    }

    /// The control: without the halt those same places DO go out. Without this
    /// the test above would pass on a broken executor that places nothing.
    #[tokio::test]
    async fn the_same_places_reach_the_venue_when_not_halted() {
        let sink = Arc::new(Recorder::default());
        let halt = Arc::new(Halt::default());
        let st = drain(sink.clone(), halt.clone(), vec![place("m1"), place("m2")]).await;
        assert_eq!(*sink.placed.lock().unwrap(), vec!["m1", "m2"]);
        assert_eq!(halt.discarded(), 0);
        assert_eq!(st.sent.load(Ordering::Relaxed), 2);
    }

    /// A halt that blocked cancels could never get clean. Cancel and
    /// sweep-and-verify are the two verbs that must survive it.
    #[tokio::test]
    async fn cancels_and_sweeps_still_reach_the_venue_after_a_halt() {
        let sink = Arc::new(Recorder::default());
        let halt = Arc::new(Halt::default());
        halt.begin();
        drain(
            sink.clone(),
            halt.clone(),
            vec![
                cancel("v1"),
                ExecCmd { t_read: Instant::now(), action: Action::SweepAndVerify },
            ],
        )
        .await;
        assert_eq!(*sink.cancelled.lock().unwrap(), vec!["v1"], "the cancel went out");
        assert!(*sink.sweeps.lock().unwrap() >= 1, "and so did the sweep");
    }

    /// The narrow race the early check cannot cover: the latch flips after the
    /// command is dequeued. `Halt::enter` is the interlock, and it must refuse.
    #[test]
    fn a_place_that_races_the_latch_is_still_refused() {
        let halt = Arc::new(Halt::default());
        let maker = PlaceRequest {
            market: "KXTEST".into(),
            side: Side::Bid,
            price: "0.4200".into(),
            qty: 5,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: "m1".into(),
        };
        let g = halt.enter(Some(&maker)).expect("not halted yet");
        assert_eq!(halt.discarded(), 0);
        halt.begin();
        assert!(halt.enter(Some(&maker)).is_none(), "no place after the latch, ever");
        assert_eq!(halt.discarded(), 1);
        assert!(halt.enter(None).is_some(), "a cancel still may");
        drop(g);
    }

    /// The verify must not race an in-flight place: at 15:40 the sweep ran while
    /// a place was on the wire, so the resting list it read was already stale.
    #[tokio::test]
    async fn quiescence_waits_for_an_inflight_place_and_reports_one_that_wedges() {
        let halt = Arc::new(Halt::default());
        let maker = PlaceRequest {
            market: "KXTEST".into(),
            side: Side::Bid,
            price: "0.4200".into(),
            qty: 5,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: "m1".into(),
        };
        assert_eq!(halt.await_quiescent(Duration::from_millis(50)).await, 0, "idle");

        let g = halt.enter(Some(&maker)).expect("slot");
        let h2 = halt.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            drop(g);
            let _ = &h2;
        });
        assert_eq!(halt.await_quiescent(Duration::from_secs(5)).await, 0, "waited it out");

        let _wedged = halt.enter(Some(&maker)).expect("slot");
        assert_eq!(
            halt.await_quiescent(Duration::from_millis(60)).await,
            1,
            "a place that never returns is REPORTED, not waited on forever"
        );
    }

    /// D1b, the other half: cancels must NOT be counted in flight. The engine
    /// emits cancels throughout a halt — that is how the book gets clean — so
    /// counting them would burn the whole quiescence budget every shutdown and
    /// make the one signal that matters an every-time warning. A cancel racing
    /// the verify is harmless: it can only ever REMOVE an order.
    #[tokio::test]
    async fn an_inflight_cancel_does_not_hold_up_quiescence() {
        let halt = Arc::new(Halt::default());
        let _cancel_in_flight = halt.enter(None).expect("a cancel always proceeds");
        assert_eq!(
            halt.await_quiescent(Duration::from_secs(5)).await,
            0,
            "quiescence is about PLACES; a cancel in flight must not starve it"
        );
    }

    /// FINDING #0, third half: `exit(0)` regardless of outcome. The Err arm
    /// logged "NOT CLEAN" and fell into the same unconditional exit, so systemd
    /// recorded a clean stop while a real order rested. Tested without ending
    /// the test process, which is the reason the decision is not inside `exit`.
    #[tokio::test]
    async fn a_venue_that_never_goes_clean_produces_a_nonzero_exit() {
        let wedged = Arc::new(Recorder { wedged: true, ..Default::default() });
        wedged.resting.lock().unwrap().push("66e1c799".into());
        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, wedged.clone());

        let out = shutdown_sweep(sinks, fast_policy(), Duration::from_secs(10)).await;
        assert_eq!(out.exit_code(), EXIT_ORDERS_LEFT_RESTING);
        assert_ne!(out.exit_code(), 0, "systemd must record a FAILURE");
        let report = out.report().join("\n");
        assert!(report.contains("NOT CLEAN"), "{report}");
        assert!(report.contains("66e1c799"), "names the orphan: {report}");
        assert!(report.contains("CANCEL BY HAND"), "tells the human what to do: {report}");
    }

    #[tokio::test]
    async fn a_clean_venue_exits_zero_and_says_it_proved_it() {
        let clean = Arc::new(Recorder::default());
        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, clean);
        let out = shutdown_sweep(sinks, fast_policy(), Duration::from_secs(10)).await;
        assert_eq!(out.exit_code(), 0);
        assert!(out.report().join("\n").contains("PROVEN clean"));
    }

    /// FINDING #0, fourth half: the sweeps were sequential against a 90s
    /// `TimeoutStopSec`, so one hanging venue could burn the whole budget and
    /// SIGKILL would leave the OTHER venue's orders resting too.
    #[tokio::test]
    async fn a_hanging_venue_does_not_cost_the_other_its_cancel() {
        let hung = Arc::new(Recorder {
            wedged: true,
            stall: Some(Duration::from_millis(400)),
            ..Default::default()
        });
        hung.resting.lock().unwrap().push("stuck".into());
        let live = Arc::new(Recorder::default());
        live.resting.lock().unwrap().push("m9".into());
        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, hung.clone());
        sinks.insert(Venue::PolymarketUs, live.clone());

        let t0 = Instant::now();
        let out = shutdown_sweep(sinks, fast_policy(), Duration::from_millis(300)).await;

        // Sequentially the hung venue would burn its whole 1s budget FIRST and
        // the aggregate deadline would not exist, so this returns in ~1.2s and
        // the second venue's cancel is at the mercy of SIGKILL. Concurrently it
        // returns at the 300ms deadline with the live venue already swept.
        assert!(
            t0.elapsed() < Duration::from_millis(800),
            "one dead venue must not serialise the other: {:?}",
            t0.elapsed()
        );
        assert_eq!(out.clean, vec![Venue::PolymarketUs], "the live venue got swept");
        assert!(live.resting.lock().unwrap().is_empty(), "and its book is empty");
        assert_eq!(out.unclean.len(), 1);
        assert_eq!(out.unclean[0].0, Venue::Kalshi);
        assert_ne!(out.exit_code(), 0, "an unproven venue is still a failure");
    }

    /// Silence is not proof: a venue that never answers before the deadline is
    /// `unclean`, not absent from the report.
    #[tokio::test]
    async fn a_venue_that_never_answers_is_reported_unclean() {
        let hung = Arc::new(Recorder {
            wedged: true,
            stall: Some(Duration::from_millis(400)),
            ..Default::default()
        });
        hung.resting.lock().unwrap().push("stuck".into());
        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, hung);
        let out = shutdown_sweep(sinks, fast_policy(), Duration::from_millis(100)).await;
        assert_eq!(out.unclean.len(), 1);
        assert!(out.unclean[0].1.contains("NOT proven clean"), "{:?}", out.unclean);
        assert_ne!(out.exit_code(), 0);
    }

    /// Production shape, no sleeps, and a budget short enough that a stalled
    /// sink's blocking thread — which cannot be cancelled, so the test runtime
    /// waits for it on drop — does not dominate the suite.
    fn fast_policy() -> SweepPolicy {
        SweepPolicy {
            rounds: 3,
            polls_per_round: 2,
            poll_delay: Duration::ZERO,
            confirm_empty_reads: 2,
            budget: Duration::from_secs(1),
        }
    }

    /// The WAL's crash-stop keeps its own code (70) when the book is provably
    /// clean — the intent, "a WAL hole must stop trading", is preserved — but an
    /// unproven book overrides it, because 17 must be the only thing a human has
    /// to recognise. Same for a panic's 101.
    ///
    /// This is the tested half of the WAL/panic wiring; the other half ends in
    /// `std::process::exit`, which no in-process test can survive.
    #[test]
    fn an_abnormal_stops_own_code_survives_a_clean_book_but_not_a_dirty_one() {
        let clean = ShutdownOutcome { clean: vec![Venue::Kalshi], ..Default::default() };
        let dirty = ShutdownOutcome {
            unclean: vec![(Venue::Kalshi, "1 order(s) SURVIVED".into())],
            ..Default::default()
        };
        assert_eq!(halt_exit_code(70, &clean), 70, "WAL hole, book clean");
        assert_eq!(halt_exit_code(70, &dirty), EXIT_ORDERS_LEFT_RESTING, "orders win");
        assert_eq!(halt_exit_code(101, &clean), 101, "panic, book clean");
        assert_eq!(halt_exit_code(101, &dirty), EXIT_ORDERS_LEFT_RESTING);
        assert_ne!(EXIT_ORDERS_LEFT_RESTING, 0, "never a clean stop");
    }

    /// A book no venue could CONFIRM must not exit 0.
    ///
    /// This deployment gives exit code exactly one consequence: `Restart=no`,
    /// no `OnFailure=`, and `scripts/freshness_check.sh` pages on
    /// `systemctl is-failed` and nothing else — its own comment calls that
    /// "the only RUNTIME condition that means something is wrong". A zero here
    /// would leave the unit `inactive/Result=success`, byte-identical to a
    /// deliberate disarm, with the evidence only in a journal nobody is told to
    /// read. The one automated alarm in the system would be silent for the one
    /// outcome nobody has ever observed.
    #[test]
    fn a_book_no_venue_could_confirm_still_fails_the_unit() {
        let unconfirmed = ShutdownOutcome {
            clean: vec![Venue::Kalshi],
            unconfirmed: vec![(
                Venue::PolymarketUs,
                "book could NOT be proven clean (last venue error: cannot list resting \
                 orders: pmus:open_orders: parse error: missing field `orders` — body \
                 was: {})"
                    .into(),
            )],
            ..Default::default()
        };
        assert_eq!(
            unconfirmed.exit_code(),
            EXIT_BOOK_UNCONFIRMED,
            "an unconfirmed book must page, not read as a clean stop"
        );
        assert_ne!(EXIT_BOOK_UNCONFIRMED, 0);
        assert_ne!(
            EXIT_BOOK_UNCONFIRMED, EXIT_ORDERS_LEFT_RESTING,
            "a human must never have to disambiguate 'unread' from 'leaked'"
        );

        // ...and a real leak still wins when both apply.
        let both = ShutdownOutcome {
            unclean: vec![(Venue::Kalshi, "1 order(s) SURVIVED".into())],
            unconfirmed: vec![(Venue::PolymarketUs, "unreadable".into())],
            ..Default::default()
        };
        assert_eq!(both.exit_code(), EXIT_ORDERS_LEFT_RESTING, "the graver verdict wins");
        assert_eq!(halt_exit_code(70, &unconfirmed), EXIT_BOOK_UNCONFIRMED);
    }

    /// ...and it says so where a human will see it: `systemctl status` shows the
    /// last log lines and the exit code, so the report has to carry the banner.
    /// It must NOT claim orders may be resting — nothing was seen resting, and
    /// an alarm that overstates on every shutdown is one nobody reads.
    #[test]
    fn an_unconfirmed_book_is_bannered_without_claiming_a_leak() {
        let out = ShutdownOutcome {
            clean: vec![Venue::Kalshi],
            unconfirmed: vec![(Venue::PolymarketUs, "body was: {}".into())],
            ..Default::default()
        };
        let r = out.report().join("\n");
        assert!(r.contains("###"), "the loud mechanism, not just loud wording: {r}");
        assert!(r.contains("BOOK NOT CONFIRMED"), "{r}");
        assert!(!r.contains("ORDERS MAY STILL BE RESTING"), "must not overstate: {r}");
        assert!(r.contains("PIN IT"), "tells the reader what the body is for: {r}");
        assert!(r.contains("body was: {}"), "carries the raw body: {r}");
        assert!(r.contains(&format!("exiting {EXIT_BOOK_UNCONFIRMED}")), "{r}");

        // a real leak keeps the stronger banner
        let leak = ShutdownOutcome {
            unclean: vec![(Venue::Kalshi, "1 order(s) SURVIVED".into())],
            ..Default::default()
        };
        assert!(leak.report().join("\n").contains("ORDERS MAY STILL BE RESTING"));
    }

    /// The report has to survive being read at 3am in `systemctl status`.
    /// **D1 — the 15:40 leak, one venue-latency away from repeating.** Ported
    /// from the adversarial review, where it passed.
    ///
    /// A place is already on the wire when shutdown begins. Quiescence gives up
    /// and reports it; the sweep then reads a resting list that cannot yet show
    /// an order the venue has not finished accepting, and calls that PROOF. The
    /// order lands right after. Same ending as 15:40, through the new code.
    ///
    /// Two things now make it non-zero: `places_inflight_at_verify` feeds the
    /// exit code instead of a warning line, and an empty read needs confirming.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_place_in_flight_at_verify_time_is_never_a_clean_exit() {
        let venue: Arc<LateLander> = Arc::new(LateLander {
            land_after: Duration::from_millis(900),
            resting: Mutex::new(Vec::new()),
        });
        let halt = Arc::new(Halt::default());
        let (tx, rx) = mpsc::channel::<ExecCmd>(8);
        let sink: Arc<dyn OrderSink> = venue.clone();
        tokio::spawn(run_executor(
            Venue::Kalshi,
            0.0,
            Some(sink.clone()),
            rx,
            stats(),
            halt.clone(),
            None,
        ));
        // The place the engine emitted a moment before SIGTERM.
        tx.send(place("m1785257819053")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await; // it is on the wire

        // SIGTERM: exactly what halt_and_sweep does, quiescence shortened.
        halt.begin();
        let stuck = halt.await_quiescent(Duration::from_millis(100)).await;
        assert_eq!(stuck, 1, "the place is still on the wire — the halt knows it");

        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, sink);
        let mut out = shutdown_sweep(sinks, fast_policy(), Duration::from_secs(5)).await;
        out.discarded_places = halt.discarded();
        out.places_inflight_at_verify = stuck;

        assert_eq!(
            out.exit_code(),
            EXIT_ORDERS_LEFT_RESTING,
            "a verify that ran while a place was on the wire proved NOTHING"
        );
        let report = out.report().join("\n");
        assert!(report.contains("STILL ON THE WIRE"), "and says why: {report}");
        assert!(!report.contains("PROVEN clean"), "it must not claim proof: {report}");
        drop(tx);
    }

    /// **D3 — the SIGTERM path must claim the exit.** Ported from the review.
    ///
    /// `HALTING` is what makes "only one halt owns the exit" true. When the
    /// SIGTERM path did not claim it, `wal.rs`'s dedup was inert and a WAL
    /// crash-stop or a `book_basket` ledger failure could start a SECOND
    /// concurrent sweep: two sets of cancel-alls against a limiter that errors
    /// rather than waits, and two racing exits with different verdicts.
    ///
    /// Serial by necessity — `HALTING` and `HALT` are process-globals, so this
    /// test owns them and asserts on the whole process's latch.
    #[tokio::test]
    async fn the_sigterm_halt_claims_the_exit_so_no_second_halt_can_start() {
        let first = halt_and_sweep("SIGTERM (test)").await;
        assert!(halt().is_on(), "the effects latch is on");
        assert!(halting(), "and the EXIT is claimed, so wal.rs's dedup is live");
        assert!(!first.already_halting, "the first halt did the work");

        // What a WAL overflow arriving mid-shutdown now does.
        let second = halt_and_sweep("WAL hole (test)").await;
        assert!(second.already_halting, "a second halt must not sweep again");
        assert!(second.clean.is_empty() && second.unclean.is_empty(), "it swept nothing");
        assert!(
            second.report().join("\n").contains("verdict stands, not ours"),
            "and it says the winner owns the verdict: {:?}",
            second.report()
        );
        // Nothing was armed in this test, so the first halt had nothing to sweep
        // and that is honest — but it must not be confused with proof.
        assert_eq!(first.exit_code(), 0, "never armed => nothing could be resting");
    }

    /// **D6 — a discarded HEDGE is a naked leg and must never exit 0.**
    /// Ported from the review.
    ///
    /// A hedge is `Action::Place` with IOC/not-post-only. The latch refuses it —
    /// correctly; reopening the latch would also let take-take ENTRIES fire on
    /// the way out — but the exit must then say a filled leg may be unhedged.
    #[tokio::test]
    async fn a_discarded_hedge_is_a_naked_leg_and_exits_nonzero() {
        let sink = Arc::new(Recorder::default());
        let halt = Arc::new(Halt::default());
        halt.begin();
        drain(sink.clone(), halt.clone(), vec![taker_place("h7"), place("m1")]).await;

        assert!(sink.placed.lock().unwrap().is_empty(), "the latch still holds");
        assert_eq!(halt.discarded(), 2);
        assert_eq!(halt.discarded_takers(), 1, "only the hedge is a naked-leg risk");

        let out = ShutdownOutcome {
            discarded_places: halt.discarded(),
            discarded_takers: halt.discarded_takers(),
            naked_risk_ids: halt.naked_risk_ids(),
            ..Default::default()
        };
        assert_eq!(out.exit_code(), EXIT_ORDERS_LEFT_RESTING, "a naked leg is not a clean stop");
        let r = out.report().join("\n");
        assert!(r.contains("TAKER place(s) DISCARDED"), "{r}");
        assert!(r.contains("NAKED"), "{r}");
        assert!(r.contains("h7"), "names the hedge: {r}");
        assert!(r.contains("h = hedge"), "tells the human how to read the id: {r}");
        assert!(r.contains("PROFITABLE"), "and that the hedge timer will not save it: {r}");
    }

    /// A take-take leg is the same hazard by a different route: the two legs go
    /// to two different executors, so the latch can catch the second one after
    /// the first is already on the wire. `Action::Place` carries no tag to tell
    /// it from a hedge, and it should not need one — both are naked legs.
    #[tokio::test]
    async fn a_discarded_take_take_leg_counts_as_a_naked_leg_too() {
        let sink = Arc::new(Recorder::default());
        let halt = Arc::new(Halt::default());
        halt.begin();
        drain(sink.clone(), halt.clone(), vec![taker_place("t42")]).await;
        assert_eq!(halt.discarded_takers(), 1);
        let out = ShutdownOutcome {
            discarded_places: 1,
            discarded_takers: 1,
            naked_risk_ids: halt.naked_risk_ids(),
            ..Default::default()
        };
        assert_ne!(out.exit_code(), 0);
        assert!(out.report().join("\n").contains("t42"));
    }

    /// **D5 — the panic path must not have a tail outside its stated budget.**
    /// The review proved a plain `drop(rt)` waits for a blocking venue call that
    /// an aborted `JoinSet` cannot stop, and that `h.join()` above it inherits
    /// that wait. Drives the REAL function, not the idiom.
    ///
    /// A 30s-stalled venue against a 300ms aggregate deadline: the whole thing
    /// must come back in about `deadline + RUNTIME_ABANDON_AFTER`, not 30s.
    #[test]
    fn the_panic_paths_private_runtime_abandons_a_stuck_venue_call() {
        let hung = Arc::new(Recorder {
            wedged: true,
            stall: Some(Duration::from_secs(30)),
            ..Default::default()
        });
        hung.resting.lock().unwrap().push("stuck".into());
        let mut sinks: HashMap<Venue, Arc<dyn OrderSink>> = HashMap::new();
        sinks.insert(Venue::Kalshi, hung);

        // No ambient runtime: exactly the context halt_and_exit_blocking creates.
        let t0 = Instant::now();
        let out = std::thread::spawn(move || {
            sweep_on_private_runtime(Some(sinks), fast_policy(), Duration::from_millis(300))
        })
        .join()
        .expect("thread")
        .expect("runtime built");
        let elapsed = t0.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "a 30s venue call must be ABANDONED, not waited on: {elapsed:?}"
        );
        assert_ne!(out.exit_code(), 0, "and the venue it could not read is not clean");
        assert!(
            RUNTIME_ABANDON_AFTER < HALT_LOSER_WAIT,
            "the abandon bound has to fit inside every wait above it"
        );
    }

    /// D4: the loser's wait has to end well inside `TimeoutStopUSec=1min 30s`,
    /// or SIGKILL arbitrates mid-sweep — which is the bug this file closes.
    #[test]
    fn the_halt_losers_wait_fits_inside_the_systemd_stop_timeout() {
        assert!(
            HALT_LOSER_WAIT < Duration::from_secs(90),
            "{HALT_LOSER_WAIT:?} must be < TimeoutStopUSec"
        );
        assert!(QUIESCE_BUDGET + SWEEP_BUDGET < Duration::from_secs(90));
    }

    /// **DEFECT 1, at the boundary.** A cancel the venue refuses must reach the
    /// engine as a refusal.
    ///
    /// `Ok(Err(e))` was one counter and one log line — the executor's only
    /// engine-bound output was an `order_ack` on a SUCCESSFUL place — so a
    /// refused cancel and a completed one were indistinguishable to everything
    /// upstream. The engine retired the obligation on `try_send`, which had
    /// already returned true.
    #[tokio::test]
    async fn a_cancel_the_venue_refuses_is_reported_to_the_engine_as_refused() {
        let refuses = Arc::new(Recorder { refuse_cancels: true, ..Default::default() });
        let (st, told) =
            drain_reporting(Venue::PolymarketUs, refuses.clone(), vec![cancel("v1")]).await;

        assert_eq!(*refuses.cancelled.lock().unwrap(), vec!["v1"], "it did reach the venue");
        assert_eq!(st.failed.load(Ordering::Relaxed), 1);
        assert_eq!(told.len(), 1, "and the engine is TOLD, which is the whole fix");
        assert_eq!(told[0]["kind"], "cancel_result");
        assert_eq!(told[0]["ok"], false, "the venue did NOT cancel it");
        assert_eq!(
            told[0]["venue_order_id"], "v1",
            "reported in the id space it was addressed in, so the engine can name the order"
        );
        assert_eq!(told[0]["venue"], "polymarket_us");
        assert_eq!(told[0]["market_id"], "KXTEST", "PM-US needs the slug to try again");
        assert!(
            told[0]["error"].as_str().unwrap_or_default().contains("502"),
            "carrying the venue's own reason: {}",
            told[0]
        );
    }

    /// The control, and the other half of the contract: a cancel the venue
    /// ACCEPTS is reported too. Without this the engine could never retire an
    /// obligation at all and every cancel would be retried until it ran out of
    /// tries.
    #[tokio::test]
    async fn a_cancel_the_venue_accepts_is_reported_as_done() {
        let sink = Arc::new(Recorder::default());
        let (st, told) = drain_reporting(Venue::Kalshi, sink, vec![cancel("v1")]).await;
        assert_eq!(st.failed.load(Ordering::Relaxed), 0);
        assert_eq!(told.len(), 1);
        assert_eq!(told[0]["kind"], "cancel_result");
        assert_eq!(told[0]["ok"], true);
        assert_eq!(told[0]["venue_order_id"], "v1");
        assert!(told[0]["error"].is_null());
    }

    /// A sweep the VENUE refused must reach the engine as a refusal.
    ///
    /// The same defect as the cancel above, one command up: the executor awaits
    /// `cancel_all_and_verify` and was the only thing in the process that knew
    /// the answer, and it kept it — `st.failed` and one `KILL SWEEP FAILED`
    /// line. The engine had already retired the obligation on `try_send`, so
    /// the halt sat over a book nothing had proven. Both venues did exactly
    /// this inside four minutes on 2026-07-29.
    ///
    /// Asserted on the WIRE VALUE the engine parses, not on a shape this test
    /// builds for itself: `ok` reading false is what keeps the entry owed.
    #[tokio::test]
    async fn a_sweep_the_venue_could_not_prove_is_reported_to_the_engine_as_failed() {
        let wedged = Arc::new(Recorder { wedged: true, ..Default::default() });
        wedged.resting.lock().unwrap().push("66e1c799".into());
        let (st, told) = drain_reporting(
            Venue::Kalshi,
            wedged.clone(),
            vec![ExecCmd { t_read: Instant::now(), action: Action::SweepAndVerify }],
        )
        .await;

        assert!(*wedged.sweeps.lock().unwrap() >= 1, "the sweep did reach the venue");
        assert_eq!(st.failed.load(Ordering::Relaxed), 1);
        assert_eq!(told.len(), 1, "and the engine is TOLD, which is the whole fix");
        assert_eq!(told[0]["kind"], "sweep_result");
        assert_eq!(told[0]["venue"], "kalshi");
        assert_eq!(told[0]["ok"], false, "the book was NOT proven clean");
        assert!(
            told[0]["error"].as_str().unwrap_or_default().contains("SURVIVED"),
            "carrying the venue's own reason: {}",
            told[0]
        );
    }

    /// The other half: a sweep the venue PROVED is reported too. Without it the
    /// engine could never retire the obligation and would re-sweep a clean book
    /// on the backoff for ever.
    #[tokio::test]
    async fn a_sweep_the_venue_proved_clean_is_reported_as_done() {
        let sink = Arc::new(Recorder::default());
        let (st, told) = drain_reporting(
            Venue::PolymarketUs,
            sink,
            vec![ExecCmd { t_read: Instant::now(), action: Action::SweepAndVerify }],
        )
        .await;
        assert_eq!(st.failed.load(Ordering::Relaxed), 0);
        assert_eq!(told.len(), 1);
        assert_eq!(told[0]["kind"], "sweep_result");
        assert_eq!(told[0]["ok"], true);
        assert!(told[0]["error"].is_null());
    }

    /// A DRY-RUN executor answers the sweep it drops.
    ///
    /// Nothing of ours can rest at a venue this process holds no sink for, so
    /// the answer is `ok` — and it has to be sent, because the engine now keeps
    /// the obligation until something proves it. `spawn_executors` makes a
    /// channel for all three venues however few are armed, so without this
    /// every armed session would owe an INTL sweep for ever and the
    /// `sweeps_owed` gauge would be useless from its first halt.
    #[tokio::test]
    async fn a_dry_run_executor_answers_the_sweep_it_drops() {
        let (tx, rx) = mpsc::channel::<ExecCmd>(8);
        tx.try_send(ExecCmd { t_read: Instant::now(), action: Action::SweepAndVerify })
            .expect("test queue fits");
        drop(tx);
        let (atx, mut arx) = mpsc::channel::<crate::feed::FeedMsg>(8);
        let st = stats();
        // `None` is the whole point: this is the posture arb-recorder runs in,
        // and the one INTL is in for the life of every armed session.
        let halt = Arc::new(Halt::default());
        run_executor(Venue::Polymarket, 0.0, None, rx, st.clone(), halt, Some(atx)).await;

        assert_eq!(st.sent.load(Ordering::Relaxed), 0, "a dry run reaches no venue, ever");
        let m = arx.try_recv().expect("the engine is answered even so");
        let v: serde_json::Value = serde_json::from_str(&m.line).expect("the engine parses these");
        assert_eq!(v["kind"], "sweep_result");
        assert_eq!(v["venue"], "polymarket");
        assert_eq!(v["ok"], true, "nothing of ours can rest where we have never placed");
    }

    /// **DEFECT 2.** A place whose RESPONSE was lost leaves an order this engine
    /// can address.
    ///
    /// The transport maps a reqwest timeout to `Transport` after 15s, so a place
    /// can fail with the order RESTING. No ack was emitted, so `oid_venue` never
    /// learned the venue's id: the cancel could not be addressed, the escalation
    /// PM-US refuses locally (it has no client_order_id on the wire at all), and
    /// a fill on it would have arrived under an id nothing could attribute —
    /// `n_unattributed`, with no hedge obligation minted and the leg NAKED.
    ///
    /// The ack is the whole remedy: it is exactly what `on_order_ack` needs to
    /// make the parked cancel addressable and to claim a held fill.
    #[tokio::test]
    async fn a_place_whose_response_was_lost_is_recovered_and_acked() {
        let lost = Arc::new(Recorder {
            refuse_places_from: 1,
            recover: Some("BH8H83AY09NG".into()),
            ..Default::default()
        });
        let (st, told) =
            drain_reporting(Venue::PolymarketUs, lost.clone(), vec![place("m1")]).await;

        assert_eq!(st.failed.load(Ordering::Relaxed), 1, "the CALL failed and still says so");
        assert_eq!(st.sent.load(Ordering::Relaxed), 0);
        assert_eq!(st.recovered.load(Ordering::Relaxed), 1, "and the order it left is found");
        assert_eq!(told.len(), 1, "one ack, for an order the venue really holds");
        assert_eq!(told[0]["kind"], "order_ack");
        assert_eq!(told[0]["order_id"], "m1", "ours");
        assert_eq!(told[0]["venue_order_id"], "BH8H83AY09NG", "and theirs — the cancel handle");
        assert_eq!(told[0]["venue"], "polymarket_us");
        assert_eq!(told[0]["market_id"], "KXTEST");
    }

    /// The control: a place that genuinely never landed adopts NOTHING. A
    /// recovery that invents an order would be worse than the leak.
    #[tokio::test]
    async fn a_place_the_venue_really_refused_recovers_nothing() {
        let refused = Arc::new(Recorder { refuse_places_from: 1, ..Default::default() });
        let (st, told) = drain_reporting(Venue::PolymarketUs, refused, vec![place("m1")]).await;
        assert_eq!(st.failed.load(Ordering::Relaxed), 1);
        assert_eq!(st.recovered.load(Ordering::Relaxed), 0);
        assert!(told.is_empty(), "nothing rests, so there is nothing to tell: {told:?}");
    }

    /// **BLOCKER 1 — a place the venue REJECTED must never start a search.**
    /// This is the difference between "we could not read the answer" and "the
    /// answer was no", and getting it wrong is a money path.
    ///
    /// Both gateways return `Status` for any `status >= 300`, so a post-only
    /// that would cross is a 400 — routine (~150 in one 3.7-day shadow replay on
    /// PM-US alone) and DEFINITIVE: nothing of ours rests. PM-US's recovery can
    /// only match on market and size, so running it here would sooner or later
    /// find a single unclaimed order in that market of that size belonging to
    /// somebody else on this SHARED account and adopt it — after which the next
    /// reprice cancels THEIR order, and a fill on it is booked as a maker fill
    /// of ours and hedged with a real taker order against a position we do not
    /// hold.
    #[tokio::test]
    async fn a_rejected_place_never_goes_looking_for_someone_elses_order() {
        for e in [
            VenueError::Status {
                endpoint: "pmus place",
                status: 400,
                body: r#"{"error":"post_only_would_cross"}"#.into(),
            },
            VenueError::NotWired,
            VenueError::Sign("bad key".into()),
            VenueError::RateLimited { priority: "critical" },
        ] {
            let sink = Arc::new(Recorder {
                refuse_places_from: 1,
                place_err: Some(e.clone()),
                // The stranger's order, resting in the same market at the same
                // size. It must not be touched.
                recover: Some("SOMEBODY-ELSES-ORDER".into()),
                ..Default::default()
            });
            let (st, told) = drain_reporting(Venue::PolymarketUs, sink, vec![place("m1")]).await;
            assert_eq!(st.failed.load(Ordering::Relaxed), 1, "{e:?}");
            assert_eq!(
                st.recovered.load(Ordering::Relaxed),
                0,
                "{e:?} is not an unreadable answer, it is a definitive one"
            );
            assert!(told.is_empty(), "and NOTHING is adopted: {told:?}");
        }
    }

    /// ...and the other side of the same line, so the gate is not simply "never
    /// recover": every error that can only happen AFTER the request left this
    /// process still recovers. `Parse`/`MissingField` mean a 2xx whose body we
    /// could not read — the order EXISTS — which is the pair
    /// `KalshiGateway::rehearse` has recovered from since 2026-07-27.
    #[tokio::test]
    async fn an_unreadable_answer_still_recovers_whichever_way_it_was_unreadable() {
        for e in [
            VenueError::Transport("operation timed out".into()),
            VenueError::Parse { endpoint: "pmus:order", detail: "expected value".into() },
            VenueError::MissingField { endpoint: "pmus:order", field: "id".into() },
        ] {
            let sink = Arc::new(Recorder {
                refuse_places_from: 1,
                place_err: Some(e.clone()),
                recover: Some("pm-lost".into()),
                ..Default::default()
            });
            let (st, told) = drain_reporting(Venue::PolymarketUs, sink, vec![place("m1")]).await;
            assert_eq!(st.recovered.load(Ordering::Relaxed), 1, "{e:?} leaves it unknown");
            assert_eq!(told[0]["venue_order_id"], "pm-lost");
        }
    }

    /// The predicate itself, exhaustively — a new `VenueError` variant must be
    /// classified deliberately, not default into "go looking for an order".
    #[test]
    fn only_the_errors_that_can_happen_after_the_request_left_are_recoverable() {
        assert!(place_answer_was_lost(&VenueError::Transport("reset".into())));
        assert!(place_answer_was_lost(&VenueError::Parse { endpoint: "e", detail: "d".into() }));
        assert!(place_answer_was_lost(&VenueError::MissingField {
            endpoint: "e",
            field: "id".into()
        }));
        // ...and everything the venue or this process settled definitively.
        assert!(!place_answer_was_lost(&VenueError::Status {
            endpoint: "e",
            status: 400,
            body: String::new()
        }));
        assert!(!place_answer_was_lost(&VenueError::NotWired));
        assert!(!place_answer_was_lost(&VenueError::Sign("x".into())));
        assert!(!place_answer_was_lost(&VenueError::RateLimited { priority: "critical" }));
    }

    /// The scope rule, at the boundary that owns it: an order this process has
    /// ALREADY claimed can never be handed back as a new one.
    ///
    /// Both venues' resting lists LAG a write, so the order we placed (and even
    /// cancelled) a moment ago is still on them. Adopting it as the id of a
    /// DIFFERENT order would point the next cancel at the wrong order — and on a
    /// SHARED account (docs/venue-quirks.md §xv-graceful-shutdown-cancels-orders)
    /// that is exactly the class of mistake worth more than the bug.
    #[tokio::test]
    async fn the_recovery_never_adopts_an_order_this_process_already_placed() {
        let sink = Arc::new(Recorder {
            refuse_places_from: 2,          // m1 lands; m2's answer is lost
            recover: Some("venue-m1".into()), // ...and m1 is what is resting
            ..Default::default()
        });
        let (st, told) =
            drain_reporting(Venue::PolymarketUs, sink, vec![place("m1"), place("m2")]).await;

        assert_eq!(st.recovered.load(Ordering::Relaxed), 0, "m1 is not m2");
        assert_eq!(told.len(), 1, "only m1's real ack: {told:?}");
        assert_eq!(told[0]["order_id"], "m1");
        assert_eq!(told[0]["venue_order_id"], "venue-m1");
    }

    /// The one case the recovery stands down: SIGTERM arrived while this very
    /// place was on the wire.
    ///
    /// The sweep is a strictly better remedy there — it reaches orders we hold
    /// no id for at all and it PROVES the outcome — and a resting-order read
    /// here would compete for the budget that proof depends on. Same rule as the
    /// cancel escalation standing down while killed.
    ///
    /// The sink latches mid-call, which is the real shape of it: the place was
    /// dispatched with the latch off and failed with it on.
    #[tokio::test]
    async fn the_recovery_stands_down_when_the_halt_beat_it() {
        let halt = Arc::new(Halt::default());
        let lost = Arc::new(Recorder {
            refuse_places_from: 1,
            recover: Some("BH8H83AY09NG".into()),
            latch: Some(halt.clone()),
            ..Default::default()
        });
        let (tx, rx) = mpsc::channel::<ExecCmd>(8);
        tx.try_send(place("m1")).unwrap();
        drop(tx);
        let st = stats();
        let (atx, mut arx) = mpsc::channel::<crate::feed::FeedMsg>(8);
        run_executor(Venue::PolymarketUs, 0.0, Some(lost), rx, st.clone(), halt, Some(atx)).await;

        assert_eq!(st.failed.load(Ordering::Relaxed), 1, "the place did go, and did fail");
        assert_eq!(st.recovered.load(Ordering::Relaxed), 0, "but the sweep owns this now");
        assert!(arx.try_recv().is_err(), "and nothing is adopted behind the sweep's back");
    }

    #[test]
    fn discarded_makers_are_reported_but_are_not_a_failure() {
        let out = ShutdownOutcome {
            clean: vec![Venue::Kalshi],
            discarded_places: 7,
            discarded_takers: 0,
            ..Default::default()
        };
        let r = out.report().join("\n");
        assert!(r.contains("discarded 7 queued maker place(s)"), "{r}");
        assert_eq!(
            out.exit_code(),
            0,
            "a maker that never reached a venue rests nothing and is naked nothing"
        );
    }
}
