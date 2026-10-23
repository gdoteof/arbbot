//! Turn the armed engine's stats line into watchdog verdicts.
//!
//! `engine/mod.rs::summary` prints a JSON line to stdout every `--stats-every`
//! seconds (60 on arbbot-trader-m3) carrying every safety counter the engine
//! has. Several of those counters carry doc comments asserting they ARE the
//! alarm for a specific money-losing condition — `fills_unattributed` is
//! documented as "money that moved in our account that we cannot explain. Must
//! stay 0." `scripts/freshness_check.sh` feeds the newest line here every poll,
//! so every "if this assumption is wrong the failure is loud, because gauge X
//! moves" mitigation in this codebase rests on this module reading gauge X.
//! Measured 2026-07-29, before anything did: zero consumers outside rust/ of
//! kalshi_fill_gaps, fills_unattributed, dropped_unconsumed, hedges_naked,
//! cancels_unresolved and exec_failed, each.
//!
//! TWO RULES, AND WHY THERE ARE TWO
//! --------------------------------
//! RISE      fire when the value went UP by more than `threshold` since the
//!           last poll. The right rule for a counter ("how many happened this
//!           window") and for a level whose danger is a jump.
//!
//! SUSTAINED fire when the value has stayed AT OR ABOVE `level` for `polls`
//!           consecutive samples AND those samples span real time (see
//!           [`SUSTAIN_MIN_POLL_S`]). The right rule for a level whose
//!           documented danger is "a number that does not come back down"
//!           (engine/mod.rs on `cancels_unresolved`) — which a RISE rule is
//!           structurally blind to, because a leak of +1 per poll never jumps.
//!
//!           The span requirement is not decoration. Counting POLLS alone
//!           assumes polls are 5 minutes apart, and they are not always: the
//!           watchdog timer is `Persistent=true`, so missed activations fire
//!           back to back after a suspend or a boot. Measured: three samples 60s
//!           apart satisfied "3 consecutive polls" and fired, turning a rule
//!           that means "this did not clear in a quarter of an hour" into one
//!           that means "this did not clear in two minutes".
//!
//! Neither rule ever fires on a level merely being non-zero, because a standing
//! value would page for ever. `hedges_undischarged` is non-zero against a real
//! naked leg whose owner is arbbot-hedge.timer. An alarm that fires every 30
//! minutes about a condition the operator already knows about is how the
//! operator learns to swipe the notification away — and the notification it
//! trains them to swipe is the same one that one day reads ARMED trader-m3
//! FAILED.
//!
//! RISE also absorbs a RATCHET. `cancels_unresolved` is `parked_cancels.len()`;
//! `engine/cancel.rs::newly_exhausted` MARKS a spent entry rather than removing
//! it, and `on_venue_answer` removes only on an `ok`, so once a cancel runs out
//! of ways to be sent that map never returns to zero for the life of the
//! process. A rise rule measures from wherever the floor now is.
//!
//! What RISE does NOT absorb is OSCILLATION above that floor, and that is why
//! `cancels_unresolved` is SUSTAINED and not RISE. `cancel.rs` parks EVERY
//! cancel on entry, so the gauge is the in-flight cancel count of an engine
//! that cancels and re-places continuously; a level alternating 0, 2, 0, 2
//! across polls fires a RISE rule on half of them, for ever. Measured: 4 pages
//! in 8 polls.
//!
//! PROCESS RESTARTS
//! ----------------
//! Every one of these counters is per-process and resets to 0 on restart, so a
//! naive delta goes negative across one. Restarts are detected by systemd's
//! `InvocationID`, which is a fresh UUID for every start of the unit and is the
//! only thing here that identifies a process EXACTLY. `elapsed_s` decreasing is
//! kept as a fallback for when the id cannot be read.
//!
//! `elapsed_s` ALONE is not enough, and its blind spot is not benign. For RISE
//! it degrades toward under-counting, because RISE clamps with
//! `base = min(base, cur)`. For SUSTAINED it is a FALSE PAGE, because the dead
//! process's streak is inherited. Measured: a process sampled at elapsed_s 60
//! and 120 with `cancels_unresolved` at 2 (streak 1, then 2), then killed; the
//! REPLACEMENT process's first sample at elapsed_s 290 is HIGHER, so no restart
//! was detected, the streak advanced to 3, and a brand-new process paged on its
//! first observation.
//!
//! Both are reset on a new `InvocationID`: baselines to 0, sustain streaks to
//! nothing.
//!
//! A detected restart is also REPORTED, once, as its own page. Every counter
//! here is per-process, so a restart is the one event that resets them all —
//! and measured 2026-08-09, an armed SIGABRT (malloc_consolidate, orders left
//! resting, swept 30s later by the auto-restart) paged nothing while restarts
//! were silent: freshness_check.sh pages on is-failed, which an auto-restart
//! satisfies for ~30 seconds between five-minute polls, so the crash of the one
//! real-money process was invisible by timing.
//!
//! VERDICTS SURVIVE BOTH THE COOLDOWN AND A RESTART
//! ------------------------------------------------
//! freshness_check.sh shares one 30-minute cooldown across every check in the
//! file, so an unrelated outage can suppress the POST that would have carried a
//! gauge verdict. Undelivered verdicts are therefore persisted as TEXT in
//! `_carry` and re-emitted every poll until an alert actually goes out.
//!
//! Holding the numeric BASELINE back instead reads as equivalent and is not:
//! the held baseline belongs to the OLD process, so if the engine restarts
//! before the cooldown expires the restart rule drops it to 0, the fresh process
//! reads 0, and the verdict is lost permanently. The armed unit restarted 40
//! minutes after the 2026-07-29 00:54:51 naked-leg event, so that is the normal
//! case and not a corner. Text carries across a restart; a number cannot.
//!
//! COLD START
//! ----------
//! With no state file — first run, /tmp cleared by a reboot, or a state file
//! that cannot be parsed — the baseline is seeded and NOTHING is reported. A
//! reboot restarts the engine too, so its counters are fresh in the same
//! instant. (`main::load_state` is where an unreadable file becomes a cold
//! start.)

use std::collections::BTreeSet;

use crate::value::{Fault, Num, Obj, Value, ZERO};
use Severity::{Note, Page};

/// The shortest gap between two samples that counts as a full poll apart, for
/// the SPAN a SUSTAINED rule must cover. 240s is the watchdog's 5-minute
/// cadence with 20% slack for timer drift. A rule of `polls` needs
/// `(polls - 1) * this` of engine uptime between its first and last sample, so
/// back-to-back catch-up runs cannot satisfy it — see SUSTAINED above.
const SUSTAIN_MIN_POLL_S: i64 = 240;

/// How far the engine's own clock may advance between two observations that
/// still count as CONSECUTIVE. 900s is 3x the watchdog's 5-minute cadence,
/// which absorbs ordinary timer drift (OnUnitActiveSec plus Persistent=true)
/// and nothing else.
///
/// Without this the streak bridges any hole. Measured: a level at 2, observed
/// twice, then EIGHT polls with no stats line at all (a wedged engine, same
/// process, so no restart to reset it), then observed at 2 once more — fired,
/// saying "held for 3 consecutive polls" about three samples spanning 55
/// minutes. "We saw it high 3 times running" and "we saw it high 3 times with
/// an unknown hole in the middle" are different facts, and a rule whose message
/// cannot tell them apart is the defect. A hole means we do not know it stayed
/// up, so the streak restarts — and the hole itself is not unreported, because
/// an engine that stops printing stats pages on its own.
const SUSTAIN_MAX_GAP_S: f64 = 900.0;

/// Verdicts held for a later poll, capped. The cap only bites if the cooldown
/// has been suppressing for hours, at which point the operator has a bigger
/// problem than a truncated list.
const CARRY_MAX: usize = 12;

#[derive(Clone, Copy)]
enum Rule {
    /// `threshold` is the largest increase in one watchdog poll (5 min) that
    /// stays SILENT; 0 therefore means "fire on any increase at all".
    Rise { threshold: i64 },
    /// `level` must persist for `polls` consecutive samples.
    Sustained { level: i64, polls: i64 },
}

#[derive(Clone, Copy)]
enum Severity {
    Page,
    Note,
}

impl Severity {
    fn tag(self) -> &'static str {
        match self {
            Severity::Page => "PAGE",
            Severity::Note => "NOTE",
        }
    }
}

struct Watch {
    gauge: &'static str,
    rule: Rule,
    severity: Severity,
    /// What the gauge moving MEANS, in the operator's terms.
    why: &'static str,
}

const fn rise(gauge: &'static str, severity: Severity, threshold: i64, why: &'static str) -> Watch {
    Watch { gauge, rule: Rule::Rise { threshold }, severity, why }
}

const fn sustained(
    gauge: &'static str,
    severity: Severity,
    level: i64,
    polls: i64,
    why: &'static str,
) -> Watch {
    Watch { gauge, rule: Rule::Sustained { level, polls }, severity, why }
}

// EVIDENCE, AND ITS LIMITS. When these thresholds were set the armed unit had
// emitted 892 stats lines. It had NOT emitted all of these gauges for all of
// them — each was added to `summary()` at a different time, and a gauge reads 0
// in a line that predates it for reasons that have nothing to do with the
// engine being healthy. The per-gauge presence counts below (out of those 892)
// are the real evidence base and some are thin. Where a threshold is not
// supported by data, it says so.
//
// A gauge may appear twice (two sustain levels); the rows are evaluated in
// this order, which is also the order verdicts print in.
const WATCH: &[Watch] = &[
    // ---- RISE, threshold 0. Each names a specific money condition and had
    // ---- never been observed non-zero on a binary that emitted it.
    //
    // doc: "Must stay 0." Present in 684/892 armed lines, max 0.
    rise(
        "fills_unattributed",
        Page,
        0,
        "money moved in this account that the engine cannot attribute to any \
         order it placed — RECONCILE BY HAND",
    ),
    // doc: "Must stay 0." Present in 684/892, max 0.
    rise(
        "hedges_overfilled",
        Page,
        0,
        "hedge contracts filled beyond what an obligation owed — a position \
         with no maker leg to pair it with",
    ),
    // doc: "programming-bug alarm ... must stay 0." Present in 892/892, max 0.
    rise(
        "dropped_unconsumed",
        Page,
        0,
        "an obligation was minted and never hedged (arb_core::fill Drop alarm) \
         — nothing will retry it",
    ),
    // NO "must stay 0" doc anywhere. Justified instead by the alarm TEXT the
    // engine prints beside it ("[hedge] NAKED ... the book has not offered a
    // price that keeps the basket profitable") and by `p.alarmed` latching it
    // to one count per obligation, so it cannot spam. Present in 892/892, max 1
    // — and that 1 is the real 2026-07-29 00:54:51 event nothing paged about.
    rise(
        "hedges_naked",
        Page,
        0,
        "a maker leg filled and its hedge is past its deadline — real \
         directional exposure, one count per obligation",
    ),
    // NO "must stay 0" doc either; `exec.rs` says only "engine try_send
    // failures (executor backlogged)". Justified instead by `Engine::dispatch`'s
    // own doc: "Returns false when the channel is full — the command is LOST
    // and only the counter moves". Present in 892/892, max 0 — and 0 across
    // 1613 dry-run lines that dispatched 20800 places, which for THIS gauge is
    // real corroboration because the shadow unit exercises `dispatch` even with
    // no order path behind it.
    rise(
        "exec_dropped",
        Page,
        0,
        "an executor channel was full and a place or cancel was LOST — a \
         dropped cancel is an order still resting at a price we rejected",
    ),
    // doc: "Never routine; must never be silent." Present in only 252/892.
    // Kalshi's recovery search covers its whole order history, so an adopted
    // order may already be EXECUTED — "the one where a duplicate hedge may be
    // on the book". That is a stronger reason to page on it, not a weaker one.
    rise(
        "exec_recovered",
        Page,
        0,
        "an order reached a venue under an id this process never learned — \
         unaddressable by any cancel, and it may already have EXECUTED",
    ),
    // doc: "Must stay 0." Present in only 252/892.
    rise(
        "kalshi_fills_unreadable",
        Page,
        0,
        "a Kalshi fill frame's count could not be read — while this rises the \
         `fills` total reads 0 and EVERY other gauge here looks healthy",
    ),
    // ---- RISE, with a real benign rate.
    //
    // A JUDGEMENT CALL, NOT A MEASUREMENT, and said plainly because a number
    // dressed as "5x the worst session ever observed" is exactly the shape of
    // claim that does not survive review. The all-time armed max is 4, but
    // those 4 were a BURST inside seconds, and a burst rate implies nothing
    // about a 5-minute total — the same shape sustained is ~200/window, so the
    // observed maximum bounds neither direction.
    //
    // 60 is therefore NOT derived from this gauge's history at all. It is
    // picked as a rate the order path cannot plausibly be working at, and its
    // real justification is that the conditions it would catch are all covered
    // elsewhere by rules that ARE grounded: a refused cancel shows up in
    // cancels_unresolved, a lost command in exec_dropped, an order adopted
    // after a lost response in exec_recovered. This one is the coarse backstop
    // for "the venue is refusing everything", and it is deliberately set where
    // only that reads. Revise it from data the first time it fires. Present in
    // 892/892.
    rise(
        "exec_failed",
        Page,
        60,
        "venue rejections in one window — at this rate the order path is not \
         working, whatever the individual errors say",
    ),
    // doc says "Must stay 0", but its listed causes include "background budget
    // spent", which is routine and self-heals on the next reconcile, so this is
    // not a 0 threshold. THIN EVIDENCE: present in only 55/892 lines — under an
    // hour of armed runtime. Treat 2 as a first guess, not a finding.
    rise(
        "kalshi_reconcile_failures",
        Page,
        2,
        "the Kalshi fill reconciliation could not run — while this rises the \
         fill totals are a local sum with no venue truth behind them",
    ),
    // 1 is the documented per-process FLOOR, so this is the one gauge where a
    // 0 threshold would fire on every single restart. Present in 252/892.
    rise(
        "kalshi_fill_gaps",
        Note,
        3,
        "the Kalshi fill feed reconnected repeatedly; each window may hide a \
         fill. The repair for it (kalshi_reconcile_failures) pages separately",
    ),
    // Its own doc ends "Nothing reads it automatically" — watching it is the
    // point.
    //
    // It did not exist when the 892 lines above were written, so the presence
    // counts have nothing to say about it. What bounds the noise here is
    // STRUCTURAL, not empirical — `sweeps_owed` is a BTreeMap keyed by Venue,
    // so `.len()` is 0, 1 or 2, and a RISE rule can therefore fire at most
    // twice in the life of a process.
    //
    // RISE and not SUSTAINED because it is a deliberate RATCHET: its doc says
    // it "does NOT come back down when a halt clears over an unproven book", so
    // a session that fully recovered from a morning outage reads non-zero for
    // the rest of its life. A level rule would page about that for ever; a rise
    // rule says "a book just went unproven", which is the event, and then goes
    // quiet.
    rise(
        "sweeps_owed",
        Page,
        0,
        "a venue's kill sweep was never proven — the book this process halted \
         over was not confirmed empty, and orders may be resting on it",
    ),
    // doc (maker_exit.rs): the module's own alarm gauge. It is an INCIDENT
    // count, not a level — `maker_exit_healed` is how many of these the engine
    // closed by itself, and the difference is what is naked right now. The
    // message tells the operator to WAIT, not to restart: restarting throws
    // away the exit debounce and the queue for a leg that is already being
    // fixed. Still cannot spam — MAX_RESTING is 1, so there is at most one exit
    // in flight and therefore at most one incident at a time, and a heal takes
    // cycles — but it is not bounded at one per PROCESS.
    rise(
        "maker_exit_unresolved",
        Page,
        0,
        "a Kalshi exit ask FILLED and its PM-US close did not complete — the \
         account is one-legged and the ledger still calls the basket OPEN. \
         maker_exit::heal is retrying it every 60s against venue truth and \
         crosses out after ~10 min; read maker_exit_healed to see if it won. \
         ACT ONLY IF THIS IS STILL OUTSTANDING IN ~15 MINUTES",
    ),
    // doc (engine/mod.rs): "MUST STAY 0 ... Alarm on any change". Present in
    // the live stats line, max 0 across ~5.9k recon cycles (which had placed 0
    // orders) as of 2026-08-14.
    rise(
        "positions_recon_act_unresolved",
        Page,
        0,
        "a recon-act order the venue ACCEPTED whose fate this process could \
         not read — contracts that may be in the account and are not in the \
         ledger",
    ),
    // NOTE, not PAGE: acting is the feature working (profitable-only, 5
    // ct/order, 2/cycle). It had been 0 for the life of the deployment against
    // thousands of refusals, so the first move is worth one low-priority line;
    // every money condition it can CREATE pages via the rows above.
    rise(
        "positions_recon_acted",
        Note,
        0,
        "the naked-leg completer ACTED for the first time in this process — \
         read it against maker_exit_unresolved, because the two climbing \
         together is the documented maker-exit/recon-act fight",
    ),
    // ---- SUSTAINED. See the two-rules note in the module doc.
    //
    // doc: "Healthy is 0, or a transient handful; a number that does not come
    // back down is orders resting that this engine has already decided
    // against." That is a SUSTAINED condition, and RISE cannot see it: a leak
    // climbing +1 per poll from 0 to 12 emits nothing under RISE, while an
    // ordinary 0,2,0,2 oscillation fires it on half of all polls. Present in
    // 684/892, max 0 — but those 684 are all from a probe regime
    // (--tt-max-clip 5, 3 relationships), the regime LEAST able to put two
    // cancels on the wire at once. "max 0" is therefore weak evidence that 2 is
    // rare and no evidence at all about heavier quoting.
    sustained(
        "cancels_unresolved",
        Page,
        2,
        3,
        "cancels the engine decided and the venue never confirmed — not in \
         flight, stuck, and each may be an order resting at a price already \
         rejected",
    ),
    // The floor the row above cannot see. A level PINNED AT EXACTLY 1 never
    // reaches a bar of 2, and neither does 1,3,1,3 — both silent for ever
    // without this row. That is precisely the gauge's OWN documented failure
    // mode ("a number that does not come back down"), and
    // `cancels_unaddressable`, which would have named the benign case, is
    // deliberately unwatched.
    //
    // NOTE and not PAGE, and an hour rather than a quarter of one, because the
    // doc also says the commonest single parked entry is a place the venue
    // REJECTED, where nothing rests and nothing is wrong. This fires ONCE per
    // streak, so the permanently-parked benign entry costs one low-priority
    // line per session, not a page every 30 minutes.
    //
    // A level stuck at 2+ trips both rows: a page at ~15 minutes and this note
    // at ~an hour. That is deliberate — the second one says the first was not
    // acted on — and it is one extra low-priority line, not a duplicate page.
    sustained(
        "cancels_unresolved",
        Note,
        1,
        12,
        "at least one cancel has been owed and unconfirmed for about an hour — \
         below the paging bar, but it is not clearing either",
    ),
];

// Gauges deliberately NOT watched, and why. Kept here because "we looked at it
// and decided no" is the part that gets lost, and the next person to notice one
// of these is unread will otherwise add it and discover the noise by paging.
//
//   hedges_undischarged     A STARTUP CONSTANT (`cfg.hedges_undischarged`), not
//                           a runtime gauge: it never moves within a process.
//                           It is non-zero today. Under the restart rule its
//                           baseline drops to 0 on every restart, so any
//                           non-zero threshold on it fires on EVERY restart,
//                           for ever. Its owner is arbbot-hedge.timer; the
//                           runtime condition that matters is `hedges_naked`.
//   kalshi_reconcile_rejected  Its own doc says it is NOT a must-stay-0 gauge
//                           and is EXPECTED on a busy account, and that no
//                           count derived from it can separate the benign cause
//                           (REST lag) from the one that matters (mismatched id
//                           spaces) — the discriminant is an order id repeating
//                           in a log line, which no threshold can see.
//   kalshi_fills_recovered  Its doc: "Not an error: nonzero is the repair
//                           working." Watching it would page on the fix.
//   cancels_unaddressable   A subset of cancels_unresolved, and its doc says
//                           the commonest member is a place the venue REJECTED
//                           where nothing rests and nothing is wrong.
//   fills_unclaimed         Transient by construction; a fill that stays
//                           unclaimed becomes `fills_unattributed`, which pages.
//   kalshi_fill_dust_hundredths  Sub-contract dust. Its failure mode is "sits
//                           high", which SUSTAINED could express — but no
//                           threshold for it is defensible from anything
//                           observed, and a guessed one on a gauge nobody has
//                           seen move is how this channel becomes noise.
//   chan_high_water, decision_latency, risk_reserved, killed, feed_pulled
//                           Performance and operator-state, not money moving.

/// Every name this module reads from the stats line, sorted and once each.
///
/// `elapsed_s` is here even though it is not a gauge: it is the restart
/// discriminator, and losing it silently would make every standing gauge page
/// its full value in one burst.
fn watched_names() -> BTreeSet<&'static str> {
    WATCH.iter().map(|w| w.gauge).chain(["elapsed_s"]).collect()
}

/// `cur.get(key)` — which only an object supports.
fn get<'a>(cur: &'a Value, key: &str) -> Result<Option<&'a Value>, Fault> {
    match cur {
        Value::Obj(o) => Ok(o.get(key)),
        _ => Err(Fault::new("the stats line is not a JSON object")),
    }
}

/// `cur[key]`, for a key `cur.contains` has already found.
fn index<'a>(cur: &'a Value, key: &str) -> Result<&'a Value, Fault> {
    get(cur, key)?.ok_or_else(|| Fault::new(format!("{key} vanished mid-poll")))
}

/// `v` if it is truthy, else `default` — Python's `v or default`.
fn or<'a>(v: Option<&'a Value>, default: &'a Value) -> &'a Value {
    v.filter(|v| v.truthy()).unwrap_or(default)
}

/// One poll's verdicts, and what the next state is built from.
pub struct Poll {
    carried: Vec<String>,
    out: Vec<String>,
    absent: Vec<&'static str>,
    streak: Obj,
    prev_seen: Vec<Value>,
}

/// Judge one stats line against the previous state.
///
/// `cur` is whatever the stats line parsed to. Rules apply Python's semantics
/// to it, so a stats line that is not an object, or a gauge that is not a
/// number, is a [`Fault`] wherever a rule first touches it.
pub fn evaluate(cur: &Value, prev: &Obj, invocation: &str) -> Result<Poll, Fault> {
    // Verdicts from an earlier poll whose alert never went out. Kept UNPREFIXED
    // in `carried` and re-carried unprefixed, so a verdict held across several
    // polls does not accumulate a "(held) (held) (held)" prefix.
    let carried: Vec<String> = match prev.get("_carry") {
        Some(c) => c.members()?,
        None => Vec::new(),
    }
    .into_iter()
    .filter_map(|c| match c {
        Value::Str(s) => Some(s),
        _ => None,
    })
    .take(CARRY_MAX)
    .collect();
    let mut out = Vec::new();

    // A watched name vanishing from the summary is the single most likely way
    // this alarm dies quietly: a rename in engine/mod.rs and every check below
    // starts reading nothing while reporting all-clear.
    //
    // Reported when the absent SET GROWS, not while it is non-empty, for the
    // same reason every other verdict here is a change. Replaying the 892 real
    // armed stats lines through an unconditional version produced 168
    // identical pages: the early sessions ran a binary that predated seven of
    // these gauges, and a downgrade would do it again.
    let names = watched_names();
    let mut absent = Vec::new();
    for &n in &names {
        if !cur.contains(n)? {
            absent.push(n);
        }
    }
    let mut new_absent = Vec::new();
    for &n in &absent {
        let was_absent = match prev.get("_absent") {
            Some(a) => a.contains(n)?,
            None => false,
        };
        if !was_absent {
            new_absent.push(n);
        }
    }
    // A name we have SEEN and then lost is a rename, and it is a page: the
    // check that used to read it now reads nothing while reporting all-clear.
    //
    // A name we have NEVER seen is a different thing entirely — the running
    // engine simply predates it. Found by running this against the live
    // journal: the armed process was started before #44 landed, so it did not
    // emit `sweeps_owed`, and an undifferentiated rule pages on the very first
    // poll after deployment about a gauge nobody could have been reading yet.
    // True, and not actionable, which is the definition of the noise this
    // module is supposed to avoid. It goes out as a NOTE, and turns into a page
    // the moment the gauge has actually been seen and then disappears.
    let prev_seen = match prev.get("_seen") {
        Some(s) => distinct(s.members()?)?,
        None => Vec::new(),
    };
    let (vanished, never_seen): (Vec<&str>, Vec<&str>) = new_absent
        .iter()
        .partition(|n| prev_seen.iter().any(|s| matches!(s, Value::Str(x) if x == *n)));
    if !vanished.is_empty() {
        out.push(format!(
            "PAGE|ARMED trader-m3 stats line no longer carries {} — those safety gauges are \
             UNWATCHED (engine summary renamed?)",
            vanished.join(", ")
        ));
    }
    if !never_seen.is_empty() {
        out.push(format!(
            "NOTE|ARMED trader-m3 has never emitted {} — the running engine predates that \
             gauge, so it is unwatched until this unit restarts onto a newer build",
            never_seen.join(", ")
        ));
    }

    // No elapsed_s means no restart discriminator, so every delta below would
    // be computed against a baseline that may belong to another process. Seed
    // instead; the page above already said why.
    let cold = prev.is_empty() || !cur.contains("elapsed_s")?;
    // InvocationID first: it is exact. `elapsed_s` decreasing is the fallback
    // for when systemd gives us nothing — it catches the common restart and
    // misses the one where the replacement process is already OLDER than the
    // dead one was when last sampled, which is the false page described in the
    // module doc.
    let restarted = !cold && {
        let prev_inv = prev.get("_invocation");
        let new_process = !invocation.is_empty()
            && prev_inv.is_some_and(Value::truthy)
            && !matches!(prev_inv, Some(Value::Str(p)) if p == invocation);
        new_process
            || get(cur, "elapsed_s")?
                .unwrap_or(&ZERO)
                .lt(prev.get("elapsed_s").unwrap_or(&ZERO))?
    };
    // Say the restart itself, once. A deliberate restart costs one page, which
    // for a unit restarted about fortnightly is the right price for never
    // missing a crash of the one process that can hold real orders.
    if restarted {
        out.push(
            "PAGE|ARMED trader-m3 RESTARTED (new process; every gauge baseline reset) — check \
             journalctl for the previous exit code and whether startup_sweep found orders \
             resting"
                .to_owned(),
        );
    }

    let prev_streak = match prev.get("_streak") {
        Some(Value::Obj(o)) => Some(o),
        _ => None,
    };
    let mut streak = Obj::default();

    for w in WATCH {
        if !cur.contains(w.gauge)? {
            continue;
        }
        let sev = w.severity.tag();
        match w.rule {
            Rule::Sustained { level, polls } => {
                // Keyed per ROW, not per gauge: one gauge may carry two
                // sustain rules at different levels (cancels_unresolved does),
                // and sharing one streak between them would let the looser rule
                // reset the stricter.
                let key = format!("{}#{level}", w.gauge);
                let now_s = or(get(cur, "elapsed_s")?, &ZERO).to_float()?;
                let empty = Obj::default();
                let p = match prev_streak.and_then(|s| s.get(&key)) {
                    Some(Value::Obj(o)) => o,
                    _ => &empty,
                };
                let (mut n, at, mut since, mut fired) =
                    read_streak(p, now_s).unwrap_or((0, 0.0, now_s, false));
                // A restart resets: a fresh process has not held anything
                // anywhere for any length of time. So does a hole — see
                // SUSTAIN_MAX_GAP_S.
                let broken = cold || restarted || (now_s - at) > SUSTAIN_MAX_GAP_S;
                let cur_v = index(cur, w.gauge)?;
                let Some(c) = cur_v.num() else {
                    return Err(Fault::new(format!("{} is not a number", w.gauge)));
                };
                if c.lt(Num::Int(level.into())) {
                    n = 0;
                    fired = false;
                } else if broken || n == 0 {
                    // `n == 0` matters as much as `broken`: a streak restarting
                    // after a DIP must re-date itself too, or the span reported
                    // below is measured from a run that already ended.
                    n = 1;
                    since = now_s;
                    fired = false;
                } else {
                    n += 1;
                }
                let span = now_s - since;
                // BOTH conditions, and `fired` so it says it once. Counting
                // samples alone would let three back-to-back catch-up runs
                // satisfy a rule that is supposed to mean "this did not clear
                // in a quarter of an hour"; requiring span alone would fire off
                // two samples an hour apart with no idea what happened between
                // them.
                if !fired
                    && n >= i128::from(polls)
                    && span >= ((polls - 1) * SUSTAIN_MIN_POLL_S) as f64
                {
                    fired = true;
                    out.push(format!(
                        "{sev}|ARMED trader-m3 {} has held at {c} across {n} consecutive samples \
                         spanning {:.0}s of engine uptime — {}",
                        w.gauge,
                        span.trunc(),
                        w.why
                    ));
                }
                let mut entry = Obj::default();
                entry.insert("n", Value::Int(n));
                entry.insert("at", Value::Float(now_s));
                entry.insert("since", Value::Float(since));
                entry.insert("fired", Value::Bool(fired));
                streak.insert(&key, Value::Obj(entry));
            }
            Rule::Rise { threshold } => {
                if cold {
                    continue;
                }
                let base = if restarted { &ZERO } else { prev.get(w.gauge).unwrap_or(&ZERO) };
                let (Some(c), Some(base)) = (index(cur, w.gauge)?.num(), base.num()) else {
                    return Err(Fault::new(format!("{} or its baseline is not a number", w.gauge)));
                };
                // Belt and braces: a level that fell needs its baseline to fall
                // with it, or its next genuine rise is measured from a peak it
                // will not reach.
                let base = if c.lt(base) { c } else { base };
                let delta = c.minus(base);
                if Num::Int(threshold.into()).lt(delta) {
                    let restart_note = if restarted { ", process restarted" } else { "" };
                    out.push(format!(
                        "{sev}|ARMED trader-m3 {} +{delta} (now {c}{restart_note}) — {}",
                        w.gauge, w.why
                    ));
                }
            }
        }
    }

    Ok(Poll { carried, out, absent, streak, prev_seen })
}

/// A sustain row's saved streak. Any field that does not read as a number
/// fails the whole entry, and the caller starts the streak afresh.
fn read_streak(p: &Obj, now_s: f64) -> Result<(i128, f64, f64, bool), Fault> {
    let n = or(p.get("n"), &ZERO).to_int()?;
    let at = or(p.get("at"), &ZERO).to_float()?;
    let since = match p.get("since").filter(|v| v.truthy()) {
        Some(v) => v.to_float()?,
        None => now_s,
    };
    let fired = p.get("fired").is_some_and(Value::truthy);
    Ok((n, at, since, fired))
}

/// The distinct members of a saved name set, in first-seen order. Only a
/// string, a number or null can be a member; numbers equal in value (1, 1.0,
/// true) are one member.
fn distinct(items: Vec<Value>) -> Result<Vec<Value>, Fault> {
    let mut set: Vec<Value> = Vec::new();
    for v in items {
        if matches!(v, Value::List(_) | Value::Obj(_)) {
            return Err(Fault::new("_seen holds a list or object"));
        }
        if !set.iter().any(|s| s.py_eq(&v)) {
            set.push(v);
        }
    }
    Ok(set)
}

/// A name set in ascending order. Strings and numbers have no order between
/// them, so a set holding both (or null beside anything) cannot be sorted.
fn sorted(mut set: Vec<Value>) -> Result<Vec<Value>, Fault> {
    if set.len() < 2 {
        return Ok(set);
    }
    if set.iter().all(|v| matches!(v, Value::Str(_))) {
        set.sort_by(|a, b| match (a, b) {
            (Value::Str(a), Value::Str(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        });
    } else if set.iter().all(|v| v.num().is_some()) {
        set.sort_by(|a, b| match (a.num(), b.num()) {
            (Some(a), Some(b)) => a.compare(b).unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        });
    } else {
        return Err(Fault::new("_seen mixes names with values that do not order against them"));
    }
    Ok(set)
}

impl Poll {
    /// The stdout lines, in order. Held verdicts lead — the oldest unreported
    /// thing is the one most at risk of never being said — and are marked so
    /// the operator can tell a fresh event from a replayed one.
    pub fn lines(&self) -> Vec<String> {
        let held = self.carried.iter().map(|c| {
            let (sev, text) = c.split_once('|').unwrap_or((c, ""));
            format!("{sev}|(held from an earlier poll) {text}")
        });
        held.chain(self.out.iter().cloned()).collect()
    }

    /// The two candidate next states, `(delivered, held)`. freshness_check.sh
    /// adopts exactly one once it knows whether a POST actually went out. The
    /// numeric baseline advances in BOTH — holding a number back does not
    /// survive a restart (see the module doc), only the text does.
    pub fn next_states(&self, cur: &Value, invocation: &str) -> Result<(Value, Value), Fault> {
        let mut base = Obj::default();
        for w in WATCH {
            if cur.contains(w.gauge)? {
                base.insert(w.gauge, index(cur, w.gauge)?.clone());
            }
        }
        base.insert("elapsed_s", get(cur, "elapsed_s")?.unwrap_or(&ZERO).clone());
        base.insert(
            "_absent",
            Value::List(self.absent.iter().map(|n| Value::Str((*n).to_owned())).collect()),
        );
        base.insert("_streak", Value::Obj(self.streak.clone()));
        base.insert("_invocation", Value::Str(invocation.to_owned()));
        // Every watched name this deployment has ever actually observed. Only
        // ever grows: it is what separates "renamed away" from "not built yet".
        let mut seen = self.prev_seen.clone();
        for n in watched_names() {
            if cur.contains(n)? && !seen.iter().any(|s| matches!(s, Value::Str(x) if x == n)) {
                seen.push(Value::Str(n.to_owned()));
            }
        }
        base.insert("_seen", Value::List(sorted(seen)?));

        // Deduplicated: a condition that keeps re-firing while suppressed
        // leaves one entry, not one per poll.
        let mut keep: Vec<&String> = Vec::new();
        for c in self.carried.iter().chain(&self.out) {
            if !keep.contains(&c) {
                keep.push(c);
            }
        }
        let tail = &keep[keep.len().saturating_sub(CARRY_MAX)..];
        let mut held = base.clone();
        held.insert(
            "_carry",
            Value::List(tail.iter().map(|c| Value::Str((*c).clone())).collect()),
        );
        let mut delivered = base;
        delivered.insert("_carry", Value::List(Vec::new()));
        Ok((Value::Obj(delivered), Value::Obj(held)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::parse;

    /// Run one poll and return its lines and the delivered state, the way the
    /// caller adopts it after a POST went out.
    fn poll(prev: &Obj, line: &str, inv: &str) -> (Vec<String>, Obj) {
        let cur = parse(line).unwrap();
        let p = evaluate(&cur, prev, inv).unwrap();
        let (delivered, _) = p.next_states(&cur, inv).unwrap();
        let Value::Obj(next) = delivered else { panic!("state is an object") };
        (p.lines(), next)
    }

    #[test]
    fn a_healed_then_relapsed_ratchet_pages_both_rises() {
        // A gauge that comes back down and goes up again re-reads as a rise:
        // the baseline re-clamps with min(base, cur) when it falls.
        let mut state = Obj::default();
        let mut paged = 0;
        for (i, v) in [0, 1, 0, 1].into_iter().enumerate() {
            let line = format!(r#"{{"elapsed_s": {}.0, "maker_exit_unresolved": {v}}}"#, 60 * (i + 1));
            let (lines, next) = poll(&state, &line, "a");
            paged += lines.iter().filter(|l| l.contains("maker_exit_unresolved +1")).count();
            state = next;
        }
        assert_eq!(paged, 2);
    }

    #[test]
    fn cold_start_seeds_silently_in_python_layout() {
        let (lines, state) =
            poll(&Obj::default(), r#"{"elapsed_s": 300.0, "cancels_unresolved": 0}"#, "x");
        assert!(lines.iter().all(|l| l.starts_with("NOTE|ARMED trader-m3 has never emitted")));
        assert_eq!(
            Value::Obj(state).to_string(),
            "{\"cancels_unresolved\": 0, \"elapsed_s\": 300.0, \"_absent\": [\"dropped_unconsumed\", \
             \"exec_dropped\", \"exec_failed\", \"exec_recovered\", \"fills_unattributed\", \
             \"hedges_naked\", \"hedges_overfilled\", \"kalshi_fill_gaps\", \
             \"kalshi_fills_unreadable\", \"kalshi_reconcile_failures\", \"maker_exit_unresolved\", \
             \"positions_recon_act_unresolved\", \"positions_recon_acted\", \"sweeps_owed\"], \
             \"_streak\": {\"cancels_unresolved#2\": {\"n\": 0, \"at\": 300.0, \"since\": 300.0, \
             \"fired\": false}, \"cancels_unresolved#1\": {\"n\": 0, \"at\": 300.0, \"since\": \
             300.0, \"fired\": false}}, \"_invocation\": \"x\", \"_seen\": [\"cancels_unresolved\", \
             \"elapsed_s\"], \"_carry\": []}"
        );
    }

    #[test]
    fn a_new_invocation_is_a_restart_even_when_elapsed_s_grew() {
        let (_, state) = poll(&Obj::default(), r#"{"elapsed_s": 60.0, "fills_unattributed": 5}"#, "a");
        let (lines, _) = poll(&state, r#"{"elapsed_s": 290.0, "fills_unattributed": 3}"#, "b");
        assert!(lines.iter().any(|l| l.contains("RESTARTED")));
        assert!(lines.iter().any(|l| l.contains("fills_unattributed +3 (now 3, process restarted)")));
    }

    #[test]
    fn a_non_numeric_gauge_is_a_fault_not_a_verdict() {
        let (_, state) = poll(&Obj::default(), r#"{"elapsed_s": 60.0, "fills_unattributed": 0}"#, "a");
        let cur = parse(r#"{"elapsed_s": 120.0, "fills_unattributed": null}"#).unwrap();
        assert!(evaluate(&cur, &state, "a").is_err());
    }
}
