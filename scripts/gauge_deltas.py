#!/usr/bin/env python3
"""Turn the armed engine's stats line into watchdog verdicts.

`engine/mod.rs::summary` prints a JSON line to stdout every `--stats-every`
seconds (60 on arbbot-trader-m3) carrying every safety counter the engine has.
Several of those counters carry doc comments asserting they ARE the alarm for a
specific money-losing condition — `fills_unattributed` is documented as "money
that moved in our account that we cannot explain. Must stay 0."

Nothing read them. Measured on 2026-07-29, consumers outside rust/ of
kalshi_fill_gaps, fills_unattributed, dropped_unconsumed, hedges_naked,
cancels_unresolved and exec_failed: zero, each. `scripts/freshness_check.sh` —
the only automated watcher this deployment has — checked unit liveness and file
staleness and never opened the stats line. So every "if this assumption is wrong
the failure is loud, because gauge X moves" mitigation in this codebase was
resting on a number in a log that no process and no human was reading.

TWO RULES, AND WHY THERE ARE TWO
--------------------------------
RISE      fire when the value went UP by more than `threshold` since the last
          poll. The right rule for a counter ("how many happened this window")
          and for a level whose danger is a jump.

SUSTAINED fire when the value has stayed AT OR ABOVE `level` for `polls`
          consecutive samples AND those samples span real time (see
          SUSTAIN_MIN_POLL_S). The right rule for a level whose documented danger
          is "a number that does not come back down" (engine/mod.rs on
          `cancels_unresolved`) — which a RISE rule is structurally blind to,
          because a leak of +1 per poll never jumps.

          The span requirement is not decoration. Counting POLLS alone assumes
          polls are 5 minutes apart, and they are not always: the watchdog timer
          is `Persistent=true`, so missed activations fire back to back after a
          suspend or a boot. Measured on this evaluator: three samples 60s apart
          satisfied "3 consecutive polls" and fired, turning a rule that means
          "this did not clear in a quarter of an hour" into one that means "this
          did not clear in two minutes".

Neither rule ever fires on a level merely being non-zero, because a standing
value would page for ever. `hedges_undischarged` is non-zero right now against a
real naked leg whose owner is arbbot-hedge.timer. An alarm that fires every 30
minutes about a condition the operator already knows about is how the operator
learns to swipe the notification away — and the notification it trains them to
swipe is the same one that one day reads ARMED trader-m3 FAILED.

RISE also absorbs a RATCHET. `cancels_unresolved` is `parked_cancels.len()`;
`engine/cancel.rs::newly_exhausted` MARKS a spent entry rather than removing it,
and `on_venue_answer` removes only on an `ok`, so once a cancel runs out of ways
to be sent that map never returns to zero for the life of the process. A rise
rule measures from wherever the floor now is.

What RISE does NOT absorb is OSCILLATION above that floor, and that is why
`cancels_unresolved` is SUSTAINED and not RISE. `cancel.rs` parks EVERY cancel
on entry, so the gauge is the in-flight cancel count of an engine that cancels
and re-places continuously; a level alternating 0, 2, 0, 2 across polls fires a
RISE rule on half of them, for ever. Measured: 4 pages in 8 polls.

PROCESS RESTARTS
----------------
Every one of these counters is per-process and resets to 0 on restart, so a naive
delta goes negative across one. Restarts are detected by systemd's
`InvocationID`, which is a fresh UUID for every start of the unit and is the only
thing here that identifies a process EXACTLY. `elapsed_s` decreasing is kept as a
fallback for when the id cannot be read.

`elapsed_s` ALONE used to be the discriminator, and the comment here used to say
its blind spot "degrades toward under-counting". That was true for RISE, which
clamps with `base = min(base, cur)` — and FALSE, in the dangerous direction, for
SUSTAINED, which inherits the dead process's streak. Driven through this
evaluator: a process sampled at elapsed_s 60 and 120 with `cancels_unresolved`
at 2 (streak 1, then 2), then killed; the REPLACEMENT process's first sample at
elapsed_s 290 is HIGHER, so no restart was detected, the streak advanced to 3,
and a brand-new process paged on its first observation. A mitigation that is
documented backwards is worse than one that is absent, because nobody re-derives
it.

Both are now reset on a new `InvocationID`: baselines to 0, sustain streaks to
nothing.

A detected restart is also REPORTED, once, as its own page. Every counter here
is per-process, so a restart is the one event that resets them all — and
measured 2026-08-09, an armed SIGABRT (malloc_consolidate, orders left resting,
swept 30s later by the auto-restart) paged nothing: freshness_check.sh pages on
is-failed, which an auto-restart satisfies for ~30 seconds between five-minute
polls, so the crash of the one real-money process was invisible by timing.

VERDICTS SURVIVE BOTH THE COOLDOWN AND A RESTART
------------------------------------------------
freshness_check.sh shares one 30-minute cooldown across every check in the file,
so an unrelated outage can suppress the POST that would have carried a gauge
verdict. Undelivered verdicts are therefore persisted as TEXT in `_carry` and
re-emitted every poll until an alert actually goes out.

The first cut of this held the numeric BASELINE back instead, which reads as
equivalent and is not: the held baseline belongs to the OLD process, so if the
engine restarted before the cooldown expired the restart rule dropped it to 0,
the fresh process read 0, and the verdict was lost permanently. The armed unit
restarted 40 minutes after the 2026-07-29 00:54:51 naked-leg event, so that is
the normal case and not a corner. Text carries across a restart; a number
cannot.

COLD START
----------
With no state file — first run, /tmp cleared by a reboot, or a state file this
cannot parse — we seed the baseline and report NOTHING. A reboot restarts the
engine too, so its counters are fresh in the same instant.

Falling back to a cold start on an unreadable state file is the whole recovery
story and it must never be an exception: an uncaught one exits non-zero, which
makes freshness_check.sh page "gauge check FAILED to run" every 30 minutes AND
read no gauge at all, until a human deletes the file by hand. Loud and blind at
once, with manual-only recovery, is strictly worse than the gap this file
closes.
"""

import json
import os
import sys
import tempfile

# The shortest gap between two samples that counts as a full poll apart, for the
# SPAN a SUSTAINED rule must cover. 240s is the watchdog's 5-minute cadence with
# 20% slack for timer drift. A rule of `polls` needs (polls - 1) * this much
# engine uptime between its first and last sample, so back-to-back catch-up runs
# cannot satisfy it — see the SUSTAINED note above.
SUSTAIN_MIN_POLL_S = 240

# How far the engine's own clock may advance between two observations that still
# count as CONSECUTIVE. 900s is 3x the watchdog's 5-minute cadence, which
# absorbs ordinary timer drift (OnUnitActiveSec plus Persistent=true) and
# nothing else.
#
# Without this the streak bridges any hole. Measured: a level at 2, observed
# twice, then EIGHT polls with no stats line at all (a wedged engine, same
# process, so no restart to reset it), then observed at 2 once more — fired,
# saying "held for 3 consecutive polls" about three samples spanning 55 minutes.
# "We saw it high 3 times running" and "we saw it high 3 times with an unknown
# hole in the middle" are different facts, and a rule whose message cannot tell
# them apart is the same defect this whole campaign keeps finding. A hole means
# we do not know it stayed up, so the streak restarts — and the hole itself is
# not unreported, because an engine that stops printing stats pages on its own.
SUSTAIN_MAX_GAP_S = 900

# gauge -> (rule, severity, threshold, what it MEANS)
#
# For RISE, `threshold` is the largest increase in one watchdog poll (5 min)
# that stays SILENT; 0 therefore means "fire on any increase at all".
# For SUSTAINED, `threshold` is the level that must persist.
#
# EVIDENCE, AND ITS LIMITS. The armed unit has emitted 892 stats lines. It has
# NOT emitted all of these gauges for all of them — each was added to
# `summary()` at a different time, and a gauge reads 0 in a line that predates
# it for reasons that have nothing to do with the engine being healthy. The
# per-gauge presence counts below are the real evidence base and some are thin.
# Where a threshold is not supported by data, it says so.
WATCH = [
    # ---- RISE, threshold 0. Each names a specific money condition and has
    # ---- never been observed non-zero on a binary that emitted it.
    (
        "fills_unattributed",
        "RISE",
        "PAGE",
        0,
        "money moved in this account that the engine cannot attribute to any "
        "order it placed — RECONCILE BY HAND",
    ),  # doc: "Must stay 0." Present in 684/892 armed lines, max 0.
    (
        "hedges_overfilled",
        "RISE",
        "PAGE",
        0,
        "hedge contracts filled beyond what an obligation owed — a position "
        "with no maker leg to pair it with",
    ),  # doc: "Must stay 0." Present in 684/892, max 0.
    (
        "dropped_unconsumed",
        "RISE",
        "PAGE",
        0,
        "an obligation was minted and never hedged (arb_core::fill Drop alarm) "
        "— nothing will retry it",
    ),  # doc: "programming-bug alarm ... must stay 0." Present in 892/892, max 0.
    (
        "hedges_naked",
        "RISE",
        "PAGE",
        0,
        "a maker leg filled and its hedge is past its deadline — real "
        "directional exposure, one count per obligation",
    ),  # NO "must stay 0" doc anywhere, and the earlier claim that there was one
    # is withdrawn. It is justified instead by the alarm TEXT the engine already
    # prints beside it ("[hedge] NAKED ... the book has not offered a price that
    # keeps the basket profitable") and by `p.alarmed` latching it to one count
    # per obligation, so it cannot spam. Present in 892/892, max 1 — and that 1
    # is the real 2026-07-29 00:54:51 event nothing paged about.
    (
        "exec_dropped",
        "RISE",
        "PAGE",
        0,
        "an executor channel was full and a place or cancel was LOST — a "
        "dropped cancel is an order still resting at a price we rejected",
    ),  # NO "must stay 0" doc either; `exec.rs` says only "engine try_send
    # failures (executor backlogged)". Justified instead by `Engine::dispatch`'s
    # own doc: "Returns false when the channel is full — the command is LOST and
    # only the counter moves". Present in 892/892, max 0 — and 0 across 1613
    # dry-run lines that dispatched 20800 places, which for THIS gauge is real
    # corroboration because the shadow unit exercises `dispatch` even with no
    # order path behind it.
    (
        "exec_recovered",
        "RISE",
        "PAGE",
        0,
        "an order reached a venue under an id this process never learned — "
        "unaddressable by any cancel, and it may already have EXECUTED",
    ),  # doc: "Never routine; must never be silent." Present in only 252/892.
    # #43 (2026-07-29) sharpened what a recovery means: Kalshi's search covers
    # its whole order history, so an adopted order may already be EXECUTED —
    # "the one where a duplicate hedge may be on the book". That is a stronger
    # reason to page on it, not a weaker one.
    (
        "kalshi_fills_unreadable",
        "RISE",
        "PAGE",
        0,
        "a Kalshi fill frame's count could not be read — while this rises the "
        "`fills` total reads 0 and EVERY other gauge here looks healthy",
    ),  # doc: "Must stay 0." Present in only 252/892.
    # ---- RISE, with a real benign rate.
    (
        "exec_failed",
        "RISE",
        "PAGE",
        60,
        "venue rejections in one window — at this rate the order path is not "
        "working, whatever the individual errors say",
    ),  # A JUDGEMENT CALL, NOT A MEASUREMENT — said plainly because the first
    # version of this number was a guess wearing the words "5x the worst session
    # ever observed", which is exactly the shape of claim that got caught in
    # review. That framing is withdrawn: the all-time armed max is 4, but those 4
    # were a BURST inside seconds, and a burst rate implies nothing about a
    # 5-minute total — the same shape sustained is ~200/window, so the observed
    # maximum bounds neither direction.
    #
    # 60 is therefore NOT derived from this gauge's history at all. It is picked
    # as a rate the order path cannot plausibly be working at, and its real
    # justification is that the conditions it would catch are all covered
    # elsewhere by rules that ARE grounded: a refused cancel shows up in
    # cancels_unresolved, a lost command in exec_dropped, an order adopted after
    # a lost response in exec_recovered. This one is the coarse backstop for
    # "the venue is refusing everything", and it is deliberately set where only
    # that reads. Revise it from data the first time it fires. Present in
    # 892/892.
    (
        "kalshi_reconcile_failures",
        "RISE",
        "PAGE",
        2,
        "the Kalshi fill reconciliation could not run — while this rises the "
        "fill totals are a local sum with no venue truth behind them",
    ),  # doc says "Must stay 0", but its listed causes include "background
    # budget spent", which is routine and self-heals on the next reconcile, so
    # this is not a 0 threshold. THIN EVIDENCE: present in only 55/892 lines —
    # under an hour of armed runtime. Treat 2 as a first guess, not a finding.
    (
        "kalshi_fill_gaps",
        "RISE",
        "NOTE",
        3,
        "the Kalshi fill feed reconnected repeatedly; each window may hide a "
        "fill. The repair for it (kalshi_reconcile_failures) pages separately",
    ),  # 1 is the documented per-process FLOOR, so this is the one gauge where
    # a 0 threshold would fire on every single restart. Present in 252/892.
    (
        "sweeps_owed",
        "RISE",
        "PAGE",
        0,
        "a venue's kill sweep was never proven — the book this process halted "
        "over was not confirmed empty, and orders may be resting on it",
    ),  # Added by #44, which landed while this branch was in review, and whose
    # own doc ends "Nothing reads it automatically" — the exact defect this file
    # exists to close, recurring in the same tree. Watching it is the point.
    #
    # NO OBSERVED HISTORY AT ALL: it did not exist when the 920 armed stats lines
    # were written, so the presence counts above have nothing to say about it.
    # What bounds the noise here is STRUCTURAL, not empirical — `sweeps_owed` is
    # a BTreeMap keyed by Venue, so `.len()` is 0, 1 or 2, and a RISE rule can
    # therefore fire at most twice in the life of a process.
    #
    # RISE and not SUSTAINED because it is a deliberate RATCHET: its doc says it
    # "does NOT come back down when a halt clears over an unproven book", so a
    # session that fully recovered from a morning outage reads non-zero for the
    # rest of its life. A level rule would page about that for ever; a rise rule
    # says "a book just went unproven", which is the event, and then goes quiet.
    (
        "maker_exit_unresolved",
        "RISE",
        "PAGE",
        0,
        "a Kalshi exit ask FILLED and its PM-US close did not complete — the "
        "account is one-legged and the ledger still calls the basket OPEN. "
        "maker_exit::heal is retrying it every 60s against venue truth and "
        "crosses out after ~10 min; read maker_exit_healed to see if it won. "
        "ACT ONLY IF THIS IS STILL OUTSTANDING IN ~15 MINUTES",
    ),  # doc (maker_exit.rs): the module's own alarm gauge. It is an INCIDENT
    # count, not a level — `maker_exit_healed` is how many of these the engine
    # closed by itself, and the difference is what is naked right now. The old
    # copy here said "latched off until restart", which was true until the
    # self-heal landed and is now the opposite of what an operator should do:
    # restarting throws away the exit debounce and the queue for a leg that was
    # already being fixed. Still cannot spam — MAX_RESTING is 1, so there is at
    # most one exit in flight and therefore at most one incident at a time, and
    # a heal takes cycles — but it is no longer bounded at one per PROCESS.
    (
        "positions_recon_act_unresolved",
        "RISE",
        "PAGE",
        0,
        "a recon-act order the venue ACCEPTED whose fate this process could "
        "not read — contracts that may be in the account and are not in the "
        "ledger",
    ),  # doc (engine/mod.rs): "MUST STAY 0 ... Alarm on any change" — and,
    # measured 2026-08-14, nothing read it: the exact defect this file exists
    # to close, recurring in the same tree. Present in the live stats line,
    # max 0 across ~5.9k recon cycles (which have placed 0 orders).
    (
        "positions_recon_acted",
        "RISE",
        "NOTE",
        0,
        "the naked-leg completer ACTED for the first time in this process — "
        "read it against maker_exit_unresolved, because the two climbing "
        "together is the documented maker-exit/recon-act fight",
    ),  # NOTE, not PAGE: acting is the feature working (profitable-only, 5
    # ct/order, 2/cycle). It has been 0 for the life of the deployment against
    # thousands of refusals, so the first move is worth one low-priority line;
    # every money condition it can CREATE pages via the rows above.
    # ---- SUSTAINED. See the two-rules note above.
    (
        "cancels_unresolved",
        "SUSTAINED",
        "PAGE",
        (2, 3),
        "cancels the engine decided and the venue never confirmed — not in "
        "flight, stuck, and each may be an order resting at a price already "
        "rejected",
    ),  # doc: "Healthy is 0, or a transient handful; a number that does not
    # come back down is orders resting that this engine has already decided
    # against." That is a SUSTAINED condition, and RISE cannot see it: a leak
    # climbing +1 per poll from 0 to 12 emits nothing under RISE, while an
    # ordinary 0,2,0,2 oscillation fires it on half of all polls. Present in
    # 684/892, max 0 — but those 684 are all from a probe regime
    # (--tt-max-clip 5, 3 relationships), the regime LEAST able to put two
    # cancels on the wire at once. "max 0" is therefore weak evidence that 2 is
    # rare and no evidence at all about heavier quoting.
    (
        "cancels_unresolved",
        "SUSTAINED",
        "NOTE",
        (1, 12),
        "at least one cancel has been owed and unconfirmed for about an hour — "
        "below the paging bar, but it is not clearing either",
    ),  # The floor the row above cannot see. A level PINNED AT EXACTLY 1 never
    # reaches a bar of 2, and neither does 1,3,1,3 — both verified silent for
    # ever against the previous table. That is precisely the gauge's OWN
    # documented failure mode ("a number that does not come back down"), and
    # `cancels_unaddressable`, which would have named the benign case, is
    # deliberately unwatched. Leaving it invisible would have been this PR
    # shipping the exact defect it exists to close.
    #
    # NOTE and not PAGE, and an hour rather than a quarter of one, because the
    # doc also says the commonest single parked entry is a place the venue
    # REJECTED, where nothing rests and nothing is wrong. This fires ONCE per
    # streak, so the permanently-parked benign entry costs one low-priority line
    # per session, not a page every 30 minutes.
    #
    # A level stuck at 2+ trips both rows: a page at ~15 minutes and this note at
    # ~an hour. That is deliberate — the second one says the first was not acted
    # on — and it is one extra low-priority line, not a duplicate page.
]

# Gauges deliberately NOT watched, and why. Kept here because "we looked at it
# and decided no" is the part that gets lost, and the next person to notice one
# of these is unread will otherwise add it and discover the noise by paging.
#
#   hedges_undischarged     A STARTUP CONSTANT (`cfg.hedges_undischarged`), not
#                           a runtime gauge: it never moves within a process. It
#                           is non-zero today. Under the restart rule its
#                           baseline drops to 0 on every restart, so any
#                           non-zero threshold on it fires on EVERY restart, for
#                           ever. Its owner is arbbot-hedge.timer; the runtime
#                           condition that matters is `hedges_naked`.
#   kalshi_reconcile_rejected  Its own doc says it is NOT a must-stay-0 gauge and
#                           is EXPECTED on a busy account, and that no count
#                           derived from it can separate the benign cause (REST
#                           lag) from the one that matters (mismatched id
#                           spaces) — the discriminant is an order id repeating
#                           in a log line, which no threshold can see.
#   kalshi_fills_recovered  Its doc: "Not an error: nonzero is the repair
#                           working." Watching it would page on the fix.
#   cancels_unaddressable   A subset of cancels_unresolved, and its doc says the
#                           commonest member is a place the venue REJECTED where
#                           nothing rests and nothing is wrong.
#   fills_unclaimed         Transient by construction; a fill that stays
#                           unclaimed becomes `fills_unattributed`, which pages.
#   kalshi_fill_dust_hundredths  Sub-contract dust. Its failure mode is "sits
#                           high", which SUSTAINED could now express — but no
#                           threshold for it is defensible from anything
#                           observed, and a guessed one on a gauge nobody has
#                           seen move is how this channel becomes noise.
#   chan_high_water, decision_latency, risk_reserved, killed, feed_pulled
#                           Performance and operator-state, not money moving.

# Verdicts held for a later poll, capped. The cap only bites if the cooldown has
# been suppressing for hours, at which point the operator has a bigger problem
# than a truncated list.
CARRY_MAX = 12


def load_state(path):
    """Previous baselines, or {} — which means COLD START, never a crash.

    Any unreadable state must degrade to seeding, because the alternative is an
    alarm that pages for ever and reads nothing until a human intervenes.
    """
    try:
        with open(path) as f:
            prev = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        sys.stderr.write(
            f"gauge_deltas: {path} unreadable ({e}) — treating as a COLD START. "
            "This poll reports nothing and reseeds; the next one is normal.\n"
        )
        return {}
    if not isinstance(prev, dict):
        sys.stderr.write(f"gauge_deltas: {path} is not an object — COLD START.\n")
        return {}
    return prev


def write_state(path, obj):
    """Same-directory temp + atomic replace.

    Writing in place truncates first, so any death in that window leaves a
    partial file that the next run would adopt as a baseline.
    """
    d = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".gauge-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    if len(sys.argv) != 5:
        sys.stderr.write(
            "usage: gauge_deltas.py STATE PENDING_DELIVERED PENDING_HELD "
            "INVOCATION_ID < stats-line\n"
        )
        return 2
    state_path, pend_ok, pend_held = sys.argv[1], sys.argv[2], sys.argv[3]
    # systemd's InvocationID for the unit: a fresh UUID per start, and the only
    # exact process identity available here. REQUIRED as an argument (empty
    # string when systemd cannot supply one) rather than optional, so a caller
    # that forgets it fails loudly instead of silently falling back.
    invocation = sys.argv[4].strip()

    cur = json.loads(sys.stdin.read())
    prev = load_state(state_path)

    # Verdicts from an earlier poll whose alert never went out. Kept UNPREFIXED
    # in `carried` and re-carried unprefixed, so a verdict held across several
    # polls does not accumulate a "(held) (held) (held)" prefix.
    carried = [c for c in prev.get("_carry", []) if isinstance(c, str)][:CARRY_MAX]
    out = []

    # A watched name vanishing from the summary is the single most likely way
    # this alarm dies quietly: a rename in engine/mod.rs and every check below
    # starts reading nothing while reporting all-clear. `elapsed_s` is in this
    # scan even though it is not a gauge — it is the restart discriminator, and
    # losing it silently would make every standing gauge page its full value in
    # one burst.
    #
    # Reported when the absent SET GROWS, not while it is non-empty, for the
    # same reason every other verdict here is a change. Replaying the 892 real
    # armed stats lines through an unconditional version produced 168 identical
    # pages: the early sessions ran a binary that predated seven of these
    # gauges, and a downgrade would do it again.
    # set(): one gauge may appear in WATCH twice (two sustain levels), and a
    # duplicate here would name it twice in the page.
    names = set(g for g, _, _, _, _ in WATCH) | {"elapsed_s"}
    absent = sorted(n for n in names if n not in cur)
    new_absent = [n for n in absent if n not in prev.get("_absent", [])]
    # A name we have SEEN and then lost is a rename, and it is a page: the check
    # that used to read it now reads nothing while reporting all-clear.
    #
    # A name we have NEVER seen is a different thing entirely — the running
    # engine simply predates it. Found by running this against the live journal:
    # the armed process was started before #44 landed, so it does not emit
    # `sweeps_owed`, and an undifferentiated rule pages on the very first poll
    # after deployment about a gauge nobody could have been reading yet. True,
    # and not actionable, which is the definition of the noise this file is
    # supposed to avoid. It goes out as a NOTE, and turns into a page the moment
    # the gauge has actually been seen and then disappears.
    prev_seen = set(prev.get("_seen", []))
    vanished = [n for n in new_absent if n in prev_seen]
    never_seen = [n for n in new_absent if n not in prev_seen]
    if vanished:
        out.append(
            "PAGE|ARMED trader-m3 stats line no longer carries "
            + ", ".join(vanished)
            + " — those safety gauges are UNWATCHED (engine summary renamed?)"
        )
    if never_seen:
        out.append(
            "NOTE|ARMED trader-m3 has never emitted "
            + ", ".join(never_seen)
            + " — the running engine predates that gauge, so it is unwatched "
            "until this unit restarts onto a newer build"
        )

    # No elapsed_s means no restart discriminator, so every delta below would be
    # computed against a baseline that may belong to another process. Seed
    # instead; the page above already said why.
    cold = not prev or "elapsed_s" not in cur
    # InvocationID first: it is exact. `elapsed_s` decreasing is the fallback for
    # when systemd gives us nothing — it catches the common restart and misses
    # the one where the replacement process is already OLDER than the dead one
    # was when last sampled, which is the false page described in the docstring.
    prev_inv = prev.get("_invocation") or ""
    restarted = not cold and (
        (bool(invocation) and bool(prev_inv) and invocation != prev_inv)
        or cur.get("elapsed_s", 0) < prev.get("elapsed_s", 0)
    )
    # Say the restart itself, once. Until 2026-08-14 a restart only reset
    # baselines, silently — see the module docstring for the 2026-08-09 armed
    # SIGABRT that missed. A deliberate restart costs one page, which for a
    # unit restarted about fortnightly is the right price for never missing a
    # crash of the one process that can hold real orders.
    if restarted:
        out.append(
            "PAGE|ARMED trader-m3 RESTARTED (new process; every gauge baseline "
            "reset) — check journalctl for the previous exit code and whether "
            "startup_sweep found orders resting"
        )

    prev_streak = prev.get("_streak", {})
    if not isinstance(prev_streak, dict):
        prev_streak = {}
    streak = {}

    for gauge, rule, sev, thresh, why in WATCH:
        if gauge not in cur:
            continue
        if rule == "SUSTAINED":
            level, polls = thresh
            # Keyed per ROW, not per gauge: one gauge may carry two sustain rules
            # at different levels (cancels_unresolved does), and sharing one
            # streak between them would let the looser rule reset the stricter.
            key = f"{gauge}#{level}"
            now_s = float(cur.get("elapsed_s", 0) or 0)
            p = prev_streak.get(key)
            p = p if isinstance(p, dict) else {}
            n, at, since, fired = 0, 0.0, now_s, False
            try:
                n = int(p.get("n", 0) or 0)
                at = float(p.get("at", 0) or 0)
                since = float(p.get("since", now_s) or now_s)
                fired = bool(p.get("fired", False))
            except (TypeError, ValueError):
                n, at, since, fired = 0, 0.0, now_s, False
            # A restart resets: a fresh process has not held anything anywhere
            # for any length of time. So does a hole — see SUSTAIN_MAX_GAP_S.
            broken = cold or restarted or (now_s - at) > SUSTAIN_MAX_GAP_S
            if cur[gauge] < level:
                n, fired = 0, False
            elif broken or n == 0:
                # `n == 0` matters as much as `broken`: a streak restarting
                # after a DIP must re-date itself too, or the span reported
                # below is measured from a run that already ended.
                n, since, fired = 1, now_s, False
            else:
                n += 1
            span = now_s - since
            # BOTH conditions, and `fired` so it says it once. Counting samples
            # alone would let three back-to-back catch-up runs satisfy a rule
            # that is supposed to mean "this did not clear in a quarter of an
            # hour"; requiring span alone would fire off two samples an hour
            # apart with no idea what happened between them.
            if not fired and n >= polls and span >= (polls - 1) * SUSTAIN_MIN_POLL_S:
                fired = True
                out.append(
                    f"{sev}|ARMED trader-m3 {gauge} has held at {cur[gauge]} across "
                    f"{n} consecutive samples spanning {int(span)}s of engine "
                    f"uptime — {why}"
                )
            streak[key] = {"n": n, "at": now_s, "since": since, "fired": fired}
            continue
        if cold:
            continue
        base = 0 if restarted else prev.get(gauge, 0)
        # Belt and braces: a level that fell needs its baseline to fall with it,
        # or its next genuine rise is measured from a peak it will not reach.
        base = min(base, cur[gauge])
        delta = cur[gauge] - base
        if delta > thresh:
            out.append(
                f"{sev}|ARMED trader-m3 {gauge} +{delta} (now {cur[gauge]}"
                f"{', process restarted' if restarted else ''}) — {why}"
            )

    # Held verdicts lead — the oldest unreported thing is the one most at risk
    # of never being said — and are marked so the operator can tell a fresh
    # event from a replayed one.
    for c in carried:
        sev, _, text = c.partition("|")
        print(f"{sev}|(held from an earlier poll) {text}")
    for line in out:
        print(line)

    # Two candidate next states; freshness_check.sh adopts exactly one once it
    # knows whether a POST actually went out. The numeric baseline advances in
    # BOTH — holding a number back does not survive a restart (see the module
    # docstring), only the text does.
    base_state = {g: cur[g] for g, _, _, _, _ in WATCH if g in cur}
    base_state["elapsed_s"] = cur.get("elapsed_s", 0)
    base_state["_absent"] = absent
    base_state["_streak"] = streak
    base_state["_invocation"] = invocation
    # Every watched name this deployment has ever actually observed. Only ever
    # grows: it is what separates "renamed away" from "not built yet".
    base_state["_seen"] = sorted(prev_seen | {n for n in names if n in cur})

    # Deduplicated: a condition that keeps re-firing while suppressed leaves one
    # entry, not one per poll.
    seen, keep = set(), []
    for c in carried + out:
        if c not in seen:
            seen.add(c)
            keep.append(c)
    held = dict(base_state, _carry=keep[-CARRY_MAX:])
    delivered = dict(base_state, _carry=[])

    write_state(pend_ok, delivered)
    write_state(pend_held, held)
    return 0


if __name__ == "__main__":
    sys.exit(main())
