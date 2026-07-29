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

THE RULE: DELTAS, NEVER LEVELS
------------------------------
Every verdict below is on the CHANGE since the previous watchdog poll, not on
the absolute value. That is not a stylistic choice, it is the only rule that
does not degenerate:

  * A standing value would page for ever. `hedges_undischarged` is non-zero
    right now against a real naked leg with a known owner (arbbot-hedge.timer).
    An alarm that fires every 30 minutes about a condition the operator already
    knows about is how the operator learns to swipe the notification away — and
    the notification it trains them to swipe is the same one that one day reads
    ARMED trader-m3 FAILED.
  * Half of these are LEVELS, not counters, and one of them ratchets.
    `cancels_unresolved` is `parked_cancels.len()`, and `newly_exhausted` marks
    a spent entry as logged WITHOUT REMOVING IT (engine/cancel.rs) — so once a
    cancel runs out of ways to be sent, that map never returns to zero for the
    life of the process. Any "level above N" rule on it becomes chronic noise on
    the first benign venue-rejected place. A rise-based rule absorbs the ratchet
    into the baseline.

One rule covers both shapes: `delta = now - last_seen`, page when the delta
crosses this gauge's threshold. For a monotone counter that is "how many
happened this window". For a level it is "how much worse it got". For a
ratcheted level the floor contributes nothing.

PROCESS RESTARTS
----------------
Every one of these counters is per-process and resets to 0 on restart, so a naive
delta goes negative across one. `elapsed_s` is the discriminator: it rises
monotonically within a process, so a stats line whose `elapsed_s` is BELOW the
last one we saw is a new process, and every baseline drops to 0. Evaluating the
new process's values against 0 rather than against the old process's values is
deliberate and is the conservative direction — a fresh process reporting a
non-zero must-stay-0 gauge is exactly the thing that must not be swallowed.

That has one blind spot, stated plainly: if the old process was itself young when
we last sampled it (say elapsed_s 60) and the restart happened just after, the
new process can be at elapsed_s 290 by the next poll, which is HIGHER, so the
restart is not detected and the delta is computed across two processes. It
degrades toward under-counting a gauge that was already non-zero in the old
process, which for every must-stay-0 gauge here means the old process had already
paged.

COLD START
----------
With no state file (first ever run, or /tmp cleared by a reboot) we seed the
baseline and report NOTHING. A reboot restarts the engine too, so its counters
are fresh in the same instant; seeding against a live process is the only case
where this can swallow something, and installing this is a supervised act.
"""

import json
import os
import sys

# gauge -> (severity, threshold, what a rise MEANS)
#
# Threshold is the largest delta in one watchdog poll (5 min) that stays SILENT.
# 0 therefore means "page on any increase at all".
#
# Every "empirically" note below is measured against the 848 stats lines the
# ARMED unit has emitted across its 10 sessions (journalctl -u arbbot-trader-m3),
# which is the entire history of this engine holding real money.
WATCH = [
    # ---- must-stay-0: each is a doc comment in engine/mod.rs or exec.rs that
    # ---- names a specific money condition. All 11 read 0 in all 848 lines, so
    # ---- a 0 threshold has an observed false-page rate of zero.
    (
        "fills_unattributed",
        "PAGE",
        0,
        "money moved in this account that the engine cannot attribute to any "
        "order it placed — RECONCILE BY HAND",
    ),
    (
        "hedges_overfilled",
        "PAGE",
        0,
        "hedge contracts filled beyond what an obligation owed — a position "
        "with no maker leg to pair it with",
    ),
    (
        "dropped_unconsumed",
        "PAGE",
        0,
        "an obligation was minted and never hedged (arb_core::fill Drop alarm) "
        "— nothing will retry it",
    ),
    (
        "hedges_naked",
        "PAGE",
        0,
        "a maker leg filled and its hedge is past its deadline — real "
        "directional exposure, one count per obligation",
    ),
    (
        "exec_dropped",
        "PAGE",
        0,
        "an executor channel was full and a place or cancel was LOST — a "
        "dropped cancel is an order still resting at a price we rejected",
    ),
    (
        "exec_recovered",
        "PAGE",
        0,
        "an order was live at a venue under an id this process never learned — "
        "unaddressable by any cancel, and a fill on it arrives unattributable",
    ),
    (
        "kalshi_fills_unreadable",
        "PAGE",
        0,
        "a Kalshi fill frame's count could not be read — while this rises the "
        "`fills` total reads 0 and EVERY other gauge here looks healthy",
    ),
    # ---- rate-based: these have a real, benign, non-zero rate, so a 0
    # ---- threshold would train the operator to ignore the channel.
    (
        "exec_failed",
        "PAGE",
        20,
        "venue rejections in one window — the order path is refusing at a rate "
        "this deployment has never produced (worst armed session total: 4)",
    ),
    (
        "kalshi_reconcile_failures",
        "PAGE",
        2,
        "the Kalshi fill reconciliation could not run — while this rises the "
        "fill totals are a local sum with no venue truth behind them",
    ),
    (
        "cancels_unresolved",
        "PAGE",
        1,
        "cancels the engine has decided and the venue has not confirmed — each "
        "one may be an order resting at a price already rejected",
    ),
    (
        "kalshi_fill_gaps",
        "NOTE",
        3,
        "the Kalshi fill feed reconnected repeatedly; each window may hide a "
        "fill. The repair for it (kalshi_reconcile_failures) pages separately",
    ),
]

# Gauges deliberately NOT watched, and why. Kept here because "we looked at it
# and decided no" is the part that gets lost, and the next person to notice one
# of these is unread will otherwise add it and discover the noise by paging.
#
#   hedges_undischarged     A STARTUP CONSTANT (`cfg.hedges_undischarged`), not
#                           a runtime gauge: it never moves within a process,
#                           and it is non-zero today. Under the restart rule its
#                           baseline drops to 0 on every restart, so any non-zero
#                           threshold on it fires on EVERY restart, for ever. Its
#                           owner is arbbot-hedge.timer; the runtime condition
#                           that matters is `hedges_naked`, which is watched.
#   kalshi_reconcile_rejected  Its own doc says it is NOT a must-stay-0 gauge and
#                           is EXPECTED on a busy account, and that no count
#                           derived from it can separate the benign cause (REST
#                           lag) from the one that matters (mismatched id
#                           spaces) — the discriminant is an order id repeating
#                           in a log line, which no threshold can see.
#   cancels_unaddressable   A subset of cancels_unresolved, and its doc says the
#                           commonest member is a place the venue REJECTED where
#                           nothing rests and nothing is wrong.
#   fills_unclaimed         Transient by construction; a fill that stays
#                           unclaimed becomes `fills_unattributed`, which pages.
#   kalshi_fill_dust_hundredths  Sub-contract dust. Its failure mode is "sits
#                           high", i.e. a level, which the delta rule cannot see.
#   chan_high_water, decision_latency, risk_reserved, killed, feed_pulled
#                           Performance and operator-state, not money moving.
#                           `killed` and `feed_pulled` are usually the operator's
#                           own action.


def main() -> int:
    if len(sys.argv) != 3:
        sys.stderr.write("usage: gauge_deltas.py STATE_PATH PENDING_PATH < stats-line\n")
        return 2
    state_path, pending_path = sys.argv[1], sys.argv[2]

    cur = json.loads(sys.stdin.read())

    prev = {}
    if os.path.exists(state_path):
        with open(state_path) as f:
            prev = json.load(f)

    out = []

    # A gauge that vanished from the summary is the single most likely way this
    # alarm dies quietly: a rename in engine/mod.rs and every check below starts
    # reading nothing while reporting all-clear. Say so instead.
    #
    # Reported when the absent SET GROWS, not while it is non-empty, for the same
    # reason every other verdict here is a delta. Replaying the 848 real armed
    # stats lines through an unconditional version of this produced 168 identical
    # pages: the early sessions ran a binary that predated seven of these gauges,
    # and a downgrade would do it again — a page every 30 minutes about a
    # condition that does not change until someone deploys. A cold start still
    # reports whatever is missing, because `prev` has no recorded set and every
    # absent gauge is therefore new.
    absent = sorted(g for g, _, _, _ in WATCH if g not in cur)
    new_absent = [g for g in absent if g not in prev.get("_absent", [])]
    if new_absent:
        out.append(
            "PAGE|ARMED trader-m3 stats line no longer carries "
            + ", ".join(new_absent)
            + " — those safety gauges are UNWATCHED (engine summary renamed?)"
        )

    cold = not prev
    # Counters are per-process. A stats line older on the engine's own clock
    # than the one we last saw is a new process; every baseline drops to 0.
    restarted = not cold and cur.get("elapsed_s", 0) < prev.get("elapsed_s", 0)

    if not cold:
        for gauge, sev, thresh, why in WATCH:
            if gauge not in cur:
                continue
            base = 0 if restarted else prev.get(gauge, 0)
            # Belt and braces: a level that fell needs its baseline to fall with
            # it, or its next genuine rise is measured from a peak it will not
            # reach again.
            base = min(base, cur[gauge])
            delta = cur[gauge] - base
            if delta > thresh:
                out.append(
                    f"{sev}|ARMED trader-m3 {gauge} +{delta} (now {cur[gauge]}"
                    f"{', process restarted' if restarted else ''}) — {why}"
                )

    for line in out:
        print(line)

    # Written unconditionally, ADOPTED conditionally: freshness_check.sh renames
    # this over the state file only once the alert carrying these verdicts was
    # actually delivered. Advancing the baseline under a suppressed alert would
    # silently retire the very increment we failed to report.
    keep = {g: cur[g] for g, _, _, _ in WATCH if g in cur}
    keep["elapsed_s"] = cur.get("elapsed_s", 0)
    keep["_absent"] = absent
    with open(pending_path, "w") as f:
        json.dump(keep, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
