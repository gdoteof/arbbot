#!/usr/bin/env bash
# Mutation check: "does any test actually PIN this guard?"
#
# Usage: mutate.sh <test-name-filter> [package]      (default package: arb-trader)
#
# Break a guard by hand, then run this with a filter naming the tests that are
# supposed to catch it. It answers with one of FOUR states, because "no test
# failed" has three very different causes and two of them are not evidence:
#
#   RED         >=1 test failed. The guard IS pinned. This is the good outcome
#               of a mutation run, so it exits 0.
#   GREEN       the filter matched tests, they all passed. NOTHING PINS THE
#               GUARD YOU BROKE.
#   NO-MATCH    the tree built and the filter matched no test at all. `cargo
#               test` prints `test result: ok. 0 passed; 0 failed` for this and
#               it reads exactly like a pass — the trap this script exists for.
#   NO-BUILD    the tree did not compile, so the failure list came back empty
#               for a reason that has nothing to do with the guard. This has
#               bitten PR #49 twice: the workspace `deny`s `unused`, so a
#               mutation that orphans a variable does not build, and an empty
#               failure list reads identically to "no test pins this".
#
# Exit codes: 0 RED, 1 GREEN, 2 NO-MATCH, 3 NO-BUILD, 4 bad usage.
#
# This is a DEVELOPMENT aid, not a gate. `scripts/gate.sh` is the gate; nothing
# here touches digests, and a mutated tree must never be committed.
set -uo pipefail

filter=${1:-}
pkg=${2:-arb-trader}
if [ -z "$filter" ]; then
  echo "usage: mutate.sh <test-name-filter> [package]" >&2
  exit 4
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT/rust" || exit 4

out=$(cargo test -p "$pkg" "$filter" 2>&1)
code=$?
echo "$out"
echo "---------------------------------------------------------------"

# One `test result:` line per test BINARY, so both numbers are summed across
# binaries: a filter that matches only lib tests legitimately scores 0/0 in
# `determinism.rs`, and that must not read as NO-MATCH on its own.
results=$(echo "$out" | grep -c '^test result:')
passed=$(echo "$out" | sed -n 's/^test result:.* \([0-9]\+\) passed.*/\1/p' | paste -sd+ - | bc 2>/dev/null)
failed=$(echo "$out" | sed -n 's/^test result:.* \([0-9]\+\) failed.*/\1/p' | paste -sd+ - | bc 2>/dev/null)
passed=${passed:-0}
failed=${failed:-0}

if [ "$results" -eq 0 ]; then
  echo "NO-BUILD — the tree did not compile or no test binary ran (cargo exit $code)."
  echo "           NOT EVIDENCE OF ANYTHING. Fix the mutation so it builds, then re-run."
  exit 3
fi
if [ "$failed" -gt 0 ]; then
  echo "RED — $failed failed, $passed passed. The guard is PINNED by:"
  echo "$out" | sed -n 's/^test \(.*\) \.\.\. FAILED$/    \1/p'
  exit 0
fi
if [ "$passed" -eq 0 ]; then
  echo "NO-MATCH — the tree built and the filter '$filter' matched NO test."
  echo '           "test result: ok. 0 passed" is not a pass. Check the filter.'
  exit 2
fi
echo "GREEN — $passed passed, 0 failed. NOTHING PINS THE GUARD YOU BROKE."
exit 1
