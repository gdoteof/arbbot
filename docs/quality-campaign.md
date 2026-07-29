# Quality campaign — the regression gate

Every PR in the 2026-07-28 code-quality campaign must clear all four gates
below before it merges. Three of them are cheap; the fourth is the one that
actually proves a refactor changed no decision.

## The gates

```bash
cd rust
cargo build --workspace                       # 1. builds
cargo test --workspace                        # 2. 382 pass, 0 fail
cargo clippy --all-targets --workspace        # 3. no NEW warnings
cargo test -p arb-trader --test determinism   #    (synthetic-tape digest fold)
```

```bash
# 4. THE REAL-TAPE DIGEST DIFF — from the repo root, release build.
cargo build --release -p arb-trader --manifest-path rust/Cargo.toml
./rust/target/release/arb-trader \
    --bench-tape data/golden/bench-tape-2026-07-28.jsonl \
    --registry   data/golden/registry-pin.yaml \
    --tradable   data/golden/tradable-pin.yaml 2>/dev/null | tail -1
```

The `sha256` field of that summary line MUST equal the baseline:

| field | baseline (main @ b900470) |
|---|---|
| `sha256` | `f4141b53831220c962f552955c079203ca0c7e2a7a7bb75bced5efe5e5e4bdc8` |
| `intents` | 31692 |
| `book_events` | 675950 |
| `would_place` / `would_cancel` | 182 / 131 |
| wall time | ~3.9 s |

`sha256` is a rolling hash of every intent line the engine emits, in order. It
is not a checksum of the summary: two runs that place the same orders at the
same prices in a different ORDER produce different digests, and so do two runs
that differ by one skip record. That is the point — it is the only gate that
fails when a refactor changes what the engine decides rather than whether it
crashes.

## Why these inputs

The fixture is 900,000 real tape events from 2026-07-28 — 300k lines each from
the Kalshi, Polymarket INTL and PM-US recorders, merged and stably sorted by
`ts_local_ns`. Real data, so it carries the shapes a synthetic tape does not:
crossed books, sequence gaps, feed-specific level spellings, markets that go
quiet mid-day.

`registry-pin.yaml` / `tradable-pin.yaml` are frozen copies of the live config.
They are pinned rather than read from `config/` because the live registry gets
edited — a digest compared against a moving registry proves nothing.

None of it is committed (`data/` is gitignored: it is market tape). Rebuild:

```bash
D=2026-07-28
for v in kalshi polymarket_us polymarket; do head -n 300000 data/raw/$v-$D.jsonl; done \
  | awk '{ if (match($0, /"ts_local_ns":-?[0-9]+/)) k=substr($0, RSTART+15, RLENGTH-15); else k=0;
           print k "\t" $0 }' \
  | sort -k1,1n -k2 -S 2G | cut -f2- > data/golden/bench-tape-$D.jsonl
cp config/registry.yaml data/golden/registry-pin.yaml
cp config/tradable.yaml data/golden/tradable-pin.yaml
```

## When the digest is ALLOWED to change

Only when the PR's whole purpose is to change a decision, and then the new
digest is recorded here with the reason. A refactor PR that changes the digest
is a refactor PR with a bug in it — including "obviously equivalent" ones. Both
prior incidents that motivated this campaign (a stringly-typed id swapped at a
call site, an `unwrap_or` default on a money field) would have been invisible to
`cargo test` and loud here.

## Scope

Rust only. `src/arbbot/` and the Python systemd units are FROZEN as of
2026-07-28 — not modified, not deleted, not run. Several of them still produce
the marks/unwind/hedge inputs the armed Rust engine reads, so they stay live
until a Rust replacement exists.
