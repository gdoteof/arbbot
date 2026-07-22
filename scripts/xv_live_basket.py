"""Execute approved cross-venue baskets: buy YES on Kalshi (cheap ask), open
NO on Polymarket US (hit the rich bid). Basket pays $1 at resolution on either
outcome iff the pair is equivalent.

AUTHORIZED SCOPE (Geoff, 2026-07-21): melenchon (France caveat signed off for
small trades), mamdani, fedcut — small clips only, caps below.

Safety: dry-run default (--live to send); re-verifies edge from LIVE books at
execution moment; sizes to min(cap, both books