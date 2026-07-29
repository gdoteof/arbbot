//! The background-read budget as a PURE type: injected `now_ns`, no tokio, no
//! clock reads. The executor tasks own the wall clock and feed monotonic nanos
//! in; this type only does the accounting, so it replays deterministically
//! through the parity harness.
//!
//! It held TWO buckets until 2026-07-29 — one of them metering the order path,
//! which is exactly what quirk `xv-shared-api-budget` forbids: "All background
//! venue reads must draw from one cross-process budget; the order/hedge path
//! must bypass it and never wait." The Python this ports
//! (`venues/pmus.py::consume_budget`) never opened the budget for an order at
//! all. A bucket the order path is required to bypass is not a budget, so
//! there is no longer one to drain.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// The order path — place, cancel, hedge, kill-switch sweep. Draws from NO
    /// local budget: it must never wait and must never be refused.
    Critical,
    /// Background venue READS — reconcile, order/status polling, balances,
    /// positions. These are what the budget exists to meter.
    Background,
}

impl Priority {
    /// The wire/log spelling, as carried by
    /// [`VenueError::RateLimited`](crate::error::VenueError::RateLimited). It
    /// belongs next to the enum: a second `match` written at the point of use
    /// is a second place the two names can drift.
    pub fn as_str(self) -> &'static str {
        match self {
            Priority::Critical => "critical",
            Priority::Background => "background",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_ns: u64,
}

impl TokenBucket {
    /// Starts full (`tokens = capacity`) at `start_ns`.
    pub fn new(capacity: f64, refill_per_sec: f64, start_ns: u64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_per_sec,
            last_ns: start_ns,
        }
    }

    fn refill(&mut self, now_ns: u64) {
        // saturating: a non-monotonic `now` never mints negative time.
        let dt_ns = now_ns.saturating_sub(self.last_ns);
        if dt_ns == 0 {
            return;
        }
        let dt_s = dt_ns as f64 / 1_000_000_000.0;
        self.tokens = (self.tokens + dt_s * self.refill_per_sec).min(self.capacity);
        self.last_ns = now_ns;
    }

    /// Try to spend one token at `now_ns`. Refills first, then spends if able.
    pub fn try_acquire(&mut self, now_ns: u64) -> bool {
        self.refill(now_ns);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Current token count (after refilling to `now_ns`) — for tests/telemetry.
    pub fn available(&mut self, now_ns: u64) -> f64 {
        self.refill(now_ns);
        self.tokens
    }
}

/// A per-venue budget for background reads. The order path is exempt, so a
/// reprice — a cancel plus a place — can no longer spend the tokens a hedge
/// would have needed.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    background: TokenBucket,
}

impl RateLimiter {
    pub fn new(background: TokenBucket) -> Self {
        Self { background }
    }

    /// Build from the per-minute rate (the venue budgets are documented per
    /// minute, e.g. PM-US `BUDGET_PER_MIN = 30`). The bucket starts full.
    pub fn from_per_minute(background_per_min: f64, start_ns: u64) -> Self {
        Self::new(TokenBucket::new(
            background_per_min,
            background_per_min / 60.0,
            start_ns,
        ))
    }

    pub fn try_acquire(&mut self, priority: Priority, now_ns: u64) -> bool {
        match priority {
            // Exempt — NOT a bucket sized large enough to usually work. A call
            // that must never wait must also never be able to run out, and a
            // hedge refused for want of a token is a naked leg.
            Priority::Critical => true,
            Priority::Background => self.background.try_acquire(now_ns),
        }
    }
}
