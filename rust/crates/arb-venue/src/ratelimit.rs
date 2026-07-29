//! Two-bucket rate limiter as a PURE type: injected `now_ns`, no tokio, no
//! clock reads. Models the P3 design's two per-venue token buckets
//! (critical/background). The executor tasks own the wall clock and feed
//! monotonic nanos in; this type only does the accounting, so it replays
//! deterministically through the parity harness.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Hedge / cancel path — must not be starved by background polling.
    Critical,
    /// Reprice / reconcile / status polling.
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

/// A per-venue limiter with independent critical and background buckets.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    critical: TokenBucket,
    background: TokenBucket,
}

impl RateLimiter {
    pub fn new(critical: TokenBucket, background: TokenBucket) -> Self {
        Self {
            critical,
            background,
        }
    }

    /// Build from per-minute rates (the venue budgets are documented per
    /// minute, e.g. PM-US `BUDGET_PER_MIN = 30`). Buckets start full.
    pub fn from_per_minute(
        critical_per_min: f64,
        background_per_min: f64,
        start_ns: u64,
    ) -> Self {
        Self::new(
            TokenBucket::new(critical_per_min, critical_per_min / 60.0, start_ns),
            TokenBucket::new(background_per_min, background_per_min / 60.0, start_ns),
        )
    }

    pub fn try_acquire(&mut self, priority: Priority, now_ns: u64) -> bool {
        match priority {
            Priority::Critical => self.critical.try_acquire(now_ns),
            Priority::Background => self.background.try_acquire(now_ns),
        }
    }

    pub fn bucket(&mut self, priority: Priority) -> &mut TokenBucket {
        match priority {
            Priority::Critical => &mut self.critical,
            Priority::Background => &mut self.background,
        }
    }
}
