//! Typed errors for the venue layer. No silent defaults, no float coercion.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueError {
    /// No transport is wired in this build. The executor seam is intentionally
    /// empty (docs/migration-plan.md M2/M3): every [`crate::VenueGateway`]
    /// method returns this until real executors land behind arb-trader's
    /// dry-run seam. It exists so the trait can be depended on now without any
    /// network code.
    NotWired,
    /// A required field was absent from a venue response. Surfaced instead of
    /// defaulting to zero — a missing money/id field is a bug, not a 0.
    MissingField {
        endpoint: &'static str,
        field: String,
    },
    /// A response body did not parse as the expected typed shape.
    Parse {
        endpoint: &'static str,
        detail: String,
    },
    /// Signing failed (bad key material or format).
    Sign(String),
    /// The request never completed (DNS, TLS, timeout, malformed method).
    Transport(String),
    /// The venue answered with a non-success status. Kept as data, not a
    /// panic: callers decide, because some statuses are success in disguise
    /// (a 404 on cancel means the order is already gone).
    Status {
        endpoint: &'static str,
        status: u16,
        body: String,
    },
    /// The local rate budget for this priority is exhausted. Refusing here is
    /// the point — a venue-side 429 costs far more than a local wait.
    RateLimited { priority: &'static str },
}

impl fmt::Display for VenueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VenueError::NotWired => write!(f, "venue transport not wired in this build"),
            VenueError::MissingField { endpoint, field } => {
                write!(f, "{endpoint}: missing required field `{field}`")
            }
            VenueError::Parse { endpoint, detail } => {
                write!(f, "{endpoint}: parse error: {detail}")
            }
            VenueError::Sign(m) => write!(f, "signing failed: {m}"),
            VenueError::Transport(m) => write!(f, "transport error: {m}"),
            VenueError::Status { endpoint, status, body } => {
                write!(f, "{endpoint}: HTTP {status}: {body}")
            }
            VenueError::RateLimited { priority } => {
                write!(f, "local rate budget exhausted ({priority})")
            }
        }
    }
}

impl std::error::Error for VenueError {}

/// WHEN a refused request could succeed if it were sent again — the missing
/// half of [`VenueError::Status`]'s "callers decide".
///
/// The body already reached the caller intact and nothing read it, so a hedge
/// retry could not tell a refusal a retry might fix from one no retry can. On
/// 2026-07-30 that cost 335 identical places over 31 minutes (04:29:06-04:59:58,
/// `arbbot-trader-m3`): Kalshi had KXNOBELPEACE-27-MKUL halted, answered every
/// one with `409 trading_is_paused`, and the engine re-sent the same price on a
/// 5s interval against a venue state no price can change.
///
/// GROUNDED IN OBSERVED CODES ONLY. Every venue refusal in this box's journals
/// since 2026-07-01 is one of:
///
/// ```text
///   335x  Kalshi 409 {"error":{"code":"trading_is_paused"}}      on place
///     4x  Kalshi 409 {"error":{"code":"order_already_exists"}}   on place
///    38x  PM-US  503 (no body logged)  GET /v1/portfolio/positions
///     1x  PM-US  400 (no body logged)
/// ```
///
/// The PM-US rows are the Python hedger's — `httpx.raise_for_status()` discards
/// the body, so no PM-US error CODE has ever been observed here. That is why
/// there is no PM-US arm below and no HTTP-status table: a 503 and a 400 both
/// land in [`Retry::Now`], which is exactly the behaviour that already existed.
/// Adding arms for codes nobody has seen would be inventing a taxonomy, and the
/// one thing worse than not classifying a refusal is classifying it wrongly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Nothing observed says otherwise — the caller's ordinary schedule stands.
    /// The DEFAULT, so an unclassified refusal behaves exactly as before.
    Now,
    /// The venue is refusing because the MARKET is not trading. No price, size
    /// or interval changes that answer; only the venue reopening does, which is
    /// a minutes-scale event (31 minutes, observed once).
    MarketHalted,
    /// Re-sending THIS request cannot change the answer. `order_already_exists`
    /// is the observed instance: the client_order_id is already spent, so the
    /// only way forward is a different request.
    Terminal,
}

impl VenueError {
    /// Classify a refusal. See [`Retry`] for the evidence each arm rests on.
    ///
    /// Keyed on the venue's own error CODE and not on the HTTP status, because
    /// the two observed 409s mean opposite things: one is "wait for the venue",
    /// the other is "this request is spent". A body that is not the Kalshi
    /// `{"error":{"code":...}}` shape, or carries a code not listed here, is
    /// [`Retry::Now`] — the status quo.
    pub fn retry(&self) -> Retry {
        let VenueError::Status { body, .. } = self else {
            return Retry::Now;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
            return Retry::Now;
        };
        match v.get("error").and_then(|e| e.get("code")).and_then(|c| c.as_str()) {
            Some("trading_is_paused") => Retry::MarketHalted,
            Some("order_already_exists") => Retry::Terminal,
            _ => Retry::Now,
        }
    }
}

/// Map a serde_json decode error to a typed venue error, distinguishing the
/// "missing required field" case (serde emits `missing field \`x\``) so callers
/// get [`VenueError::MissingField`] rather than an opaque parse blob.
pub(crate) fn from_serde(endpoint: &'static str, e: &serde_json::Error) -> VenueError {
    let msg = e.to_string();
    const NEEDLE: &str = "missing field `";
    if let Some(start) = msg.find(NEEDLE) {
        let rest = &msg[start + NEEDLE.len()..];
        if let Some(end) = rest.find('`') {
            return VenueError::MissingField {
                endpoint,
                field: rest[..end].to_string(),
            };
        }
    }
    VenueError::Parse {
        endpoint,
        detail: msg,
    }
}

/// The bodies below are VERBATIM from `arbbot-trader-m3`'s journal, not
/// hand-written approximations of them: the classification is a claim about
/// what this venue actually sends, and a test that invents its own input can
/// only prove the parser matches the test.
#[cfg(test)]
mod retry_tests {
    use super::*;

    fn place(body: &str) -> VenueError {
        VenueError::Status { endpoint: "kalshi place", status: 409, body: body.to_string() }
    }

    /// THE 2026-07-30 storm, in one assertion.
    #[test]
    fn a_halted_kalshi_market_is_not_retryable_now() {
        let e = place(r#"{"error":{"code":"trading_is_paused","message":"trading is paused"}}"#);
        assert_eq!(e.retry(), Retry::MarketHalted);
    }

    /// The OTHER 409, four of them on the same endpoint in the same journal.
    /// Same status, opposite answer — which is why the status is not the key.
    #[test]
    fn a_spent_client_order_id_is_terminal_not_halted() {
        let e = place(
            r#"{"error":{"code":"order_already_exists","message":"order already exists","service":"exchange"}}"#,
        );
        assert_eq!(e.retry(), Retry::Terminal);
    }

    /// Everything unrecognised keeps the behaviour it already had. A PM-US 503
    /// logs no body at all, and inventing a meaning for it is the failure this
    /// classification is supposed to avoid.
    #[test]
    fn an_unclassified_refusal_is_still_retryable_now() {
        for e in [
            VenueError::Status { endpoint: "pmus positions", status: 503, body: String::new() },
            VenueError::Status { endpoint: "pmus place", status: 400, body: "not json".into() },
            place(r#"{"error":{"code":"some_code_nobody_has_seen"}}"#),
            VenueError::Transport("timed out".into()),
            VenueError::RateLimited { priority: "critical" },
        ] {
            assert_eq!(e.retry(), Retry::Now, "{e}");
        }
    }
}
