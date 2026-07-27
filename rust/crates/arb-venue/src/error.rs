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
