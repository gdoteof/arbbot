//! The `VenueGateway` trait — the typed executor seam. Signatures only: the
//! stub implementations here return [`VenueError::NotWired`] because this crate
//! has NO transport (no reqwest/hyper). Real executors land later behind
//! arb-trader's dry-run seam (docs/migration-plan.md M2/M3), reusing the
//! signers/wire/resp types in this crate.
//!
//! One venue per file: the trait, the request types and the two helpers both
//! gateways share live here; `kalshi` and `pmus` hold nothing but their own
//! venue's paths and quirks.

mod kalshi;
mod pmus;

pub use kalshi::KalshiGateway;
pub use pmus::PmusGateway;

use crate::error::VenueError;
use crate::ratelimit::{Priority, RateLimiter};
use std::sync::Mutex;

/// Raw venue side on the YES price axis. `Bid` buys YES; `Ask` sells YES
/// (== opens NO on PM-US via BUY_SHORT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/// Time in force. `Gtc` rests as a maker; `Ioc` is a taker hedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tif {
    Gtc,
    Ioc,
}

/// A place request in venue-neutral terms. `price` is a pre-formatted decimal
/// string (the stack's decimal-parity convention) — no float ever crosses this
/// boundary.
#[derive(Debug, Clone)]
pub struct PlaceRequest {
    pub market: String,
    pub side: Side,
    pub price: String,
    pub qty: i64,
    pub tif: Tif,
    pub post_only: bool,
    pub client_order_id: String,
}

/// Which id namespace a cancel is addressed in.
///
/// This was a bare `order_id: String`, and every per-order cancel the trader
/// ever sent put OUR client_order_id in it (audit 2026-07-28): the venue had
/// never heard of that id, Kalshi answered 404 — which quirk K4 maps to success
/// — and PM-US answered <300 for an order it had never issued. Both venues
/// reported cancelling something that went on resting, so `exec_failed` stayed
/// 0 while stale quotes accumulated. Naming the namespace in the TYPE is what
/// stops a caller expressing that mistake again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelBy {
    /// The venue's own order id, as learned from the place response. The only
    /// handle either venue's cancel endpoint accepts directly.
    VenueId(String),
    /// OUR client_order_id, for an order whose venue id we never learned (a
    /// place whose response was lost, or one whose ack never came back).
    /// Kalshi can resolve it against its own order list; PM-US has no
    /// client-order-id field on the wire at all
    /// ([`crate::wire::pmus_order_body`]), so it refuses rather than address a
    /// phantom.
    ClientId(String),
}

// No accessor returns the bare id: an `id() -> &str` erases exactly the
// namespace this enum exists to keep, and a caller that only wants to NAME the
// target (a log line, a test record) should print the variant — `{:?}` gives
// `VenueId("BH9…")` / `ClientId("m1")`, which says which id space it was in.

/// A cancel request. PM-US requires the `market_slug` in the body; Kalshi
/// cancels by id alone (`market_slug` ignored there).
#[derive(Debug, Clone)]
pub struct CancelRequest {
    pub by: CancelBy,
    pub market_slug: Option<String>,
}

/// The venue-adapter interface. Every method is fallible; the only outcome in
/// this build is [`VenueError::NotWired`].
pub trait VenueGateway {
    /// Order-shaped response of a place / single-order status call.
    type Order;
    /// Balance response.
    type Balances;
    /// Positions response.
    type Positions;

    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError>;
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError>;
    /// The venue's own id for an order. Kalshi spells it `order_id`, PM-US
    /// spells it `id`; callers that work across venues need one name.
    fn order_id(order: &Self::Order) -> String;
    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError>;
    /// Cancel every RESTING order owned by this account (kill-switch sweep).
    fn cancel_all_open(&self) -> Result<(), VenueError>;
    /// Ids of every order still RESTING on this account. This is how a caller
    /// proves a sweep actually worked — an empty list is the only real
    /// evidence, since a 200 from cancel_all_open only says the venue accepted
    /// the request.
    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError>;
    /// Place one far-off-touch contract, confirm it rests, cancel it. Live
    /// auth rehearsal — returns the order id on success.
    fn rehearse(&self, market: &str) -> Result<String, VenueError>;
    fn balances(&self) -> Result<Self::Balances, VenueError>;
    fn positions(&self) -> Result<Self::Positions, VenueError>;
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn ts_ms() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".into())
}

/// Spend one token of `priority` before a venue call. An exhausted LOCAL budget
/// refuses here rather than earning a venue-side 429.
///
/// Shared because both gateways opened every call with the same sixteen lines,
/// and a rate limit that is enforced in two places is a rate limit that can be
/// relaxed in one of them.
fn spend_token(limiter: &Mutex<RateLimiter>, priority: Priority) -> Result<(), VenueError> {
    let mut lim = limiter.lock().expect("rate limiter mutex");
    if lim.try_acquire(priority, now_ns()) {
        return Ok(());
    }
    Err(VenueError::RateLimited { priority: priority.as_str() })
}

/// How long to keep asking a venue about an order it has just accepted.
///
/// Neither venue's create is read-your-writes: Kalshi 404s a GET on a
/// just-placed order for a beat (observed live 2026-07-27) and its order LIST
/// lags further still. Python papered over this with a flat `time.sleep(1.0)`;
/// poll instead, so a fast venue costs nothing and a slow one still succeeds.
/// Zero delay in tests.
#[derive(Clone, Copy)]
struct Settle {
    delay: std::time::Duration,
    attempts: u32,
}

impl Default for Settle {
    fn default() -> Self {
        Settle { delay: std::time::Duration::from_millis(500), attempts: 8 }
    }
}

impl Settle {
    /// Run `read` until it answers something other than 404, or the attempts are
    /// spent. A 404 in this window means "not yet", NOT "no such order" — the
    /// create already told us it exists, so the last 404 is what is returned
    /// rather than a synthesised error that would read as a missing order.
    ///
    /// `endpoint` names the caller for the give-up error only.
    fn retry_404<R>(
        &self,
        endpoint: &'static str,
        order_id: &str,
        read: impl Fn() -> Result<R, VenueError>,
    ) -> Result<R, VenueError> {
        let attempts = self.attempts.max(1);
        let mut last = None;
        for attempt in 0..attempts {
            match read() {
                Err(VenueError::Status { status: 404, endpoint, body }) => {
                    last = Some(VenueError::Status { status: 404, endpoint, body });
                    if attempt + 1 < attempts && !self.delay.is_zero() {
                        std::thread::sleep(self.delay);
                    }
                }
                other => return other,
            }
        }
        Err(last.unwrap_or(VenueError::Status {
            endpoint,
            status: 404,
            body: format!("order {order_id} never became visible"),
        }))
    }
}
