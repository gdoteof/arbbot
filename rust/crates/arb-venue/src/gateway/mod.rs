//! The `VenueGateway` trait — the typed executor seam. The implementations are
//! real: they compose the signed request (where every venue quirk lives) and
//! hand it to a [`crate::transport::Transport`], which is the only thing that
//! touches the network.
//!
//! This header said "signatures only, no transport (no reqwest/hyper)" until
//! [`crate::transport::HttpTransport`] landed for M2, and it is worth knowing
//! why that claim was ever safe to make. The inertness did not go away with it;
//! it moved into the type. Both gateways are generic over their transport with
//! [`crate::transport::NotWired`] as the DEFAULT, so `KalshiGateway::new` and
//! `PmusGateway::new` still cannot reach a venue and still answer
//! [`VenueError::NotWired`] to everything. Reaching one is an explicit act with
//! a name: `with_transport`. arb-trader's sink builders and arb-order are the
//! only callers that perform it.
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
use crate::resp;
use std::collections::HashSet;
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

/// Does this `client_order_id` belong to THIS STACK?
///
/// `docs/venue-quirks.md` §`xv-graceful-shutdown-cancels-orders` requires it:
/// *"scope any sweep to orders this process owns (shared-account contract)"* —
/// and names the failure it prevents, *"a blanket cancel-all from one
/// workstream kills another workstream's orders on the SHARED Kalshi
/// account"*. `arbbot-hedge.timer` fires every 5 minutes under the same primary
/// Kalshi key.
///
/// OURS MEANS "MINTED BY THIS CODEBASE", NOT "MINTED BY THIS PROCESS". The
/// distinction is the whole design. `Engine::id_base` seeds the counters from
/// the wall clock, so this process's ids ARE distinguishable from the last
/// run's — and scoping to them would have broken the startup sweep, whose ONLY
/// job is to clean up what a previous run left behind when it could not clean
/// up after itself (SIGKILL, an OOM kill, an abort). The quirk's own unit is
/// the workstream, not the process instance, because the order it protects is
/// another workstream's.
///
/// Every prefix below is minted somewhere in this tree and nowhere else:
///   * `m`, `h`, `t` + a decimal counter — the maker (`arb_core::quoter`),
///     hedge (`engine::hedge`/`engine::fill`) and take-take (`engine`) ids the
///     trader sends as its `client_order_id` verbatim (`engine::cancel`);
///   * `rehearse-` + millis — [`KalshiGateway::rehearse`];
///   * `sweep-` + pid — `arb-order verify`, whose whole purpose is to watch the
///     kill sweep cancel a REAL resting order, so it has to be inside the
///     scope the sweep now applies.
///
/// Nothing else on this account can collide with them. The retired Python
/// gateway tags every order `uuid.uuid4().hex` (`exec/kalshi_gateway.py:51`) —
/// 32 characters drawn from `[0-9a-f]`, and `m`/`h`/`t`/`r`/`s` are not hex
/// digits, so no id it ever minted can match. The trailing run is required to be
/// digits for the same reason: it is what the counters actually emit, and it
/// keeps a bare word that happens to start with `t` from reading as ours.
pub fn is_ours(client_order_id: &str) -> bool {
    // `continue`, not `return`, on a prefix that matches but whose tail is not a
    // counter. With today's five prefixes no id can reach a second arm, but
    // `t` shadows any future `take-…` and the bug would be a SILENTLY NARROWER
    // sweep — the exact class this function exists to prevent.
    for p in ["m", "h", "t", "rehearse-", "sweep-"] {
        if let Some(n) = client_order_id.strip_prefix(p) {
            if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) {
                return true;
            }
        }
    }
    false
}

/// A cancel request. PM-US requires the `market_slug` in the body; Kalshi
/// cancels by id alone (`market_slug` ignored there).
#[derive(Debug, Clone)]
pub struct CancelRequest {
    pub by: CancelBy,
    pub market_slug: Option<String>,
}

/// The venue-adapter interface. Every method is fallible. On a gateway built
/// with `new` the only outcome is [`VenueError::NotWired`]; on one built with
/// `with_transport` these reach the venue.
pub trait VenueGateway {
    /// Order-shaped response of a place / single-order status call.
    type Order;
    /// Balance response.
    type Balances;
    /// Positions response.
    type Positions;

    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError>;
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError>;
    /// The venue's own id for an order this process PLACED but could not read
    /// the answer for — `Ok(None)` when the venue's own book offers nothing to
    /// hand back.
    ///
    /// HOW MUCH OF THE BOOK that is depends on what the venue lets an
    /// implementation ask. PM-US can only read `/v1/orders/open`, so `Ok(None)`
    /// there means "nothing RESTING matches"; Kalshi's list is the full history
    /// and it answers over all of it, because a fill also arrives under the
    /// venue's id and an order that finished by EXECUTING is one the engine
    /// still has to be able to name.
    ///
    /// A place that failed may still have REACHED the venue: the transport maps
    /// a timeout to [`VenueError::Transport`] after 15s, and a body we cannot
    /// parse is a 2xx we could not read. Without a way back to that order's id
    /// it rests unaddressable, and a fill on it arrives under an id this process
    /// never learned.
    ///
    /// The default is NO recovery, deliberately. A venue that can be searched
    /// for our own order has to say HOW — Kalshi by `client_order_id`, PM-US by
    /// the fields of the order itself — and guessing on a SHARED account means
    /// cancelling somebody else's order (docs/venue-quirks.md
    /// §xv-graceful-shutdown-cancels-orders). `claimed` is the venue-id set this
    /// process has already taken responsibility for; an implementation must
    /// never hand one of those back as a new order.
    fn recover_place(
        &self,
        _req: &PlaceRequest,
        _claimed: &HashSet<String>,
    ) -> Result<Option<String>, VenueError> {
        Ok(None)
    }
    /// Every fill on this account at or after `min_ts` (unix seconds), oldest
    /// first — venue truth for a private fill feed, carrying the venue's own
    /// per-fill id so a caller can MERGE rather than overwrite.
    ///
    /// The default is NO history, and for PM-US that is not a stub but the
    /// fact: quirk `pmus-no-history-endpoints` records `/v1/portfolio/fills`,
    /// `/v1/portfolio/trades` and six more probed and 404 on 2026-07-27. Its
    /// feed does not need one anyway — it reports the venue's own cumulative
    /// `cumQuantity`, so the next frame on an order restates the total.
    fn fills_since(&self, _min_ts: i64) -> Result<Vec<resp::KalshiFillRow>, VenueError> {
        Ok(Vec::new())
    }
    /// The venue's own id for an order. Kalshi spells it `order_id`, PM-US
    /// spells it `id`; callers that work across venues need one name.
    fn order_id(order: &Self::Order) -> String;
    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError>;
    /// Cancel every RESTING order THIS STACK owns (kill-switch sweep) — see
    /// [`is_ours`] for what that means and why it is not "this process".
    ///
    /// Kalshi scopes by `client_order_id`. PM-US CANNOT: its create body
    /// carries no tag of ours at all (`docs/venue-quirks.md`
    /// §`pmus-no-client-order-id`), so its one-call cancel-all stays
    /// account-wide and says so at the implementation.
    fn cancel_all_open(&self) -> Result<(), VenueError>;
    /// Ids still RESTING that this sweep is answerable for. This is how a caller
    /// proves a sweep actually worked — an empty list is the only real
    /// evidence, since a 200 from cancel_all_open only says the venue accepted
    /// the request.
    ///
    /// It reports what [`Self::cancel_all_open`] is able to cancel, with ONE
    /// deliberate exception on Kalshi: a resting row carrying no tag at all is
    /// not cancelled (nothing claims it) and is not counted either (a human
    /// order placed through the venue's own web UI has no `client_order_id`,
    /// and must not be able to stop this process arming). What stops that
    /// exception from silently narrowing the sweep to nothing is a separate
    /// check — see `KalshiGateway::cancel_all_open` — that the venue is
    /// echoing the tag AT ALL before any of this scoping is trusted.
    ///
    /// Beyond that the two must agree, or they disagree in one of two bad
    /// directions: wider, and a sweep can never come back clean while another
    /// workstream is quoting; narrower, and it proves less than it claims.
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
/// refuses a background READ here rather than earning a venue-side 429.
///
/// [`Priority::Critical`] — the order path — spends nothing and can never be
/// refused (quirk `xv-shared-api-budget`). It used to be metered by a bucket
/// that ordinary quoting drained two tokens at a time, so a hedge for a maker
/// leg that had ALREADY filled could be turned into an `exec_failed` counter
/// before it was signed, leaving the position naked while the bucket refilled
/// at 1/s. What keeps the order path off a venue's 429 list is arb-trader's
/// per-venue executor shaper, which WAITS instead of failing.
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
