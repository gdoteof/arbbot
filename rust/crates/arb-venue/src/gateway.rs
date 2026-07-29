//! The `VenueGateway` trait — the typed executor seam. Signatures only: the
//! stub implementations here return [`VenueError::NotWired`] because this crate
//! has NO transport (no reqwest/hyper). Real executors land later behind
//! arb-trader's dry-run seam (docs/migration-plan.md M2/M3), reusing the
//! signers/wire/resp types in this crate.

use crate::error::VenueError;
use crate::ratelimit::{Priority, RateLimiter};
use crate::resp;
use crate::sign::{KalshiSigner, PmusSigner};
use crate::transport::{NotWired, Transport};
use crate::wire;
use serde_json::Value;
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
    /// client-order-id field on the wire at all ([`wire::pmus_order_body`]), so
    /// it refuses rather than address a phantom.
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

/// Kalshi V2 API paths. The signature covers the FULL path including the
/// `/trade-api/v2` prefix (Python `_headers` prepends it), so these constants
/// are what gets signed AND what builds the URL — one string, no drift.
/// Create moved here; the legacy `POST /portfolio/orders` now 410s.
const K_PLACE: &str = "/trade-api/v2/portfolio/events/orders";
/// GET/list + status still live under `/portfolio/orders`.
const K_ORDERS: &str = "/trade-api/v2/portfolio/orders";
const K_BALANCE: &str = "/trade-api/v2/portfolio/balance";
const K_POSITIONS: &str = "/trade-api/v2/portfolio/positions";

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
    Err(VenueError::RateLimited {
        priority: match priority {
            Priority::Critical => "critical",
            Priority::Background => "background",
        },
    })
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

/// Kalshi gateway. Generic over its [`Transport`]; the default is
/// [`NotWired`], so a gateway built with [`KalshiGateway::new`] still cannot
/// reach a venue — the inert seam is preserved, and reaching a venue is an
/// explicit act ([`KalshiGateway::with_transport`]).
pub struct KalshiGateway<T: Transport = NotWired> {
    pub signer: KalshiSigner,
    pub limiter: Mutex<RateLimiter>,
    pub transport: T,
    settle: Settle,
}

impl KalshiGateway<NotWired> {
    /// The inert gateway: holds the signer + rate limiter it WILL use, with
    /// no transport behind it.
    pub fn new(signer: KalshiSigner, limiter: RateLimiter) -> Self {
        Self::with_transport(signer, limiter, NotWired)
    }
}

impl<T: Transport> KalshiGateway<T> {
    pub fn with_transport(signer: KalshiSigner, limiter: RateLimiter, transport: T) -> Self {
        Self {
            signer,
            limiter: Mutex::new(limiter),
            transport,
            settle: Settle::default(),
        }
    }

    /// Override the create-visibility poll (tests use a zero delay).
    pub fn with_settle(mut self, delay: std::time::Duration, attempts: u32) -> Self {
        self.settle = Settle { delay, attempts };
        self
    }

    /// `order_status`, tolerating the window where a just-created order is not
    /// yet visible to the query service. See [`Settle::retry_404`].
    fn order_status_settled(&self, order_id: &str) -> Result<resp::KalshiOrder, VenueError> {
        self.settle.retry_404("kalshi order_status", order_id, || self.order_status(order_id))
    }

    /// Sign `path` (never the query — quirk K2) and send. Spends one token of
    /// `priority` first; an exhausted local budget refuses rather than earning
    /// a venue-side 429.
    fn call(
        &self,
        priority: Priority,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<&Value>,
    ) -> Result<crate::transport::Response, VenueError> {
        spend_token(&self.limiter, priority)?;
        let ts = ts_ms();
        let headers = self.signer.headers(&ts, method, path);
        self.transport.send(method, path, query, &headers, body)
    }

    /// ALL orders across pages. `/portfolio/orders` is paginated (100/page +
    /// cursor) and `?status=resting` returns NOTHING, so page the full list and
    /// filter status client-side. Skipping pagination orphans older resting
    /// orders — that is naked-leg risk, not a cosmetic bug.
    pub fn all_orders(&self) -> Result<Vec<resp::KalshiOrder>, VenueError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let q = cursor.as_ref().map(|c| format!("limit=100&cursor={c}"));
            let r = self.call(
                Priority::Background,
                "GET",
                K_ORDERS,
                q.as_deref().or(Some("limit=100")),
                None,
            )?;
            if r.status != 200 {
                return Err(VenueError::Status {
                    endpoint: "kalshi orders",
                    status: r.status,
                    body: r.body,
                });
            }
            let page = resp::kalshi_orders_page(&r.body)?;
            out.extend(page.orders);
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Where an order carrying OUR `client_order_id` stands on this account.
    /// The three cases are deliberately distinct: "resting", "on the account
    /// but finished" and "not here at all" call for three different answers
    /// from a cancel, and collapsing them is how a cancel reports success for
    /// an order it never touched.
    fn find_ours(&self, coid: &str) -> Result<Ours, VenueError> {
        let mut seen = false;
        for o in self.all_orders()? {
            if o.client_order_id.as_deref() != Some(coid) {
                continue;
            }
            if o.is_resting() {
                return Ok(Ours::Resting(o.order_id));
            }
            seen = true;
        }
        Ok(if seen { Ours::Gone } else { Ours::Absent })
    }

    /// Find a RESTING order by our own `client_order_id` and cancel it.
    /// `Ok(None)` means there was nothing to clean up. This is the recovery
    /// path for a create whose response we could not read: the order may exist
    /// under an id we never learned, and the client_order_id is the only handle
    /// we have on it.
    pub fn cancel_by_client_order_id(&self, coid: &str) -> Result<Option<String>, VenueError> {
        match self.find_ours(coid)? {
            Ours::Resting(id) => {
                self.cancel(&CancelRequest {
                    by: CancelBy::VenueId(id.clone()),
                    market_slug: None,
                })?;
                Ok(Some(id))
            }
            Ours::Gone | Ours::Absent => Ok(None),
        }
    }
}

/// The state of an order we tagged with a `client_order_id`, as the venue's own
/// order list reports it.
enum Ours {
    /// Still resting, under the venue's own id.
    Resting(String),
    /// On the account but finished (canceled or executed) — a cancel has
    /// nothing left to do.
    Gone,
    /// Not on the account at all.
    Absent,
}

impl<T: Transport> VenueGateway for KalshiGateway<T> {
    type Order = resp::KalshiOrder;
    type Balances = resp::KalshiBalance;
    type Positions = resp::KalshiPositions;

    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError> {
        let body = wire::kalshi_place_body(
            &req.market,
            req.side,
            &req.price,
            req.qty,
            &req.client_order_id,
            req.post_only,
        );
        let r = self.call(Priority::Critical, "POST", K_PLACE, None, Some(&body))?;
        // A post_only order that would cross is a 400, NOT a silent taker
        // fill (quirk K5). Surfacing it is the whole point: crossing when we
        // asked to rest means our book view was wrong.
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "kalshi place",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_created_order(&r.body)
    }

    fn order_id(order: &Self::Order) -> String {
        order.order_id.clone()
    }

    /// DELETE /portfolio/events/orders/{venue order id}.
    ///
    /// A [`CancelBy::ClientId`] target is resolved against the venue's own order
    /// list first — that is the recovery handle for an order whose id we never
    /// learned. "Not on the account" is an ERROR there, never success: the order
    /// list also LAGS a write (see [`Self::order_status_settled`]), so absence
    /// can mean "not visible yet" just as easily as "never accepted", and
    /// claiming a cancel we did not make is the failure mode this whole path
    /// exists to end.
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        let oid = match &req.by {
            CancelBy::VenueId(id) => id.clone(),
            CancelBy::ClientId(coid) => match self.find_ours(coid)? {
                Ours::Resting(id) => id,
                // Genuinely already gone. This is the ONE case where "we
                // cancelled nothing" is the right answer.
                Ours::Gone => return Ok(()),
                // Not on the account. Still an error rather than a success —
                // nothing was cancelled and the order list LAGS a write, so this
                // cannot be read as proof the order is gone. Stated plainly, not
                // shouted: the commonest cause is a place the venue rejected, in
                // which case there was never anything here to cancel.
                Ours::Absent => {
                    return Err(VenueError::Status {
                        endpoint: "kalshi cancel",
                        status: 0,
                        body: format!(
                            "no order carries client_order_id `{coid}`; nothing cancelled \
                             (the place may have been rejected, or the order list has not \
                             caught up)"
                        ),
                    })
                }
            },
        };
        if oid.is_empty() {
            // An empty id would DELETE the collection path, which is not a
            // cancel of anything and may not be a no-op either.
            return Err(VenueError::MissingField {
                endpoint: "kalshi cancel",
                field: "order_id".into(),
            });
        }
        let path = format!("{K_PLACE}/{oid}");
        let r = self.call(Priority::Critical, "DELETE", &path, None, None)?;
        // 404 == already gone == successfully cancelled (quirk K4). Treating
        // it as an error would orphan a resting order on a retry path.
        //
        // That reading is only sound because `oid` is the VENUE's own id: a 404
        // on an id the venue never issued means "wrong id", not "already gone",
        // and mapping THAT to success is what hid every phantom cancel until
        // 2026-07-28. `CancelBy` makes an unresolved id impossible to send here.
        if r.status == 404 || r.status < 300 {
            return Ok(());
        }
        Err(VenueError::Status { endpoint: "kalshi cancel", status: r.status, body: r.body })
    }

    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError> {
        let path = format!("{K_ORDERS}/{order_id}");
        let r = self.call(Priority::Background, "GET", &path, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi order_status",
                status: r.status,
                body: r.body,
            });
        }
        Ok(resp::kalshi_order_envelope(&r.body)?.order)
    }

    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
        Ok(self.all_orders()?.into_iter().filter(|o| o.is_resting()).map(|o| o.order_id).collect())
    }

    /// Kill-switch sweep. `/portfolio/orders` returns history (canceled and
    /// executed too), so filter to `resting` — never try to cancel a dead order.
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        for o in self.all_orders()? {
            if o.is_resting() {
                self.cancel(&CancelRequest {
                    by: CancelBy::VenueId(o.order_id),
                    market_slug: None,
                })?;
            }
        }
        Ok(())
    }

    /// Live auth rehearsal (docs/migration-plan.md M2): place ONE contract at
    /// 1c — far below any real bid, so the fill risk is ~zero and the worst
    /// case is a $0.01 position to flatten by hand — confirm it rests, cancel
    /// it. Proves the signed POST/GET/DELETE path against the real account.
    fn rehearse(&self, market: &str) -> Result<String, VenueError> {
        let coid = format!("rehearse-{}", ts_ms());
        let placed = self.place(&PlaceRequest {
            market: market.to_string(),
            side: Side::Bid,
            price: "0.0100".into(),
            qty: 1,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: coid.clone(),
        });

        let oid = match placed {
            Ok(o) => o.order_id,
            // A place that FAILED TO PARSE may still have reached the venue —
            // the POST was accepted, we just could not read the answer. That is
            // how the first live smoke left an order resting (2026-07-27). The
            // client_order_id is our only handle on it, so sweep for it.
            Err(e @ VenueError::Parse { .. }) | Err(e @ VenueError::MissingField { .. }) => {
                match self.cancel_by_client_order_id(&coid) {
                    Ok(Some(id)) => {
                        return Err(VenueError::Status {
                            endpoint: "kalshi rehearse",
                            status: 0,
                            body: format!(
                                "unreadable create response ({e}); \
                                 recovered and CANCELLED orphan order {id}"
                            ),
                        })
                    }
                    Ok(None) => return Err(e),
                    Err(sweep) => {
                        return Err(VenueError::Status {
                            endpoint: "kalshi rehearse",
                            status: 0,
                            body: format!(
                                "unreadable create response ({e}); \
                                 orphan sweep ALSO failed ({sweep}) — \
                                 CHECK THE VENUE BY HAND"
                            ),
                        })
                    }
                }
            }
            Err(e) => return Err(e),
        };

        // From here the order EXISTS. Every exit path below must cancel it, or
        // the rehearsal leaves behind exactly what it exists to prove we never
        // leave behind.
        let rested = match self.order_status_settled(&oid) {
            Ok(seen) if seen.is_resting() => Ok(()),
            Ok(seen) => Err(VenueError::Status {
                endpoint: "kalshi rehearse",
                status: 0,
                body: format!(
                    "order {oid} did not rest (status={:?})",
                    seen.status.as_deref().unwrap_or("<absent>")
                ),
            }),
            Err(e) => Err(e),
        };

        let cancelled =
            self.cancel(&CancelRequest { by: CancelBy::VenueId(oid.clone()), market_slug: None });

        match (rested, cancelled) {
            (Ok(()), Ok(())) => Ok(oid),
            (Err(e), Ok(())) => Err(e), // check failed, but nothing left resting
            (Ok(()), Err(c)) | (Err(_), Err(c)) => Err(VenueError::Status {
                endpoint: "kalshi rehearse",
                status: 0,
                body: format!("order {oid} COULD NOT BE CANCELLED ({c}) — CHECK THE VENUE"),
            }),
        }
    }

    fn balances(&self) -> Result<Self::Balances, VenueError> {
        let r = self.call(Priority::Background, "GET", K_BALANCE, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi balance",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_balance(&r.body)
    }

    fn positions(&self) -> Result<Self::Positions, VenueError> {
        let r = self.call(Priority::Background, "GET", K_POSITIONS, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi positions",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_positions(&r.body)
    }
}

/// PM-US (QCX) paths. Unlike Kalshi there is no API prefix on the base
/// (`https://api.polymarket.us`), so the signed path and the URL path are the
/// same string.
const P_CREATE: &str = "/v1/orders";
const P_OPEN: &str = "/v1/orders/open";
const P_CANCEL_ALL: &str = "/v1/orders/open/cancel";
const P_BALANCES: &str = "/v1/account/balances";
const P_POSITIONS: &str = "/v1/portfolio/positions";

/// PM-US gateway. Same [`Transport`] seam and same inert default as
/// [`KalshiGateway`].
pub struct PmusGateway<T: Transport = NotWired> {
    pub signer: PmusSigner,
    pub limiter: Mutex<RateLimiter>,
    pub transport: T,
    settle: Settle,
}

impl PmusGateway<NotWired> {
    pub fn new(signer: PmusSigner, limiter: RateLimiter) -> Self {
        Self::with_transport(signer, limiter, NotWired)
    }
}

impl<T: Transport> PmusGateway<T> {
    pub fn with_transport(signer: PmusSigner, limiter: RateLimiter, transport: T) -> Self {
        Self {
            signer,
            limiter: Mutex::new(limiter),
            transport,
            settle: Settle::default(),
        }
    }

    pub fn with_settle(mut self, delay: std::time::Duration, attempts: u32) -> Self {
        self.settle = Settle { delay, attempts };
        self
    }

    fn call(
        &self,
        priority: Priority,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<crate::transport::Response, VenueError> {
        spend_token(&self.limiter, priority)?;
        let ts = ts_ms();
        let headers = self.signer.headers(&ts, method, path);
        self.transport.send(method, path, None, &headers, body)
    }

    /// GET /v1/orders/open — resting orders. The body is either a bare array
    /// or `{"orders": [...]}` (Python: `j if isinstance(j, list) else
    /// j.get("orders", [])`).
    pub fn open_orders(&self) -> Result<Vec<resp::PmOrder>, VenueError> {
        let r = self.call(Priority::Background, "GET", P_OPEN, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "pmus open_orders",
                status: r.status,
                body: r.body,
            });
        }
        if let Ok(v) = serde_json::from_str::<Vec<resp::PmOrder>>(&r.body) {
            return Ok(v);
        }
        #[derive(serde::Deserialize)]
        struct Envelope {
            #[serde(default)]
            orders: Vec<resp::PmOrder>,
        }
        serde_json::from_str::<Envelope>(&r.body)
            .map(|e| e.orders)
            .map_err(|e| VenueError::Parse {
                endpoint: "pmus:open_orders",
                detail: e.to_string(),
            })
    }

    /// POST /v1/order/preview — projected fills and fees. NEVER submits, so it
    /// is safe to call before a real place. This is the ONLY endpoint that
    /// wraps the order body in `{"request": ...}`; create takes it bare, and a
    /// mismatched wrapper answers with a misleading "Market not found".
    pub fn preview(&self, req: &PlaceRequest) -> Result<String, VenueError> {
        let body = wire::pmus_preview_body(wire::pmus_order_body(
            &req.market,
            req.side,
            &req.price,
            req.qty,
            req.post_only,
        ));
        let r = self.call(Priority::Background, "POST", "/v1/order/preview", Some(&body))?;
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "pmus preview",
                status: r.status,
                body: r.body,
            });
        }
        Ok(r.body)
    }

    /// `order_status` tolerating the not-yet-visible window after a create.
    fn order_status_settled(&self, order_id: &str) -> Result<resp::PmOrder, VenueError> {
        self.settle.retry_404("pmus order_status", order_id, || self.order_status(order_id))
    }
}

impl<T: Transport> VenueGateway for PmusGateway<T> {
    type Order = resp::PmOrder;
    type Balances = resp::PmBalances;
    type Positions = resp::PmPositions;

    /// POST /v1/orders with the BARE order body — see [`Self::preview`] for
    /// why the wrapper matters.
    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError> {
        let body = wire::pmus_order_body(
            &req.market,
            req.side,
            &req.price,
            req.qty,
            req.post_only,
        );
        let r = self.call(Priority::Critical, "POST", P_CREATE, Some(&body))?;
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "pmus place",
                status: r.status,
                body: r.body,
            });
        }
        resp::pmus_order(&r.body)
    }

    fn order_id(order: &Self::Order) -> String {
        order.id.clone()
    }

    /// POST /v1/order/{id}/cancel, with the marketSlug in the BODY.
    ///
    /// Three things differ from Kalshi and all three are deliberate:
    ///   * `market_slug` is REQUIRED. We do NOT self-resolve it via
    ///     open_orders — doing that on every reprice hammered the API into
    ///     429s.
    ///   * a non-2xx is an ERROR, never success. Kalshi's 404-means-already-
    ///     gone does NOT transfer: treating a failed PM cancel as success is
    ///     what caused stray-order accumulation.
    ///   * a [`CancelBy::ClientId`] target is REFUSED, locally. PM-US has no
    ///     client-order-id field on the wire ([`wire::pmus_order_body`] — and
    ///     the retired Python `_order_body` had none either), so there is
    ///     nothing on the venue to resolve our id against. Sending it anyway
    ///     would POST `/v1/order/m1/cancel`, which PM-US answers with <300 for
    ///     an id it has never issued — 11 such "successes" were logged on
    ///     2026-07-28 while every quote kept resting.
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        let CancelBy::VenueId(oid) = &req.by else {
            return Err(VenueError::MissingField {
                endpoint: "pmus cancel",
                field: "venue order id (PM-US cannot resolve a client_order_id)".into(),
            });
        };
        if oid.is_empty() {
            return Err(VenueError::MissingField {
                endpoint: "pmus cancel",
                field: "venue order id".into(),
            });
        }
        let Some(slug) = req.market_slug.as_deref() else {
            return Err(VenueError::MissingField {
                endpoint: "pmus cancel",
                field: "market_slug".into(),
            });
        };
        let path = format!("/v1/order/{oid}/cancel");
        let body = wire::pmus_cancel_body(slug);
        let r = self.call(Priority::Critical, "POST", &path, Some(&body))?;
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "pmus cancel",
                status: r.status,
                body: r.body,
            });
        }
        Ok(())
    }

    /// GET /v1/order/{id} — authoritative. The create response omits fill data
    /// entirely, so this is the only way to learn cumQuantity/avgPx.
    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError> {
        let path = format!("/v1/order/{order_id}");
        let r = self.call(Priority::Background, "GET", &path, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "pmus order_status",
                status: r.status,
                body: r.body,
            });
        }
        resp::pmus_order(&r.body)
    }

    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
        Ok(self.open_orders()?.into_iter().map(|o| o.id).collect())
    }

    /// One idempotent call cancels every resting order (kill-switch sweep) —
    /// no pagination, unlike Kalshi.
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        let r = self.call(Priority::Critical, "POST", P_CANCEL_ALL, Some(&json_empty()))?;
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "pmus cancel_all_open",
                status: r.status,
                body: r.body,
            });
        }
        Ok(())
    }

    /// M2 for PM-US: place ONE contract at 1c, confirm it rests, cancel it.
    /// `market` is the slug, which the cancel needs in its body.
    fn rehearse(&self, market: &str) -> Result<String, VenueError> {
        let placed = self.place(&PlaceRequest {
            market: market.to_string(),
            side: Side::Bid,
            price: "0.0100".into(),
            qty: 1,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: String::new(), // PM-US has no client order id
        })?;
        let oid = placed.id;

        // Every exit path below must cancel — same guarantee as Kalshi.
        let rested = match self.order_status_settled(&oid) {
            Ok(o) => {
                if o.filled_qty() > 0 {
                    Err(VenueError::Status {
                        endpoint: "pmus rehearse",
                        status: 0,
                        body: format!("order {oid} FILLED {} — not a rehearsal", o.filled_qty()),
                    })
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        };

        let cancelled = self.cancel(&CancelRequest {
            by: CancelBy::VenueId(oid.clone()),
            market_slug: Some(market.to_string()),
        });

        match (rested, cancelled) {
            (Ok(()), Ok(())) => Ok(oid),
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(c)) | (Err(_), Err(c)) => Err(VenueError::Status {
                endpoint: "pmus rehearse",
                status: 0,
                body: format!("order {oid} COULD NOT BE CANCELLED ({c}) — CHECK THE VENUE"),
            }),
        }
    }

    fn balances(&self) -> Result<Self::Balances, VenueError> {
        let r = self.call(Priority::Background, "GET", P_BALANCES, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "pmus balances",
                status: r.status,
                body: r.body,
            });
        }
        resp::pmus_balances(&r.body)
    }

    fn positions(&self) -> Result<Self::Positions, VenueError> {
        let r = self.call(Priority::Background, "GET", P_POSITIONS, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "pmus positions",
                status: r.status,
                body: r.body,
            });
        }
        resp::pmus_positions(&r.body)
    }
}

fn json_empty() -> Value {
    serde_json::json!({})
}
