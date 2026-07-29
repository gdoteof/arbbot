//! The PM-US (QCX) half of the gateway. Shared types and helpers live in the
//! parent module; the differences from Kalshi are documented where they bite —
//! the create/preview body wrapper, and the cancel that refuses a client id.

use super::{spend_token, ts_ms, CancelBy, CancelRequest, PlaceRequest, Settle, Side, Tif,
            VenueGateway};
use crate::error::VenueError;
use crate::ratelimit::{Priority, RateLimiter};
use crate::resp;
use crate::sign::PmusSigner;
use crate::transport::{NotWired, Transport};
use crate::wire;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Mutex;

/// PM-US (QCX) paths. Unlike Kalshi there is no API prefix on the base
/// (`https://api.polymarket.us`), so the signed path and the URL path are the
/// same string.
const P_CREATE: &str = "/v1/orders";
const P_OPEN: &str = "/v1/orders/open";
const P_CANCEL_ALL: &str = "/v1/orders/open/cancel";
const P_BALANCES: &str = "/v1/account/balances";
const P_POSITIONS: &str = "/v1/portfolio/positions";

/// PM-US gateway. Same [`Transport`] seam and same inert default as
/// [`super::KalshiGateway`].
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
    ///
    /// The envelope's `orders` is REQUIRED — the same defect
    /// [`resp::KalshiOrdersPage`] carried, reached through the fallback instead
    /// of the struct. Python's `.get("orders", [])` really does default, and
    /// copying that here meant any json object that was not a list answered
    /// `Ok(vec![])`: a 200 error envelope became an EMPTY BOOK, which the sweep
    /// then accepted as proof. A body with no `orders` key is a body we cannot
    /// read, and this function's callers — `resting_order_ids` and
    /// `recover_place` — both treat "empty" as a licence to stop looking.
    ///
    /// [`Priority::Critical`] for the same reason as Kalshi's
    /// [`super::KalshiGateway::all_orders`]: its one caller is
    /// `resting_order_ids`, which is the only evidence `cancel_all_and_verify`
    /// accepts that a sweep worked. A refused read there is a halt that cannot
    /// prove itself clean. (Python agrees — `open_orders` there is
    /// `priority="critical"` too.)
    pub fn open_orders(&self) -> Result<Vec<resp::PmOrder>, VenueError> {
        let r = self.call(Priority::Critical, "GET", P_OPEN, None)?;
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

    /// Find an order this process placed but could not read the answer for.
    ///
    /// PM-US HAS NO CLIENT ORDER ID ON THE WIRE. [`wire::pmus_order_body`] sends
    /// no such field and the retired Python `_order_body` sent none either, so
    /// unlike [`super::KalshiGateway::cancel_by_client_order_id`] there is no
    /// tag of ours on the venue to look ourselves up by. The only handle left is
    /// what the order LOOKS like on `/v1/orders/open`.
    ///
    /// That is a weak handle on a SHARED account, so the rule is deliberately
    /// narrow — an adopted order gets CANCELLED later, and cancelling another
    /// workstream's order is worse than the leak this recovers from
    /// (docs/venue-quirks.md §xv-graceful-shutdown-cancels-orders: "scope any
    /// sweep to orders this process owns"):
    ///   * same market slug and same quantity. Those are the fields an open
    ///     order carries that we also chose — there is no limit price on the row
    ///     at all, and `side` has only ever been captured live in one direction
    ///     (`ORDER_SIDE_SELL`), so matching on a guessed spelling would silently
    ///     never fire;
    ///   * NOT already `claimed` by this process. This is what stops the
    ///     recovery adopting one of our OWN earlier orders: the resting list
    ///     LAGS a write, so an order cancelled a moment ago is still on it, and
    ///     mapping a new order to that id would cancel the wrong one;
    ///   * and EXACTLY ONE candidate. Two indistinguishable orders is a refusal
    ///     with a name, never a coin flip.
    ///
    /// The matching rule is NOT the whole guard, and must not be asked to be:
    /// the caller may only reach here when the place's answer was LOST. Called
    /// on a place the venue REJECTED — a routine 400 for a post-only that would
    /// cross — nothing of ours is resting, so the single candidate it finds can
    /// only be somebody else's. `exec::place_answer_was_lost` is that gate.
    fn recover_place(
        &self,
        req: &PlaceRequest,
        claimed: &HashSet<String>,
    ) -> Result<Option<String>, VenueError> {
        let mut hits: Vec<String> = self
            .open_orders()?
            .into_iter()
            .filter(|o| {
                o.market_slug.as_deref() == Some(req.market.as_str())
                    && o.quantity == Some(req.qty)
                    && !claimed.contains(&o.id)
            })
            .map(|o| o.id)
            .collect();
        if hits.len() > 1 {
            return Err(VenueError::Status {
                endpoint: "pmus recover_place",
                status: 0,
                body: format!(
                    "{} unclaimed orders of {} contract(s) are resting on {} — none of them \
                     is distinguishable from the place whose response was lost, and this \
                     account is SHARED, so NONE is adopted: {}",
                    hits.len(),
                    req.qty,
                    req.market,
                    hits.join(" ")
                ),
            });
        }
        Ok(hits.pop())
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
    ///
    /// AND UNLIKE KALSHI IT IS STILL ACCOUNT-WIDE. `docs/venue-quirks.md`
    /// §`xv-graceful-shutdown-cancels-orders` asks for a sweep scoped to orders
    /// this stack owns and PM-US cannot answer it: the create body carries no
    /// tag of ours (§`pmus-no-client-order-id`), so nothing on the wire
    /// distinguishes our resting order from anybody else's — the same absence
    /// that forces [`Self::recover_place`] to match on market and size and to
    /// REFUSE when two rows are indistinguishable.
    ///
    /// The fields an open-orders row does carry (slug, quantity) are not
    /// ownership. Scoping a KILL SWEEP by them would leave a real order of ours
    /// resting whenever the guess missed, which is the failure this sweep is the
    /// last backstop against; the recovery path can afford that trade because it
    /// only ever ADOPTS an id, and a wrong guess there is caught by its
    /// single-candidate rule. So this stays wide, deliberately, and
    /// `--sweep-only`'s blast-radius banner names it as the one that is.
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
