//! Typed response structs for the money-path endpoints.
//!
//! Contract (task spec):
//! - Unknown fields are TOLERATED (serde ignores them by default).
//! - Missing REQUIRED fields become [`VenueError::MissingField`] — never a
//!   silent zero/default.
//! - Money fields stay STRINGS — no float coercion. Integer COUNTS (contract
//!   quantities) are numbers on the wire and are parsed as integers.
//!
//! Every fixture shape here is drawn from the live-captured contracts in
//! `tests/test_venue_contracts.py` / `test_venue_contract_gaps.py`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::error::{from_serde, VenueError};

fn parse<'a, T: Deserialize<'a>>(endpoint: &'static str, body: &'a str) -> Result<T, VenueError> {
    serde_json::from_str::<T>(body).map_err(|e| from_serde(endpoint, &e))
}

// ------------------------------------------------------------------ Kalshi ---

/// One row from GET /portfolio/orders (history) or the `{"order": ...}` in a
/// single-order GET. `fill_count_fp` is a PLAIN contract count string
/// ("2.00" == 2), not a scaled fixed-point.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrder {
    pub order_id: String,
    /// ABSENT on a create response — `POST /portfolio/events/orders` answers
    /// with `{client_order_id, fill_count, order_id, remaining_count, ts_ms}`
    /// and no status at all (observed live 2026-07-27). Requiring it here is
    /// what made the first two live smokes fail after the order was already
    /// resting. A create tells you an order EXISTS; only a GET tells you what
    /// it is doing.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub ticker: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub fill_count_fp: Option<String>,
    /// The create response's spelling of the same thing.
    #[serde(default)]
    pub fill_count: Option<String>,
    #[serde(default)]
    pub remaining_count: Option<String>,
    /// Our own idempotency tag. It is the ONLY way to find an order we created
    /// but whose create response we failed to read.
    #[serde(default)]
    pub client_order_id: Option<String>,
}

impl KalshiOrder {
    /// A create response has no status, so this is false for one — deliberately.
    /// "It exists" and "it is resting" are different claims.
    pub fn is_resting(&self) -> bool {
        self.status.as_deref() == Some("resting")
    }

    /// Cumulative filled contracts, replicating Python
    /// `int(float(o.get("fill_count_fp") or 0))` exactly (0 when absent).
    pub fn filled_qty(&self) -> i64 {
        self.fill_count_fp
            .as_deref()
            .or(self.fill_count.as_deref())
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f as i64)
            .unwrap_or(0)
    }

    /// The same number, but `None` rather than 0 when the venue did not say.
    ///
    /// FOR THE ONE CALLER THAT ACTS ON "IT DID NOT FILL" BY SENDING AN ORDER.
    /// `filled_qty` folds absent, unparseable and genuinely-zero into the same
    /// `0`, which is right for a display or a rehearsal guard and wrong here:
    /// this file's own header calls a defaulted money field "the one place a
    /// wrong default is an AFFIRMATIVE CLAIM", and a claim of "did not trade"
    /// is what authorises a second hedge. A shape change on this field family
    /// is not hypothetical — the create response ships `fill_count` where the
    /// docs say `fill_count_fp`, live on 2026-07-27 — so absence must be
    /// answerable as "unknown".
    ///
    /// Zero is reported as `Some(0)`, and that is not a guess: every recorded
    /// Kalshi order row carries the field explicitly when nothing has filled
    /// (`"fill_count_fp":"0.00"` on a resting row, `"fill_count":"0.00"` on the
    /// live create). So an ABSENT field really does mean the shape moved.
    ///
    /// A negative is refused rather than floored: it cannot be a fill, and
    /// letting it through as `<= credited` would authorise a place.
    pub fn try_filled_qty(&self) -> Option<i64> {
        let raw = self.fill_count_fp.as_deref().or(self.fill_count.as_deref())?;
        let n = raw.trim().parse::<f64>().ok()?;
        (n >= 0.0 && n.is_finite()).then_some(n as i64)
    }
}

/// `{"order": { ... }}` envelope (single-order GET, create response).
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderEnvelope {
    pub order: KalshiOrder,
}

/// GET /portfolio/orders — HISTORY, paginated. `cursor` absent on the last page.
///
/// `orders` is REQUIRED, which is this module's own header contract and not a
/// style preference. It carried `#[serde(default)]`, so EVERY json object
/// deserialized: a 200 whose body has no top-level `orders` — an error envelope
/// served with 200, a venue-side re-nesting — became `Ok(vec![])`. That is the
/// one place a wrong default is an AFFIRMATIVE CLAIM. `all_orders` answered
/// "nothing", `cancel_all_open` cancelled nothing and returned `Ok(())`,
/// `resting_order_ids` answered "empty", two of those satisfied
/// `confirm_empty_reads`, and the process printed "book PROVEN clean at exit"
/// over a book it had never read. `bins/arb-trader/src/sink.rs` is explicit
/// that an UNREADABLE list must be UNPROVEN and refuses to let a read error
/// stand in for proof; defaulting laundered one class of unreadable list into
/// "empty" upstream of that refusal, where it could no longer be told apart.
///
/// A required field rather than a custom deserializer or a post-parse presence
/// check because serde's own `missing field` error is already what
/// [`crate::error::from_serde`] turns into [`VenueError::MissingField`] — the
/// mechanism the header promises exists; this struct just was not using it.
///
/// NEITHER VENUE HAS BEEN OBSERVED SERVING SUCH A BODY. This closes a contract,
/// it does not record an incident. `cursor` keeps its default: it is genuinely
/// absent on the last page.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrdersPage {
    pub orders: Vec<KalshiOrder>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// One row of GET /portfolio/fills — the venue's own record of a single fill.
///
/// Shape pinned from the live dump in `data/venue/kalshi_fills.json` — the
/// untracked snapshot `arb-books` reconciles the Kalshi account from. Every row carries BOTH `trade_id` and `order_id`, and that pair is
/// what makes the fill feed reconcilable at all: the count alone would only let
/// a caller overwrite a running total, while the id lets it MERGE — see
/// `arb-trader`'s `KalshiFills::claim`.
///
/// `fill_id == trade_id` on every row of it. They are read as separate fields
/// anyway, because the WS `fill` frame carries only `trade_id` and that is the
/// one that has to match.
///
/// `count_fp` is a plain contract count (quirk `kalshi-fill-count-fp-plain-count`)
/// and it is NOT always an integer: about a tenth of the rows are fractional,
/// in pieces that sum to their order's whole size. It stays a STRING here under
/// the module contract above; the caller does the fixed-point arithmetic.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiFillRow {
    /// The WS `fill` frame's dedupe key, and the whole point of this struct.
    pub trade_id: String,
    pub order_id: String,
    pub count_fp: String,
    #[serde(default)]
    pub market_ticker: Option<String>,
    #[serde(default)]
    pub ticker: Option<String>,
    /// Unix SECONDS. Rows come back newest-first in the dump, which is what lets
    /// a caller stop paging instead of walking the account's whole history —
    /// but see `KalshiGateway::fills_since` for why that is not ASSUMED.
    #[serde(default)]
    pub ts: i64,
}

impl KalshiFillRow {
    /// The market this fill was on. `market_ticker` is the WS frame's spelling
    /// and `ticker` the REST row's; both appear in the live dump and they are
    /// equal on every row of it.
    pub fn market(&self) -> &str {
        self.market_ticker.as_deref().or(self.ticker.as_deref()).unwrap_or_default()
    }
}

/// GET /portfolio/fills — paginated, `cursor` absent/empty on the last page.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiFillsPage {
    #[serde(default)]
    pub fills: Vec<KalshiFillRow>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// GET /portfolio/balance. `balance_dollars` is money → string.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiBalance {
    pub balance_dollars: String,
}

/// GET /portfolio/positions.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiPositions {
    #[serde(default)]
    pub market_positions: Vec<KalshiMarketPosition>,
    /// Kalshi's paging convention, the same one `KalshiOrdersPage` and
    /// `KalshiFillsPage` carry: a non-empty cursor means MORE ROWS, and the
    /// last page sends `""` or omits it. It was absent from this struct while
    /// the endpoint's one caller wanted a single page, which made a truncated
    /// position list indistinguishable from a complete one — see
    /// `KalshiGateway::net_positions`, which is the caller that cannot survive
    /// that.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// `position_fp` is a SIGNED contract count string; the *_dollars are money.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiMarketPosition {
    pub ticker: String,
    #[serde(default)]
    pub position_fp: Option<String>,
    #[serde(default)]
    pub market_exposure_dollars: Option<String>,
    #[serde(default)]
    pub realized_pnl_dollars: Option<String>,
    #[serde(default)]
    pub fees_paid_dollars: Option<String>,
}

pub fn kalshi_order_envelope(body: &str) -> Result<KalshiOrderEnvelope, VenueError> {
    parse("kalshi:order", body)
}

/// The CREATE response, which does NOT have one fixed shape.
///
/// `POST /portfolio/events/orders` (the current create path) does not answer
/// with the `{"order": ...}` envelope a single-order GET uses. Python tolerates
/// this by reaching for `resp["order"]["order_id"]` and falling back to
/// `resp["order_id"]`; a strict envelope parse instead fails with "missing
/// required field `order`" AFTER the order is already resting on the venue —
/// which is exactly how the first live smoke left an order behind
/// (2026-07-27).
///
/// So accept every shape the venue has been seen to return, and if none match,
/// put the RAW body in the error: a create response we cannot read is a fact we
/// need in the log, not a guess to make twice.
pub fn kalshi_created_order(body: &str) -> Result<KalshiOrder, VenueError> {
    if let Ok(env) = serde_json::from_str::<KalshiOrderEnvelope>(body) {
        return Ok(env.order);
    }
    if let Ok(o) = serde_json::from_str::<KalshiOrder>(body) {
        return Ok(o);
    }
    if let Ok(page) = serde_json::from_str::<KalshiOrdersPage>(body) {
        if let Some(o) = page.orders.into_iter().next() {
            return Ok(o);
        }
    }
    let mut detail = String::from("unrecognized create response: ");
    detail.push_str(&body.chars().take(600).collect::<String>());
    Err(VenueError::Parse { endpoint: "kalshi:create", detail })
}
pub fn kalshi_orders_page(body: &str) -> Result<KalshiOrdersPage, VenueError> {
    parse("kalshi:orders", body)
}
/// The envelope is `{"fills": [...], "cursor": ...}` per `docs/venue-quirks.md`
/// §`kalshi-fills-are-the-fee-authority`, but the only capture in this repo is
/// the dump, which was already unwrapped to a bare array. So accept BOTH, the
/// way `kalshi_created_order` accepts every create shape the venue has been
/// seen to return — and for the same reason: an envelope we guessed wrong must
/// fail loudly with the raw body, not silently reconcile zero fills.
pub fn kalshi_fills_page(body: &str) -> Result<KalshiFillsPage, VenueError> {
    // Dispatch on the SHAPE before deserializing. `#[serde(default)]` on
    // `fills` — which `cursor` genuinely needs, since it is absent on the last
    // page — would otherwise let any JSON object at all parse as a page of
    // zero fills. "Nothing to reconcile" is precisely the wrong answer to give
    // for a response we could not read: it leaves contracts unhedged and moves
    // no counter. So the `fills` key must actually be there.
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return Err(from_serde("kalshi:fills", &e)),
    };
    let has_fills = v.get("fills").map(|f| f.is_array()).unwrap_or(false);
    if has_fills {
        return parse("kalshi:fills", body);
    }
    if v.is_array() {
        return Ok(KalshiFillsPage { fills: parse("kalshi:fills", body)?, cursor: None });
    }
    let mut detail = String::from("neither {\"fills\":[..]} nor a bare array; raw body: ");
    detail.push_str(&body.chars().take(600).collect::<String>());
    Err(VenueError::Parse { endpoint: "kalshi:fills", detail })
}
pub fn kalshi_balance(body: &str) -> Result<KalshiBalance, VenueError> {
    parse("kalshi:balance", body)
}
pub fn kalshi_positions(body: &str) -> Result<KalshiPositions, VenueError> {
    parse("kalshi:positions", body)
}

// ------------------------------------------------------------------- PM-US ---

/// PM-US sends money BOTH ways: order prices are strings
/// (`{"value":"0.2600"}`) but `/v1/account/balances` sends bare JSON numbers
/// (`"buyingPower":329.29805`, observed live 2026-07-27). Accept either and
/// keep the string form, so nothing downstream ever sees a float.
///
/// A JSON number is rendered shortest-roundtrip, which reproduces the wire text
/// exactly at account-balance magnitudes. This is the same coercion Python does
/// (`Decimal(str(bal["buyingPower"]))`). PRICES stay strings end to end — those
/// are the values decimal parity actually depends on.
fn money_string<'de, D>(d: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    match serde_json::Value::deserialize(d)? {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(D::Error::custom(format!(
            "expected a money string or number, got {other}"
        ))),
    }
}

/// A PM-US money value: `{"value": "0.2600", "currency": "USD"}`.
#[derive(Debug, Clone, Deserialize)]
pub struct MoneyVal {
    #[serde(deserialize_with = "money_string")]
    pub value: String,
    #[serde(default)]
    pub currency: Option<String>,
}

/// A PM-US order. Quantities are wire NUMBERS (int); money is nested string
/// `MoneyVal`. On CREATE the response omits execution data entirely
/// (`{"id": ..., "executions": []}`) — `cum_quantity`/`avg_px` are then `None`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmOrder {
    pub id: String,
    #[serde(default)]
    pub market_slug: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub quantity: Option<i64>,
    #[serde(default)]
    pub cum_quantity: Option<i64>,
    #[serde(default)]
    pub leaves_quantity: Option<i64>,
    #[serde(default)]
    pub avg_px: Option<MoneyVal>,
    #[serde(default)]
    pub commission_notional_total_collected: Option<MoneyVal>,
    #[serde(default)]
    pub executions: Option<Vec<serde_json::Value>>,
}

/// Typed encoding of the `pmus-create-omits-fill-data` / `avgPx=0-on-create`
/// quirk: a create response carries NO fill data; the authoritative re-fetched
/// order carries `cumQuantity` + `avgPx`. Callers must branch on this, never
/// read `avgPx` off a create response (it would be absent/zero).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PmFillState {
    /// Create response (or any order before the async fill lands).
    NoFillData,
    /// Authoritative order: cumulative filled qty + average fill price (string).
    Filled { cum_quantity: i64, avg_px: String },
}

impl PmOrder {
    pub fn fill_state(&self) -> PmFillState {
        match (self.cum_quantity, self.avg_px.as_ref()) {
            (Some(q), Some(px)) if q >= 1 => PmFillState::Filled {
                cum_quantity: q,
                avg_px: px.value.clone(),
            },
            _ => PmFillState::NoFillData,
        }
    }

    /// Filled contracts from `cumQuantity` (0 if unknown) — the PM analogue of
    /// Python `int(float(o.get("cumQuantity") or 0))`.
    pub fn filled_qty(&self) -> i64 {
        self.cum_quantity.unwrap_or(0)
    }

    /// The same number, but `None` rather than 0 when the venue did not say.
    /// See [`KalshiOrder::try_filled_qty`] for why the distinction has to exist.
    ///
    /// Only a CREATE response omits `cumQuantity` — it "omits execution data
    /// entirely", as the struct above records — and this is never read off one:
    /// the caller has an order id and re-fetches. Every recorded order ROW
    /// carries it, `0` included. Note this is a different question from
    /// [`Self::fill_state`], which also requires `avgPx` and so answers
    /// `NoFillData` for a perfectly readable `cumQuantity` of 0.
    pub fn try_filled_qty(&self) -> Option<i64> {
        self.cum_quantity.filter(|n| *n >= 0)
    }
}

/// `{"order": {...}}` or a bare order — the get_order path unwraps `order` if
/// present (`j.get("order", j)`).
pub fn pmus_order(body: &str) -> Result<PmOrder, VenueError> {
    #[derive(Deserialize)]
    struct Envelope {
        order: PmOrder,
    }
    // Try the envelope first; fall back to a bare order (create response shape).
    if let Ok(env) = serde_json::from_str::<Envelope>(body) {
        return Ok(env.order);
    }
    parse("pmus:order", body)
}

/// GET /v1/portfolio/positions — a dict KEYED BY SLUG with string netPosition.
///
/// THE PAGING FIELDS ARE UNVERIFIED. Nothing in this repo has ever seen this
/// endpoint return one: no fixture carries them, the Python reader
/// (`PmusSession.get_positions`) does not look for either, and
/// `docs/venue-quirks.md` §`pmus-positions-dict-keyed-by-slug` does not mention
/// paging. They are read anyway because `PmusGateway::net_positions` refuses a
/// map that says it is partial, and a guard that costs nothing is worth having
/// against an endpoint documented to serve partial sets for OTHER reasons.
///
/// The `alias` is part of that, and is a guess with a stated basis rather than
/// a fact: every other field this venue sends is camelCase (`netPosition`,
/// `costPerShare`, `qtyBought`, `avgPx`, `buyingPower`), so `nextCursor` is the
/// far likelier spelling of the two. Accepting both costs one attribute and
/// cannot break a parse. Treat the guard as a belt, not a proof: if this
/// endpoint ever does paginate under a third spelling, the map goes short and
/// nothing here notices.
#[derive(Debug, Clone, Deserialize)]
pub struct PmPositions {
    #[serde(default)]
    pub positions: BTreeMap<String, PmPosition>,
    #[serde(default, alias = "nextCursor")]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub eof: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmPosition {
    pub net_position: String,
    #[serde(default)]
    pub qty_sold: Option<String>,
    #[serde(default)]
    pub avg_px: Option<MoneyVal>,
}

pub fn pmus_positions(body: &str) -> Result<PmPositions, VenueError> {
    parse("pmus:positions", body)
}

/// GET /v1/account/balances — `{"balances": [{"buyingPower": "..."}]}`.
#[derive(Debug, Clone, Deserialize)]
pub struct PmBalances {
    pub balances: Vec<PmBalance>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmBalance {
    #[serde(deserialize_with = "money_string")]
    pub buying_power: String,
}

pub fn pmus_balances(body: &str) -> Result<PmBalances, VenueError> {
    parse("pmus:balances", body)
}
