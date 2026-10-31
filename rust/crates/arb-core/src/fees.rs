//! Per-role fee curves, computed in the shared 28-digit half-even context
//! (see scan::Cx). Modelled on what the venues CHARGE, checked against
//! `data/venue/kalshi_fills.json` (`fee_cost`) and
//! `data/venue/pmus_activities.json` (`commissionNotionalCollected`) on
//! 2026-10-24:
//!
//!   * Kalshi: `coef * size * p * (1 - p)` per order, ceiled to $0.0001 (238 of
//!     242 fills that could tell a ceiling from half-even rounding ceiled; the
//!     other four are prorated partial fills). Taker 0.07 everywhere; MAKER
//!     0.0175 only on series the venue flags `quadratic_with_maker_fees`
//!     (`GET /trade-api/v2/series/{series}` → `fee_type`), zero on the rest.
//!     Our fills agree: 83 charged maker fills, all on the flagged series; 144
//!     free ones, none on them.
//!   * PM-US: taker `0.0695 * size * p * (1 - p)` per order, rounded half-even
//!     to the cent (404 of 420 taker fills since 2026-09-19; it was 0.06 before,
//!     405 of 425). Makers pay nothing; the small rebates seen until 2026-10-17
//!     are not modelled.
//!   * Polymarket intl: the per-category taker table of the old Python
//!     `curves.py`, unchanged and unverified here.

use crate::model::Venue;
use crate::scan::{Cx, D};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Maker,
    Taker,
}

/// Kalshi series whose `fee_type` is `quadratic_with_maker_fees` (or the combo
/// variant): resting orders pay `0.0175 * p * (1 - p)` per contract on these
/// and nothing anywhere else. Read from the venue on 2026-10-24 for every
/// series in the registry and every series we have ever filled on:
///
/// ```text
/// curl -s https://api.elections.kalshi.com/trade-api/v2/series/KXPAYROLLS | jq .series.fee_type
/// ```
///
/// [`FeeSchedule::for_market`] charges the maker fee on a series in this list,
/// none on one in [`KALSHI_FREE_MAKER_SERIES`], and — the safe reading — charges
/// it on a series in neither; `kalshi_series_fee_known` lets the trader name
/// those at boot. Query the endpoint for a series before it enters the
/// registry. (`fee_multiplier` is also reported — 0.5 on KXMLBGAME, 0 on
/// KXGREENLAND, where nobody pays — and is not modelled.)
pub const KALSHI_MAKER_FEE_SERIES: &[&str] = &[
    "KXATPMATCH",
    "KXBTCMAX150",
    "KXCPI",
    "KXCPIYOY",
    "KXFED",
    "KXFEDDECISION",
    "KXGDP",
    "KXHEISMAN",
    "KXLLM1",
    "KXMLBGAME",
    "KXMVESPORTSMULTIGAMEEXTENDED",
    "KXNFLCOTY",
    "KXNFLCPOTY",
    "KXNFLDPOTY",
    "KXNFLDROTY",
    "KXNFLMVP",
    "KXNFLOPOTY",
    "KXNFLOROTY",
    "KXPAYROLLS",
    "KXU3",
    "KXWTAMATCH",
];

/// Series the endpoint reported as plain `quadratic`, on 2026-10-24 or on the
/// day they entered the registry: makers pay nothing. With the list above this
/// covers every Kalshi series in the live registry except KXNEWOUTBREAK, which
/// the endpoint does not know.
pub const KALSHI_FREE_MAKER_SERIES: &[&str] = &[
    "CONTROLH",
    "CONTROLS",
    "GOVPARTYAZ",
    "GOVPARTYFL",
    "GOVPARTYGA",
    "GOVPARTYIA",
    "GOVPARTYNV",
    "GOVPARTYOH",
    "GOVPARTYOR",
    "GOVPARTYTX",
    "GOVPARTYVT",
    "GOVPARTYWI",
    "GTA6",
    "KXAAAGASMAX",
    "KXALIENS",
    "KXBALANCEPOWERCOMBO",
    "KXBRPRES",
    "KXBTCMAXY",
    "KXCAWEALTHTAX",
    "KXCPICORE",
    "KXCPICOREYOY",
    "KXCRYPTOSTRUCTURE",
    "KXF1",
    "KXFLPROPERTYTAX",
    "KXFRENCHPRES",
    "KXGPT",
    "KXGREENLAND",
    "KXGREENTERRITORY",
    "KXGTA6ONTIME",
    "KXHOUSERACE",
    "KXITFMATCH",
    "KXITFWMATCH",
    "KXLEADERSOUT",
    "KXLTGOVVT",
    "KXMLBALMVP",
    "KXMLBALROTY",
    "KXMLBNLROTY",
    "KXNOBELPEACE",
    "KXOSCARACTO",
    "KXOSCARACTR",
    "KXOSCARPIC",
    "KXPRESNOMD",
    "KXPRESNOMR",
    "KXPRESPERSON",
    "KXRATECUT",
    "KXSENATELA",
    "KXTIME",
    "KXTOPARTIST",
    "KXTOPARTISTUSA",
    "KXTOPSONGSPOTIFY",
    "KXTOPSONGSPOTIFYUSA",
    "KXTSLA",
    "KXUSAIRANAGREEMENT",
    "KXVENEZUELALEADER",
    "SENATEFLS",
    "SENATEIA",
    "SENATEKS",
    "SENATEME",
    "SENATEMI",
    "SENATEMN",
    "SENATEMS",
    "SENATENH",
    "SENATEOHS",
    "SENATESC",
    "SENATETX",
];

/// The series is the ticker's first dash-separated field (`KXCPI-26SEP-T0.4` →
/// `KXCPI`), compared whole: `KXCPI` must not match `KXCPICORE`.
fn kalshi_series(ticker: &str) -> &str {
    ticker.split('-').next().unwrap_or("")
}

/// Whether either list names this ticker's series. A series on neither is
/// charged as if it did; the trader names such series at boot.
pub fn kalshi_series_fee_known(ticker: &str) -> bool {
    let s = kalshi_series(ticker);
    KALSHI_MAKER_FEE_SERIES.contains(&s) || KALSHI_FREE_MAKER_SERIES.contains(&s)
}

/// Whether Kalshi charges a resting order on this ticker: only a series on
/// [`KALSHI_FREE_MAKER_SERIES`] is free. An unknown series, or an empty
/// ticker, is charged — the reading that cannot price a rest too tight.
pub fn kalshi_charges_makers(ticker: &str) -> bool {
    !KALSHI_FREE_MAKER_SERIES.contains(&kalshi_series(ticker))
}

#[derive(Clone, Copy)]
pub struct FeeSchedule {
    pub kalshi_taker_coef: D,
    /// The maker coefficient this schedule charges on Kalshi. `new` sets the
    /// venue's 0.0175, which is right for the maker-fee series and the SAFE
    /// reading for an unknown market; [`Self::for_market`] zeroes it for a
    /// ticker the venue does not charge.
    pub kalshi_maker_coef: D,
    pub pmus_taker_coef: D,
    /// Polymarket intl taker feeRate per category (curves.py table).
    intl: [(&'static str, D); 10],
    intl_default: D,
    zero: D,
}

impl FeeSchedule {
    pub fn new(cx: &mut Cx) -> Self {
        let mut d = |s: &str| cx.parse_exact(s);
        let intl = [
            ("geopolitics", "0.00"),
            ("politics", "0.04"),
            ("finance", "0.04"),
            ("tech", "0.04"),
            ("mentions", "0.04"),
            ("sports", "0.05"),
            ("economics", "0.05"),
            ("culture", "0.05"),
            ("weather", "0.05"),
            ("crypto", "0.07"),
        ]
        .map(|(k, v)| (k, d(v)));
        FeeSchedule {
            kalshi_taker_coef: d("0.07"),
            kalshi_maker_coef: d("0.0175"),
            pmus_taker_coef: d("0.0695"),
            intl,
            intl_default: d("0.04"),
            zero: d("0"),
        }
    }

    /// The schedule for one market: on Kalshi, a ticker on
    /// [`KALSHI_FREE_MAKER_SERIES`] pays no maker fee; one on
    /// [`KALSHI_MAKER_FEE_SERIES`] or on neither list pays 0.0175. Other venues
    /// are unchanged. Callers that price a Kalshi maker leg should go through
    /// here; the plain schedule charges every Kalshi rest 0.0175, which
    /// over-states the fee on most of the markets we quote.
    pub fn for_market(&self, venue: Venue, market_id: &str) -> FeeSchedule {
        let mut s = *self;
        if venue == Venue::Kalshi && !kalshi_charges_makers(market_id) {
            s.kalshi_maker_coef = self.zero;
        }
        s
    }

    fn intl_coef(&self, category: &str) -> D {
        self.intl
            .iter()
            .find(|(k, _)| *k == category)
            .map(|(_, v)| *v)
            .unwrap_or(self.intl_default)
    }

    /// The fee on an order of `size` contracts at `price`. `category` is read
    /// only by the Polymarket intl curve.
    pub fn fee(&self, cx: &mut Cx, venue: Venue, role: Role, price: D, size: D,
               category: &str) -> D {
        match venue {
            Venue::Kalshi => {
                let coef = if role == Role::Taker {
                    self.kalshi_taker_coef
                } else {
                    self.kalshi_maker_coef
                };
                if coef.is_zero() {
                    return self.zero;
                }
                // ceil to $0.0001 of coef * size * price * (1 - price)
                let a = cx.mul(coef, size);
                let b = cx.mul(a, price);
                let omp = cx.one_minus(price);
                let x = cx.mul(b, omp);
                cx.quantize_ceil_4dp(x)
            }
            Venue::PolymarketUs => {
                if role == Role::Maker {
                    return self.zero;
                }
                let a = cx.mul(self.pmus_taker_coef, price);
                let omp = cx.one_minus(price);
                let b = cx.mul(a, omp);
                let x = cx.mul(b, size);
                cx.quantize_cent_even(x)
            }
            Venue::Polymarket => {
                if role == Role::Maker {
                    return self.zero;
                }
                let k = self.intl_coef(category);
                let a = cx.mul(k, price);
                let omp = cx.one_minus(price);
                let b = cx.mul(a, omp);
                cx.mul(b, size)
            }
        }
    }
}

/// leg_fee — venue-reported per-market coefficient beats the schedule.
// Every argument is an input to the fee formula itself (plus the decimal `Cx`
// this workspace threads explicitly rather than hiding in a thread-local).
// Grouping them into a struct would name the formula's terms twice.
#[allow(clippy::too_many_arguments)]
pub fn leg_fee(cx: &mut Cx, schedule: &FeeSchedule, venue: Venue, role: Role, price: D,
               size: D, category: &str, taker_coef_override: Option<D>) -> D {
    if matches!(venue, Venue::Polymarket | Venue::PolymarketUs)
        && role == Role::Taker
    {
        if let Some(k) = taker_coef_override {
            let a = cx.mul(k, price);
            let omp = cx.one_minus(price);
            let b = cx.mul(a, omp);
            let raw = cx.mul(b, size);
            // PM-US rounds the order's commission to the cent whatever the
            // coefficient; intl's rounding is not established.
            return if venue == Venue::PolymarketUs { cx.quantize_cent_even(raw) } else { raw };
        }
    }
    schedule.fee(cx, venue, role, price, size, category)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fee6(cx: &mut Cx, s: &FeeSchedule, venue: Venue, role: Role, p: &str, n: i64) -> String {
        let p = cx.parse_exact(p);
        let n = cx.from_i64(n);
        let f = s.fee(cx, venue, role, p, n, "default");
        cx.emit_6dp(f)
    }

    /// Fills from `data/venue/kalshi_fills.json`, `fee_cost` as the venue
    /// charged it: the ceiling is to $0.0001, not the cent.
    #[test]
    fn kalshi_ceils_the_order_fee_to_a_hundredth_of_a_cent() {
        let mut cx = Cx::default();
        let s = FeeSchedule::new(&mut cx);
        // 27 x KXFRENCHPRES-27-BRET yes @ 0.029, taker: raw 0.05322051 → 0.0533
        assert_eq!(fee6(&mut cx, &s, Venue::Kalshi, Role::Taker, "0.029", 27), "0.053300");
        // 1 x KXTIME-26-POP no @ 0.78, taker: raw 0.012012 → 0.0121 (the cent
        // ceiling booked 0.02 here, 0.79c nobody paid)
        assert_eq!(fee6(&mut cx, &s, Venue::Kalshi, Role::Taker, "0.78", 1), "0.012100");
        // 17 x KXFEDDECISION-26OCT-H25 no @ 0.33, taker: raw 0.263109 → 0.2632
        assert_eq!(fee6(&mut cx, &s, Venue::Kalshi, Role::Taker, "0.33", 17), "0.263200");
        // 9 x KXCPIYOY-26SEP-T3.7 no @ 0.17, maker: raw 0.02222325 → 0.0223
        assert_eq!(fee6(&mut cx, &s, Venue::Kalshi, Role::Maker, "0.17", 9), "0.022300");
    }

    /// The venue's `fee_type` per series: KXCPI charges makers, KXCPICORE and
    /// KXTIME do not, and the series is matched whole.
    #[test]
    fn kalshi_maker_fee_follows_the_series() {
        let mut cx = Cx::default();
        let s = FeeSchedule::new(&mut cx);
        assert!(kalshi_charges_makers("KXCPI-26SEP-T0.4"));
        assert!(kalshi_charges_makers("KXPAYROLLS-26OCT-T150"));
        assert!(!kalshi_charges_makers("KXCPICORE-26SEP-T0.3"));
        assert!(!kalshi_charges_makers("KXTIME-26-ZOH"));
        assert!(!kalshi_charges_makers("KXGREENLAND-26"), "multiplier 0: nobody pays there");
        // A series on neither list, or no ticker at all, is charged.
        assert!(kalshi_charges_makers("KXNEWOUTBREAK-26-EBOLA"));
        assert!(!kalshi_series_fee_known("KXNEWOUTBREAK-26-EBOLA"));
        assert!(kalshi_charges_makers(""));
        assert!(kalshi_series_fee_known("KXCPI-26SEP-T0.4"));
        assert!(kalshi_series_fee_known("KXTIME-26-ZOH"));
        for s in KALSHI_MAKER_FEE_SERIES {
            assert!(!KALSHI_FREE_MAKER_SERIES.contains(s), "{s} is on both lists");
            assert!(!s.contains('-'), "{s} is a ticker, not a series");
        }
        assert!(KALSHI_FREE_MAKER_SERIES.iter().all(|s| !s.contains('-')));

        let cpi = s.for_market(Venue::Kalshi, "KXCPI-26SEP-T0.4");
        assert_eq!(fee6(&mut cx, &cpi, Venue::Kalshi, Role::Maker, "0.91", 25), "0.035900");
        let time = s.for_market(Venue::Kalshi, "KXTIME-26-ZOH");
        assert_eq!(fee6(&mut cx, &time, Venue::Kalshi, Role::Maker, "0.72", 4), "0.000000");
        // takers pay on every series
        assert_eq!(fee6(&mut cx, &time, Venue::Kalshi, Role::Taker, "0.72", 4), "0.056500");
        // the unspecialised schedule keeps charging: the safe reading
        assert_eq!(fee6(&mut cx, &s, Venue::Kalshi, Role::Maker, "0.72", 4), "0.014200");
        // a PM-US market id never touches the Kalshi coefficient
        let pm = s.for_market(Venue::PolymarketUs, "ewc-pres-bra-2026-10-04-flabol");
        assert_eq!(fee6(&mut cx, &pm, Venue::Kalshi, Role::Maker, "0.72", 4), "0.014200");
    }

    /// `commissionNotionalCollected` on taker executions since 2026-09-19:
    /// 0.0695 x size x p(1-p), rounded half-even to the cent, per order.
    #[test]
    fn pmus_taker_is_0695_rounded_to_the_cent() {
        let mut cx = Cx::default();
        let s = FeeSchedule::new(&mut cx);
        // 2 @ 0.81 (Brazil, 2026-10-24): raw 0.0213930 → 0.02
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Taker, "0.81", 2), "0.020000");
        // 1 @ 0.50: raw 0.017375 → 0.02; the old 0.06 unrounded said 0.015
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Taker, "0.50", 1), "0.020000");
        // 1 @ 0.03: raw 0.00202245 → 0.00
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Taker, "0.03", 1), "0.000000");
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Maker, "0.50", 10), "0.000000");
        // The cent rounding makes the per-contract fee non-monotone in the
        // clip: 2 @ 0.50 pay 0.03 (1.5c/ct), 3 pay 0.05 (1.67c/ct).
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Taker, "0.50", 2), "0.030000");
        assert_eq!(fee6(&mut cx, &s, Venue::PolymarketUs, Role::Taker, "0.50", 3), "0.050000");
        // A per-market coefficient rounds the same way.
        let p = cx.parse_exact("0.50");
        let n = cx.from_i64(2);
        let k = Some(cx.parse_exact("0.06"));
        let f = leg_fee(&mut cx, &s, Venue::PolymarketUs, Role::Taker, p, n, "default", k);
        assert_eq!(cx.emit_6dp(f), "0.030000", "0.03 raw, to the cent");
    }
}
