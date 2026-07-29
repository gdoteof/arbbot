//! Exact per-role fee curves — port of src/arbbot/fees/curves.py, computed
//! in the shared 28-digit half-even context (see scan::Cx) so results match
//! Python's decimal arithmetic bit-for-bit at emit precision.

use crate::model::Venue;
use crate::scan::{Cx, D};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Maker,
    Taker,
}

pub struct FeeSchedule {
    pub kalshi_taker_coef: D,
    pub kalshi_maker_coef: D,
    pub pmus_taker_coef: D,
    /// Polymarket intl taker feeRate per category (curves.py table).
    intl: Vec<(&'static str, D)>,
    intl_default: D,
}

impl FeeSchedule {
    pub fn new(cx: &mut Cx) -> Self {
        let mut d = |s: &str| cx.parse_exact(s);
        let table = [
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
        ];
        FeeSchedule {
            kalshi_taker_coef: d("0.07"),
            kalshi_maker_coef: d("0.0175"),
            pmus_taker_coef: d("0.06"),
            intl: table.iter().map(|(k, v)| (*k, d(v))).collect(),
            intl_default: d("0.04"),
        }
    }

    fn intl_coef(&self, category: &str) -> D {
        self.intl
            .iter()
            .find(|(k, _)| *k == category)
            .map(|(_, v)| *v)
            .unwrap_or(self.intl_default)
    }

    /// schedule.fee(...) — golden path never hits the pm.mode=="us" branch
    /// (mode defaults to intl), so it is not ported.
    pub fn fee(&self, cx: &mut Cx, venue: Venue, role: Role, price: D, size: D,
               category: &str) -> D {
        match venue {
            Venue::Kalshi => {
                let coef = if role == Role::Taker {
                    self.kalshi_taker_coef
                } else {
                    self.kalshi_maker_coef
                };
                // ceil_cents(coef * size * price * (1 - price))
                let a = cx.mul(coef, size);
                let b = cx.mul(a, price);
                let omp = cx.one_minus(price);
                let x = cx.mul(b, omp);
                cx.quantize_ceil(x)
            }
            Venue::PolymarketUs => {
                if role == Role::Maker {
                    return cx.zero();
                }
                let a = cx.mul(self.pmus_taker_coef, price);
                let omp = cx.one_minus(price);
                let b = cx.mul(a, omp);
                cx.mul(b, size)
            }
            Venue::Polymarket => {
                if role == Role::Maker {
                    return cx.zero();
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
pub fn leg_fee(cx: &mut Cx, schedule: &FeeSchedule, venue: Venue, role: Role, price: D,
               size: D, category: &str, taker_coef_override: Option<D>) -> D {
    if matches!(venue, Venue::Polymarket | Venue::PolymarketUs)
        && role == Role::Taker
    {
        if let Some(k) = taker_coef_override {
            let a = cx.mul(k, price);
            let omp = cx.one_minus(price);
            let b = cx.mul(a, omp);
            return cx.mul(b, size);
        }
    }
    schedule.fee(cx, venue, role, price, size, category)
}
