//! Risk manager — pure decision fold, statement-for-statement port of
//! `RiskManager.check_order` in src/arbbot/risk/manager.py, pinned by the
//! Python-generated parity fixtures in tests/fixtures/risk/cases.json
//! (scripts/risk_fixtures.py). P4 provable #4 in docs/p3-shell.md.
//!
//! This crate has zero I/O by design, and so does the risk gate here: the two
//! environmental inputs the Python code reads from the world — the kill switch
//! (a file's existence) and the topic's weakest deployed forward APR (from
//! data/exec/marks.json) — arrive as DATA (`kill`, `weakest_fwd_apr`). Ledger
//! seeding, balance polling and marks bookkeeping stay in the engine; only the
//! allow/deny arithmetic lives here.
//!
//! All arithmetic runs in the same 28-significant-digit ROUND_HALF_EVEN context
//! Python's Decimal defaults to (via the `dec` crate, the General Decimal
//! Arithmetic spec), and every value that appears in a reason string is
//! rendered with `to_standard_notation_string()` — which matches CPython
//! `str(Decimal)` for the small-exponent monetary values in play — so the
//! decision matches byte-for-byte, reasons included.

use crate::scan::D;
use dec::{Context, Rounding};
use serde::Deserialize;
use std::cmp::Ordering;

// ---------- decimal context (same idiom as scan::Cx) ----------

struct Ctx {
    even: Context<D>,
    q_cent: D,
}

impl Ctx {
    fn new() -> Self {
        let mut even = Context::<D>::default();
        even.set_precision(28).expect("prec 28");
        even.set_rounding(Rounding::HalfEven);
        let mut t = even.clone();
        let q_cent = t.parse("0.01").expect("q_cent");
        Ctx { even, q_cent }
    }

    fn parse(&mut self, s: &str) -> D {
        self.even.parse(s).expect("decimal string")
    }

    fn add(&mut self, mut a: D, b: D) -> D {
        self.even.add(&mut a, &b);
        a
    }

    fn sub(&mut self, mut a: D, b: D) -> D {
        self.even.sub(&mut a, &b);
        a
    }

    fn mul(&mut self, mut a: D, b: D) -> D {
        self.even.mul(&mut a, &b);
        a
    }

    fn div(&mut self, mut a: D, b: D) -> D {
        self.even.div(&mut a, &b);
        a
    }

    /// Sign-of-difference comparison — mirrors scan::Cx::cmp so ordering
    /// follows Decimal semantics (2.0 == 2.00), not float bit patterns.
    fn cmp(&mut self, a: D, b: D) -> Ordering {
        let d = self.sub(a, b);
        if d.is_zero() {
            Ordering::Equal
        } else if d.is_negative() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }

    /// Decimal.min: returns the operand unchanged (exponent preserved), so
    /// `min(Decimal("8.0"), Decimal("25.0"))` renders "8.0" like Python.
    fn min(&mut self, a: D, b: D) -> D {
        if self.cmp(a, b) == Ordering::Greater {
            b
        } else {
            a
        }
    }

    /// f"{util:.2f}" — quantize to 2dp HALF_EVEN, fixed two-decimal notation.
    fn fmt2(&mut self, mut util: D) -> String {
        let q = self.q_cent;
        self.even.quantize(&mut util, &q);
        util.to_standard_notation_string()
    }
}

/// str(Decimal) for the monetary range here (no scientific notation).
fn s(d: D) -> String {
    d.to_standard_notation_string()
}

fn oracle_scaler(risk: &str) -> &'static str {
    match risk {
        "low" => "1.0",
        "medium" => "0.5",
        "high" => "0.25",
        other => panic!("unknown oracle_risk {other:?}"),
    }
}

// ---------- inputs (mirror scripts/risk_fixtures.py case schema) ----------

#[derive(Deserialize)]
pub struct TopicIn {
    pub family: String,
    pub budget_usd: String,
    pub only_below_util: Option<String>,
}

#[derive(Deserialize)]
pub struct ConfigIn {
    pub bankroll: String,
    pub tail_fraction: String,
    pub per_rel_cap: Option<String>,
    pub per_class_cap: String,
    pub global_cap: String,
    pub overflow_min_apr: String,
    pub overflow_frac: String,
    pub default_topic_budget: Option<String>,
    pub default_only_below_util: Option<String>,
    pub topics: Vec<TopicIn>,
}

#[derive(Deserialize)]
pub struct RelIn {
    pub id: String,
    #[serde(rename = "type")]
    pub rtype: String,
    pub oracle_risk: String,
}

#[derive(Deserialize)]
pub struct ExposureIn {
    pub by_relationship: Vec<(String, String)>,
    pub by_class: Vec<(String, String)>,
    pub by_topic: Vec<(String, String)>,
}

#[derive(Deserialize)]
pub struct Input {
    pub config: ConfigIn,
    pub rel: RelIn,
    pub exposure: ExposureIn,
    pub balances: Vec<(String, String)>,
    pub notional: String,
    pub venue_costs: Vec<(String, String)>,
    pub opportunity_apr: Option<String>,
    pub weakest_fwd_apr: Option<String>,
    pub kill: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Decision {
    pub allowed: bool,
    pub max_notional: String,
    pub reasons: Vec<String>,
}

// ---------- helpers ----------

fn topic_of(rel_id: &str, topics: &[TopicIn]) -> String {
    let hay = format!("-{rel_id}-");
    let mut best = "";
    for t in topics {
        if hay.contains(&format!("-{}-", t.family)) && t.family.len() > best.len() {
            best = &t.family;
        }
    }
    if best.is_empty() {
        "other".to_string()
    } else {
        best.to_string()
    }
}

fn topic_budget(config: &ConfigIn, topic: &str) -> (Option<String>, Option<String>) {
    for t in &config.topics {
        if t.family == topic {
            return (Some(t.budget_usd.clone()), t.only_below_util.clone());
        }
    }
    (
        config.default_topic_budget.clone(),
        config.default_only_below_util.clone(),
    )
}

fn lookup(cx: &mut Ctx, pairs: &[(String, String)], key: &str, zero: D) -> D {
    for (k, v) in pairs {
        if k == key {
            return cx.parse(v);
        }
    }
    zero
}

// ---------- the fold ----------

/// Port of `RiskManager.check_order`: gate one new basket. Every reason string
/// and the clamped `max_notional` reproduce the Python output exactly.
pub fn check_order(inp: &Input) -> Decision {
    // kill_switch_active() reduced to data: `_kill or file.exists()`.
    if inp.kill {
        return Decision {
            allowed: false,
            max_notional: "0".to_string(),
            reasons: vec!["kill switch active".to_string()],
        };
    }

    let mut cx = Ctx::new();
    let zero = cx.parse("0");
    let mut reasons: Vec<String> = Vec::new();

    let bankroll = cx.parse(&inp.config.bankroll);
    let notional = cx.parse(&inp.notional);

    // --- per-relationship tail cap ---
    let base = match &inp.config.per_rel_cap {
        Some(p) => cx.parse(p),
        None => {
            let tf = cx.parse(&inp.config.tail_fraction);
            cx.mul(bankroll, tf)
        }
    };
    let scaler = cx.parse(oracle_scaler(&inp.rel.oracle_risk));
    let cap = cx.mul(base, scaler);
    let open_rel = lookup(&mut cx, &inp.exposure.by_relationship, &inp.rel.id, zero);
    let headroom = cx.sub(cap, open_rel);
    if cx.cmp(notional, headroom) == Ordering::Greater {
        reasons.push(format!(
            "per-relationship tail cap: notional {} > headroom {} (cap {}, oracle_risk={})",
            s(notional),
            s(headroom),
            s(cap),
            inp.rel.oracle_risk,
        ));
    }

    // --- class cap (with bounded great-opportunity overflow) ---
    let per_class = cx.parse(&inp.config.per_class_cap);
    let class_cap = cx.mul(bankroll, per_class);
    let open_class = lookup(&mut cx, &inp.exposure.by_class, &inp.rel.rtype, zero);
    let class_sum = cx.add(open_class, notional);
    if cx.cmp(class_sum, class_cap) == Ordering::Greater {
        let of = cx.parse(&inp.config.overflow_frac);
        let bank_of = cx.mul(bankroll, of);
        let hard_ceiling = cx.add(class_cap, bank_of);
        let omin = cx.parse(&inp.config.overflow_min_apr);
        let overflow_ok = match &inp.opportunity_apr {
            Some(a) => {
                let ad = cx.parse(a);
                cx.cmp(ad, omin) != Ordering::Less
                    && cx.cmp(class_sum, hard_ceiling) != Ordering::Greater
            }
            None => false,
        };
        if !overflow_ok {
            let mut r = format!(
                "class cap: {}+{} > {}",
                s(open_class),
                s(notional),
                s(class_cap)
            );
            if inp.opportunity_apr.is_some() {
                r.push_str(&format!(" (overflow needs apr>={})", s(omin)));
            }
            reasons.push(r);
        }
    }

    // --- global cap ---
    let global_frac = cx.parse(&inp.config.global_cap);
    let global_cap = cx.mul(bankroll, global_frac);
    let mut total = zero;
    for (_, v) in &inp.exposure.by_relationship {
        let vd = cx.parse(v);
        total = cx.add(total, vd);
    }
    let total_sum = cx.add(total, notional);
    if cx.cmp(total_sum, global_cap) == Ordering::Greater {
        reasons.push(format!(
            "global cap: {}+{} > {}",
            s(total),
            s(notional),
            s(global_cap)
        ));
    }

    // --- topic budget + gate ---
    let topic = topic_of(&inp.rel.id, &inp.config.topics);
    let (budget, gate) = topic_budget(&inp.config, &topic);
    if let Some(budget_s) = budget {
        let budget_d = cx.parse(&budget_s);
        let open_topic = lookup(&mut cx, &inp.exposure.by_topic, &topic, zero);
        let topic_sum = cx.add(open_topic, notional);
        if cx.cmp(topic_sum, budget_d) == Ordering::Greater {
            // overflow bar = min(weakest deployed fwd APR, overflow_min_apr);
            // no marks -> overflow_min_apr alone.
            let omin = cx.parse(&inp.config.overflow_min_apr);
            let bar = match &inp.weakest_fwd_apr {
                Some(w) => {
                    let wd = cx.parse(w);
                    cx.min(wd, omin)
                }
                None => omin,
            };
            let overflow_ok = match &inp.opportunity_apr {
                Some(a) => {
                    let ad = cx.parse(a);
                    cx.cmp(ad, bar) != Ordering::Less
                }
                None => false,
            };
            if !overflow_ok {
                let mut r = format!(
                    "topic budget [{}]: {}+{} > {}",
                    topic,
                    s(open_topic),
                    s(notional),
                    s(budget_d)
                );
                if inp.opportunity_apr.is_some() {
                    r.push_str(&format!(" (overflow needs apr>={})", s(bar)));
                }
                reasons.push(r);
            }
        }
    }
    if let Some(gate_s) = gate {
        let gate_d = cx.parse(&gate_s);
        let util = if class_cap.is_zero() {
            cx.parse("1")
        } else {
            cx.div(total, class_cap)
        };
        if cx.cmp(util, gate_d) != Ordering::Less {
            reasons.push(format!(
                "topic [{}] gated to util<{}: at {}",
                topic,
                s(gate_d),
                cx.fmt2(util)
            ));
        }
    }

    // --- per-venue cash ---
    for (venue, cost) in &inp.venue_costs {
        let cost_d = cx.parse(cost);
        let avail = lookup(&mut cx, &inp.balances, venue, zero);
        if cx.cmp(cost_d, avail) == Ordering::Greater {
            reasons.push(format!(
                "insufficient {} balance: {} > {}",
                venue,
                s(cost_d),
                s(avail)
            ));
        }
    }

    // max_notional = max(headroom, 0): ties (headroom == 0) keep headroom's
    // exponent, matching Python max()'s first-argument-wins on equality.
    let max_notional = if cx.cmp(headroom, zero) == Ordering::Less {
        s(zero)
    } else {
        s(headroom)
    };

    Decision {
        allowed: reasons.is_empty(),
        max_notional,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct ExpectedOut {
        allowed: bool,
        max_notional: String,
        reasons: Vec<String>,
    }

    #[derive(Deserialize)]
    struct Case {
        name: String,
        input: Input,
        output: ExpectedOut,
    }

    #[derive(Deserialize)]
    struct Doc {
        cases: Vec<Case>,
    }

    const FIXTURES: &str =
        include_str!("../../../../tests/fixtures/risk/cases.json");

    #[test]
    fn parity_with_python_fixtures() {
        let doc: Doc = serde_json::from_str(FIXTURES).expect("cases.json parses");
        assert!(doc.cases.len() >= 30, "expected a broad matrix");
        for c in &doc.cases {
            let got = check_order(&c.input);
            assert_eq!(got.allowed, c.output.allowed, "allowed [{}]", c.name);
            assert_eq!(
                got.max_notional, c.output.max_notional,
                "max_notional [{}]",
                c.name
            );
            assert_eq!(got.reasons, c.output.reasons, "reasons [{}]", c.name);
        }
    }

    // --- named invariants (independent of the fixture matrix) ---

    fn base() -> Input {
        Input {
            config: ConfigIn {
                bankroll: "1000".into(),
                tail_fraction: "0.02".into(),
                per_rel_cap: None,
                per_class_cap: "0.35".into(),
                global_cap: "0.50".into(),
                overflow_min_apr: "25.0".into(),
                overflow_frac: "0.10".into(),
                default_topic_budget: None,
                default_only_below_util: None,
                topics: vec![],
            },
            rel: RelIn {
                id: "rel-x".into(),
                rtype: "equivalent-pair".into(),
                oracle_risk: "low".into(),
            },
            exposure: ExposureIn {
                by_relationship: vec![],
                by_class: vec![],
                by_topic: vec![],
            },
            balances: vec![("kalshi".into(), "100000".into())],
            notional: "10".into(),
            venue_costs: vec![("kalshi".into(), "5".into())],
            opportunity_apr: None,
            weakest_fwd_apr: None,
            kill: false,
        }
    }

    #[test]
    fn kill_switch_denies_and_masks_everything() {
        let mut inp = base();
        inp.kill = true;
        inp.notional = "999999".into(); // would trip every cap too
        let d = check_order(&inp);
        assert!(!d.allowed);
        assert_eq!(d.reasons, vec!["kill switch active".to_string()]);
        assert_eq!(d.max_notional, "0");
    }

    #[test]
    fn allow_has_no_reasons_and_positive_clamp() {
        let d = check_order(&base());
        assert!(d.allowed);
        assert!(d.reasons.is_empty());
        assert_eq!(d.max_notional, "20.000"); // 1000 * 0.02 * 1.0
    }

    #[test]
    fn negative_headroom_clamps_to_zero() {
        let mut inp = base();
        inp.exposure.by_relationship = vec![("rel-x".into(), "30".into())];
        inp.notional = "0".into();
        let d = check_order(&inp);
        assert!(!d.allowed);
        assert_eq!(d.max_notional, "0");
    }

    #[test]
    fn oracle_risk_scales_the_cap() {
        // high risk -> 0.25 scaler -> cap 5.0000, headroom shown scaled
        let mut inp = base();
        inp.rel.oracle_risk = "high".into();
        inp.notional = "6".into();
        let d = check_order(&inp);
        assert!(!d.allowed);
        assert_eq!(d.max_notional, "5.0000");
    }
}
