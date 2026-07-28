//! The engine's risk view — the [`RiskGate`] the quoters consult.
//!
//! `arb_core::risk::check_order` is the pure fold (ported with fixtures); this
//! is the state it folds over. The engine owns it because exposure is a
//! whole-book quantity: a per-quoter copy would let each relationship spend the
//! same headroom.
//!
//! Config comes from the same files the Python runner used (`config/exec.yaml`,
//! `config/topics.yaml`) so the caps are one source of truth, not a second
//! transcription.

use arb_core::model::Venue;
use arb_core::quoter::{RiskGate, RiskVerdict};
use arb_core::risk::{check_order, ConfigIn, ExposureIn, Input, RelIn, TopicIn};
use arb_core::scan::Rel;
use std::collections::HashMap;
use std::sync::Mutex;

/// Python `RiskConfig` defaults (src/arbbot/risk/manager.py:37-59). Only
/// bankroll and per_class_cap are file-driven; the rest are code defaults there
/// and are pinned here so the two cannot drift silently.
const TAIL_FRACTION: &str = "0.02";
const PER_REL_CAP: &str = "150";
const GLOBAL_CAP: &str = "0.50";
const OVERFLOW_MIN_APR: &str = "0.12";
const OVERFLOW_FRAC: &str = "0.10";

#[derive(Default)]
struct Exposure {
    by_rel: HashMap<String, f64>,
    by_class: HashMap<String, f64>,
    by_topic: HashMap<String, f64>,
}

pub struct RiskView {
    bankroll: String,
    per_class_cap: String,
    topics: Vec<(String, String, Option<String>)>, // family, budget, only_below_util
    default_topic_budget: Option<String>,
    default_only_below_util: Option<String>,
    /// venue -> available cash. Empty means the per-venue cash check sees $0
    /// and refuses everything, so this is REQUIRED for the gate to pass — an
    /// unconfigured balance fails closed, which is the right direction.
    balances: Vec<(String, String)>,
    /// rel id -> oracle_risk, from the registry. It scales the per-rel cap and
    /// is not carried on `Rel`, so it is looked up by id.
    oracle_risk: HashMap<String, String>,
    exposure: Mutex<Exposure>,
    /// Counts, for the stats line. Rejections are not errors — a gate that
    /// never fires is a gate nobody can see working.
    pub checked: Mutex<(u64, u64)>, // (allowed, rejected)
}

fn topics_from_yaml(path: &str) -> (Vec<(String, String, Option<String>)>, Option<String>, Option<String>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (Vec::new(), None, None);
    };
    let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&text) else {
        return (Vec::new(), None, None);
    };
    let num = |v: Option<&serde_yaml::Value>| -> Option<String> {
        let v = v?;
        v.as_str().map(|s| s.to_string()).or_else(|| v.as_f64().map(|f| f.to_string()))
    };
    let mut out = Vec::new();
    if let Some(list) = doc.get("topics").and_then(|t| t.as_sequence()) {
        for t in list {
            let Some(family) = t.get("family").and_then(|f| f.as_str()) else { continue };
            let Some(budget) = num(t.get("budget_usd")) else { continue };
            out.push((family.to_string(), budget, num(t.get("only_below_util"))));
        }
    }
    (out, num(doc.get("default_topic_budget")), num(doc.get("default_only_below_util")))
}

impl RiskView {
    /// `balances` are venue->cash pairs. The dry-run engine has no credentials,
    /// so they arrive on the command line; once the engine reads them from the
    /// venue at startup (as the Python runner did) that becomes the source.
    pub fn load(
        exec_yaml: &str,
        topics_yaml: &str,
        balances: Vec<(String, String)>,
        oracle_risk: HashMap<String, String>,
    ) -> RiskView {
        let (mut bankroll, mut per_class) = ("980".to_string(), "0.35".to_string());
        if let Ok(text) = std::fs::read_to_string(exec_yaml) {
            if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&text) {
                if let Some(b) = doc.get("bankroll_usd").and_then(|v| v.as_f64()) {
                    bankroll = b.to_string();
                }
                if let Some(c) = doc.get("per_class_cap").and_then(|v| v.as_f64()) {
                    per_class = c.to_string();
                }
            }
        }
        let (topics, default_topic_budget, default_only_below_util) = topics_from_yaml(topics_yaml);
        RiskView {
            bankroll,
            per_class_cap: per_class,
            topics,
            default_topic_budget,
            default_only_below_util,
            balances,
            oracle_risk,
            exposure: Mutex::new(Exposure::default()),
            checked: Mutex::new((0, 0)),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "bankroll ${} per_class {} topics {} balances [{}]",
            self.bankroll,
            self.per_class_cap,
            self.topics.len(),
            self.balances
                .iter()
                .map(|(v, b)| format!("{v}=${b}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }

    /// A fill opened `qty` more contracts on this relationship — the exposure
    /// the NEXT check must see. Python: `risk.record_open(rel, filled)`.
    /// Takes the id/class rather than a `&Rel` so the engine can record from a
    /// fill without holding the quoter's relationship.
    pub fn record_open(&self, rel_id: &str, rtype: &str, qty: f64) {
        let mut e = self.exposure.lock().expect("exposure");
        *e.by_rel.entry(rel_id.to_string()).or_default() += qty;
        *e.by_class.entry(rtype.to_string()).or_default() += qty;
        let topic = topic_of(rel_id, &self.topics);
        *e.by_topic.entry(topic).or_default() += qty;
    }

    pub fn stats(&self) -> (u64, u64) {
        *self.checked.lock().expect("checked")
    }

    fn config(&self) -> ConfigIn {
        ConfigIn {
            bankroll: self.bankroll.clone(),
            tail_fraction: TAIL_FRACTION.into(),
            per_rel_cap: Some(PER_REL_CAP.into()),
            per_class_cap: self.per_class_cap.clone(),
            global_cap: GLOBAL_CAP.into(),
            overflow_min_apr: OVERFLOW_MIN_APR.into(),
            overflow_frac: OVERFLOW_FRAC.into(),
            default_topic_budget: self.default_topic_budget.clone(),
            default_only_below_util: self.default_only_below_util.clone(),
            topics: self
                .topics
                .iter()
                .map(|(f, b, u)| TopicIn {
                    family: f.clone(),
                    budget_usd: b.clone(),
                    only_below_util: u.clone(),
                })
                .collect(),
        }
    }
}

/// Same longest-family-match rule as `arb_core::risk::topic_of`, which is
/// private to that module.
fn topic_of(rel_id: &str, topics: &[(String, String, Option<String>)]) -> String {
    let hay = format!("-{rel_id}-");
    let mut best = "";
    for (family, _, _) in topics {
        if hay.contains(&format!("-{family}-")) && family.len() > best.len() {
            best = family;
        }
    }
    if best.is_empty() { "other".to_string() } else { best.to_string() }
}

impl RiskGate for RiskView {
    fn check(&self, rel: &Rel, venue: Venue, notional: i64) -> RiskVerdict {
        let n = notional.to_string();
        let e = self.exposure.lock().expect("exposure");
        let pairs = |m: &HashMap<String, f64>| -> Vec<(String, String)> {
            m.iter().map(|(k, v)| (k.clone(), v.to_string())).collect()
        };
        let inp = Input {
            config: self.config(),
            rel: RelIn {
                id: rel.id.clone(),
                rtype: rel.rtype.as_str().to_string(),
                // Unknown oracle_risk is treated as HIGH (0.25x cap), not low:
                // a relationship we cannot classify is not one to size up.
                oracle_risk: self
                    .oracle_risk
                    .get(&rel.id)
                    .cloned()
                    .unwrap_or_else(|| "high".to_string()),
            },
            exposure: ExposureIn {
                by_relationship: pairs(&e.by_rel),
                by_class: pairs(&e.by_class),
                by_topic: pairs(&e.by_topic),
            },
            balances: self.balances.clone(),
            notional: n.clone(),
            venue_costs: vec![(venue.as_str().to_string(), n)],
            // The maker path passes no APR, so the great-opportunity overflow
            // never applies to a resting quote (quoter.py:302 — only the
            // take-take path supplies one).
            opportunity_apr: None,
            weakest_fwd_apr: None,
            // KILL is handled by the engine's own deadline, which cancels
            // everything rather than merely refusing new orders.
            kill: false,
        };
        drop(e);
        let d = check_order(&inp);
        let mut c = self.checked.lock().expect("checked");
        if d.allowed {
            c.0 += 1;
        } else {
            c.1 += 1;
        }
        RiskVerdict { allowed: d.allowed, reasons: d.reasons }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    fn rel(id: &str) -> Rel {
        Rel {
            id: id.into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        }
    }

    fn view(balances: Vec<(&str, &str)>, oracle: &str) -> RiskView {
        let mut o = HashMap::new();
        o.insert("r1".to_string(), oracle.to_string());
        RiskView::load(
            "/nonexistent/exec.yaml", // falls back to the pinned defaults
            "/nonexistent/topics.yaml",
            balances.into_iter().map(|(a, b)| (a.to_string(), b.to_string())).collect(),
            o,
        )
    }

    /// FAIL-CLOSED: an unconfigured venue balance reads as $0 cash, so the
    /// order is refused rather than waved through. Getting this backwards would
    /// let a misconfigured engine trade with imaginary capital.
    #[test]
    fn no_balance_configured_refuses_every_order() {
        let v = view(vec![], "low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed);
        assert!(d.reasons.iter().any(|r| r.contains("insufficient kalshi balance")), "{:?}", d.reasons);
    }

    #[test]
    fn a_funded_venue_within_caps_is_allowed() {
        let v = view(vec![("kalshi", "340")], "low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(d.allowed, "{:?}", d.reasons);
    }

    /// Cash is checked per VENUE: funding Kalshi does not fund PM-US.
    #[test]
    fn cash_is_checked_on_the_venue_being_quoted() {
        let v = view(vec![("kalshi", "340")], "low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        assert!(!v.check(&rel("r1"), Venue::PolymarketUs, 5).allowed);
    }

    /// Exposure accumulates across fills and eventually closes the per-rel cap
    /// ($150 at oracle_risk=low). This is the whole point of the engine owning
    /// one view: per-quoter copies would each spend the same headroom.
    #[test]
    fn accumulated_exposure_eventually_refuses() {
        let v = view(vec![("kalshi", "1000")], "low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        v.record_open("r1", "cross-venue-equivalent", 150.0);
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed, "150 open against a 150 cap leaves no headroom");
        assert!(d.reasons.iter().any(|r| r.contains("per-relationship")), "{:?}", d.reasons);
    }

    /// oracle_risk scales the per-rel cap (low 1.0, medium 0.5, high 0.25), so
    /// a high-risk name refuses far sooner on the same exposure.
    #[test]
    fn oracle_risk_scales_the_cap() {
        for (oracle, open, want_allowed) in
            // caps: low 150, medium 75, high 37.5; a clip of 5 needs that much headroom
            [("low", 100.0, true), ("medium", 100.0, false), ("high", 35.0, false)]
        {
            let v = view(vec![("kalshi", "1000")], oracle);
            v.record_open("r1", "cross-venue-equivalent", open);
            assert_eq!(
                v.check(&rel("r1"), Venue::Kalshi, 5).allowed,
                want_allowed,
                "oracle_risk={oracle} open={open}"
            );
        }
    }

    /// An unknown relationship is treated as HIGH oracle risk, not low: a name
    /// we cannot classify is not one to size up.
    #[test]
    fn an_unclassified_relationship_gets_the_tightest_cap() {
        let v = view(vec![("kalshi", "1000")], "low");
        v.record_open("unknown-rel", "cross-venue-equivalent", 30.0);
        let d = v.check(&rel("unknown-rel"), Venue::Kalshi, 10);
        assert!(!d.allowed, "unknown => high risk => 0.25x cap: {:?}", d.reasons);
    }

    #[test]
    fn allowed_and_rejected_are_counted_for_the_stats_line() {
        let v = view(vec![("kalshi", "340")], "low");
        v.check(&rel("r1"), Venue::Kalshi, 5);
        v.check(&rel("r1"), Venue::PolymarketUs, 5); // unfunded => refused
        assert_eq!(v.stats(), (1, 1));
    }
}
