//! arb-registry — the vetted pair registry.
//!
//! `config/registry.yaml` is PRIVATE and gitignored (it is the trading pair
//! set), so it is always read from a runtime path, never embedded.
//!
//! NOTE: `arb-recorder`, `arb-trader`, and `arb-golden` each carry
//! their own partial `RegistryDoc`, each deserializing only the fields it
//! happens to need. This crate is the full model. Those four are working code
//! and are deliberately left alone; they can migrate here when something else
//! makes them worth touching.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Leg {
    pub venue: String,
    pub market_id: String,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Relationship {
    pub id: String,
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub legs: Vec<Leg>,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub verdict: Option<String>,
    #[serde(default)]
    pub caveats: Vec<String>,
    #[serde(default)]
    pub vetted_by: Option<String>,
    #[serde(default)]
    pub vetted_at: Option<String>,
    #[serde(default)]
    pub confidence: Option<String>,
    #[serde(default)]
    pub oracle_risk: Option<String>,
    #[serde(default)]
    pub tranche: Option<String>,
}

impl Relationship {
    /// The registry's REVOCATION channel, and the verdict that triggered it.
    /// Deciding a pair is not equivalent is recorded by editing the entry in
    /// place to `verdict: rejected` — that is how the four `neh2026-*-REJECTED`
    /// entries are stored. Anything else the gate cannot read as approval
    /// (missing, misspelled, `unreviewed`, a half-finished edit) vetoes too: a
    /// verdict nobody can classify is not permission.
    pub fn veto(&self) -> Option<&str> {
        match self.verdict.as_deref() {
            Some("equivalent") | Some("equivalent-with-caveat") => None,
            other => Some(other.unwrap_or("(missing)")),
        }
    }

    /// THE gate: an `equivalent` / `equivalent-with-caveat` verdict read by a
    /// `human` or an `agent`, either of which may greenlight — an agent
    /// greenlights a pair it is confident resolves identically on both
    /// venues. Rule-generated rows (`vetted_by: mechanical`, the sports map)
    /// and rows with no vetter never quote on their verdict alone. The flip
    /// side: an agent verdict that is NOT meant as a greenlight must be
    /// written as something else (`unreviewed`), never as `equivalent`.
    pub fn tradable(&self) -> bool {
        self.veto().is_none() && matches!(self.vetted_by.as_deref(), Some("human" | "agent"))
    }
}

#[derive(Debug, Default, Deserialize)]
struct Doc {
    #[serde(default)]
    relationships: Vec<Relationship>,
}

#[derive(Debug, Default, Clone)]
pub struct Registry {
    pub relationships: Vec<Relationship>,
}

impl Registry {
    pub fn load(path: &str) -> Result<Registry, String> {
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
        let doc: Doc = serde_yaml::from_str(&text).map_err(|e| format!("parse {path}: {e}"))?;
        Ok(Registry { relationships: doc.relationships })
    }

    pub fn get(&self, id: &str) -> Option<&Relationship> {
        self.relationships.iter().find(|r| r.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
relationships:
  - id: xvus-time-poty-26-zohranmamdani
    type: cross-venue-equivalent
    legs:
      - venue: kalshi
        market_id: KXTIME-26-ZOH
        side: yes
        role: taker
      - venue: polymarket_us
        market_id: tpoyc-2026-zohmam
        side: yes
        role: taker
    direction: "outcome(a) == outcome(b)"
    verdict: equivalent-with-caveat
    caveats: ["group picks must be named identically"]
    vetted_by: agent
    oracle_risk: low
    tranche: head
  - id: human-vetted-one
    legs:
      - venue: kalshi
        market_id: K
      - venue: polymarket_us
        market_id: P
    verdict: equivalent
    vetted_by: human
  - id: rejected-agent
    verdict: rejected
    vetted_by: agent
  - id: rejected-human
    verdict: rejected
    vetted_by: human
  - id: unreviewed-agent
    verdict: unreviewed
    vetted_by: agent
  - id: unknown-verdict
    verdict: not-equivalent
    vetted_by: human
  - id: no-verdict
    vetted_by: human
  - id: mechanical-equivalent
    verdict: equivalent
    vetted_by: mechanical
  - id: unvetted-equivalent
    verdict: equivalent
"#;

    #[test]
    fn parses_legs_and_greenlights_on_the_verdict_whoever_vetted() {
        let p = std::env::temp_dir().join(format!("arb-reg-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");
        assert_eq!(reg.relationships.len(), 9);

        let r = reg.get("xvus-time-poty-26-zohranmamdani").expect("found");
        assert_eq!(r.legs.len(), 2);
        assert_eq!(r.legs[0].venue, "kalshi");
        assert_eq!(r.legs[1].market_id, "tpoyc-2026-zohmam");
        assert_eq!(r.oracle_risk.as_deref(), Some("low"));
        assert_eq!(r.vetted_by.as_deref(), Some("agent"), "the label survives the parse");
        assert!(r.tradable(), "an agent greenlight is a greenlight");

        assert!(reg.get("human-vetted-one").unwrap().tradable());
        assert!(reg.get("nope").is_none());
        let _ = std::fs::remove_file(p);
    }

    /// `verdict: rejected` in place is the registry's only way to say "looked
    /// at it, NOT equivalent" (the four `neh2026-*-REJECTED` entries), and an
    /// agent verdict that was never a greenlight is `unreviewed`. Neither
    /// quotes, and who vetted cannot outvote either.
    #[test]
    fn anything_but_an_equivalent_verdict_vetoes() {
        let p = std::env::temp_dir().join(format!("arb-reg-veto-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");

        for (id, why) in [
            ("rejected-agent", "rejected"),
            ("rejected-human", "rejected"),
            ("unreviewed-agent", "unreviewed"),
            // PINNED: an unrecognised or absent verdict vetoes too. Fail
            // closed — a revocation spelled some other way must still revoke.
            ("unknown-verdict", "not-equivalent"),
            ("no-verdict", "(missing)"),
        ] {
            let r = reg.get(id).unwrap();
            assert_eq!(r.veto(), Some(why), "{id}");
            assert!(!r.tradable(), "{id} must not quote");
        }
        let _ = std::fs::remove_file(p);
    }

    /// A greenlight is a human's or an agent's reading. The sports map writes
    /// `verdict: equivalent` with `vetted_by: mechanical` on every candidate,
    /// and a row with no vetter has no reading behind it at all; neither
    /// quotes, though neither is a revocation either.
    #[test]
    fn an_equivalent_verdict_needs_a_human_or_agent_vetter() {
        let p = std::env::temp_dir().join(format!("arb-reg-vetter-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");

        for id in ["mechanical-equivalent", "unvetted-equivalent"] {
            let r = reg.get(id).unwrap();
            assert_eq!(r.veto(), None, "{id}: the verdict itself is fine");
            assert!(!r.tradable(), "{id} must not quote");
        }
        let _ = std::fs::remove_file(p);
    }
}
