//! arb-registry — the vetted pair registry.
//!
//! `config/registry.yaml` is PRIVATE and gitignored (it is the trading pair
//! set), so it is always read from a runtime path, never embedded.
//!
//! NOTE: `arb-recorder`, `arb-trader`, `arb-golden` and `arb-intent` each carry
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
    /// Registry-only half of the gate: a HUMAN verdict, not an agent one.
    /// Mirrors `Relationship.tradable` in `src/arbbot/registry/model.py:132`.
    pub fn human_vetted(&self) -> bool {
        matches!(self.verdict.as_deref(), Some("equivalent") | Some("equivalent-with-caveat"))
            && self.vetted_by.as_deref() == Some("human")
    }

    /// The registry's REVOCATION channel, and the verdict that triggered it.
    /// Deciding a pair is not equivalent is recorded by editing the entry in
    /// place to `verdict: rejected` — that is how the four `neh2026-*-REJECTED`
    /// entries are stored. Anything else the gate cannot read as approval
    /// (missing, misspelled, a half-finished edit) vetoes too: a verdict nobody
    /// can classify is not permission.
    pub fn veto(&self) -> Option<&str> {
        match self.verdict.as_deref() {
            Some("equivalent") | Some("equivalent-with-caveat") => None,
            other => Some(other.unwrap_or("(missing)")),
        }
    }

    /// The FULL gate the runner actually applies (`exec/main.py:131-146`):
    /// human-vetted in the registry OR explicitly listed in
    /// `config/tradable.yaml`. The allowlist exists for pairs approved through
    /// board cards before the registry tracked `vetted_by`, and it records that
    /// provenance WITHOUT stamping a false "human" verdict on the entry — so
    /// checking only the registry half understates what is permitted.
    ///
    /// A veto beats BOTH halves. Approval and revocation are not symmetric:
    /// the allowlist supplies an approval the registry never recorded, but
    /// nothing may override an approval the registry has withdrawn — otherwise
    /// `verdict: rejected`, the only revocation the registry has, is a no-op on
    /// exactly the ids most likely to be trading.
    pub fn tradable(&self, allowlist: &Allowlist) -> bool {
        self.veto().is_none() && (self.human_vetted() || allowlist.contains(&self.id))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Allowlist(std::collections::BTreeSet<String>);

#[derive(Debug, Default, Deserialize)]
struct AllowDoc {
    #[serde(default)]
    allow: Vec<String>,
}

impl Allowlist {
    /// Missing file is an EMPTY allowlist, not an error: the gate then falls
    /// back to registry vetting alone, which is the conservative direction.
    pub fn load(path: &str) -> Allowlist {
        let Ok(text) = std::fs::read_to_string(path) else { return Allowlist::default() };
        let doc: AllowDoc = serde_yaml::from_str(&text).unwrap_or_default();
        Allowlist(doc.allow.into_iter().collect())
    }

    pub fn contains(&self, id: &str) -> bool {
        self.0.contains(id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
  - id: rejected-but-allowlisted
    verdict: rejected
    vetted_by: agent
  - id: rejected-human
    verdict: rejected
    vetted_by: human
  - id: unknown-verdict
    verdict: not-equivalent
    vetted_by: human
  - id: no-verdict
    vetted_by: human
"#;

    #[test]
    fn parses_legs_and_applies_the_human_vetting_gate() {
        let p = std::env::temp_dir().join(format!("arb-reg-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");
        assert_eq!(reg.relationships.len(), 6);

        let r = reg.get("xvus-time-poty-26-zohranmamdani").expect("found");
        assert_eq!(r.legs.len(), 2);
        assert_eq!(r.legs[0].venue, "kalshi");
        assert_eq!(r.legs[1].market_id, "tpoyc-2026-zohmam");
        assert_eq!(r.oracle_risk.as_deref(), Some("low"));
        // agent-vetted: recordable and scannable, but NOT tradable...
        let empty = Allowlist::default();
        assert!(!r.human_vetted());
        assert!(!r.tradable(&empty), "agent verdict alone must not be tradable");

        assert!(reg.get("human-vetted-one").unwrap().tradable(&empty));
    }

    #[test]
    fn the_allowlist_is_the_other_half_of_the_gate() {
        let p = std::env::temp_dir().join(format!("arb-reg-allow-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");
        let ap = std::env::temp_dir().join(format!("arb-allow-{}.yaml", std::process::id()));
        std::fs::write(&ap, "allow:\n  - xvus-time-poty-26-zohranmamdani\n").unwrap();
        let allow = Allowlist::load(ap.to_str().unwrap());
        assert_eq!(allow.len(), 1);

        let r = reg.get("xvus-time-poty-26-zohranmamdani").unwrap();
        assert!(!r.human_vetted(), "registry still says agent");
        assert!(r.tradable(&allow), "allowlist opens the gate without faking a human verdict");

        // a missing allowlist file must not error, and must not widen the gate
        let none = Allowlist::load("/nonexistent/tradable.yaml");
        assert!(none.is_empty());
        assert!(!r.tradable(&none));
        let _ = std::fs::remove_file(ap);
        assert!(reg.get("nope").is_none());
        let _ = std::fs::remove_file(p);
    }

    /// A rejection is a VETO the allowlist cannot override. `verdict: rejected`
    /// in place is the registry's only way to say "looked at it, NOT
    /// equivalent" (the four `neh2026-*-REJECTED` entries), and on an
    /// allowlisted id it used to be a no-op: `human_vetted()` went false while
    /// `contains()` stayed true, so the gate still opened.
    #[test]
    fn an_explicit_rejection_vetoes_the_allowlist() {
        let p = std::env::temp_dir().join(format!("arb-reg-veto-{}.yaml", std::process::id()));
        std::fs::write(&p, YAML).unwrap();
        let reg = Registry::load(p.to_str().unwrap()).expect("loads");
        let ap = std::env::temp_dir().join(format!("arb-allow-veto-{}.yaml", std::process::id()));
        std::fs::write(
            &ap,
            "allow:\n  - xvus-time-poty-26-zohranmamdani\n  - rejected-but-allowlisted\n  \
             - unknown-verdict\n  - no-verdict\n",
        )
        .unwrap();
        let allow = Allowlist::load(ap.to_str().unwrap());

        let r = reg.get("rejected-but-allowlisted").unwrap();
        assert_eq!(r.veto(), Some("rejected"));
        assert!(!r.tradable(&allow), "a rejection the allowlist can override is not a rejection");

        // ...and the allowlist keeps doing its job for everything else
        assert!(reg.get("xvus-time-poty-26-zohranmamdani").unwrap().tradable(&allow));

        // a human vetting cannot outvote a rejection either
        let h = reg.get("rejected-human").unwrap();
        assert!(!h.human_vetted());
        assert!(!h.tradable(&allow));

        // PINNED: an unrecognised or absent verdict vetoes too. Fail closed —
        // a revocation spelled some other way must still revoke.
        let u = reg.get("unknown-verdict").unwrap();
        assert_eq!(u.veto(), Some("not-equivalent"));
        assert!(!u.tradable(&allow));
        let n = reg.get("no-verdict").unwrap();
        assert_eq!(n.veto(), Some("(missing)"));
        assert!(!n.tradable(&allow));

        let _ = std::fs::remove_file(ap);
        let _ = std::fs::remove_file(p);
    }
}
