//! recorder.yaml + registry.yaml + credentials — mirrors ops/config.py and
//! the registry universe extraction in record/main.py.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct RecorderConfig {
    pub registry_path: String,
    pub data_dir: String,
    pub health_path: String,
    pub socket_path: String,
    #[serde(default = "default_poll_interval")]
    pub kalshi_poll_interval_s: f64,
    #[serde(default)]
    pub ntfy_topic: String,
    #[serde(default)]
    pub polymarket_us_tags: Vec<String>,
    /// Defaults to TRUE, matching `record_polymarket_intl: bool = True` in
    /// ops/config.py — a recorder.yaml that predates the key must keep
    /// recording INTL, not silently stop.
    #[serde(default = "default_record_polymarket_intl")]
    pub record_polymarket_intl: bool,
}

fn default_poll_interval() -> f64 {
    5.0
}

fn default_record_polymarket_intl() -> bool {
    true
}

pub fn load_recorder_config(path: &str) -> Result<RecorderConfig> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    serde_yaml::from_str(&text).with_context(|| format!("parse {path}"))
}

// registry.yaml: only venue/market_id per leg matter here (market_ids() in
// registry/model.py takes ALL relationships' legs, vetted or not)
#[derive(Deserialize)]
struct RegistryDoc {
    #[serde(default)]
    relationships: Vec<RelationshipDoc>,
}
#[derive(Deserialize)]
struct RelationshipDoc {
    #[serde(default)]
    legs: Vec<LegDoc>,
}
#[derive(Deserialize)]
struct LegDoc {
    venue: String,
    market_id: String,
}

pub struct Universe {
    pub kalshi_tickers: Vec<String>,
    pub pm_tokens: Vec<String>,
    /// PM-US market ids named by the registry. The PM-US subscription universe
    /// is otherwise TAG-driven, and tag-driven alone is silently lossy: between
    /// 2026-07-27 and 07-31 the france and brazil election events fell out of
    /// the tag listing and 19 of the registry's 88 PM-US markets stopped being
    /// recorded — every `xvus-france-pres-27` leg, the family holding most of
    /// the capital — while their Kalshi legs ticked normally. Nothing said so.
    /// `record/main.py:76` fixed that for Python; this is the same fix.
    pub pmus_slugs: Vec<String>,
}

pub fn load_universe(registry_path: &str) -> Result<Universe> {
    let text = std::fs::read_to_string(registry_path)
        .with_context(|| format!("read {registry_path}"))?;
    let doc: RegistryDoc = serde_yaml::from_str(&text).context("parse registry")?;
    let mut kalshi = BTreeSet::new();
    let mut pm = BTreeSet::new();
    let mut pmus = BTreeSet::new();
    for r in doc.relationships {
        for leg in r.legs {
            match leg.venue.as_str() {
                "kalshi" => {
                    kalshi.insert(leg.market_id);
                }
                "polymarket" => {
                    pm.insert(leg.market_id);
                }
                "polymarket_us" => {
                    pmus.insert(leg.market_id);
                }
                _ => {}
            }
        }
    }
    Ok(Universe {
        kalshi_tickers: kalshi.into_iter().collect(),
        pm_tokens: pm.into_iter().collect(),
        pmus_slugs: pmus.into_iter().collect(),
    })
}

/// systemd LoadCredential dir or ARBBOT_CREDENTIALS_DIR (ops/config.py).
pub fn credentials_dir() -> Option<PathBuf> {
    std::env::var_os("CREDENTIALS_DIRECTORY")
        .or_else(|| std::env::var_os("ARBBOT_CREDENTIALS_DIR"))
        .map(PathBuf::from)
}

pub fn load_credential(name: &str) -> Option<Vec<u8>> {
    let dir = credentials_dir()?;
    let p: &Path = &dir.join(name);
    std::fs::read(p).ok().filter(|b| !b.is_empty())
}

pub fn load_credential_str(name: &str) -> Option<String> {
    load_credential(name).map(|b| String::from_utf8_lossy(&b).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "registry_path: config/registry.yaml\n\
                           data_dir: data/raw\n\
                           health_path: data/health.jsonl\n\
                           socket_path: data/arbbot.sock\n";

    /// The default is the whole safety property: a recorder.yaml written
    /// before the key existed must keep recording INTL. Defaulting to false
    /// would silently drop a venue on every config that has not been updated.
    #[test]
    fn record_polymarket_intl_defaults_to_true_when_absent() {
        let cfg: RecorderConfig = serde_yaml::from_str(MINIMAL).expect("parse");
        assert!(
            cfg.record_polymarket_intl,
            "absent key must mean ON, matching ops/config.py's `bool = True`"
        );
    }

    #[test]
    fn record_polymarket_intl_false_is_honored() {
        let text = format!("{MINIMAL}record_polymarket_intl: false\n");
        let cfg: RecorderConfig = serde_yaml::from_str(&text).expect("parse");
        assert!(!cfg.record_polymarket_intl);
    }

    /// `polymarket_us` legs were silently dropped on the floor here — the match
    /// had arms for `kalshi` and `polymarket` only — so the registry could not
    /// contribute anything to the PM-US subscription set.
    #[test]
    fn load_universe_collects_polymarket_us_legs() {
        let d = std::env::temp_dir().join(format!("uni-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join("registry.yaml");
        std::fs::write(
            &p,
            "relationships:\n\
             - id: r1\n\
             \x20 legs:\n\
             \x20 - venue: kalshi\n\
             \x20   market_id: KX-A\n\
             \x20 - venue: polymarket_us\n\
             \x20   market_id: ewc-pres-fra-2027-04-11-frahol\n\
             - id: r2\n\
             \x20 legs:\n\
             \x20 - venue: polymarket\n\
             \x20   market_id: '12345'\n",
        )
        .unwrap();
        let u = load_universe(p.to_str().unwrap()).expect("parse");
        assert_eq!(u.pmus_slugs, vec!["ewc-pres-fra-2027-04-11-frahol"]);
        assert_eq!(u.kalshi_tickers, vec!["KX-A"]);
        assert_eq!(u.pm_tokens, vec!["12345"], "INTL tokens must not absorb PM-US slugs");
        std::fs::remove_dir_all(&d).ok();
    }

    /// The live registry is what the recorder actually starts against, and the
    /// france family is the concrete case this was built for.
    #[test]
    fn live_registry_carries_the_france_pmus_legs() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../config/registry.yaml");
        let u = load_universe(path).expect("live registry parses");
        assert!(u.pmus_slugs.len() >= 80, "expected the registry's PM-US legs, got {}", u.pmus_slugs.len());
        assert!(
            u.pmus_slugs.iter().any(|s| s == "ewc-pres-fra-2027-04-11-frahol"),
            "france-pres-27 legs are the ones the tag listing dropped for four days"
        );
    }

    /// The live config/recorder.yaml is the file this binary is promoted
    /// against; if it stops parsing, the recorder does not start.
    #[test]
    fn live_recorder_yaml_parses_and_has_intl_off() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../config/recorder.yaml");
        let cfg = load_recorder_config(path).expect("live recorder.yaml parses");
        assert!(
            !cfg.record_polymarket_intl,
            "INTL was turned off on Geoff's call 2026-07-31; if this flips back \
             on, the promoted recorder resumes the ntfy alert storm"
        );
    }
}
