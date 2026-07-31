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
}

pub fn load_universe(registry_path: &str) -> Result<Universe> {
    let text = std::fs::read_to_string(registry_path)
        .with_context(|| format!("read {registry_path}"))?;
    let doc: RegistryDoc = serde_yaml::from_str(&text).context("parse registry")?;
    let mut kalshi = BTreeSet::new();
    let mut pm = BTreeSet::new();
    for r in doc.relationships {
        for leg in r.legs {
            match leg.venue.as_str() {
                "kalshi" => {
                    kalshi.insert(leg.market_id);
                }
                "polymarket" => {
                    pm.insert(leg.market_id);
                }
                _ => {}
            }
        }
    }
    Ok(Universe {
        kalshi_tickers: kalshi.into_iter().collect(),
        pm_tokens: pm.into_iter().collect(),
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
