//! The Trades view — a thin HTTP skin over `crate::trades`, which owns the
//! ledger fold and its accounting.

use std::time::UNIX_EPOCH;

use crate::{Args, FEE_CATEGORY};

/// Trades this system made, priced. Everything is derived from the ledger on
/// each request — no cache, so the tab cannot show a number the ledger does
/// not support.
pub fn json(a: &Args) -> String {
    let text = match std::fs::read_to_string(&a.ledger_path) {
        Ok(t) => t,
        // A missing ledger is the NORMAL state before the engine has ever been
        // armed, so it must read as an empty book rather than an error.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return serde_json::json!({
                "error": format!("read {}: {e}", a.ledger_path),
            })
            .to_string()
        }
    };
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    crate::trades::build(&text, FEE_CATEGORY, now).to_string()
}
