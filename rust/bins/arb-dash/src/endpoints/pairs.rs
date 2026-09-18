//! Registry inventory. Historical quote drill-downs were removed with the
//! top-of-book rollup; this endpoint is intentionally registry-only.

use arb_registry::{Allowlist, Registry};

use crate::Args;

pub fn list_json(a: &Args) -> String {
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);
    let rows: Vec<serde_json::Value> = reg
        .relationships
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "kind": r.kind,
                "verdict": r.verdict,
                "vetted_by": r.vetted_by,
                "tradable": r.tradable(&allow),
                "human_vetted": r.human_vetted(),
                "oracle_risk": r.oracle_risk,
                "tranche": r.tranche,
                "legs": r.legs.iter().map(|l| format!("{}:{}", l.venue, l.market_id))
                        .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "count": rows.len(),
        "tradable": reg.relationships.iter().filter(|r| r.tradable(&allow)).count(),
        "human_vetted": reg.relationships.iter().filter(|r| r.human_vetted()).count(),
        "allowlisted": allow.len(),
        "rows": rows,
    })
    .to_string()
}
