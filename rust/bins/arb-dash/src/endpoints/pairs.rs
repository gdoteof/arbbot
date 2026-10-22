//! Registry inventory. Historical quote drill-downs were removed with the
//! top-of-book rollup; this endpoint is intentionally registry-only.

use arb_registry::Registry;

use crate::Args;

pub fn list_json(a: &Args) -> String {
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let rows: Vec<serde_json::Value> = reg
        .relationships
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "kind": r.kind,
                "verdict": r.verdict,
                "vetted_by": r.vetted_by,
                "tradable": r.tradable(),
                "oracle_risk": r.oracle_risk,
                "tranche": r.tranche,
                "legs": r.legs.iter().map(|l| format!("{}:{}", l.venue, l.market_id))
                        .collect::<Vec<_>>(),
            })
        })
        .collect();
    let tradable_by = |who: &str| {
        reg.relationships
            .iter()
            .filter(|r| r.tradable() && r.vetted_by.as_deref() == Some(who))
            .count()
    };
    serde_json::json!({
        "count": rows.len(),
        "tradable": reg.relationships.iter().filter(|r| r.tradable()).count(),
        "tradable_human": tradable_by("human"),
        "tradable_agent": tradable_by("agent"),
        "rows": rows,
    })
    .to_string()
}
