//! Opportunity coverage per pair, straight off the scanner's tape.

use arb_query::{opps, sources_for_range};

use crate::http::query_param;
use crate::{integrity, Args};

/// Opportunity coverage per vetted pair, read straight off the tape —
/// Parquet for closed days, today's JSONL for the live one, resolved by
/// `arb_query::source_for`. Timed and reported so a slow range is visible
/// rather than mysterious.
pub fn json(a: &Args, query: &str) -> String {
    let to = query_param(query, "to").unwrap_or_else(|| integrity::build(&a.data_dir).today);
    let from = query_param(query, "from").unwrap_or_else(|| to.clone());
    let rel = query_param(query, "rel");

    let t0 = std::time::Instant::now();
    let sources = sources_for_range(&a.scan_dir, &a.parquet_dir, "opportunities", &from, &to);
    let n_sources = sources.len();
    match opps::summarize(&sources, rel.as_deref()) {
        Ok(rows) => {
            let ms = t0.elapsed().as_secs_f64() * 1000.0;
            let obs: u64 = rows.iter().map(|r| r.observations).sum();
            format!(
                "{{\"from\":\"{from}\",\"to\":\"{to}\",\"days\":{n_sources},\
                 \"relationships\":{},\"observations\":{obs},\"query_ms\":{ms:.1},\
                 \"rows\":{}}}",
                rows.len(),
                serde_json::to_string(&rows).unwrap_or_else(|_| "[]".into())
            )
        }
        Err(e) => format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    }
}
