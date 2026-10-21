//! Orders a venue is refusing — the banner on every view.
//!
//! Every refused place reaches the armed engine's journal verbatim, as
//! `{endpoint}: HTTP {status}: {body}` (`arb_venue::error`): on `[exec] … FAILED:`
//! for hedges and take-take, and on `[maker-exit] PLACE REFUSED` for exits. No
//! view showed them. From 2026-10-09 Kalshi refused every hedge, take-take and
//! exit with a 403 — the account's location attestation had expired, which only
//! a human logging in can renew — and for twelve days the Now view counted each
//! retry as a placement.
//!
//! The Now view's rule holds here: the reason is the venue's own, verbatim.
//! Rejections are grouped by venue, HTTP status and the venue's error code, and
//! the only judgement added is which statuses need a human. 401 and 403 refuse
//! the ACCOUNT, not the order, and the engine retrying them changes nothing;
//! everything else (a paused exchange, a 503, a rejected price) is the venue's to
//! clear or the engine's to retry.
//!
//! Accepted orders are not counted. Kalshi took ~750 maker entry quotes during
//! that outage while refusing everything that hedges or exits them, so "some
//! orders are getting through" is no evidence that the order path is working.

use std::collections::BTreeMap;

use arb_core::clock::now_secs;
use serde_json::{json, Value};

/// The same window the Now view reports over.
const WINDOW_S: u64 = 900;
/// Text a refusal carries in the journal, for `journalctl --grep`.
const MARK: &str = " place: HTTP ";

/// One refused place, parsed out of a `journalctl -o short-unix` line.
#[derive(Debug, PartialEq)]
struct Refusal {
    ts: f64,
    venue: String,
    status: u16,
    code: String,
    message: String,
}

fn parse(line: &str) -> Option<Refusal> {
    let ts = line.split_whitespace().next()?.parse().ok()?;
    let at = line.find(MARK)?;
    // The endpoint is `kalshi place` / `pmus place`: the word before MARK.
    let venue = line[..at].rsplit(|c: char| !c.is_ascii_alphanumeric() && c != '_').next()?;
    let (status, body) = line[at + MARK.len()..].split_once(": ")?;
    let status = status.parse().ok()?;
    // The body is JSON, but the line may continue past it — a maker-exit refusal
    // closes a parenthesis and keeps talking — so read ONE value and stop.
    let doc = serde_json::Deserializer::from_str(body).into_iter::<Value>().next().and_then(Result::ok);
    // Kalshi nests its error under "error"; PM-US does not.
    let err = doc.as_ref().map(|d| d.get("error").unwrap_or(d));
    let field = |k: &str| {
        err.and_then(|e| e.get(k)).map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))
    };
    let code = field("code").unwrap_or_default();
    let message = field("message")
        .or_else(|| (!code.is_empty()).then(|| code.clone()))
        .unwrap_or_else(|| body.chars().take(200).collect());
    Some(Refusal { ts, venue: venue.to_string(), status, code, message })
}

/// Group refusals into banner rows: the ones that need a human first, then the
/// most frequent.
fn rows<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<Value> {
    struct Seen {
        count: u64,
        first: f64,
        last: f64,
        message: String,
    }
    let mut by: BTreeMap<(String, u16, String), Seen> = BTreeMap::new();
    for r in lines.filter_map(parse) {
        let e = by
            .entry((r.venue, r.status, r.code))
            .or_insert(Seen { count: 0, first: r.ts, last: r.ts, message: r.message });
        e.count += 1;
        e.first = e.first.min(r.ts);
        e.last = e.last.max(r.ts);
    }
    let mut out: Vec<Value> = by
        .into_iter()
        .map(|((venue, status, code), s)| {
            json!({
                "venue": venue,
                "status": status,
                "code": code,
                "message": s.message,
                "count": s.count,
                "first_ts": s.first,
                "last_ts": s.last,
                "needs_human": status == 401 || status == 403,
            })
        })
        .collect();
    out.sort_by_key(|r| (!r["needs_human"].as_bool().unwrap_or(false), std::cmp::Reverse(r["count"].as_u64())));
    out
}

pub fn json() -> String {
    let Some((unit, _)) = super::now::armed_engine() else {
        return json!({"rows": [], "error": "no arb-trader process found"}).to_string();
    };
    let since = format!("-{WINDOW_S}s");
    let out = std::process::Command::new("journalctl")
        .args(["--user", "-u", &unit, "--since", &since, "-o", "short-unix", "--no-pager", "--grep", MARK])
        .output();
    let Ok(out) = out else {
        return json!({"rows": [], "error": "journalctl is not reachable from here"}).to_string();
    };
    let text = String::from_utf8_lossy(&out.stdout);
    json!({
        "unit": unit,
        "window_s": WINDOW_S,
        "generated_at": now_secs(),
        "rows": rows(text.lines()),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ATTESTATION: &str = r#"1792599502.123456 shame arb-trader[3370874]: [exec] Kalshi FAILED: kalshi place: HTTP 403: {"error":{"code":"Your_location_attestation_for_API_trading_is_missing_or_expired._Please_verify_your_location_in_the_Kalshi_app_or_website,_and_then_retry.","message":"Your location attestation for API trading is missing or expired. Please verify your location in the Kalshi app or website, and then retry.","details":"","service":"exchange"}}"#;
    const EXIT_PAUSED: &str = r#"1792599510.5 shame arb-trader[3370874]: [maker-exit] PLACE REFUSED on KXTIME-26-POP (kalshi place: HTTP 409: {"error":{"code":"trading_is_paused"}}) — if this is a post-only rejection the book moved"#;
    const PMUS_503: &str = r#"1792599520.0 shame arb-trader[3370874]: [exec] PolymarketUs FAILED: pmus place: HTTP 503: {"code":14,"message":"upstream connect error"}"#;

    #[test]
    fn the_venues_own_reason_is_read_verbatim() {
        let r = parse(ATTESTATION).unwrap();
        assert_eq!(r.venue, "kalshi");
        assert_eq!(r.status, 403);
        assert!(r.code.starts_with("Your_location_attestation"));
        assert_eq!(
            r.message,
            "Your location attestation for API trading is missing or expired. Please verify your location in the Kalshi app or website, and then retry."
        );
        assert_eq!(r.ts, 1792599502.123456);
    }

    #[test]
    fn a_body_followed_by_more_prose_still_parses() {
        let r = parse(EXIT_PAUSED).unwrap();
        assert_eq!((r.venue.as_str(), r.status, r.code.as_str()), ("kalshi", 409, "trading_is_paused"));
        // No message in the body: the code is the most the venue said.
        assert_eq!(r.message, "trading_is_paused");
    }

    #[test]
    fn a_pmus_error_is_not_nested_and_its_code_is_a_number() {
        let r = parse(PMUS_503).unwrap();
        assert_eq!((r.venue.as_str(), r.status, r.code.as_str()), ("pmus", 503, "14"));
        assert_eq!(r.message, "upstream connect error");
    }

    #[test]
    fn a_body_that_is_not_json_survives_as_itself() {
        let r = parse("1.0 h t[1]: [exec] Kalshi FAILED: kalshi place: HTTP 502: <html>bad gateway</html>").unwrap();
        assert_eq!(r.code, "");
        assert_eq!(r.message, "<html>bad gateway</html>");
    }

    #[test]
    fn lines_that_are_not_refusals_are_ignored() {
        assert_eq!(parse("1.0 h t[1]: [exec] Kalshi placed m1 -> 01a1"), None);
        assert_eq!(parse("-- No entries --"), None);
    }

    #[test]
    fn an_account_refusal_outranks_a_more_frequent_order_refusal() {
        let lines = [ATTESTATION, PMUS_503, PMUS_503, PMUS_503, ATTESTATION.replacen("1792599502.123456", "1792599600.0", 1).as_str()]
            .map(str::to_string);
        let rows = rows(lines.iter().map(String::as_str));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["venue"], "kalshi");
        assert_eq!(rows[0]["needs_human"], true);
        assert_eq!(rows[0]["count"], 2);
        assert_eq!(rows[0]["first_ts"], 1792599502.123456);
        assert_eq!(rows[0]["last_ts"], 1792599600.0);
        assert_eq!(rows[1]["venue"], "pmus");
        assert_eq!(rows[1]["needs_human"], false);
        assert_eq!(rows[1]["count"], 3);
    }
}
