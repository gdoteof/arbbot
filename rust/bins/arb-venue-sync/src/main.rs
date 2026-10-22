//! arb-venue-sync — copy both venues' account history into `data/venue/`.
//!
//!   arb-venue-sync [--out data/venue] [--creds ~/.arbbot-credentials]
//!                  [--kalshi-key kalshi_recorder] [--pmus-key polymarket_usa]
//!
//! Signed GETs only; nothing here can place or cancel an order. The default
//! keys are the retired Python stack's, which nothing live uses, so this never
//! spends the trader's or the recorder's rate budget.
//!
//! History files are an ARCHIVE: each run merges what the venue returns into
//! what is already on disk, keyed by the venue's own id, and never drops a row.
//! Kalshi moves fills off `/portfolio/fills` to `/historical/fills` after a
//! cutoff, and settlements have no historical endpoint at all
//! (`kalshi-history-cutoff` in docs/venue-quirks.md), so once a settlement
//! ages out this archive is the only copy. Balances and positions are point-in-
//! time snapshots and are replaced.
//!
//! Each venue's balance is read before and after its history. `sync.json`
//! records whether the two agree: if they differ, something moved mid-read and
//! the history and the balance are not a consistent pair to reconcile.
//!
//! A venue whose fetch fails keeps its previous files untouched, is named in
//! `sync.json`, and makes the run exit 1.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::Duration;

use arb_venue::{HttpTransport, KalshiSigner, PmusSigner, Transport};
use serde_json::{json, Value};

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";
const PMUS_REST: &str = "https://api.polymarket.us";
/// A cursor still live after this many pages is an error, not a stopping point:
/// a truncated history reads exactly like a complete one.
const MAX_PAGES: usize = 1000;

type Sign = Box<dyn Fn(&str, &str) -> Vec<(&'static str, String)>>;

struct Client {
    transport: HttpTransport,
    sign: Sign,
    /// Between requests. PM-US answered a 56-page crawl at ~4/s with a
    /// Cloudflare 429 (error code 1015) on the activities endpoint.
    pause: Duration,
}

impl Client {
    fn get(&self, path: &str, query: Option<&str>) -> Result<Value, String> {
        let ts = format!("{}", now_ms());
        let headers = (self.sign)(&ts, path);
        std::thread::sleep(self.pause);
        let r = self
            .transport
            .send("GET", path, query, &headers, None)
            .map_err(|e| format!("GET {path}: {e}"))?;
        if r.status != 200 {
            let body: String = r.body.chars().take(200).collect();
            return Err(format!("GET {path}: HTTP {} {body}", r.status));
        }
        serde_json::from_str(&r.body).map_err(|e| format!("GET {path}: unparseable body: {e}"))
    }

    /// Every row of a cursor-paged list. Kalshi ends with an empty `cursor`;
    /// PM-US with `eof: true` (and an empty `nextCursor`).
    ///
    /// `known`, for a newest-first list, ends the walk at the first page whose
    /// rows are ALL already archived: everything older is on disk.
    fn pages(
        &self,
        path: &str,
        page_query: &str,
        items: &str,
        cursor_key: &str,
        known: Option<&dyn Fn(&Value) -> bool>,
    ) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_PAGES {
            let q = if cursor.is_empty() {
                page_query.to_string()
            } else {
                format!("{page_query}&cursor={}", encode(&cursor))
            };
            let page = self.get(path, Some(&q))?;
            let rows: Vec<Value> = match &page[items] {
                Value::Array(rows) => rows.clone(),
                Value::Object(rows) => rows.values().cloned().collect(),
                Value::Null => Vec::new(),
                other => return Err(format!("GET {path}: `{items}` is {other}, not a list")),
            };
            let caught_up = known.is_some_and(|k| !rows.is_empty() && rows.iter().all(k));
            out.extend(rows);
            if caught_up {
                return Ok(out);
            }
            cursor = page[cursor_key].as_str().unwrap_or("").to_string();
            if cursor.is_empty() || page["eof"] == true {
                return Ok(out);
            }
        }
        Err(format!("GET {path}: cursor still live after {MAX_PAGES} pages"))
    }
}

/// Percent-encode a cursor for the query string. PM-US cursors are standard
/// base64, whose `+` would otherwise arrive as a space.
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// The venue's own id for an archived row, so a re-fetch replaces rather than
/// duplicates. `None` means the row cannot be archived safely.
fn kalshi_fill_key(v: &Value) -> Option<String> {
    v["fill_id"].as_str().map(str::to_string)
}

fn kalshi_settlement_key(v: &Value) -> Option<String> {
    Some(format!("{}|{}", v["ticker"].as_str()?, v["settled_time"].as_str()?))
}

fn id_key(v: &Value) -> Option<String> {
    v["id"].as_str().map(str::to_string)
}

/// Trades by trade id, balance changes by transaction id, resolutions by market:
/// a market resolves once, and the resolution rows carry an empty `tradeId`.
fn pmus_activity_key(v: &Value) -> Option<String> {
    if let Some(id) = v["trade"]["id"].as_str() {
        return Some(format!("trade:{id}"));
    }
    if let Some(id) = v["accountBalanceChange"]["transactionId"].as_str() {
        return Some(format!("balance:{id}"));
    }
    if let Some(slug) = v["positionResolution"]["marketSlug"].as_str() {
        return Some(format!("resolution:{slug}"));
    }
    None
}

/// Union of the archive and a fresh fetch, keyed; the fresh copy of a row wins
/// (a balance change can go PENDING -> COMPLETED). Nothing in the archive is
/// ever dropped. Sorted by key so the file is deterministic.
fn merge(archive: Vec<Value>, fresh: Vec<Value>, key: fn(&Value) -> Option<String>) -> Result<Vec<Value>, String> {
    let mut by: BTreeMap<String, Value> = BTreeMap::new();
    for v in archive.into_iter().chain(fresh) {
        let k = key(&v).ok_or_else(|| format!("row has no id: {}", v.to_string().chars().take(160).collect::<String>()))?;
        by.insert(k, v);
    }
    Ok(by.into_values().collect())
}

fn read_archive(path: &Path) -> Result<Vec<Value>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Array(rows)) => Ok(rows),
            Ok(_) => Err(format!("{}: archive is not a JSON array", path.display())),
            Err(e) => Err(format!("{}: {e}", path.display())),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Write-then-rename, so a reader never sees half a file.
fn write_atomic(path: &Path, v: &Value) -> Result<(), String> {
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(v).map_err(|e| e.to_string())?;
    std::fs::write(&tmp, text).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
}

/// Everything one venue produced, written only once ALL of it was fetched.
struct Fetched {
    archives: Vec<(&'static str, Vec<Value>, fn(&Value) -> Option<String>)>,
    snapshots: Vec<(&'static str, Value)>,
    consistent: bool,
}

fn kalshi(c: &Client) -> Result<Fetched, String> {
    let p = "/trade-api/v2";
    let before = c.get(&format!("{p}/portfolio/balance"), None)?;
    let deposits = c.pages(&format!("{p}/portfolio/deposits"), "limit=200", "deposits", "cursor", None)?;
    let withdrawals = c.pages(&format!("{p}/portfolio/withdrawals"), "limit=200", "withdrawals", "cursor", None)?;
    let mut fills = c.pages(&format!("{p}/historical/fills"), "limit=1000", "fills", "cursor", None)?;
    fills.extend(c.pages(&format!("{p}/portfolio/fills"), "limit=1000", "fills", "cursor", None)?);
    let settlements = c.pages(&format!("{p}/portfolio/settlements"), "limit=1000", "settlements", "cursor", None)?;
    let positions = c.pages(&format!("{p}/portfolio/positions"), "limit=1000", "market_positions", "cursor", None)?;
    let after = c.get(&format!("{p}/portfolio/balance"), None)?;
    Ok(Fetched {
        consistent: before["balance_dollars"] == after["balance_dollars"],
        archives: vec![
            ("kalshi_deposits.json", deposits, id_key),
            ("kalshi_withdrawals.json", withdrawals, id_key),
            ("kalshi_fills.json", fills, kalshi_fill_key),
            ("kalshi_settlements.json", settlements, kalshi_settlement_key),
        ],
        snapshots: vec![("kalshi_balance.json", after), ("kalshi_positions.json", Value::Array(positions))],
    })
}

fn pmus(c: &Client, out: &Path) -> Result<Fetched, String> {
    let archived: std::collections::HashSet<String> =
        read_archive(&out.join("pmus_activities.json"))?.iter().filter_map(pmus_activity_key).collect();
    let known = |v: &Value| pmus_activity_key(v).is_some_and(|k| archived.contains(&k));
    let balance = |c: &Client| -> Result<Value, String> {
        let b = c.get("/v1/account/balances", None)?;
        b["balances"].get(0).cloned().ok_or_else(|| "GET /v1/account/balances: no balances[0]".to_string())
    };
    let before = balance(c)?;
    let activities = c.pages(
        "/v1/portfolio/activities",
        "limit=100&sortOrder=SORT_ORDER_DESCENDING",
        "activities",
        "nextCursor",
        Some(&known),
    )?;
    let positions = c.pages("/v1/portfolio/positions", "limit=100", "positions", "nextCursor", None)?;
    let after = balance(c)?;
    Ok(Fetched {
        consistent: before["currentBalance"] == after["currentBalance"]
            && before["marginRequirement"] == after["marginRequirement"],
        archives: vec![("pmus_activities.json", activities, pmus_activity_key)],
        snapshots: vec![("pmus_balances.json", after), ("pmus_positions.json", Value::Array(positions))],
    })
}

/// Merge and write one venue's files, returning the row counts written.
fn store(out: &Path, f: Fetched) -> Result<Value, String> {
    let mut counts = serde_json::Map::new();
    let mut merged = Vec::new();
    for (name, fresh, key) in f.archives {
        let path = out.join(name);
        let rows = merge(read_archive(&path)?, fresh, key)?;
        counts.insert(name.to_string(), json!(rows.len()));
        merged.push((path, Value::Array(rows)));
    }
    for (path, v) in merged {
        write_atomic(&path, &v)?;
    }
    for (name, v) in f.snapshots {
        write_atomic(&out.join(name), &v)?;
    }
    Ok(json!({"ok": true, "consistent": f.consistent, "rows": counts}))
}

fn read_secret(dir: &Path, name: &str) -> Result<String, String> {
    std::fs::read_to_string(dir.join(name))
        .map(|s| s.trim().to_string())
        .map_err(|e| format!("{}: {e}", dir.join(name).display()))
}

fn kalshi_client(creds: &Path, key: &str) -> Result<Client, String> {
    let id = read_secret(creds, &format!("{key}_api_key_id"))?;
    let pem = read_secret(creds, &format!("{key}_private_key.pem"))?;
    let signer = KalshiSigner::from_pkcs8_pem(id, &pem).map_err(|e| e.to_string())?;
    Ok(Client {
        transport: HttpTransport::new(KALSHI_REST, 30).map_err(|e| e.to_string())?,
        sign: Box::new(move |ts, path| signer.headers(ts, "GET", path)),
        pause: Duration::from_millis(250),
    })
}

fn pmus_client(creds: &Path, key: &str) -> Result<Client, String> {
    let id = read_secret(creds, &format!("{key}_key_id"))?;
    let secret = read_secret(creds, &format!("{key}_private_key"))?;
    let signer = PmusSigner::from_secret_b64(id, &secret).map_err(|e| e.to_string())?;
    Ok(Client {
        transport: HttpTransport::new(PMUS_REST, 30).map_err(|e| e.to_string())?,
        sign: Box::new(move |ts, path| signer.headers(ts, "GET", path)),
        pause: Duration::from_secs(1),
    })
}

fn main() {
    let mut out = PathBuf::from("data/venue");
    let mut creds = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(".arbbot-credentials");
    let mut kalshi_key = "kalshi_recorder".to_string();
    let mut pmus_key = "polymarket_usa".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| {
            eprintln!("{a} needs a value");
            exit(2)
        });
        match a.as_str() {
            "--out" => out = PathBuf::from(val()),
            "--creds" => creds = PathBuf::from(val()),
            "--kalshi-key" => kalshi_key = val(),
            "--pmus-key" => pmus_key = val(),
            other => {
                eprintln!("unknown argument {other}");
                exit(2)
            }
        }
    }

    let mut report = json!({"at": now_ms() / 1000});
    let mut failed = false;
    let venues: [(&str, Result<Fetched, String>); 2] = [
        ("kalshi", kalshi_client(&creds, &kalshi_key).and_then(|c| kalshi(&c))),
        ("polymarket_us", pmus_client(&creds, &pmus_key).and_then(|c| pmus(&c, &out))),
    ];
    for (venue, fetched) in venues {
        let result = fetched.and_then(|f| store(&out, f));
        report[venue] = match result {
            Ok(v) => v,
            Err(e) => {
                eprintln!("{venue}: {e}");
                failed = true;
                json!({"ok": false, "error": e})
            }
        };
    }
    if let Err(e) = write_atomic(&out.join("sync.json"), &report) {
        eprintln!("sync.json: {e}");
        failed = true;
    }
    println!("{report}");
    if failed {
        exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_keeps_every_archived_row_and_lets_the_fresh_copy_win() {
        let archive = vec![json!({"fill_id": "a", "n": 1}), json!({"fill_id": "b", "n": 1})];
        let fresh = vec![json!({"fill_id": "b", "n": 2}), json!({"fill_id": "c", "n": 2})];
        let rows = merge(archive, fresh, kalshi_fill_key).unwrap();
        let got: Vec<(String, i64)> = rows
            .iter()
            .map(|r| (r["fill_id"].as_str().unwrap().to_string(), r["n"].as_i64().unwrap()))
            .collect();
        assert_eq!(got, vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 2)]);
    }

    /// A row with no id cannot be deduplicated; archiving it would double it on
    /// the next run, so the merge refuses instead.
    #[test]
    fn a_row_without_an_id_fails_the_merge() {
        assert!(merge(vec![], vec![json!({"ticker": "X"})], kalshi_settlement_key).is_err());
    }

    #[test]
    fn a_base64_cursor_survives_the_query_string() {
        assert_eq!(encode("ab+/c=="), "ab%2B%2Fc%3D%3D");
    }

    #[test]
    fn pmus_rows_key_by_their_own_kind_of_id() {
        let trade = json!({"type": "ACTIVITY_TYPE_TRADE", "trade": {"id": "T1"}});
        let cash = json!({"type": "ACTIVITY_TYPE_ACCOUNT_DEPOSIT", "accountBalanceChange": {"transactionId": "X1"}});
        let res = json!({"type": "ACTIVITY_TYPE_POSITION_RESOLUTION",
                         "positionResolution": {"marketSlug": "m", "tradeId": ""}});
        assert_eq!(pmus_activity_key(&trade).as_deref(), Some("trade:T1"));
        assert_eq!(pmus_activity_key(&cash).as_deref(), Some("balance:X1"));
        assert_eq!(pmus_activity_key(&res).as_deref(), Some("resolution:m"));
        assert_eq!(pmus_activity_key(&json!({"type": "?"})), None);
    }
}
