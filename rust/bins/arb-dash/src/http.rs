//! The server: an accept loop, a router, and one response writer.
//!
//! No HTTP crate, for the reason given in the crate header. That buys a
//! parser small enough to read in one sitting — a request line, a path, a
//! query string — and nothing else here needs to know about sockets.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;
use std::sync::{Arc, Mutex};

use crate::endpoints::{books, current, intents, now, opportunities, pairs, trades};
use crate::rollup::{self, Rollup, Shared};
use crate::{integrity, series, stream, Args};

const PAGE: &str = include_str!("index.html");

pub fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key && !v.is_empty() {
                return Some(v.replace("%3A", ":").replace('+', " "));
            }
        }
    }
    None
}

fn respond(mut s: TcpStream, status: &str, ctype: &str, body: &str) {
    let out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = s.write_all(out.as_bytes());
    let _ = s.flush();
}

fn handle(s: TcpStream, a: &Args, sh: &Shared) {
    let mut line = String::new();
    {
        let mut r = BufReader::new(match s.try_clone() {
            Ok(c) => c,
            Err(_) => return,
        });
        if r.read_line(&mut line).is_err() {
            return;
        }
    }
    let method = line.split_whitespace().next().unwrap_or("GET").to_string();
    let full = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let path = full.split('?').next().unwrap_or("/");
    let query = full.split_once('?').map(|(_, q)| q.to_string()).unwrap_or_default();
    if path.starts_with("/pair/") {
        respond(s, "200 OK", "text/html; charset=utf-8", PAGE);
        return;
    }
    match path {
        // Every view is a real URL. The shell is the same document; its
        // router picks the view from the path and fetches ONLY that view's
        // endpoints, which is the point — a single page would fan out to
        // every endpoint on every load as views are added.
        "/" | "/recording" | "/opportunities" | "/pairs" | "/current" | "/intents"
        | "/trades" | "/live" | "/architecture" | "/now" => {
            respond(s, "200 OK", "text/html; charset=utf-8", PAGE)
        }
        // Long-lived: these return only when the client goes away.
        "/api/stream" => stream::state(s, a, sh),
        "/api/tape" => stream::tape(s, a),
        "/api/books" => respond(s, "200 OK", "application/json", &books::json(a)),
        "/api/now" => respond(s, "200 OK", "application/json", &now::json(a)),
        // Built from /proc, the unit files and the artifacts on disk on every
        // request — it holds no picture of its own to go stale.
        "/api/architecture" => {
            respond(s, "200 OK", "application/json", &crate::architecture::json())
        }
        "/api/integrity" => {
            let i = integrity::build(&a.data_dir);
            let body = serde_json::to_string(&i).unwrap_or_else(|_| "{}".into());
            respond(s, "200 OK", "application/json", &body)
        }
        "/api/opportunities" => {
            respond(s, "200 OK", "application/json", &opportunities::json(a, &query))
        }
        "/api/pairs" => respond(s, "200 OK", "application/json", &pairs::list_json(a)),
        "/api/intents" => respond(s, "200 OK", "application/json", &intents::json(a)),
        "/api/trades" => respond(s, "200 OK", "application/json", &trades::json(a)),
        "/api/top-series" => respond(s, "200 OK", "application/json", &series::top_json(a, &query)),
        "/api/intent-series" => {
            respond(s, "200 OK", "application/json", &series::intent_json(a, &query))
        }
        // The single write surface. GET reports status; only POST can start a
        // build, so nothing fires it by merely loading a page.
        "/api/rollup" => {
            if method == "POST" {
                respond(s, "200 OK", "application/json", &rollup::start(a, sh, &query))
            } else {
                respond(s, "200 OK", "application/json", &rollup::status(a, sh))
            }
        }
        "/api/current" => respond(s, "200 OK", "application/json", &current::json(a, &query)),
        "/api/pair" => respond(s, "200 OK", "application/json", &pairs::detail_json(a, &query)),
        _ => respond(s, "404 Not Found", "text/plain", "not found"),
    }
}

/// Bind and serve until killed. Takes `Args` by value because every connection
/// thread shares one immutable copy for the life of the process.
pub fn serve(a: Args) {
    let addr = format!("127.0.0.1:{}", a.port);
    let l = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot bind {addr}: {e}");
            exit(1);
        }
    };
    println!("arb-dash on http://{addr}  (read-only, 127.0.0.1 only)");
    let shared: Shared = Arc::new(Mutex::new(Rollup::default()));
    // A thread per connection. Not for throughput — one person reads this —
    // but because /api/stream never returns, and a serial loop would let the
    // first subscriber wedge every later request.
    let args = Arc::new(a);
    for s in l.incoming().flatten() {
        let (a, sh) = (Arc::clone(&args), Arc::clone(&shared));
        std::thread::spawn(move || handle(s, &a, &sh));
    }
}

#[cfg(test)]
mod tests {
    use super::query_param;

    /// An absent key must be absent, so the caller's own default applies
    /// rather than something this parser invented.
    #[test]
    fn a_missing_key_is_none() {
        assert_eq!(query_param("n=5&day=2026-07-28", "clip"), None);
        assert_eq!(query_param("", "n"), None);
    }

    /// `?day=` is a blank, and every caller of this feeds `day` straight into
    /// `tob-<venue>-<day>.jsonl`. Returning `Some("")` would name a file that
    /// cannot exist while looking like a deliberate choice; returning None
    /// falls back to today, which is what the operator meant.
    #[test]
    fn an_empty_value_is_absent_not_an_empty_string() {
        assert_eq!(query_param("day=", "day"), None);
        assert_eq!(query_param("day=&n=5", "day"), None);
        assert_eq!(query_param("day=&n=5", "n"), Some("5".into()), "and the rest still parses");
    }

    /// A repeated key is a client bug, not a crash and not a merge: the first
    /// wins, deterministically.
    #[test]
    fn the_first_of_a_repeated_key_wins() {
        assert_eq!(query_param("n=5&n=9", "n"), Some("5".into()));
    }

    /// Keys match WHOLE. `max_spread` must never answer a request for
    /// `spread`, or the scenario board would silently price at a bound nobody
    /// set.
    #[test]
    fn a_key_is_matched_whole_never_as_a_substring() {
        assert_eq!(query_param("max_spread=0.02", "spread"), None);
        assert_eq!(query_param("nn=5", "n"), None);
        assert_eq!(query_param("n=5", "nn"), None);
    }

    /// A bare flag carries no `=`, so it reads as absent. That is why
    /// `/api/current?all` does NOT open the untradable universe — only
    /// `all=1` does, and the gate is written to require exactly that.
    #[test]
    fn a_bare_flag_with_no_equals_is_not_a_value() {
        assert_eq!(query_param("all&n=3", "all"), None);
        assert_eq!(query_param("all&n=3", "n"), Some("3".into()));
        assert_eq!(query_param("all", "all"), None);
    }

    /// Relationship ids and market keys carry colons and names carry spaces
    /// (`sports-rehedge-Tamara Korpatsch@Julia Stusek`), and a browser sends
    /// those as `%3A` and `+`. Those two substitutions are the WHOLE decoder:
    /// there is no general percent-decoding here, so anything else arrives
    /// literally.
    #[test]
    fn only_the_colon_and_the_space_are_decoded() {
        assert_eq!(query_param("rel=kalshi%3AKXTEST", "rel"), Some("kalshi:KXTEST".into()));
        assert_eq!(
            query_param("rel=Tamara+Korpatsch", "rel"),
            Some("Tamara Korpatsch".into())
        );
        assert_eq!(query_param("rel=a%2Fb", "rel"), Some("a%2Fb".into()));
    }
}
