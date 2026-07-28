//! Feed tasks — drain a source into the engine's bounded channel.
//!
//! The reader-never-stalls property (docs/bench-recorder-baseline.md): the
//! socket reader shares NO thread/loop with anything that can block on venue
//! I/O. The engine is pure compute (~28x headroom over recorded peak burst),
//! so the bounded channel never backpressures in practice; if it ever does,
//! the recorder's own 1MB slow-subscriber buffer is the second stage, and
//! the disconnect+welcome-snapshot protocol heals the rest.

use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc::Sender;

pub struct FeedMsg {
    pub line: String,
    pub t_read: Instant,
}

/// CONNECTION control lines. `kind` values no recorder ever emits, injected by
/// `socket_feed` into the SAME ordered channel as book events so the engine
/// learns about an outage exactly where the outage happened in its own event
/// stream — not up to one health-tick later, and not never.
///
/// Never was the old answer: `socket_feed` logged "disconnected; reconnecting
/// in 2s" and re-subscribed while the engine's `feed_pulled` stayed false, so
/// the armed engine of 2026-07-28 quoted straight across every outage of the
/// session believing its books were current. A dropped subscriber receives NO
/// delta for the length of the outage; the welcome snapshot burst that repairs
/// its books arrives AFTER the reconnect, not with it. See `engine::Link`.
pub const FEED_DOWN: &str = "feed_down";
pub const FEED_UP: &str = "feed_up";

/// A control line, with a wall timestamp so the WAL's incident record can be
/// lined up against the recorder's journal.
fn control(kind: &str, note: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    // Built through serde_json, not format!: `note` carries io::Error text.
    serde_json::json!({"kind": kind, "note": note, "ts": ts}).to_string()
}

/// Send one control line. `Err` = the engine is gone.
async fn send_control(tx: &Sender<FeedMsg>, kind: &str, note: &str) -> Result<(), ()> {
    tx.send(FeedMsg { line: control(kind, note), t_read: Instant::now() })
        .await
        .map_err(|_| ())
}

/// Live shadow feed: subscribe to a recorder unix socket (line JSON, welcome
/// snapshots first), reconnect on EOF/error. The old stream is dropped before
/// reconnecting — never a zombie connection (xv-zombie-socket-on-reconnect).
///
/// The `FEED_UP`/`FEED_DOWN` control lines bracket each subscription, and their
/// POSITION in the stream is the whole point: `FEED_UP` goes out before the
/// first line of the welcome burst is read, so the burst is what proves the
/// books current again, and `FEED_DOWN` goes out after the last line the old
/// subscription delivered, so the pull covers exactly the gap.
pub async fn socket_feed(path: String, tx: Sender<FeedMsg>) {
    loop {
        match tokio::net::UnixStream::connect(&path).await {
            Ok(stream) => {
                eprintln!("[feed] connected {path}");
                if send_control(&tx, FEED_UP, &format!("connected {path}")).await.is_err() {
                    return; // engine gone
                }
                let mut lines = tokio::io::BufReader::new(stream).lines();
                let mut note = "subscription ended (EOF)".to_string();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            if tx.send(FeedMsg { line, t_read: Instant::now() }).await.is_err() {
                                return; // engine gone
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("[feed] read error: {e}");
                            note = format!("read error: {e}");
                            break;
                        }
                    }
                }
                eprintln!("[feed] disconnected; reconnecting in 2s");
                if send_control(&tx, FEED_DOWN, &note).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                eprintln!("[feed] connect {path}: {e}; retrying in 2s");
                // Also a down edge: at startup this is the recorder not being
                // there yet, and the engine must not quote on empty books.
                if send_control(&tx, FEED_DOWN, &format!("connect failed: {e}")).await.is_err() {
                    return;
                }
            }
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Bench feed: stream a merged tape (scripts/export_merged_tape.py) through
/// the SAME channel the live feed uses, from a blocking thread.
///
/// pace_x = 0: full speed (throughput mode; the channel saturates, so
/// decision latency reads as queue depth — use eps). pace_x = N: replay at
/// N x recorded arrival times, so recorded bursts are N x compressed and
/// decision latency is a true (conservative) under-load measurement.
pub fn tape_feed(path: String, max_events: u64, pace_x: f64, tx: Sender<FeedMsg>) {
    use std::io::BufRead;
    let f = std::fs::File::open(&path).expect("open tape");
    let wall_start = Instant::now();
    let mut t0_ns: Option<i64> = None;
    let mut n = 0u64;
    for line in std::io::BufReader::new(f).lines() {
        let line = line.expect("read tape line");
        if line.is_empty() {
            continue;
        }
        n += 1;
        if max_events > 0 && n > max_events {
            break;
        }
        if pace_x > 0.0 {
            // cheap ts extraction; a line without one is sent immediately
            if let Some(ts) = extract_ts_ns(&line) {
                let t0 = *t0_ns.get_or_insert(ts);
                let target_s = ((ts - t0).max(0) as f64 / 1e9) / pace_x;
                let elapsed = wall_start.elapsed().as_secs_f64();
                if target_s > elapsed {
                    std::thread::sleep(std::time::Duration::from_secs_f64(target_s - elapsed));
                }
            }
        }
        if tx.blocking_send(FeedMsg { line, t_read: Instant::now() }).is_err() {
            return;
        }
    }
}

/// Replay feed: stream the `line` payloads of an engine WAL (crate::wal) back
/// through the SAME channel the live feed uses, so an incident re-enters the
/// engine exactly as it originally did. Records are fed in file order, which
/// is engine seq order within a run (seq restarts per process — see wal.rs).
pub fn wal_replay_feed(path: String, max_events: u64, tx: Sender<FeedMsg>) {
    use std::io::BufRead;
    let f = std::fs::File::open(&path).expect("open wal");
    let mut n = 0u64;
    for rec in std::io::BufReader::new(f).lines() {
        let rec = rec.expect("read wal line");
        if rec.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&rec).expect("wal record json");
        let Some(line) = v.get("line").and_then(|x| x.as_str()) else { continue };
        n += 1;
        if max_events > 0 && n > max_events {
            break;
        }
        if tx
            .blocking_send(FeedMsg { line: line.to_owned(), t_read: Instant::now() })
            .is_err()
        {
            return;
        }
    }
}

fn extract_ts_ns(line: &str) -> Option<i64> {
    let i = line.find("\"ts_local_ns\":")?;
    let rest = &line[i + 14..];
    let end = rest.find(|c: char| !c.is_ascii_digit() && c != ' ').unwrap_or(rest.len());
    rest[..end].trim().parse().ok()
}
