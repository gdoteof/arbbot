//! Engine-sequenced write-ahead log (P4 provable #2, docs/p3-shell.md).
//!
//! Every event that leaves the feed channel is stamped, at the engine's single
//! merge point, with a monotonically increasing sequence number and appended
//! verbatim:
//!
//!     {"seq":N,"line":"<the original feed line, JSON-escaped>"}
//!
//! `seq` is the engine's merge order — the only order that matters, since the
//! engine is a pure fold over that sequence. Replaying the embedded lines
//! through the identical engine path (`--replay-wal`) therefore reproduces an
//! incident byte-exactly, intent digest included. The line is stored as an
//! opaque string, not re-parsed JSON: a re-serialized event is a different
//! event, and events the engine skips (unknown kinds, malformed lines) are
//! part of the incident too.
//!
//! `seq` restarts at 1 each process run; the file is opened append-only (a WAL
//! never clobbers the previous run's record), so file order — not the seq
//! value alone — is the replay authority across a restart boundary.
//!
//! **One file per UTC day.** At the first event after midnight UTC the writer
//! renames the live file to `<stem>-YYYY-MM-DD.jsonl` (the day just ended) and
//! reopens the configured path, so the live file never holds more than a day
//! and closed days can be compressed and pruned without a restart (the live
//! file grew ~8.5 GB/day, and a full disk panics this writer). A file that was
//! already there at startup keeps its earlier lines, so a day file can begin
//! with the tail of a previous run — file order stays the replay authority. A
//! failed rename is logged and the writer carries on in the same file.
//!
//! The writer never runs in the engine task: the engine `try_send`s to a
//! bounded channel drained by a dedicated OS thread, so a slow disk backs up
//! the WAL queue and nothing else. The engine's own loop does no file I/O.
//!
//! **Overflow policy: crash-stop, but CLEAN.** If the bounded queue is full (or
//! the writer thread has died), the process stops trading immediately instead of
//! skipping the line. A WAL with silent holes is worse than no WAL: it replays
//! "successfully" into a state the live engine never occupied, so an incident
//! review draws conclusions from a fiction. Refusing to run is the honest
//! failure. (The buffered tail is lost on that exit — but the hole that caused
//! it is already unrecoverable, so there is nothing left worth saving.)
//!
//! What this used to do was `std::process::exit(70)` on the spot, which
//! bypassed the shutdown sweep entirely: an armed engine died with its quotes
//! resting on both venues and `Restart=no`. The intent was right and is kept —
//! `exec::spawn_halt_and_exit` latches the effects boundary before it returns,
//! so nothing further reaches a venue from the instant the hole appears — but
//! the process now dies AFTER the book has been cancelled and proven empty, and
//! exits `EXIT_ORDERS_LEFT_RESTING` instead of 70 if it could not be.

use std::io::Write;
use tokio::sync::mpsc;

/// Same order of magnitude as the feed channel: absorbs a full burst without
/// the engine ever waiting on the disk.
const WAL_QUEUE: usize = 65536;

pub struct Wal {
    tx: mpsc::Sender<String>,
    next_seq: u64,
}

impl Wal {
    pub fn spawn(path: &str) -> Wal {
        if let Some(dir) = std::path::Path::new(path).parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).expect("wal dir");
            }
        }
        let path = std::path::PathBuf::from(path);
        let open = |p: &std::path::Path| {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .expect("open wal");
            std::io::BufWriter::with_capacity(1 << 20, f)
        };
        let mut w = open(&path);
        let (tx, mut rx) = mpsc::channel::<String>(WAL_QUEUE);
        std::thread::Builder::new()
            .name("wal".into())
            .spawn(move || {
                let mut day = utc_day_now();
                while let Some(rec) = rx.blocking_recv() {
                    let today = utc_day_now();
                    if today != day {
                        w.flush().expect("wal flush");
                        match roll(&path, day) {
                            Ok(to) => {
                                eprintln!("[wal] rolled day {} -> {}", arb_core::resolve::iso_from_day(day), to.display());
                                w = open(&path);
                            }
                            Err(e) => eprintln!("[wal] roll failed, still writing {}: {e}", path.display()),
                        }
                        day = today;
                    }
                    w.write_all(rec.as_bytes()).expect("wal write");
                    w.write_all(b"\n").expect("wal write");
                    // Caught up => flush, so a live WAL is at most one event
                    // behind the engine. Under a burst the buffer fills and
                    // amortizes instead.
                    if rx.is_empty() {
                        w.flush().expect("wal flush");
                    }
                }
                w.flush().expect("wal final flush");
            })
            .expect("spawn wal thread");
        Wal { tx, next_seq: 0 }
    }

    /// Stamp and enqueue one merged event. Never blocks; see the crash-stop
    /// policy above.
    pub fn append(&mut self, line: &str) {
        self.next_seq += 1;
        let rec = format!(
            "{{\"seq\":{},\"line\":{}}}",
            self.next_seq,
            serde_json::to_string(line).expect("json string")
        );
        if self.tx.try_send(rec).is_err() {
            // The engine keeps folding events until the halt task exits the
            // process, and every append after this one fails too. Say it once.
            //
            // The latch, not `halting()`: `begin()` happens synchronously inside
            // `spawn_halt_and_exit`, whereas the claim is taken later inside the
            // spawned sweep, so testing the latch closes the window in which a
            // few thousand FATAL lines could still get out.
            if crate::exec::halt().is_on() {
                return;
            }
            eprintln!(
                "[wal] FATAL: queue full or writer dead at seq {} — crash-stop \
                 rather than continue with a WAL that has a silent hole",
                self.next_seq
            );
            crate::exec::spawn_halt_and_exit(
                70,
                format!("WAL hole at seq {}", self.next_seq),
            );
        }
    }
}

fn utc_day_now() -> i64 {
    arb_core::clock::now_secs() as i64 / 86_400
}

/// Rename the live WAL to `<stem>-YYYY-MM-DD.jsonl` for `day`, taking the
/// first free `-N` suffix so a roll never overwrites an earlier file.
fn roll(path: &std::path::Path, day: i64) -> std::io::Result<std::path::PathBuf> {
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let ext = path.extension().map(|e| format!(".{}", e.to_string_lossy())).unwrap_or_default();
    let iso = arb_core::resolve::iso_from_day(day);
    let mut n = 0;
    loop {
        let suffix = if n == 0 { String::new() } else { format!("-{n}") };
        let to = path.with_file_name(format!("{stem}-{iso}{suffix}{ext}"));
        // A compressed roll counts as taken too: the compressor deletes the
        // .jsonl once its .zst is verified.
        let zst = to.with_file_name(format!("{}.zst", to.file_name().unwrap().to_string_lossy()));
        if !to.exists() && !zst.exists() {
            std::fs::rename(path, &to)?;
            return Ok(to);
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roll_names_the_ended_day_and_never_overwrites() {
        let dir = std::env::temp_dir().join(format!("wal-roll-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let live = dir.join("m3-wal.jsonl");
        let day = arb_core::resolve::parse_iso("2026-09-24").unwrap();

        std::fs::write(&live, "a\n").unwrap();
        assert_eq!(roll(&live, day).unwrap(), dir.join("m3-wal-2026-09-24.jsonl"));
        assert!(!live.exists());

        // Same day again (e.g. an earlier roll already compressed): next free suffix.
        std::fs::rename(dir.join("m3-wal-2026-09-24.jsonl"), dir.join("m3-wal-2026-09-24.jsonl.zst")).unwrap();
        std::fs::write(&live, "b\n").unwrap();
        assert_eq!(roll(&live, day).unwrap(), dir.join("m3-wal-2026-09-24-1.jsonl"));
        assert_eq!(std::fs::read_to_string(dir.join("m3-wal-2026-09-24-1.jsonl")).unwrap(), "b\n");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
