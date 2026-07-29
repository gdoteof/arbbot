//! Unix-socket fan-out — the line-JSON protocol every Python subscriber
//! already speaks, verbatim. Mirrors src/arbbot/record/recorder.py
//! UnixBroadcaster: welcome snapshots on connect, slow subscribers are
//! DISCONNECTED (never allowed to backpressure the recorder), periodic
//! rebroadcast heals gapped subscribers.
//!
//! Implementation: one writer task per subscriber fed by an unbounded
//! channel with byte accounting; queued bytes over `max_buffer` drops the
//! subscriber — same policy as the Python transport-buffer check, and it says
//! so on stderr (`eviction_notice`).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

// Bytes queued per subscriber before it is dropped as slow. NOT 1MB: the
// welcome and the 30s rebroadcast heal enqueue the whole universe's books
// (~1.4MB today) SYNCHRONOUSLY before the writer task gets scheduled to
// drain, so a 1MB cap dropped even fast subscribers once per burst — found
// 2026-07-23 by the first real subscriber of the rs socket (arb-trader
// shadow flapping on exact 30s boundaries). 16MB never triggers on a burst
// but still sheds a genuinely stalled subscriber within ~35s of recorded
// peak rate.
//
// This used to end "(the Python recorder's transport-buffer policy is
// measured at the socket, so it never had this enqueue-race)". Right about
// the RACE, wrong about the CONSEQUENCE — and that sentence is why the
// Python recorder kept a 1MB cap for another five days while serving the
// ARMED trader. It has no enqueue race; its real socket buffer fills
// instead, because the burst outruns the kernel plus the reader. Measured
// 2026-07-28: an eviction on every single 30s rebroadcast, 21 in ten
// minutes. Both recorders sit at 16MB now.
//
// The burst is the thing that actually wants fixing: one snapshot per
// tracked book in a single cycle — ~0.94MB across 1,163 markets on
// 2026-07-28 — so it grows with the universe and will chase any fixed cap.
// Rotating it across cycles is the change that scales, and it is still
// available: the burst is now emitted under the core lock for ORDERING, and a
// sliced heal that captures-and-publishes each slice inside that lock keeps
// the ordering guarantee while giving up the atomicity of the whole cycle,
// which nothing needs. Slice it; do not split capture from publish.
pub const MAX_BUFFER: usize = 16_000_000;

struct Subscriber {
    /// Connect ordinal. The eviction notice is the only line this crate ever
    /// prints and "a subscriber" names nothing when several are attached.
    id: usize,
    tx: mpsc::UnboundedSender<Arc<str>>,
    queued: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct Broadcaster {
    subs: Arc<Mutex<Vec<Subscriber>>>,
    next_id: Arc<AtomicUsize>,
    max_buffer: usize,
}

/// What an eviction costs, said out loud — `arb-tape` had no log statement of
/// any kind, so the recorder could drop the ARMED TRADER and the only trace of
/// it anywhere in the system was on the client. PR #11 (0f1ccea) fixed exactly
/// this on the Python recorder and the Rust side never got it; this mirrors
/// that line's information content.
///
/// The subscriber reads the closed socket as `subscription ended (EOF)` and
/// pulls every resting quote plus a verified sweep of both venues before it can
/// reconnect. `eprintln!` because that is what arb-recorder uses.
///
/// A function so the test can assert the content it must keep carrying.
///
/// The caller writes it with `writeln!` and DISCARDS the result rather than
/// using `eprintln!`, which panics if the write fails. This fires from inside
/// `subs.retain`, which during a heal is inside the core lock: an EPIPE on
/// stderr (piped supervisor, reader gone) would panic there and poison BOTH
/// mutexes, and every later `.expect("core lock")` would then kill a recorder
/// that must not die. A diagnostic is not worth the process.
fn eviction_notice(id: usize, queued: usize, max_buffer: usize) -> String {
    format!(
        "[recorder] DROPPED subscriber #{id}: {queued} bytes queued, over the \
         {max_buffer} limit. It sees an EOF and must reconnect; if that subscriber \
         is the trader, it has just pulled every quote it had resting."
    )
}

impl Broadcaster {
    pub fn new(max_buffer: usize) -> Self {
        Self {
            subs: Arc::new(Mutex::new(Vec::new())),
            next_id: Arc::new(AtomicUsize::new(1)),
            max_buffer,
        }
    }

    /// Bind the socket and accept subscribers forever.
    ///
    /// `welcome` is handed a `register` callback rather than returning the
    /// lines, because the current book state and the registration have to be
    /// ONE step: see `add_subscriber`.
    pub async fn serve(
        &self,
        socket_path: impl AsRef<Path>,
        welcome: impl Fn(&mut dyn FnMut(Vec<String>)) + Send + Sync + 'static,
    ) -> std::io::Result<()> {
        let path: PathBuf = socket_path.as_ref().to_owned();
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let welcome = Arc::new(welcome);
        loop {
            let (stream, _) = listener.accept().await?;
            self.add_subscriber(stream, welcome.clone());
        }
    }

    /// Queue the welcome burst and register the subscriber as ONE step under
    /// the subs lock, from inside `welcome` — which holds the lock the state
    /// was read under (`Core::with_snapshot_lines`) for the whole call.
    ///
    /// This used to enqueue the burst first and push the subscriber after, so
    /// anything published during the ~1,163-line enqueue was queued for nobody:
    /// a LOSS, not a reorder. The engine replayed seq 100, received seq 102,
    /// gapped and deleted the book — on the reconnect it had just been forced
    /// into, having already pulled its quotes.
    ///
    /// LOCK ORDER: the caller's core lock, then `subs`. Same nesting as
    /// `publish` under `Core::on_event`; nothing takes `subs` first.
    ///
    /// `welcome` must call `register` EXACTLY ONCE, and the `tx.clone()` below
    /// is the tell that the type cannot say so — `FnOnce` is unavailable behind
    /// `&mut dyn`, so the contract is a bool and two asserts. Both directions
    /// fail silently otherwise. Twice pushes two `Subscriber`s sharing one `tx`
    /// and one `queued`: double delivery, a doubled `subscriber_count`, a
    /// double-counted `queued` that halves the effective cap, and duplicates
    /// that `apply_delta` swallows as `Ok(false)` so nothing downstream ever
    /// says a word. Never is worse — an "optimization" as small as
    /// `if lines.is_empty() { return; }` drops `tx`, hands every subscriber an
    /// instant EOF, and reconnect-loops the fleet.
    fn add_subscriber(
        &self,
        stream: UnixStream,
        welcome: Arc<impl Fn(&mut dyn FnMut(Vec<String>)) + Send + Sync + 'static>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Arc<str>>();
        let queued = Arc::new(AtomicUsize::new(0));
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut registered = false;
        welcome(&mut |lines| {
            assert!(!registered, "welcome registered subscriber #{id} twice");
            registered = true;
            let mut subs = self.subs.lock().expect("subs lock");
            for line in lines {
                queued.fetch_add(line.len(), Ordering::Relaxed);
                let _ = tx.send(Arc::from(line));
            }
            subs.push(Subscriber { id, tx: tx.clone(), queued: queued.clone() });
        });
        assert!(registered, "welcome never registered subscriber #{id}");
        tokio::spawn(async move {
            let (_read_half, mut write_half) = stream.into_split();
            // subscribers never send; dropping the read half closes on EOF
            while let Some(line) = rx.recv().await {
                queued.fetch_sub(line.len(), Ordering::Relaxed);
                if write_half.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        });
    }

    /// Publish one event line to all subscribers; disconnect any whose
    /// queued bytes exceed the cap (the write path never blocks).
    pub fn publish(&self, line_with_newline: &str) {
        let mut subs = self.subs.lock().expect("subs lock");
        subs.retain(|s| {
            let queued = s.queued.load(Ordering::Relaxed);
            if queued > self.max_buffer {
                let _ = writeln!(
                    std::io::stderr(),
                    "{}",
                    eviction_notice(s.id, queued, self.max_buffer)
                );
                return false; // dropping tx ends the writer task -> disconnect
            }
            s.queued.fetch_add(line_with_newline.len(), Ordering::Relaxed);
            s.tx.send(Arc::from(line_with_newline)).is_ok()
        });
    }

    pub fn subscriber_count(&self) -> usize {
        self.subs.lock().expect("subs lock").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;

    #[tokio::test]
    async fn welcome_then_published_lines() {
        let dir = std::env::temp_dir().join(format!("arb-bcast-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("t.sock");
        let b = Broadcaster::new(MAX_BUFFER);
        let b2 = b.clone();
        let s2 = sock.clone();
        tokio::spawn(async move {
            b2.serve(&s2, |register: &mut dyn FnMut(Vec<String>)| {
                register(vec!["{\"w\":1}\n".to_string()])
            })
            .await
            .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let stream = UnixStream::connect(&sock).await.unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"w\":1}\n");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        b.publish("{\"e\":2}\n");
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"e\":2}\n");
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn slow_subscriber_is_dropped() {
        let b = Broadcaster::new(10); // tiny cap
        let dir = std::env::temp_dir().join(format!("arb-bcast2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("t.sock");
        let b2 = b.clone();
        let s2 = sock.clone();
        tokio::spawn(async move {
            b2.serve(&s2, |register: &mut dyn FnMut(Vec<String>)| register(Vec::new()))
                .await
                .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _stream = UnixStream::connect(&sock).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(b.subscriber_count(), 1);
        // saturate way past the cap without the subscriber reading fast enough
        for _ in 0..1000 {
            b.publish("0123456789ABCDEF\n");
        }
        assert_eq!(b.subscriber_count(), 0);
        std::fs::remove_dir_all(dir).ok();
    }

    /// The eviction above must SAY SO, and the assertion has to be on the REAL
    /// stderr: a test that only checked `eviction_notice`'s text would still
    /// pass if the `eprintln!` were deleted, which is exactly the regression —
    /// this crate had no log statement of any kind. The harness swallows
    /// stderr, so re-run `slow_subscriber_is_dropped` as a child process with
    /// --nocapture and read what it printed.
    #[test]
    fn an_eviction_is_announced_with_subscriber_and_queue_depth() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "broadcast::tests::slow_subscriber_is_dropped", "--nocapture"])
            .output()
            .expect("re-exec test binary");
        let err = String::from_utf8_lossy(&out.stderr);
        let line = err
            .lines()
            .find(|l| l.contains("DROPPED subscriber #1:"))
            .unwrap_or_else(|| panic!("no eviction notice on stderr:\n{err}"));
        // the queue depth has to be the REAL one, not a constant
        let depth: usize = line
            .split("DROPPED subscriber #1: ")
            .nth(1)
            .and_then(|t| t.split(" bytes queued").next())
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("no queue depth in: {line}"));
        assert!(depth > 10, "queue depth {depth} is not over the cap: {line}");
        assert!(line.contains("over the 10 limit"), "the cap is missing from: {line}");
    }
}
