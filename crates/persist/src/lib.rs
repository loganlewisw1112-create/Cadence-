//! Phase 4b — persistence/ingest path: std I/O vs io_uring.
//!
//! ## Design
//!
//! Both writers implement the same interface: append a batch of `Message`
//! records to an append-only log file, flush, and return the number written.
//! The 64-byte fixed-size `Message` is ideal for sequential I/O: no length
//! prefix needed, random access by record index is trivial (`offset = i * 64`).
//!
//! ## What we benchmark
//!
//! - **`StdWriter`** — `BufWriter<File>` + explicit `flush()`. One `write`
//!   syscall per flush (the buf absorbs individual record writes). Available
//!   on all platforms.
//!
//! - **`UringWriter`** (Linux only) — `io_uring` with `opcode::Write` ops
//!   submitted in batches. The ring depth controls how many writes are in
//!   flight simultaneously before a `submit_and_wait` call drains them.
//!   This is where io_uring's advantage (fewer syscalls via batching,
//!   optional registered buffers) should show up.
//!
//! ## Honest scope
//!
//! This is a **single-machine, single-file append benchmark** — not a
//! full WAL or journal. The results are reported as "where io_uring helps
//! and where it doesn't" per the BENCHMARKING.md standard.

use message_core::Message;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

// ── Shared interface ──────────────────────────────────────────────────────────

pub trait MessageWriter {
    /// Append `msgs` to the log. Returns number written or an error.
    fn write_batch(&mut self, msgs: &[Message]) -> std::io::Result<usize>;
    /// Ensure all data is on disk (fsync or equivalent).
    fn sync(&mut self) -> std::io::Result<()>;
    fn name(&self) -> &'static str;
}

// ── Std I/O writer ────────────────────────────────────────────────────────────

/// Append-only log writer backed by `BufWriter<File>`.
///
/// One `write` syscall per flush — the `BufWriter` absorbs the individual
/// 64-byte record writes and emits a single larger write on flush.
pub struct StdWriter {
    inner: BufWriter<File>,
}

impl StdWriter {
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { inner: BufWriter::with_capacity(64 * 1024, file) })
    }
}

impl MessageWriter for StdWriter {
    fn write_batch(&mut self, msgs: &[Message]) -> std::io::Result<usize> {
        for msg in msgs {
            self.inner.write_all(&msg.to_bytes())?;
        }
        self.inner.flush()?;
        Ok(msgs.len())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.inner.get_ref().sync_data()
    }

    fn name(&self) -> &'static str { "std-BufWriter" }
}

// ── io_uring writer (Linux only) ──────────────────────────────────────────────

#[cfg(target_os = "linux")]
pub mod uring {
    use super::*;
    use io_uring::{opcode, types, IoUring};
    use std::os::unix::io::AsRawFd;

    /// Append-only log writer backed by `io_uring`.
    ///
    /// Submits up to `ring_depth` write ops per batch in a single
    /// `submit_and_wait` call — reducing syscall overhead vs. one write
    /// syscall per message.
    ///
    /// **Registered buffers** (the main performance lever for io_uring
    /// beyond naive batching) are left as a stretch goal. The current
    /// implementation uses regular `opcode::Write`, which still requires
    /// a kernel copy from userspace. Registered buffers (`register_buffers`)
    /// would eliminate that copy on supported kernels (5.1+).
    pub struct UringWriter {
        ring: IoUring,
        file: File,
        offset: u64,
        ring_depth: u32,
    }

    impl UringWriter {
        pub fn create(path: impl AsRef<Path>, ring_depth: u32) -> std::io::Result<Self> {
            let file = OpenOptions::new().create(true).append(false).write(true).open(path)?;
            let ring = IoUring::new(ring_depth)?;
            Ok(Self { ring, file, offset: 0, ring_depth })
        }
    }

    impl MessageWriter for UringWriter {
        fn write_batch(&mut self, msgs: &[Message]) -> std::io::Result<usize> {
            let depth = self.ring_depth as usize;
            let mut written = 0usize;

            for chunk in msgs.chunks(depth) {
                // Build SQEs for this chunk
                let bufs: Vec<[u8; 64]> = chunk.iter().map(|m| m.to_bytes()).collect();

                // SAFETY: bufs lives for the duration of submit_and_wait below.
                unsafe {
                    let mut sq = self.ring.submission();
                    for (i, buf) in bufs.iter().enumerate() {
                        let op = opcode::Write::new(
                            types::Fd(self.file.as_raw_fd()),
                            buf.as_ptr(),
                            buf.len() as u32,
                        )
                        .offset(self.offset + (i as u64 * 64))
                        .build()
                        .user_data(i as u64);

                        sq.push(&op).expect("ring submission full");
                    }
                }

                self.ring.submit_and_wait(chunk.len())?;

                // Drain completions and check for errors
                for cqe in self.ring.completion() {
                    let ret = cqe.result();
                    if ret < 0 {
                        return Err(std::io::Error::from_raw_os_error(-ret));
                    }
                }

                self.offset += chunk.len() as u64 * 64;
                written += chunk.len();
            }

            Ok(written)
        }

        fn sync(&mut self) -> std::io::Result<()> {
            self.file.sync_data()
        }

        fn name(&self) -> &'static str { "io_uring-Write" }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn roundtrip_file(writer: &mut dyn MessageWriter, path: &str) {
        let msgs: Vec<Message> = (1u8..=8)  // start at 1 — payload_bytes() trims trailing zeros
            .map(|i| Message::new("log.test", &[i; 8]))
            .collect();

        let n = writer.write_batch(&msgs).unwrap();
        assert_eq!(n, 8);
        writer.sync().unwrap();

        // Read back raw bytes and verify
        let mut raw = Vec::new();
        File::open(path).unwrap().read_to_end(&mut raw).unwrap();
        assert_eq!(raw.len(), 8 * 64);

        for (i, chunk) in raw.chunks(64).enumerate() {
            let decoded = Message::from_bytes(chunk.try_into().unwrap());
            assert_eq!(decoded.topic_str(), "log.test");
            assert_eq!(decoded.payload_bytes()[0], (i + 1) as u8);
        }
    }

    #[test]
    fn std_writer_roundtrip() {
        let path = "test_std_log.bin";
        let _ = std::fs::remove_file(path);
        let mut w = StdWriter::create(path).unwrap();
        roundtrip_file(&mut w, path);
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uring_writer_roundtrip() {
        let path = "test_uring_log.bin";
        let _ = std::fs::remove_file(path);
        // Create file first so UringWriter can open it
        std::fs::File::create(path).unwrap();
        let mut w = uring::UringWriter::create(path, 16).unwrap();
        roundtrip_file(&mut w, path);
        std::fs::remove_file(path).unwrap();
    }
}
