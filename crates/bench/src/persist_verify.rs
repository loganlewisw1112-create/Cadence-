//! Test D — disk-write / io_uring verification (VERIFICATION_PLAN.md).
//!
//! Proves (or falsifies) the claim: on Linux, `io_uring` with **registered
//! buffers** reaches ≥ 1.5× the std `BufWriter` throughput at large batch,
//! under an identical durability contract.
//!
//! What this fixes vs the Phase-4b `persist_bench`:
//!   * three writers — `std-BufWriter`, naive `io_uring-Write`, and
//!     `io_uring-WriteFixed` (registered buffers, G2) — so any gain is
//!     attributable to batching vs zero-copy;
//!   * **matched batch knob** (G3): std now issues one underlying write per
//!     batch (see STD_BUF_CAP), same cadence as the uring writers;
//!   * **defined durability contract** (D.1d): every writer runs under both
//!     Mode A (sync once at end) and Mode B (fsync per batch);
//!   * **multiple trials** + median + `ratio_vs_std` (G4, §5);
//!   * a real block device is required for a meaningful number — the run
//!     script controls the page cache and records the device/fs.
//!
//! Run: `cargo run --release --bin persist_verify`
//! Knobs (env): CADENCE_MSGS=200000  CADENCE_TRIALS=5
//!              CADENCE_DEVICE=/dev/nvme0n1  CADENCE_FS=ext4 (stamped by script)
use message_core::Message;
use persist::{MessageWriter, StdWriter};
use serde::Serialize;
use std::time::Instant;

const DEFAULT_MSGS: usize = 200_000;
const DEFAULT_TRIALS: usize = 5;
const BATCH_SIZES: &[usize] = &[1, 8, 64, 512, 4096];
const MAX_BATCH: usize = 4096; // registered-buffer capacity, in messages
const LOG_PATH: &str = "persist_verify_log.bin";

#[derive(Clone, Copy, PartialEq)]
enum Durability {
    SyncEnd,      // Mode A: submit to kernel, sync_data once at end
    SyncPerBatch, // Mode B: fsync/sync_data after every batch (durable commit)
}

impl Durability {
    fn label(self) -> &'static str {
        match self {
            Durability::SyncEnd => "sync-end",
            Durability::SyncPerBatch => "sync-per-batch",
        }
    }
}

#[derive(Serialize)]
struct CellResult {
    writer: &'static str,
    batch_size: usize,
    durability: &'static str,
    msg_count: usize,
    trials: usize,
    throughput_mb_s_per_trial: Vec<f64>,
    throughput_mmsg_s_median: f64,
    throughput_mb_s_median: f64,
    ratio_vs_std: Option<f64>,
    passes_1_5x: Option<bool>,
    platform_note: &'static str,
}

#[derive(Serialize)]
struct EnvBlock {
    os: String,
    arch: String,
    kernel: String,
    device: String,
    filesystem: String,
    rustc: String,
    record_bytes: usize,
}

#[derive(Serialize)]
struct AllResults {
    env: EnvBlock,
    results: Vec<CellResult>,
}

#[cfg(target_os = "linux")]
fn platform_note() -> &'static str { "linux — io_uring available" }
#[cfg(not(target_os = "linux"))]
fn platform_note() -> &'static str { "non-linux — std baseline only" }

fn read_trim(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// One timed run of `msg_count` records through `writer` at `batch_size`.
/// Returns (mmsg_s, mb_s).
fn time_writer(
    writer: &mut dyn MessageWriter,
    batch_size: usize,
    durability: Durability,
    msg_count: usize,
) -> (f64, f64) {
    let msgs: Vec<Message> = (0..batch_size)
        .map(|i| Message::new("persist.bench", &(i as u64).to_le_bytes()))
        .collect();

    let t0 = Instant::now();
    let mut total = 0usize;
    while total < msg_count {
        let n = writer.write_batch(&msgs).unwrap();
        if durability == Durability::SyncPerBatch {
            writer.sync().unwrap();
        }
        total += n;
    }
    writer.sync().unwrap(); // durable end-state for both modes
    let secs = t0.elapsed().as_secs_f64();

    let mmsg_s = total as f64 / secs / 1e6;
    let mb_s = (total as f64 * 64.0) / secs / (1024.0 * 1024.0);
    (mmsg_s, mb_s)
}

/// Build a fresh writer of `kind` for a trial. `None` for unavailable kinds.
fn build_writer(kind: &str, batch_size: usize) -> Option<Box<dyn MessageWriter>> {
    let _ = std::fs::remove_file(LOG_PATH);
    match kind {
        "std-BufWriter" => Some(Box::new(StdWriter::create(LOG_PATH).unwrap())),
        #[cfg(target_os = "linux")]
        "io_uring-Write" => {
            std::fs::File::create(LOG_PATH).unwrap();
            let depth = batch_size.min(512).max(1) as u32;
            Some(Box::new(persist::uring::UringWriter::create(LOG_PATH, depth).unwrap()))
        }
        #[cfg(target_os = "linux")]
        "io_uring-WriteFixed" => {
            std::fs::File::create(LOG_PATH).unwrap();
            Some(Box::new(persist::uring::UringFixedWriter::create(LOG_PATH, MAX_BATCH).unwrap()))
        }
        _ => None,
    }
}

fn median_f64(xs: &[f64]) -> f64 {
    if xs.is_empty() { return 0.0; }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 }
}

fn writer_kinds() -> Vec<&'static str> {
    #[cfg(target_os = "linux")]
    { vec!["std-BufWriter", "io_uring-Write", "io_uring-WriteFixed"] }
    #[cfg(not(target_os = "linux"))]
    { vec!["std-BufWriter"] }
}

fn run_cell(
    kind: &'static str,
    batch: usize,
    dur: Durability,
    msgs: usize,
    trials: usize,
) -> CellResult {
    let mut mb: Vec<f64> = Vec::with_capacity(trials);
    let mut mmsg: Vec<f64> = Vec::with_capacity(trials);
    for _ in 0..trials {
        let mut w = build_writer(kind, batch).expect("writer available");
        let (mm, m) = time_writer(&mut *w, batch, dur, msgs);
        mb.push(m);
        mmsg.push(mm);
    }
    let _ = std::fs::remove_file(LOG_PATH);

    CellResult {
        writer: kind,
        batch_size: batch,
        durability: dur.label(),
        msg_count: msgs,
        trials,
        throughput_mb_s_per_trial: mb.clone(),
        throughput_mmsg_s_median: median_f64(&mmsg),
        throughput_mb_s_median: median_f64(&mb),
        ratio_vs_std: None, // filled in a second pass
        passes_1_5x: None,
        platform_note: platform_note(),
    }
}

fn main() {
    let msgs: usize = std::env::var("CADENCE_MSGS").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_MSGS);
    let trials: usize = std::env::var("CADENCE_TRIALS").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_TRIALS);

    let env = EnvBlock {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        kernel: read_trim("/proc/sys/kernel/osrelease").unwrap_or_else(|| "unknown".into()),
        device: std::env::var("CADENCE_DEVICE").unwrap_or_else(|_| "unset (record the block device!)".into()),
        filesystem: std::env::var("CADENCE_FS").unwrap_or_else(|_| "unset".into()),
        rustc: std::env::var("CADENCE_RUSTC").unwrap_or_else(|_| "unset".into()),
        record_bytes: 64,
    };

    println!("Cadence — Test D: disk-write / io_uring verification");
    println!("  OS={} arch={} kernel={}", env.os, env.arch, env.kernel);
    println!("  device={} fs={}", env.device, env.filesystem);
    println!("  msgs/trial={} trials={} batches={:?}", msgs, trials, BATCH_SIZES);
    if env.os != "linux" {
        println!("  ⚠ Not Linux — std baseline only; io_uring writers skipped.");
    }
    if env.device.starts_with("unset") {
        println!("  ⚠ No block device recorded — run via run_verification.sh on a real disk,");
        println!("    not tmpfs/overlay, or the MB/s figures are meaningless.");
    }
    println!();

    let kinds = writer_kinds();
    let mut results: Vec<CellResult> = Vec::new();

    for &dur in &[Durability::SyncEnd, Durability::SyncPerBatch] {
        println!("=== durability: {} ===", dur.label());
        for &batch in BATCH_SIZES {
            // std first so we can compute ratios against it.
            let mut std_med = None;
            for &kind in &kinds {
                let r = run_cell(kind, batch, dur, msgs, trials);
                if kind == "std-BufWriter" { std_med = Some(r.throughput_mb_s_median); }
                println!(
                    "  {:22} batch={:>5}  {:>7.2} MB/s (median)  {:.3} Mmsg/s",
                    r.writer, r.batch_size, r.throughput_mb_s_median, r.throughput_mmsg_s_median
                );
                results.push(r);
            }
            // Second pass: fill ratio_vs_std for this (batch, dur) group.
            if let Some(base) = std_med.filter(|b| *b > 0.0) {
                for r in results.iter_mut().filter(|r| r.batch_size == batch && r.durability == dur.label()) {
                    let ratio = r.throughput_mb_s_median / base;
                    r.ratio_vs_std = Some(ratio);
                    if r.writer == "io_uring-WriteFixed" {
                        r.passes_1_5x = Some(ratio >= 1.5 && batch >= 64);
                    }
                }
            }
        }
        println!();
    }

    // Verdict line for the headline claim.
    let proven = results.iter().any(|r| r.passes_1_5x == Some(true));
    println!("Claim D (WriteFixed ≥1.5× std at batch≥64): {}",
        if proven { "PASS (see ratio_vs_std)" } else { "NOT met on this run — report honestly per BENCHMARKING.md §8" });

    let all = AllResults { env, results };
    let json = serde_json::to_string_pretty(&all).unwrap();
    std::fs::write("persist_verify_results.json", &json).unwrap();
    println!("\nExported → persist_verify_results.json");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn std_writer_produces_throughput() {
        let (mmsg, mb) = {
            let mut w = build_writer("std-BufWriter", 64).unwrap();
            let r = time_writer(&mut *w, 64, Durability::SyncEnd, 4096);
            let _ = std::fs::remove_file(LOG_PATH);
            r
        };
        assert!(mmsg > 0.0 && mb > 0.0);
    }

    #[test]
    fn median_f64_even_odd() {
        assert_eq!(median_f64(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median_f64(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }
}
