//! Test T — tail-latency verification (VERIFICATION_PLAN.md).
//!
//! Proves (or falsifies) the claim: on Linux with isolated cores, the
//! core-pinned SPSC ring buffer with a **busy-spin consumer** sustains
//! p99 < 1 µs, stable across trials.
//!
//! Differences from the Phase-4 `bench` binary, all required to make the
//! claim testable rather than assumed:
//!   * consumer **busy-spins** (`spin_loop`) instead of yielding (G1) — the
//!     yielding consumer measures scheduler wakeup, not queue latency;
//!   * **multiple trials** per rate with median + IQR (BENCHMARKING.md §5);
//!   * **rate sweep** to find where p99 crosses 1 µs;
//!   * **environment capture** (governor, turbo, isolcpus, TSC, kernel) so
//!     every number carries its disclosure (BENCHMARKING.md §3).
//!
//! Run: `taskset -c <isolated cores> cargo run --release --bin tail_bench`
//! Knobs (env): CADENCE_RATES=10000,100000,1000000  CADENCE_TRIALS=10
use bus::{spsc, WaitStrategy};
use hdrhistogram::serialization::{Serializer, V2Serializer};
use hdrhistogram::Histogram;
use message_core::Message;
use quanta::Instant as QInstant;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

// ── Parameters ────────────────────────────────────────────────────────────────

const LAT_MSGS: usize = 100_000;
const LAT_WARMUP: usize = 10_000;
const CAPACITY: usize = 1 << 17; // 131 072

const DEFAULT_RATES: &[u64] = &[10_000, 100_000, 1_000_000];
const DEFAULT_TRIALS: usize = 10;

// ── Result types ────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct RateResult {
    label: String,
    wait_strategy: String,
    pinned: bool,
    rate_hz: u64,
    trials: usize,
    // Per-trial p99, so stability is auditable, not just a point estimate.
    p99_ns_per_trial: Vec<u64>,
    p99_ns_median: u64,
    p99_ns_iqr: u64,
    // Aggregate distribution across all trials (the committed histogram).
    agg_min_ns: u64,
    agg_p50_ns: u64,
    agg_p99_ns: u64,
    agg_p99_9_ns: u64,
    agg_max_ns: u64,
    agg_hdr_base64: String,
    total_drops: u64,
    passes_sub_us: bool,
}

#[derive(Serialize)]
struct AllResults {
    env: EnvBlock,
    results: Vec<RateResult>,
}

#[derive(Serialize)]
struct EnvBlock {
    os: String,
    arch: String,
    logical_cores: usize,
    cpu_model: String,
    governor: String,
    turbo_disabled: String,
    isolcpus: String,
    tsc_flags: String,
    kernel: String,
    rustc: String,
    rustflags: String,
    lat_msgs: usize,
    lat_warmup: usize,
    capacity: usize,
}

// ── Env capture (BENCHMARKING.md §3) ────────────────────────────────────────────

fn read_trim(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

fn env_block(cores: usize) -> EnvBlock {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let cpu_model = cpuinfo
        .lines()
        .find(|l| l.starts_with("model name"))
        .and_then(|l| l.split(':').nth(1))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let tsc_flags = {
        let mut v: Vec<&str> = Vec::new();
        if cpuinfo.contains("constant_tsc") { v.push("constant_tsc"); }
        if cpuinfo.contains("nonstop_tsc") { v.push("nonstop_tsc"); }
        if v.is_empty() { "none (TSC may be unreliable)".into() } else { v.join(",") }
    };
    let cmdline = read_trim("/proc/cmdline").unwrap_or_default();
    let isolcpus = cmdline
        .split_whitespace()
        .find(|t| t.starts_with("isolcpus="))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none".into());

    EnvBlock {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        logical_cores: cores,
        cpu_model,
        governor: read_trim("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .unwrap_or_else(|| "unknown".into()),
        turbo_disabled: read_trim("/sys/devices/system/cpu/intel_pstate/no_turbo")
            .unwrap_or_else(|| "unknown".into()),
        isolcpus,
        tsc_flags,
        kernel: read_trim("/proc/sys/kernel/osrelease").unwrap_or_else(|| "unknown".into()),
        // These are stamped by the run script (it knows the compiler invocation).
        rustc: std::env::var("CADENCE_RUSTC").unwrap_or_else(|_| "unset".into()),
        rustflags: std::env::var("CADENCE_RUSTFLAGS").unwrap_or_else(|_| "unset".into()),
        lat_msgs: LAT_MSGS,
        lat_warmup: LAT_WARMUP,
        capacity: CAPACITY,
    }
}

// ── Core pinning ────────────────────────────────────────────────────────────────

fn pin(core_idx: usize) {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    if let Some(c) = cores.get(core_idx) {
        core_affinity::set_for_current(*c);
    }
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

// ── One trial: open-loop generator, busy-spin consumer ──────────────────────────

fn spsc_trial(rate_hz: u64, pinned: bool, wait: WaitStrategy) -> (Histogram<u64>, u64) {
    let interval_ns: u64 = 1_000_000_000 / rate_hz;
    let epoch = QInstant::now();
    let (tx, rx) = spsc::<Message>(CAPACITY);
    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    let e = epoch;
    let sub = thread::spawn(move || {
        if pinned { pin(1); }
        let mut hist = new_hist();
        let mut received = 0usize;
        loop {
            match rx.try_recv() {
                Some(msg) => {
                    let now = e.elapsed().as_nanos() as u64;
                    let intended = u64::from_le_bytes(msg.payload[..8].try_into().unwrap());
                    let lat = now.saturating_sub(intended).max(1);
                    if received >= LAT_WARMUP {
                        hist.record_correct(lat, interval_ns).unwrap_or(());
                    }
                    received += 1;
                }
                None => {
                    if done.load(Ordering::Relaxed) { break; }
                    // The change that makes the tail claim testable: a
                    // dedicated consumer busy-spins rather than yielding.
                    match wait {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Yield => std::thread::yield_now(),
                    }
                }
            }
        }
        hist
    });

    let e = epoch;
    let pub_h = thread::spawn(move || {
        if pinned { pin(0); }
        let start = e.elapsed().as_nanos() as u64;
        for i in 0..(LAT_MSGS + LAT_WARMUP) {
            let intended = start + i as u64 * interval_ns;
            while (e.elapsed().as_nanos() as u64) < intended {}
            let mut m = Message::new("bench", &[]);
            m.payload[..8].copy_from_slice(&intended.to_le_bytes());
            tx.send(m, WaitStrategy::BusySpin);
        }
    });
    pub_h.join().unwrap();
    done2.store(true, Ordering::Relaxed);
    let hist = sub.join().unwrap();
    (hist, 0) // SPSC never drops (blocking send)
}

// ── Stats over trials ───────────────────────────────────────────────────────────

fn median(sorted: &[u64]) -> u64 {
    let n = sorted.len();
    if n == 0 { return 0; }
    if n % 2 == 1 { sorted[n / 2] } else { (sorted[n / 2 - 1] + sorted[n / 2]) / 2 }
}

/// IQR via nearest-rank on a sorted slice.
fn iqr(sorted: &[u64]) -> u64 {
    let n = sorted.len();
    if n < 4 { return 0; }
    let q1 = sorted[n / 4];
    let q3 = sorted[(3 * n) / 4];
    q3.saturating_sub(q1)
}

fn hdr_b64(h: &Histogram<u64>) -> String {
    let mut buf = Vec::new();
    V2Serializer::new().serialize(h, &mut buf).unwrap();
    base64_encode(&buf)
}

// ── Driver ──────────────────────────────────────────────────────────────────────

fn run_rate(rate_hz: u64, trials: usize, pinned: bool, wait: WaitStrategy) -> RateResult {
    let wait_label = match wait {
        WaitStrategy::BusySpin => "busy-spin",
        WaitStrategy::Yield => "yield",
    };
    let mut agg = new_hist();
    let mut p99s: Vec<u64> = Vec::with_capacity(trials);
    let mut total_drops = 0u64;

    for _ in 0..trials {
        let (h, drops) = spsc_trial(rate_hz, pinned, wait);
        p99s.push(h.value_at_quantile(0.99));
        total_drops += drops;
        agg.add(&h).unwrap();
    }
    p99s.sort_unstable();

    let p99_med = median(&p99s);
    let iqr_ns = iqr(&p99s);
    // "Stable sub-µs" requires both: median under 1 µs AND spread under 25%.
    let stable = iqr_ns as f64 <= 0.25 * p99_med.max(1) as f64;
    let passes = p99_med < 1_000 && stable;

    println!(
        "  rate={:>9}/s  [{}{}]  trials={}  p99 median={:>6} ns  IQR={:>6} ns  {}",
        rate_hz,
        wait_label,
        if pinned { ", pinned" } else { "" },
        trials,
        p99_med,
        iqr_ns,
        if passes { "PASS <1µs stable" } else { "—" },
    );

    RateResult {
        label: "spsc-ring-buffer".into(),
        wait_strategy: wait_label.into(),
        pinned,
        rate_hz,
        trials,
        p99_ns_per_trial: p99s,
        p99_ns_median: p99_med,
        p99_ns_iqr: iqr_ns,
        agg_min_ns: agg.min(),
        agg_p50_ns: agg.value_at_quantile(0.50),
        agg_p99_ns: agg.value_at_quantile(0.99),
        agg_p99_9_ns: agg.value_at_quantile(0.999),
        agg_max_ns: agg.max(),
        agg_hdr_base64: hdr_b64(&agg),
        total_drops,
        passes_sub_us: passes,
    }
}

fn main() {
    let cores = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(0);
    let rates: Vec<u64> = std::env::var("CADENCE_RATES")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_RATES.to_vec());
    let trials: usize = std::env::var("CADENCE_TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TRIALS);

    let env = env_block(cores);
    println!("Cadence — Test T: tail-latency verification");
    println!("  OS={} arch={} cores={}", env.os, env.arch, env.logical_cores);
    println!("  governor={} turbo_disabled={} isolcpus={}", env.governor, env.turbo_disabled, env.isolcpus);
    println!("  tsc={} kernel={}", env.tsc_flags, env.kernel);
    if env.os != "linux" || env.isolcpus == "none" {
        println!("  ⚠ NOT a valid tail-latency environment (need Linux + isolcpus).");
        println!("    Numbers below are indicative only — see VERIFICATION_PLAN.md §Where to run.");
    }
    println!();

    let mut results = Vec::new();

    // Core result: pinned, busy-spin consumer.
    println!("=== pinned, busy-spin consumer (the claim) ===");
    for &r in &rates {
        results.push(run_rate(r, trials, true, WaitStrategy::BusySpin));
    }

    // Contrast row: pinned, yielding consumer — quantifies the tail cost of
    // yielding, and shows why the original bench could not prove sub-µs.
    println!("\n=== pinned, yielding consumer (contrast: cost of yielding) ===");
    for &r in &rates {
        results.push(run_rate(r, trials, true, WaitStrategy::Yield));
    }

    let all = AllResults { env, results };
    let json = serde_json::to_string_pretty(&all).unwrap();
    std::fs::write("tail_bench_results.json", &json).unwrap();
    println!("\nExported → tail_bench_results.json");
    println!("Reproduce a histogram: paste agg_hdr_base64 into");
    println!("  https://hdrhistogram.github.io/HdrHistogramJSDemo/logparser.html");
}

// ── base64 (no external dep; matches bench/src/main.rs) ──────────────────────────

fn base64_encode(input: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 0x3F) as usize] as char);
        out.push(T[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 { T[((n >> 6) & 0x3F) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[(n & 0x3F) as usize] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_and_iqr_basic() {
        let mut v = vec![10u64, 20, 30, 40, 50, 60, 70, 80];
        v.sort_unstable();
        assert_eq!(median(&v), 45);
        // q1 = v[2]=30, q3 = v[6]=70 → 40
        assert_eq!(iqr(&v), 40);
    }

    #[test]
    fn base64_encode_known() {
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
    }

    #[test]
    fn short_trial_records() {
        // A tiny run must produce a non-empty histogram without panicking.
        let (h, drops) = spsc_trial(100_000, false, WaitStrategy::BusySpin);
        assert_eq!(drops, 0);
        assert!(h.max() >= 1);
    }
}
