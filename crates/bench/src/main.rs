/// Phase 3 + Phase 4 benchmark.
///
/// Two distinct measurements per implementation:
///   1. **Latency** — open-loop at a moderate rate (10 k msg/s) so OS
///      scheduling jitter doesn't inflate every percentile. Uses
///      hdrhistogram::record_correct() for coordinated-omission correction.
///   2. **Throughput** — closed-loop max-rate, 1 M messages; measures
///      sustainable msg/s under ideal conditions (no rate limiting).
///
/// Implementations compared:
///   A. crossbeam-channel Bus (Phase 3 baseline)
///   B. Hand-written SPSC ring buffer — unpinned  (Phase 4)
///   C. Hand-written SPSC ring buffer — core-pinned (Phase 4)
///
/// See BENCHMARKING.md for full methodology, hardware disclosure, and
/// coordinated-omission rationale.
use bus::{spsc, Bus, WaitStrategy};
use hdrhistogram::{serialization::Serializer, Histogram};
use message_core::Message;
use quanta::Instant as QInstant;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ── Parameters ───────────────────────────────────────────────────────────────

/// Latency trial: target publish rate. Kept low so OS scheduling jitter
/// (Windows; no isolcpus) does not dominate every percentile.
const LAT_RATE_HZ:  u64   = 10_000;
const LAT_INTERVAL: u64   = 1_000_000_000 / LAT_RATE_HZ;
const LAT_MSGS:     usize = 100_000;
const LAT_WARMUP:   usize = 5_000;

/// Throughput trial: push this many messages as fast as possible.
const THR_MSGS:     usize = 1_000_000;

const CAPACITY: usize = 1 << 17; // 131 072 — must be power of two

// ── Serializable result ───────────────────────────────────────────────────────

#[derive(Serialize, Clone)]
struct Pct {
    min_ns: u64, mean_ns: f64,
    p50_ns: u64, p95_ns: u64, p99_ns: u64, p99_9_ns: u64, max_ns: u64,
}

#[derive(Serialize)]
struct LatResult {
    label: String, rate_hz: u64, msg_count: usize,
    drop_count: u64, pinned: bool,
    latency: Pct, hdr_base64: String,
}

#[derive(Serialize)]
struct ThrResult {
    label: String, msg_count: usize,
    elapsed_ms: u64, throughput_mmsg_per_sec: f64, pinned: bool,
}

#[derive(Serialize)]
struct AllResults { latency: Vec<LatResult>, throughput: Vec<ThrResult> }

// ── Helpers ───────────────────────────────────────────────────────────────────

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap()
}

fn to_pct(h: &Histogram<u64>) -> Pct {
    Pct {
        min_ns: h.min(), mean_ns: h.mean(),
        p50_ns: h.value_at_quantile(0.50), p95_ns: h.value_at_quantile(0.95),
        p99_ns: h.value_at_quantile(0.99), p99_9_ns: h.value_at_quantile(0.999),
        max_ns: h.max(),
    }
}

fn hdr_b64(h: &Histogram<u64>) -> String {
    let mut buf = Vec::new();
    hdrhistogram::serialization::V2Serializer::new().serialize(h, &mut buf).unwrap();
    base64_encode(&buf)
}

fn pin(core_idx: usize) {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    if let Some(c) = cores.get(core_idx) {
        core_affinity::set_for_current(*c);
    }
}

// ── A: crossbeam-channel latency ──────────────────────────────────────────────

fn crossbeam_latency(label: &str) -> LatResult {
    let epoch = QInstant::now();
    let bus = Bus::new(CAPACITY);
    let (_id, rx) = bus.subscribe("bench");
    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    let e = epoch;
    let sub = thread::spawn(move || {
        let mut hist = new_hist();
        let mut received = 0usize;
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(msg) => {
                    let now = e.elapsed().as_nanos() as u64;
                    let intended = u64::from_le_bytes(msg.payload[..8].try_into().unwrap());
                    let lat = now.saturating_sub(intended).max(1);
                    if received >= LAT_WARMUP {
                        hist.record_correct(lat, LAT_INTERVAL).unwrap_or(());
                    }
                    received += 1;
                }
                Err(_) => { if done.load(Ordering::Relaxed) { break; } }
            }
        }
        hist
    });

    let b = bus.clone();
    let e = epoch;
    let pub_h = thread::spawn(move || {
        let start = e.elapsed().as_nanos() as u64;
        for i in 0..(LAT_MSGS + LAT_WARMUP) {
            let intended = start + i as u64 * LAT_INTERVAL;
            while (e.elapsed().as_nanos() as u64) < intended {}
            let mut m = Message::new("bench", &[]);
            m.payload[..8].copy_from_slice(&intended.to_le_bytes());
            b.offer(m);
        }
    });
    pub_h.join().unwrap();
    done2.store(true, Ordering::Relaxed);
    let hist = sub.join().unwrap();

    LatResult {
        label: label.to_string(), rate_hz: LAT_RATE_HZ,
        msg_count: LAT_MSGS, drop_count: bus.drop_count(), pinned: false,
        latency: to_pct(&hist), hdr_base64: hdr_b64(&hist),
    }
}

// ── B/C: SPSC latency ─────────────────────────────────────────────────────────

fn spsc_latency(label: &str, pinned: bool) -> LatResult {
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
                        hist.record_correct(lat, LAT_INTERVAL).unwrap_or(());
                    }
                    received += 1;
                }
                None => {
                    if done.load(Ordering::Relaxed) { break; }
                    std::thread::yield_now(); // Yield avoids starving the publisher
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
            let intended = start + i as u64 * LAT_INTERVAL;
            while (e.elapsed().as_nanos() as u64) < intended {}
            let mut m = Message::new("bench", &[]);
            m.payload[..8].copy_from_slice(&intended.to_le_bytes());
            tx.send(m, WaitStrategy::BusySpin);
        }
    });
    pub_h.join().unwrap();
    done2.store(true, Ordering::Relaxed);
    let hist = sub.join().unwrap();

    LatResult {
        label: label.to_string(), rate_hz: LAT_RATE_HZ,
        msg_count: LAT_MSGS, drop_count: 0, pinned,
        latency: to_pct(&hist), hdr_base64: hdr_b64(&hist),
    }
}

// ── Throughput: crossbeam ─────────────────────────────────────────────────────

fn crossbeam_throughput(label: &str) -> ThrResult {
    let bus = Bus::new(CAPACITY);
    let (_id, rx) = bus.subscribe("bench");
    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    let sub = thread::spawn(move || {
        let mut count = 0usize;
        loop {
            while rx.try_recv().is_ok() { count += 1; }
            if done.load(Ordering::Relaxed) { break; }
        }
        count
    });

    let b = bus.clone();
    let t0 = std::time::Instant::now();
    for _ in 0..THR_MSGS {
        b.publish(Message::new("bench", &[]));
    }
    let elapsed = t0.elapsed();
    done2.store(true, Ordering::Relaxed);
    let _ = sub.join();

    ThrResult {
        label: label.to_string(), msg_count: THR_MSGS, pinned: false,
        elapsed_ms: elapsed.as_millis() as u64,
        throughput_mmsg_per_sec: THR_MSGS as f64 / elapsed.as_secs_f64() / 1e6,
    }
}

// ── Throughput: SPSC ──────────────────────────────────────────────────────────

fn spsc_throughput(label: &str, pinned: bool) -> ThrResult {
    let (tx, rx) = spsc::<Message>(CAPACITY);
    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    let sub = thread::spawn(move || {
        if pinned { pin(1); }
        let mut count = 0usize;
        loop {
            while let Some(_) = rx.try_recv() { count += 1; }
            if done.load(Ordering::Relaxed) { break; }
            std::hint::spin_loop();
        }
        count
    });

    let t0 = std::time::Instant::now();
    let pub_h = thread::spawn(move || {
        if pinned { pin(0); }
        for _ in 0..THR_MSGS {
            tx.send(Message::new("bench", &[]), WaitStrategy::BusySpin);
        }
    });
    pub_h.join().unwrap();
    let elapsed = t0.elapsed();
    done2.store(true, Ordering::Relaxed);
    let _ = sub.join();

    ThrResult {
        label: label.to_string(), msg_count: THR_MSGS, pinned,
        elapsed_ms: elapsed.as_millis() as u64,
        throughput_mmsg_per_sec: THR_MSGS as f64 / elapsed.as_secs_f64() / 1e6,
    }
}

// ── Print helpers ─────────────────────────────────────────────────────────────

fn print_lat(r: &LatResult) {
    let pin = if r.pinned { "pinned" } else { "unpinned" };
    let drop_warn = if r.drop_count > 0 { format!(" ⚠ {} drops", r.drop_count) } else { String::new() };
    println!("\n  ┌─ {} [{}]{}", r.label, pin, drop_warn);
    println!("  │  min     {:>12} ns", r.latency.min_ns);
    println!("  │  mean    {:>15.0} ns", r.latency.mean_ns);
    println!("  │  p50     {:>12} ns", r.latency.p50_ns);
    println!("  │  p95     {:>12} ns", r.latency.p95_ns);
    println!("  │  p99     {:>12} ns", r.latency.p99_ns);
    println!("  │  p99.9   {:>12} ns", r.latency.p99_9_ns);
    println!("  └─ max     {:>12} ns", r.latency.max_ns);
}

fn print_thr(r: &ThrResult) {
    let pin = if r.pinned { "pinned" } else { "unpinned" };
    println!("  {} [{}]: {:.2} Mmsg/s  ({} msgs in {} ms)",
        r.label, pin, r.throughput_mmsg_per_sec, r.msg_count, r.elapsed_ms);
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() {
    let cores = core_affinity::get_core_ids().unwrap_or_default();
    println!("Cadence — Phase 3 vs Phase 4 benchmark");
    println!("  Logical cores: {}  OS: {}  Arch: {}", cores.len(), std::env::consts::OS, std::env::consts::ARCH);
    println!("  Note: for sub-µs tail latency, rerun on Linux with isolated cores (isolcpus).");
    println!("  Latency: {} k msg/s open-loop, {} msgs + {} warmup", LAT_RATE_HZ / 1000, LAT_MSGS, LAT_WARMUP);
    println!("  Throughput: {} M msgs max-rate push", THR_MSGS / 1_000_000);
    println!();

    let mut lat_results = Vec::new();
    let mut thr_results = Vec::new();

    // ── Latency ───────────────────────────────────────────────────────────────
    println!("=== Latency (open-loop {} k msg/s, CO-corrected) ===", LAT_RATE_HZ / 1000);

    let r = crossbeam_latency("crossbeam-Bus");
    print_lat(&r); lat_results.push(r);

    let r = spsc_latency("spsc-ring-buffer", false);
    print_lat(&r); lat_results.push(r);

    let r = spsc_latency("spsc-ring-buffer", true);
    print_lat(&r); lat_results.push(r);

    // ── Throughput ────────────────────────────────────────────────────────────
    println!("\n=== Throughput (max-rate, {} M msgs) ===", THR_MSGS / 1_000_000);

    let r = crossbeam_throughput("crossbeam-Bus");
    print_thr(&r); thr_results.push(r);

    let r = spsc_throughput("spsc-ring-buffer", false);
    print_thr(&r); thr_results.push(r);

    let r = spsc_throughput("spsc-ring-buffer", true);
    print_thr(&r); thr_results.push(r);

    // ── Export ────────────────────────────────────────────────────────────────
    let all = AllResults { latency: lat_results, throughput: thr_results };
    let json = serde_json::to_string_pretty(&all).unwrap();
    std::fs::write("bench_results.json", &json).unwrap();
    println!("\nExported → bench_results.json");

    // Generate SVG histogram (log-scale, Disruptor/Chronicle convention)
    let svg = render_latency_svg(&all.latency);
    std::fs::write("latency_histogram.svg", &svg).unwrap();
    println!("Generated → latency_histogram.svg");
}

// ── base64 ───────────────────────────────────────────────────────────────────

fn base64_encode(input: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
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

// ── SVG histogram (log-scale, Disruptor/Chronicle convention) ────────────────

const SVG_W: f64 = 900.0;
const SVG_H: f64 = 480.0;
const SVG_PL: f64 = 88.0;
const SVG_PR: f64 = 210.0;
const SVG_PT: f64 = 44.0;
const SVG_PB: f64 = 64.0;
const SVG_COLORS: &[&str] = &["#4e79a7", "#f28e2b", "#59a14f"];

fn pct_x(p: f64) -> f64 {
    let inv = (1.0 - p).max(1e-5_f64);
    let lmin = (1.0_f64 - 0.9999_f64).log10();
    let t = (inv.log10() - lmin) / (0.0_f64 - lmin);
    SVG_PL + (1.0 - t) * (SVG_W - SVG_PL - SVG_PR)
}

fn lat_y(ns: u64, max_ns: u64) -> f64 {
    let lo = 1.5_f64; // ~30 ns floor on log scale
    let hi = (max_ns as f64).max(1.0).log10();
    let t = ((ns as f64).max(1.0).log10() - lo) / (hi - lo);
    SVG_PT + (1.0 - t) * (SVG_H - SVG_PT - SVG_PB)
}

fn render_latency_svg(results: &[LatResult]) -> String {
    let gmax = results.iter().map(|r| r.latency.max_ns).max().unwrap_or(10_000_000);

    let mut s = format!(
        "<svg xmlns='http://www.w3.org/2000/svg' width='{}' height='{}' style='font-family:monospace;background:#1e1e2e'>",
        SVG_W, SVG_H
    );

    // Title
    s += &format!("<text x='{}' y='26' fill='#cdd6f4' font-size='14' text-anchor='middle' font-weight='bold'>Cadence — Latency Histogram (log scale, CO-corrected)</text>", SVG_W / 2.0);

    // Y grid + labels
    for (ns, lbl) in [(100u64,"100ns"),(1_000,"1µs"),(10_000,"10µs"),(100_000,"100µs"),(1_000_000,"1ms"),(10_000_000,"10ms")] {
        if ns > gmax * 3 { continue; }
        let y = lat_y(ns, gmax);
        s += &format!("<line x1='{SVG_PL}' y1='{y:.1}' x2='{}' y2='{y:.1}' stroke='#45475a' stroke-width='1'/>", SVG_W - SVG_PR);
        s += &format!("<text x='{}' y='{y:.1}' fill='#a6adc8' font-size='10' text-anchor='end' dominant-baseline='middle'>{lbl}</text>", SVG_PL - 5.0);
    }

    // X grid + labels
    for (p, lbl) in [(0.0,"p0"),(0.50,"p50"),(0.90,"p90"),(0.95,"p95"),(0.99,"p99"),(0.999,"p99.9"),(0.9999,"p99.99")] {
        let x = pct_x(p);
        s += &format!("<line x1='{x:.1}' y1='{SVG_PT}' x2='{x:.1}' y2='{}' stroke='#45475a' stroke-width='1'/>", SVG_H - SVG_PB);
        s += &format!("<text x='{x:.1}' y='{}' fill='#a6adc8' font-size='10' text-anchor='middle'>{lbl}</text>", SVG_H - SVG_PB + 13.0);
    }

    // Axes
    s += &format!("<line x1='{SVG_PL}' y1='{SVG_PT}' x2='{SVG_PL}' y2='{}' stroke='#cdd6f4' stroke-width='1.5'/>", SVG_H - SVG_PB);
    s += &format!("<line x1='{SVG_PL}' y1='{}' x2='{}' y2='{}' stroke='#cdd6f4' stroke-width='1.5'/>", SVG_H - SVG_PB, SVG_W - SVG_PR, SVG_H - SVG_PB);

    // Lines + legend
    for (i, r) in results.iter().enumerate() {
        let color = SVG_COLORS[i % SVG_COLORS.len()];
        let pts: Vec<(f64,f64)> = vec![
            (0.0,    r.latency.min_ns),
            (0.50,   r.latency.p50_ns),
            (0.95,   r.latency.p95_ns),
            (0.99,   r.latency.p99_ns),
            (0.999,  r.latency.p99_9_ns),
            (0.9999, r.latency.max_ns),
        ].into_iter().map(|(p,ns)| (pct_x(p), lat_y(ns, gmax))).collect();

        let pts_str: String = pts.iter().map(|(x,y)| format!("{x:.1},{y:.1}")).collect::<Vec<_>>().join(" ");
        s += &format!("<polyline points='{pts_str}' fill='none' stroke='{color}' stroke-width='2.5' stroke-linejoin='round'/>");
        for (x,y) in &pts {
            s += &format!("<circle cx='{x:.1}' cy='{y:.1}' r='3.5' fill='{color}'/>");
        }

        // Legend box
        let lx = SVG_W - SVG_PR + 12.0;
        let ly = SVG_PT + i as f64 * 22.0;
        s += &format!("<rect x='{lx}' y='{ly}' width='12' height='12' fill='{color}'/>");
        let lbl = if r.label.len() > 22 { &r.label[..22] } else { &r.label };
        s += &format!("<text x='{}' y='{}' fill='#cdd6f4' font-size='10'>{lbl}</text>", lx + 16.0, ly + 10.0);
    }

    // Axis labels
    s += &format!("<text x='{}' y='{}' fill='#a6adc8' font-size='11' text-anchor='middle'>Percentile</text>", SVG_W/2.0, SVG_H - 6.0);
    s += &format!("<text x='12' y='{}' fill='#a6adc8' font-size='11' text-anchor='middle' transform='rotate(-90,12,{})'>Latency (ns, log)</text>", SVG_H/2.0, SVG_H/2.0);

    s += "</svg>";
    s
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_records_and_reports() {
        let mut h = new_hist();
        for v in [100u64, 500, 1000, 5000, 10_000] {
            h.record_correct(v, LAT_INTERVAL).unwrap();
        }
        assert!(h.value_at_quantile(0.50) >= 100);
        assert!(h.value_at_quantile(0.99) >= 1000);
    }

    #[test]
    fn base64_encode_known() {
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"),  "TWE=");
        assert_eq!(base64_encode(b"M"),   "TQ==");
    }

    #[test]
    fn svg_contains_key_elements() {
        let dummy = vec![LatResult {
            label: "test".to_string(), rate_hz: 1, msg_count: 1,
            drop_count: 0, pinned: false,
            latency: Pct { min_ns: 100, mean_ns: 500.0, p50_ns: 300,
                           p95_ns: 800, p99_ns: 1000, p99_9_ns: 2000, max_ns: 5000 },
            hdr_base64: String::new(),
        }];
        let svg = render_latency_svg(&dummy);
        assert!(svg.contains("<svg"));
        assert!(svg.contains("polyline"));
        assert!(svg.contains("p99"));
    }

    #[test]
    fn spsc_bench_smoke() {
        let (tx, rx) = spsc::<Message>(256);
        for i in 0u64..100 {
            let mut m = Message::new("t", &[]);
            m.payload[..8].copy_from_slice(&i.to_le_bytes());
            tx.send(m, WaitStrategy::BusySpin);
        }
        let mut count = 0;
        while rx.try_recv().is_some() { count += 1; }
        assert_eq!(count, 100);
    }
}
