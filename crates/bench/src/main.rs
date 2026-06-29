/// Phase 3 benchmark: end-to-end latency + throughput.
///
/// Methodology (see BENCHMARKING.md):
/// - Open-loop constant-arrival-rate generator on a dedicated publisher thread.
/// - Subscriber thread records (intended_send_ns, receive_ns) pairs.
/// - Latency = receive_ns − intended_send_ns.
/// - `hdrhistogram::Histogram::record_correct(value, interval_ns)` corrects
///   for coordinated omission: if the subscriber falls behind the histogram
///   is backfilled for all intervals that were missed.
/// - `quanta::Instant` provides TSC-based nanosecond timestamps. Durations
///   are measured relative to a shared epoch so they fit in a u64.
/// - Results are printed to stdout (percentile table) and exported to
///   `bench_results.json` for reproducibility / audit.
///
/// Note: this is the **Phase 3 crossbeam-channel baseline**. The Mutex
/// inside the bus limits throughput to ~50 k msg/s across threads — that is
/// expected and honest. Phase 4 replaces the hot path with a lock-free SPSC
/// ring buffer and re-runs this same benchmark for a fair comparison.
use bus::Bus;
use hdrhistogram::{serialization::Serializer, Histogram};
use message_core::Message;
use quanta::Instant as QInstant;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// ── Benchmark parameters ────────────────────────────────────────────────────

/// Messages to publish (after warmup).
const MSG_COUNT: usize = 100_000;
/// Warmup messages (discarded from histogram).
const WARMUP: usize = 5_000;
/// Target publish rate (msgs/sec). Kept conservative for the Mutex baseline.
/// Phase 4 will push this much higher with the ring buffer.
const RATE_HZ: u64 = 20_000;
/// Nanoseconds between sends at target rate.
const INTERVAL_NS: u64 = 1_000_000_000 / RATE_HZ;
/// Subscriber queue capacity.
const CAPACITY: usize = 65_536;

// ── Serializable result ──────────────────────────────────────────────────────

#[derive(Serialize)]
struct Percentiles {
    min_ns: u64,
    mean_ns: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    p99_9_ns: u64,
    max_ns: u64,
}

#[derive(Serialize)]
struct BenchResult {
    implementation: &'static str,
    msg_count: usize,
    warmup: usize,
    rate_hz: u64,
    interval_ns: u64,
    queue_capacity: usize,
    coordinated_omission: &'static str,
    latency: Percentiles,
    throughput_mmsg_per_sec: f64,
    drop_count: u64,
    /// Raw histogram in HdrHistogram V2 base64 for external tooling.
    hdr_base64: String,
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    println!("Cadence Phase 3 — latency benchmark");
    println!("  msgs:     {} (+ {} warmup)", MSG_COUNT, WARMUP);
    println!("  rate:     {} msg/s  ({} ns/msg interval)", RATE_HZ, INTERVAL_NS);
    println!("  capacity: {}", CAPACITY);
    println!("  impl:     crossbeam-channel (Phase 3 baseline)");
    println!();

    // Shared epoch: durations from this point are stored as u64 nanos in payload.
    let epoch = QInstant::now();

    let bus = Bus::new(CAPACITY);
    let (_sub_id, rx) = bus.subscribe("bench");
    let done = Arc::new(AtomicBool::new(false));
    let done_pub = done.clone();

    // ── Subscriber thread ────────────────────────────────────────────────────
    let epoch_sub = epoch;
    let mut hist: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000_000, 3)
        .expect("histogram config valid");

    let sub_handle = thread::spawn(move || {
        let mut received = 0usize;
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(msg) => {
                    let now_ns = epoch_sub.elapsed().as_nanos() as u64;
                    let intended_ns = u64::from_le_bytes(msg.payload[..8].try_into().unwrap());
                    let latency_ns = now_ns.saturating_sub(intended_ns).max(1);

                    if received >= WARMUP {
                        hist.record_correct(latency_ns, INTERVAL_NS).unwrap_or(());
                    }
                    received += 1;
                }
                Err(_) => {
                    if done.load(Ordering::Relaxed) { break; }
                }
            }
        }
        (hist, received)
    });

    // ── Publisher thread (open-loop) ─────────────────────────────────────────
    let epoch_pub = epoch;
    let bus_pub = bus.clone();
    let total = MSG_COUNT + WARMUP;

    let wall_start = std::time::Instant::now();
    let pub_handle = thread::spawn(move || {
        let start_ns = epoch_pub.elapsed().as_nanos() as u64;
        for i in 0..total {
            let intended_ns = start_ns + (i as u64 * INTERVAL_NS);
            // Busy-wait until the intended send time.
            while (epoch_pub.elapsed().as_nanos() as u64) < intended_ns {}

            let mut msg = Message::new("bench", &[]);
            msg.payload[..8].copy_from_slice(&intended_ns.to_le_bytes());
            bus_pub.offer(msg);
        }
    });

    pub_handle.join().unwrap();
    let wall_elapsed = wall_start.elapsed();
    done_pub.store(true, Ordering::Relaxed);
    let (hist, received) = sub_handle.join().unwrap();

    // ── Print report ──────────────────────────────────────────────────────────
    let drops = bus.drop_count();
    println!("Results ({} received, {} dropped):", received, drops);
    println!();
    println!("  Latency (CO-corrected, open-loop @ {} msg/s):", RATE_HZ);
    println!("    min     {:>12} ns", hist.min());
    println!("    mean    {:>15.1} ns", hist.mean());
    println!("    p50     {:>12} ns", hist.value_at_quantile(0.50));
    println!("    p95     {:>12} ns", hist.value_at_quantile(0.95));
    println!("    p99     {:>12} ns", hist.value_at_quantile(0.99));
    println!("    p99.9   {:>12} ns", hist.value_at_quantile(0.999));
    println!("    max     {:>12} ns", hist.max());
    println!();

    let throughput = MSG_COUNT as f64 / wall_elapsed.as_secs_f64() / 1e6;
    println!("  Throughput: {:.3} Mmsg/s ({} msgs in {:.3}s)",
        throughput, MSG_COUNT, wall_elapsed.as_secs_f64());
    if drops > 0 {
        println!("  WARNING: {} messages dropped — consider lowering RATE_HZ", drops);
    }
    println!();

    // ── JSON export ───────────────────────────────────────────────────────────
    let mut serialized = Vec::new();
    hdrhistogram::serialization::V2Serializer::new()
        .serialize(&hist, &mut serialized)
        .unwrap();
    let hdr_base64 = base64_encode(&serialized);

    let result = BenchResult {
        implementation: "crossbeam-channel (Phase 3 baseline)",
        msg_count: MSG_COUNT,
        warmup: WARMUP,
        rate_hz: RATE_HZ,
        interval_ns: INTERVAL_NS,
        queue_capacity: CAPACITY,
        coordinated_omission: "open-loop + hdrhistogram::record_correct(value, interval_ns)",
        latency: Percentiles {
            min_ns:   hist.min(),
            mean_ns:  hist.mean(),
            p50_ns:   hist.value_at_quantile(0.50),
            p95_ns:   hist.value_at_quantile(0.95),
            p99_ns:   hist.value_at_quantile(0.99),
            p99_9_ns: hist.value_at_quantile(0.999),
            max_ns:   hist.max(),
        },
        throughput_mmsg_per_sec: throughput,
        drop_count: drops,
        hdr_base64,
    };

    let json = serde_json::to_string_pretty(&result).unwrap();
    std::fs::write("bench_results.json", &json).unwrap();
    println!("  Exported → bench_results.json");
}

/// Minimal base64 encoder (avoids an extra crate).
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_records_and_reports() {
        let mut h: Histogram<u64> = Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap();
        for v in [100u64, 200, 300, 1000, 5000] {
            h.record_correct(v, INTERVAL_NS).unwrap();
        }
        assert!(h.value_at_quantile(0.50) >= 100);
        assert!(h.value_at_quantile(0.99) >= 1000);
        assert!(h.max() >= 5000);
    }

    #[test]
    fn base64_encode_known() {
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"),  "TWE=");
        assert_eq!(base64_encode(b"M"),   "TQ==");
    }

    #[test]
    fn json_export_is_valid() {
        let r = BenchResult {
            implementation: "test",
            msg_count: 1, warmup: 0, rate_hz: 1, interval_ns: 1,
            queue_capacity: 1,
            coordinated_omission: "test",
            latency: Percentiles { min_ns: 0, mean_ns: 2.5, p50_ns: 1,
                                   p95_ns: 2, p99_ns: 3, p99_9_ns: 4, max_ns: 5 },
            throughput_mmsg_per_sec: 1.0,
            drop_count: 0,
            hdr_base64: "abc=".into(),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"implementation\""));
        assert!(json.contains("\"p99_ns\""));
    }
}
