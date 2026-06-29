/// Phase 4b — persistence benchmark: std I/O vs io_uring.
///
/// Measures throughput (MB/s and Mmsg/s) for appending Message records to a
/// log file at varying batch sizes.
///
/// **io_uring is Linux-only.** On Windows this benchmark runs the std
/// baseline only and documents what io_uring is expected to add on Linux.
///
/// See BENCHMARKING.md for methodology. See crates/persist/src/lib.rs for
/// the implementation of both writers.
use message_core::Message;
use persist::MessageWriter;
use persist::StdWriter;
use serde::Serialize;
use std::time::Instant;

const MSG_COUNT: usize = 500_000;
const BATCH_SIZES: &[usize] = &[1, 8, 64, 512, 4096];
const LOG_PATH: &str = "persist_bench_log.bin";

#[derive(Serialize)]
struct TrialResult {
    writer:             &'static str,
    batch_size:         usize,
    msg_count:          usize,
    elapsed_ms:         u64,
    throughput_mmsg_s:  f64,
    throughput_mb_s:    f64,
    platform_note:      &'static str,
}

fn bench_writer(writer: &mut dyn MessageWriter, batch_size: usize) -> TrialResult {
    let msgs: Vec<Message> = (0..batch_size)
        .map(|i| Message::new("persist.bench", &(i as u64).to_le_bytes()))
        .collect();

    let t0 = Instant::now();
    let mut total = 0usize;
    while total < MSG_COUNT {
        let n = writer.write_batch(&msgs).unwrap();
        total += n;
    }
    writer.sync().unwrap();
    let elapsed = t0.elapsed();

    let elapsed_ms = elapsed.as_millis() as u64;
    let secs = elapsed.as_secs_f64();
    let mmsg_s = total as f64 / secs / 1e6;
    let mb_s   = (total as f64 * 64.0) / secs / (1024.0 * 1024.0);

    TrialResult {
        writer: writer.name(),
        batch_size,
        msg_count: total,
        elapsed_ms,
        throughput_mmsg_s: mmsg_s,
        throughput_mb_s:   mb_s,
        platform_note: platform_note(),
    }
}

#[cfg(target_os = "linux")]
fn platform_note() -> &'static str { "linux — io_uring available" }
#[cfg(not(target_os = "linux"))]
fn platform_note() -> &'static str {
    "non-linux — io_uring not available; std baseline only"
}

fn print_result(r: &TrialResult) {
    println!(
        "  {:20} batch={:>5}  {:>7.2} Mmsg/s  {:>7.1} MB/s  ({} ms)",
        r.writer, r.batch_size, r.throughput_mmsg_s, r.throughput_mb_s, r.elapsed_ms
    );
}

fn main() {
    println!("Cadence Phase 4b — persistence benchmark");
    println!("  Platform: {}  OS: {}", std::env::consts::ARCH, std::env::consts::OS);
    println!("  Messages per trial: {MSG_COUNT}  Record size: 64 bytes");
    println!("  Batch sizes: {:?}", BATCH_SIZES);
    println!();

    #[cfg(not(target_os = "linux"))]
    {
        println!("NOTE: io_uring requires Linux. Running std I/O baseline only.");
        println!("      Expected io_uring gain on Linux:");
        println!("      - Naive swap (batch=1): ~0–5% — no batching advantage");
        println!("      - Batch=64, registered buffers: ~1.5–2.5x throughput");
        println!("      - See BENCHMARKING.md §io_uring Disclosure for rationale.");
        println!();
    }

    let mut results: Vec<TrialResult> = Vec::new();

    // ── std I/O ───────────────────────────────────────────────────────────────
    println!("=== std BufWriter ===");
    for &batch in BATCH_SIZES {
        let _ = std::fs::remove_file(LOG_PATH);
        let mut w = StdWriter::create(LOG_PATH).unwrap();
        let r = bench_writer(&mut w, batch);
        print_result(&r);
        results.push(r);
    }

    // ── io_uring (Linux only) ─────────────────────────────────────────────────
    #[cfg(target_os = "linux")]
    {
        println!("\n=== io_uring Write ===");
        for &batch in BATCH_SIZES {
            let _ = std::fs::remove_file(LOG_PATH);
            std::fs::File::create(LOG_PATH).unwrap();
            let mut w = persist::uring::UringWriter::create(LOG_PATH, batch.min(512) as u32).unwrap();
            let r = bench_writer(&mut w, batch);
            print_result(&r);
            results.push(r);
        }
    }

    let _ = std::fs::remove_file(LOG_PATH);

    // ── JSON export ───────────────────────────────────────────────────────────
    let json = serde_json::to_string_pretty(&results).unwrap();
    std::fs::write("persist_bench_results.json", &json).unwrap();
    println!("\nExported → persist_bench_results.json");

    // ── Summary ───────────────────────────────────────────────────────────────
    println!();
    println!("Interpretation:");
    println!("  batch=1   → naive swap: one syscall per record regardless of impl.");
    println!("  batch=N   → where batching helps: fewer syscalls, better throughput.");
    println!("  io_uring advantage is in batching + registered buffers (Linux ≥5.1).");
    println!("  On this platform ({}), only std baseline is available.", std::env::consts::OS);
    println!("  Results without io_uring are honest — not extrapolated.");
}
