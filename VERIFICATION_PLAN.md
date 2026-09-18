# Verification Plan — Tail Latency & Disk-Write Claims

Companion to `BENCHMARKING.md`. Its job: promote the two claims the README
currently marks *projected* / *cited* to **measured**, held to the same standard
as every other number in the repo (§1–9 of BENCHMARKING.md), with committed raw
data anyone can reproduce.

Two claims are in scope:

- **T (tail latency):** "Running on Linux with isolated cores would show sub-µs
  p99." — currently a projection, never run.
- **D (disk write / io_uring):** "io_uring with registered buffers on Linux is
  expected to reach ~1.5–2.5× over the std baseline at large batch sizes." —
  currently cited from published benchmarks, not measured on this code.

---

## 0. Premise check — read this first

Neither claim can be proven by "just running on Linux." Each is blocked by a code
gap that makes the current harness measure the wrong thing. These are verified by
reading the source at HEAD, not assumed:

| # | Gap | Evidence | Consequence if not fixed |
|---|-----|----------|--------------------------|
| G1 | Latency consumer **yields**, does not busy-spin | `bench/src/main.rs:172` — `try_recv()` + `std::thread::yield_now()` on empty | Measures scheduler wakeup latency, not queue latency. p99 can never go sub-µs because the tail *is* the wakeup. |
| G2 | `UringWriter` uses naive `opcode::Write`, **no registered buffers** | `persist/src/lib.rs:123`; comment at `lib.rs:90–94` calls registered buffers "the main performance lever… stretch goal" | Naive io_uring Write ≈ or < BufWriter. The 1.5–2.5× lever is not implemented, so the claim cannot be reached. |
| G3 | Writers expose **different batching knobs** | `StdWriter.write_batch` flushes every call (`lib.rs:65`); `UringWriter` chunks by `ring_depth` (`lib.rs:112,115`) | Not apples-to-apples. Violates BENCHMARKING.md §6. Ratio is uninterpretable. |
| G4 | Committed std baseline is **non-monotonic** | `persist_bench_results.json`: batch=4096 → 0.021 Mmsg/s / 24.5 s, *below* batch=8 → 0.142 Mmsg/s | Throughput falling as batch grows = harness artifact, not writer behavior. Any ratio against this baseline is noise. |

**Order of work: close G1–G4, then run the tests below.** A test run before the
gaps close produces a number that looks like data and isn't.

---

## Test T — Tail Latency

### Hypothesis (falsifiable)

> On x86-64 Linux with dedicated isolated cores, the core-pinned SPSC ring buffer
> with a **busy-spin consumer** sustains **p99 < 1 000 ns** end-to-end at the target
> arrival rate, and that p99 is **stable across ≥ 10 trials** (IQR < 25% of median).

Falsified if p99 ≥ 1 µs at the target rate on isolated cores, or if p99 varies more
than the stated spread across trials (→ the number is not a floor, it's a sample).

### T.1 — Prerequisite code change (closes G1)

Add a busy-spin consumer latency path. Minimal diff to the existing `spsc_latency`
— swap the empty-queue branch only:

```rust
// bench/src/main.rs — new fn, or a `wait: WaitStrategy` param on spsc_latency
fn spsc_latency_busyspin(label: &str, pinned: bool) -> LatResult {
    // ...identical setup to spsc_latency()...
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
                    std::hint::spin_loop();   // <-- was yield_now(); THIS is the change
                }
            }
        }
        hist
    });
    // ...producer unchanged (already BusySpin)...
}
```

Keep the `yield_now` variant too — the comparison *busy-spin vs yield on the same
isolated cores* is itself a result worth committing (it quantifies the tail cost of
yielding). This is the wait-strategy row BENCHMARKING.md §Benchmark Matrix already
asks for and the bench doesn't yet fill.

### T.2 — Environment (bare-metal Linux, disclosed per BENCHMARKING.md §3)

Not a VM, not a shared cloud instance. Kernel boot cmdline, ≥ 2 cores to isolate
(one producer, one consumer):

```
isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3
```

Then per boot, before the run:

```bash
# performance governor, no frequency scaling surprises
sudo cpupower frequency-set -g performance
# disable turbo so the clock is fixed and reproducible (intel_pstate)
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
# confirm SMT siblings of 2,3 are not also loaded (isolate physical cores, not HT twins)
lscpu -e
# TSC must be invariant for quanta to be trustworthy (BENCHMARKING.md §Tooling)
grep -o 'constant_tsc\|nonstop_tsc' /proc/cpuinfo | sort -u
lstopo --of console > topology.txt   # commit this (NUMA disclosure, §NUMA)
```

Pin to the isolated physical cores explicitly:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release -p bench
taskset -c 2,3 ./target/release/bench      # pin() inside still selects cores 0/1 of the mask
```

> Note: `pin(0)`/`pin(1)` in the harness index into `core_affinity::get_core_ids()`.
> Under `taskset -c 2,3` that set is `{2,3}`, so `pin(0)→core 2`, `pin(1)→core 3`.
> Verify with a print of the resolved core id — don't assume the mapping.

### T.3 — Protocol

- **Rate sweep**, not a single rate. The claim must hold at a stated production
  rate, and you want the load at which p99 crosses 1 µs. Sweep
  `LAT_RATE_HZ ∈ {10k, 100k, 500k, 1M, 5M}`.
- **≥ 10 trials per rate.** BENCHMARKING.md §5: "a single run is an anecdote."
  Report **median p99** and **IQR** across trials, not one p99.
- **Warmup** unchanged (5 000 msgs discarded) — already stated, keep it.
- Report the full percentile set (min, p50, p95, p99, p99.9, max) per §1.
- **CO correction stays on** (`record_correct`) — but at busy-spin on an isolated
  core the correction should be near-inert; if it's still inflating p99.9, that
  inflation is real backlog and must be reported, not hidden.

### T.4 — Output (extends the existing JSON, so it stays auditable)

Add per-trial fields to `LatResult` and a run-level env block:

```jsonc
{
  "env": {
    "cpu_model": "...", "base_ghz": 0.0, "turbo": false,
    "governor": "performance", "isolcpus": "2,3", "smt": "off-on-isolated",
    "kernel": "6.x", "rustc": "1.95.0", "rustflags": "-C target-cpu=native",
    "tsc": "constant_tsc,nonstop_tsc", "numa_nodes": 1
  },
  "latency": [
    { "label": "spsc-ring-buffer-busyspin", "pinned": true, "rate_hz": 1000000,
      "trials": 10, "p99_ns_median": 0, "p99_ns_iqr": 0,
      "p50_ns_median": 0, "p99_9_ns_median": 0, "hdr_base64": "..." }
  ]
}
```

Regenerate `latency_histogram.svg` (the code already does this) and commit both.

### T.5 — Pass / fail

| Result | Verdict |
|--------|---------|
| p99 median < 1 µs at target rate, IQR < 25% of median | **T proven.** Update README: change "would show" → measured, cite this run. |
| p99 median < 1 µs but IQR ≥ 25% | Partial. Report as "sub-µs typical, tail unstable" — do **not** headline a single number. |
| p99 median ≥ 1 µs on isolated cores | **T falsified.** Move to Future Work with the measured number; state the rate at which it *does* cross 1 µs. |

---

## Test D — Disk Write / io_uring

### Hypothesis (falsifiable)

> On Linux ≥ 5.1 with a real block device, `UringWriter` **using registered
> buffers** (`WriteFixed`) reaches **≥ 1.5× the std `BufWriter` throughput** at
> large batch (≥ 64), under an **identical durability contract**, median over
> ≥ 5 trials.

Falsified if the ratio < 1.5× at every tested config. Per BENCHMARKING.md §8 that
is a legitimate result — it goes in Future Work as "no measured gain on this
workload," not buried.

### D.1 — Prerequisite code changes (close G2, G3, G4)

**(a) Registered buffers (G2).** Implement the `WriteFixed` path. API is
`io-uring = "0.6"` — **verify these signatures against the 0.6 docs before
trusting them; I have not compiled this** (fast-moving crate):

```rust
// persist/src/lib.rs, uring module — sketch, VERIFY against io-uring 0.6
use io_uring::{opcode, types, IoUring};

// register a pool of fixed 64-byte buffers once at create():
let iovecs: Vec<libc::iovec> = buffers.iter_mut().map(|b| libc::iovec {
    iov_base: b.as_mut_ptr() as *mut _, iov_len: 64,
}).collect();
unsafe { ring.submitter().register_buffers(&iovecs)?; }

// per write, use WriteFixed with the buffer index (no per-op kernel copy):
let op = opcode::WriteFixed::new(
    types::Fd(fd), buf_ptr, 64, buf_index as u16,
).offset(offset).build().user_data(i);
```

Keep the naive `Write` writer as a third row: **std vs naive-uring vs
registered-uring** is the honest picture, and it shows exactly how much of any gain
is batching vs zero-copy.

**(b) Unified batching knob (G3).** Give `StdWriter` a real batch: accumulate
`batch_size` records, one `write_all` of the whole slice, then flush — so
"batch=N" means the same thing (N records per syscall-group) for both writers.
Right now Std flushes every `write_batch` call irrespective of N.

**(c) Fix the non-monotonic artifact (G4).** The 24 s at batch=4096 is almost
certainly the interaction of per-call `flush()` + fsync policy + page-cache
thrash. Before comparing anything, get the std curve **monotonic non-decreasing
in batch size on a fixed device**. If it isn't, the harness is still broken.

**(d) Defined durability contract.** Pick one and apply it identically to both
writers; report which:

- **Mode A — buffered throughput:** `sync_data()` once at end. Measures
  kernel-submission rate. (What the code does now.)
- **Mode B — durable-commit throughput:** `fsync`/`sync_data` per batch. Measures
  what a WAL actually pays. **This is the mode a "disk-write" claim should lead
  with.**

Run both; they answer different questions and differ by an order of magnitude.

### D.2 — Environment (disclosed)

- **Real block device**, not tmpfs (tmpfs measures memcpy, not disk). State the
  device (NVMe/SATA/…), filesystem, and mount options.
- **Control the page cache** between trials:
  `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches` — or open with `O_DIRECT`
  and state which. Without this you measure RAM, not the device.
- Kernel version (io_uring feature level), rustc, `--release`, record size (64 B),
  `MSG_COUNT`.

### D.3 — Protocol

- Batch sweep `{1, 8, 64, 512, 4096}` (matched knob from D.1b).
- Both durability modes (A and B).
- Three writers: `std-BufWriter`, `io_uring-Write`, `io_uring-WriteFixed`.
- **≥ 5 trials per cell**, report median + spread (§5).
- Report **MB/s and Mmsg/s** (already computed) plus the **ratio vs std** per cell.

### D.4 — Output

Extend `persist_bench_results.json` with `durability_mode`, `trials`,
`throughput_mb_s_median`, `ratio_vs_std`, and the env block from D.2. Commit.

### D.5 — Pass / fail

| Result | Verdict |
|--------|---------|
| registered-uring ≥ 1.5× std at batch ≥ 64, Mode B, median over trials | **D proven.** README: cite measured ratio + config + device. Report the exact number, not the "1.5–2.5×" range. |
| gain only in Mode A (buffered) | Report as buffered-throughput gain; state it does not hold for durable commits. |
| ratio < 1.5× everywhere | **D falsified** for this workload. Future Work: "naive+registered io_uring showed no ≥1.5× gain on <device>; the published range assumes <what>." Honest per §8. |

---

## Where to run

- **Test T:** not on a VM or shared cloud host. Verified on the machine this plan
  was drafted from: 2 shared vCPUs, no `isolcpus`, no cpufreq governor exposed —
  scheduler and clock are not controllable, so any p99 there is non-indicative.
  Needs **bare-metal Linux** with the boot cmdline in T.2.
- **Test D:** the io_uring syscall is reachable in a generic Linux container
  (setup returned `EFAULT` on a null-pointer probe, i.e. reached the kernel — not
  `EPERM`/`ENOSYS`), so **correctness + a rough ratio can run in-container**. The
  **headline MB/s needs a real, disclosed block device** — a container's overlay
  or a tmpfs will not produce a device number.
- Practical split: build + functional tests + smoke ratio anywhere; the two
  committable headline runs on your dedicated Linux box.

---

## Verification of this plan

- **Verified:** G1–G4 by reading `bench/src/main.rs`, `persist/src/lib.rs`,
  `bus/src/spsc.rs`, `persist_bench_results.json` at HEAD; toolchain (rustc
  1.95.0), core count (2), absence of isolcpus/governor, and io_uring syscall
  reachability by direct probe on the drafting host.
- **Not verified:** the `io-uring` 0.6 `WriteFixed`/`register_buffers` signatures
  in D.1a (not compiled — verify against the crate before relying on them); no
  benchmark was run, so no latency or throughput number here is measured — this
  is the method, not the result.
- **Could break:** `core_affinity` core-index→physical-core mapping under
  `taskset` (T.2 note); `O_DIRECT` alignment requirements if that path is chosen
  in D.2; registered-buffer count vs ring depth limits on older kernels.
