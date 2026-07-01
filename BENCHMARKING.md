# Benchmarking Standard — Cadence

This document defines the methodology every benchmark in this repo follows.

## Principles

1. **Report distributions, not just means.** Always p50, p95, p99, p99.9,
   and max — alongside min/mean if useful, never instead of percentiles.
   The interesting behavior is in the tail.
2. **Avoid coordinated omission.** Closed-loop benchmarking (send next
   request only after the previous one completes) systematically hides the
   worst latencies — exactly the ones that matter. Use one of:
   - An **open-loop generator**: produce messages at a fixed rate
     regardless of consumer drain speed, and measure actual latency
     against the *intended* send time.
   - **`hdrhistogram`'s `record_correct(value, expected_interval)`**,
     which statistically corrects for the omission.
   Document which approach is used for each benchmark and why.
3. **Disclose the full environment, every time:**
   - CPU model and base/boost frequency
   - Core count, and whether any cores were isolated (`isolcpus`) or
     pinned (`taskset` / `core_affinity`)
   - CPU frequency governor (`performance` vs `powersave`)
   - Turbo boost and hyperthreading/SMT on or off
   - Kernel version
   - Rust version and relevant `RUSTFLAGS` (e.g. `target-cpu=native`)
   - Message size and send rate
   - Whether run in `--release`
4. **Warm up explicitly.** State how many iterations are discarded before
   measurement starts, and why that number was chosen.
5. **Run multiple trials and report variance.** A single run is an
   anecdote.
6. **Be fair to the baseline.** Same machine, same run, same message size,
   same semantics where comparable. If a comparison is *inherently* unfair
   in some respect (e.g. compile-time-sized vs runtime-sized buffers),
   say so directly rather than letting the reader assume apples-to-apples.
7. **Separate latency from throughput.** They trade off against each other
   (busy-spin lowers latency at the cost of burning a core continuously).
   Report both, and name the tradeoff.
8. **Never claim what wasn't measured.** If a result requires hardware or
   setup this project doesn't have (multi-socket NUMA, a NIC with
   zero-copy AF_XDP support), it goes in "Future Work" with a cited
   hypothesis if one exists — never presented as a measured result.
9. **Commit raw results.** Export benchmark output as JSON and commit
   `hdrhistogram` logs alongside the code, so results are reproducible and
   auditable by anyone who clones the repo — not just summarized in prose.

## Tooling

- **`quanta`** for hot-path timestamps (TSC-based). Document whether TSC is
  invariant on the benchmark machine.
- **`hdrhistogram`** for all latency-distribution capture.
- **`criterion`** for throughput/per-operation microbenchmarks only (e.g.
  cost of a single enqueue/dequeue call). Criterion's clock-granularity
  behavior makes it the wrong tool for capturing end-to-end latency
  *distributions* — don't use it for that.

## Benchmark Matrix (Phase 4)

The core Phase 4 result compares:

| Dimension          | Values                                          |
|---------------------|--------------------------------------------------|
| Implementation      | `crossbeam-channel` baseline vs. hand-written ring buffer |
| Burst size           | 1, N (define N once chosen)                     |
| Pinning              | unpinned vs. core-pinned (`core_affinity`)       |
| Wait strategy (ring buffer only) | busy-spin, yield, block            |

Every cell in this matrix that gets run should report the full percentile
set and the environment disclosure above.

## NUMA Disclosure

Run an `hwloc` topology dump (`lstopo` or equivalent) regardless of
hardware, and include it in the repo. If the benchmark machine is
single-socket (one NUMA node), say so explicitly and do **not** claim
cross-NUMA measurements. Cross-node latency figures may be cited from
published sources (e.g. hwloc/OpenMPI tutorial material) as context for
*why* NUMA-awareness matters in production, clearly marked as cited, not
measured.

## io_uring Disclosure (Phase 4b, if built)

io_uring's benefit is workload-dependent — a naive swap from epoll can
yield as little as ~1.06x, while batching with registered buffers can
reach ~2–2.5x, and some workloads see no gain or a regression. Report
which configuration was tested and don't generalize beyond it.

## Phase 3 Decision: Coordinated Omission Handling

**Decision:** Both strategies from the TASKS checklist are used together.

1. **Open-loop constant-arrival-rate generator** — the publisher spins on
   `quanta::Clock::raw()` and sends each message at its *intended* time
   (`start + i * interval_ns`), independent of whether the subscriber has
   drained. This ensures the latency measurement includes any time the
   message spent waiting in the queue because the subscriber was behind.

2. **`hdrhistogram::Histogram::record_corrected(value, interval_ns)`** — on
   the subscriber side, each recorded value is corrected using HDR's built-in
   coordinated-omission fill. If a latency of 5 ms is observed at a 5 µs
   interval, `record_corrected` backfills 999 synthetic samples at every
   interval up to 5 ms. This produces the distribution that *would* have been
   seen had every message been measured, even under sustained backlog.

Using both provides defense-in-depth: the open-loop generator prevents the
artificial latency deflation caused by a closed-loop sender, and
`record_corrected` catches any residual omission from OS scheduling jitter on
the publisher thread.

**Rationale for not using `criterion`:** Criterion's default closed-loop
iteration model and limited HDR support make it unsuitable for capturing
end-to-end latency distributions. It is reserved for per-operation
microbenchmarks (e.g., cost of a single ring buffer enqueue) in Phase 4.

## What Goes in the README vs. Here

The README shows the headline results and links here. This file is the
standard those results are held to — if a number in the README can't be
traced back to a method described here, it shouldn't be in the README.
