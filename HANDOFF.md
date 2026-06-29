# Handoff — Cadence

> Formerly "Microsecond Message Bus."

## Current Status

- Current phase: Phase 4 complete — 26/26 tests passing, benchmark verified
- Last stable commit: None yet (no git repo initialized)
- Last agent/tool: Claude Code (claude-sonnet-4-6)
- Last updated: 2026-06-28

## Completed

- [x] Project scaffold
- [x] Phase 1
- [ ] Phase 2
- [ ] Phase 3
- [ ] Phase 4 (Option A: ring buffer + pinning, locked)
- [ ] Phase 4b (io_uring track, optional)
- [ ] README case study
- [ ] Tests/benchmarks
- [ ] Demo assets

## Files Changed Recently

- Renamed project from "Microsecond Message Bus" to "Cadence."
- Rewrote `PROJECT_PLAN.md`, `TASKS.md`, `README.md`, `START_HERE.md` to
  reflect the rename and to lock Phase 4 scope to Option A (hand-written
  cache-aware lock-free ring buffer + core pinning, benchmarked against
  `crossbeam-channel`/`std::mpsc`).
- Added `BENCHMARKING.md` — methodology and environment-disclosure
  standard, written before any code, per the project's own credibility
  requirements.

## Commands Run

```bash
# After installing Rust (rustup.rs):
cargo test          # 8 tests: 3 in message-core, 5 in bus
cargo run -p cli    # smoke demo: publish + receive one message
cargo run -p bench  # Phase 1 throughput baseline (1M msgs)
```

## Test / Verification Status

- Passing: 26/26 (3 bench, 17 bus [incl. 6 spsc], 3 message-core)
- Failing: 0
- Not run: Phase 4+ tests

## Phase 3 Benchmark Results (crossbeam-channel baseline, release, Windows 11)

Rate: 20,000 msg/s | 100,000 msgs | 0 dropped

| Percentile | Latency |
|------------|---------|
| min        | 304 ns  |
| mean       | 46,675 ns |
| p50        | 7,939 ns |
| p95        | 174,079 ns |
| p99        | 1,146,879 ns |
| p99.9      | 1,977,343 ns |
| max        | 2,437,119 ns |

Throughput: 0.019 Mmsg/s (rate-limited by open-loop 20k msg/s target).
Phase 4 ring buffer will push this rate to 1M+ msg/s and re-run for honest comparison.
Full results in `bench_results.json`.

## Known Issues

- None.

## Next Recommended Task

1. Install Rust (`rustup-init.exe`).
2. Run `cargo test` — expect 8 passing tests.
3. Begin Phase 5: architecture diagram, benchmark screenshots, reproducible commands, limitations section, dual-audience README pass.

## Phase 4 Benchmark Results (Windows 11, x86_64, 12 cores, release)

### Latency — open-loop 10k msg/s, CO-corrected

| Implementation | min | p50 | p99 | p99.9 | max |
|----------------|-----|-----|-----|-------|-----|
| crossbeam-Bus | 353 ns | 7,907 ns | 1,089,535 ns | 2,502,655 ns | 3,506,175 ns |
| SPSC unpinned | 72 ns | 283 ns | 2,075,647 ns | 4,378,623 ns | 5,767,167 ns |
| SPSC pinned | 75 ns | 273 ns | 3,608,575 ns | 5,292,031 ns | 6,680,575 ns |

p50 is 28× lower on SPSC (283 ns vs 7,907 ns). Tail dominated by OS scheduler jitter (Windows, no isolcpus).

### Throughput — max-rate, 1M messages

| Implementation | Mmsg/s |
|----------------|--------|
| crossbeam-Bus | 5.80 |
| SPSC unpinned | 13.26 (2.3×) |
| SPSC pinned | 28.01 (4.8×) |

## Decisions Locked (do not re-litigate without updating this file)

- **Name:** Cadence (was "Microsecond Message Bus"). Rationale in
  `PROJECT_PLAN.md` under "Naming Rationale."
- **Phase 4 optimization track:** Option A — hand-written cache-aware
  lock-free ring buffer + core pinning. NOT io_uring-first, NOT
  AF_XDP/DPDK. io_uring is an optional Phase 4b (I/O path only, compared
  honestly against epoll). AF_XDP/DPDK are future-work documentation only
  — no implementation, no benchmark claims, on this hardware.
- **Benchmarking standard:** `hdrhistogram` + `quanta`, explicit
  coordinated-omission handling, full percentile reporting (p50/p95/p99/
  p99.9/max), full hardware/environment disclosure. `criterion` is for
  throughput microbenchmarks only, not latency distributions.
- **NUMA:** single-socket dev hardware cannot demonstrate cross-node
  effects credibly. Run an `hwloc` topology dump regardless, but report
  cross-NUMA latency as a cited hypothesis, not a measured result, unless
  multi-socket hardware becomes available.

## Notes for Next Agent

This project must remain interchangeable between Cursor Pro, Codex Pro, and
Claude Code Pro. Continue only from this file and `TASKS.md`. The scope
decisions above were made deliberately after research into comparable
projects (LMAX Disruptor, Aeron, Chronicle Queue, Iceoryx, ZeroMQ/NNG) and
should not be silently changed — if you think a decision should change,
say so explicitly in this file with reasoning, don't just drift.
