# Verification Runbook — the two headline claims

Companion to `VERIFICATION_PLAN.md` (methodology/thresholds) and `BENCHMARKING.md`
(disclosure standard). This file is the **operator checklist**: the exact commands to
turn the two claims below from *projected/cited* into *measured*, on the only hardware
that can produce a valid number.

Two claims:
- **T — tail latency:** sub-µs p99 on isolated Linux cores.
- **D — disk write:** io_uring `WriteFixed` ≥ 1.5× std `BufWriter` at large batch.

## Status going in (verified 2026-09-17)

| Gate | Result | Where |
|---|---|---|
| `cargo test --all` | ✅ pass | Windows + Linux |
| io_uring code compiles (`io-uring 0.6.4`) | ✅ pass | Linux (WSL2, kernel 6.6) |
| `uring_writer_roundtrip`, `uring_fixed_writer_roundtrip` | ✅ pass | Linux |
| Test D headline (≥1.5×?) | ⏳ **not yet run on real hardware** | needs bare-metal, below |
| Test T headline (sub-µs p99?) | ⏳ **not yet run on real hardware** | needs bare-metal, below |

Directional-only note: on a WSL2 ext4 VHD (virtualized io_uring, no `drop_caches`,
no `O_DIRECT`, small counts), `WriteFixed` did **not** reach 1.5× std (best ~1.14×),
though it beat naive io_uring `Write` by ~2–4×. That is not the verdict — it is why the
runs below must happen on real, disclosed hardware. **WSL cannot produce either headline
number** (virtualized scheduler kills Test T; virtualized block layer kills Test D MB/s).

---

## Prerequisites (bare-metal Linux, not a VM, not shared cloud)

```bash
# toolchain
rustup toolchain install stable && rustc --version   # repo pins stable + rustfmt/clippy
# build the harness
RUSTFLAGS="-C target-cpu=native" cargo build --release -p bench
cargo test --all                                      # gate must be green first
```

---

## Test T — tail latency (needs isolated cores + a reboot)

### T.1 — one-time kernel boot config
Add to the kernel cmdline (GRUB), then **reboot**:
```
isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3
```

### T.2 — per-boot, before the run
```bash
sudo cpupower frequency-set -g performance
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo   # fix the clock
lscpu -e                                                          # confirm 2,3 physical, not HT twins
grep -o 'constant_tsc\|nonstop_tsc' /proc/cpuinfo | sort -u       # TSC must be invariant
lstopo --of console > topology.txt                               # commit this (NUMA disclosure)
```

### T.3 — run (rate sweep, ≥10 trials/rate)
```bash
CADENCE_RATES=10000,100000,500000,1000000,5000000 \
CADENCE_TRIALS=10 \
CADENCE_RUSTC="$(rustc --version)" CADENCE_RUSTFLAGS="-C target-cpu=native" \
  taskset -c 2,3 ./target/release/tail_bench
# emits tail_bench_results.json + latency_histogram.svg
```
Verify the harness prints a valid environment (not the `⚠ NOT a valid tail-latency
environment` banner). Confirm the resolved pinned core id it prints is actually 2/3.

### T.4 — verdict (from VERIFICATION_PLAN.md §T.5)
- p99 median < 1 µs at target rate **and** IQR < 25% of median → **T proven.**
- p99 median < 1 µs but IQR ≥ 25% → "sub-µs typical, tail unstable" (don't headline one number).
- p99 median ≥ 1 µs on isolated cores → **T falsified;** state the rate at which it *does* cross 1 µs.

---

## Test D — disk write / io_uring (needs a real block device)

### D.1 — environment
```bash
# a REAL block device + filesystem, NOT tmpfs/overlay/9p:
export CADENCE_DEVICE=/dev/nvme0n1   # the actual device the log lands on
export CADENCE_FS=ext4
# control the page cache between trials, or the number is RAM not disk:
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

### D.2 — run (all 3 writers, both durability modes, ≥5 trials)
```bash
CADENCE_MSGS=1000000 CADENCE_TRIALS=5 \
CADENCE_DEVICE=$CADENCE_DEVICE CADENCE_FS=$CADENCE_FS \
CADENCE_RUSTC="$(rustc --version)" \
  ./target/release/persist_verify
# emits persist_verify_results.json with ratio_vs_std per cell
```
`writer_kinds()` returns all three on Linux (std / io_uring-Write / io_uring-WriteFixed).
Sync-per-batch at `batch=1` is intentionally slow (one fsync per record) — expect it to
crawl; that is the durable-commit cost, not a bug.

### D.3 — verdict (from VERIFICATION_PLAN.md §D.5)
- WriteFixed ≥ 1.5× std at batch ≥ 64, **Mode B (sync-per-batch)**, median over trials → **D proven;** cite the exact ratio + device, not the 1.5–2.5× range.
- Gain only in Mode A (sync-end) → report as buffered-throughput gain; say it doesn't hold for durable commits.
- Ratio < 1.5× everywhere → **D falsified** for this workload → Future Work, honest per BENCHMARKING.md §8.
- First fix G4: confirm the std curve is monotonic non-decreasing in batch size before trusting any ratio.

---

## After a real run

1. Commit the JSON + `topology.txt` + regenerated `latency_histogram.svg`.
2. Update `README.md`:
   - **T proven:** change line ~46 "…would show sub-µs p99." → the measured p99 + rate + this run.
   - **D proven:** replace the ~1.5–2.5× *cited* sentence (line ~281) with the measured ratio + device + config.
   - **Either falsified:** move to Future Work with the measured number and the condition under which it *does* hold. Do not delete the negative result.
3. Re-run `cargo test --all` on that host and note it passed there.

One number per claim, bound to the run that produced it. A run before G1–G4 are closed
produces a number that looks like data and isn't.
