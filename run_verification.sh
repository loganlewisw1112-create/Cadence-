#!/usr/bin/env bash
# Cadence — Test T (tail latency) + Test D (disk write) driver.
# See VERIFICATION_PLAN.md. Run on bare-metal Linux for meaningful numbers.
#
# Usage:
#   ./run_verification.sh                 # run both, no privileged tuning
#   CORES=2,3 ./run_verification.sh       # pin latency test to isolated cores
#   ./run_verification.sh --tune          # also set governor/turbo (needs sudo)
#   TEST=tail ./run_verification.sh       # only Test T   (TEST=disk for only D)
#
# Env knobs passed through to the binaries:
#   CADENCE_RATES=10000,100000,1000000  CADENCE_TRIALS=10   (tail)
#   CADENCE_MSGS=200000  CADENCE_TRIALS=5                    (disk)
set -euo pipefail

CORES="${CORES:-}"                 # e.g. "2,3" — the isolcpus set. Empty = no pinning.
DEVICE="${CADENCE_DEVICE:-}"       # e.g. /dev/nvme0n1 — recorded into the JSON.
FS="${CADENCE_FS:-}"               # e.g. ext4
TEST="${TEST:-both}"
TUNE=0
[[ "${1:-}" == "--tune" ]] && TUNE=1

cd "$(dirname "$0")"

# ── Environment disclosure stamped into results (BENCHMARKING.md §3) ────────────
export CADENCE_RUSTC="$(rustc --version 2>/dev/null || echo unknown)"
export CADENCE_RUSTFLAGS="${RUSTFLAGS:-"-C target-cpu=native"}"
export RUSTFLAGS="$CADENCE_RUSTFLAGS"
[[ -n "$DEVICE" ]] && export CADENCE_DEVICE="$DEVICE"
[[ -n "$FS" ]] && export CADENCE_FS="$FS"

echo "rustc:     $CADENCE_RUSTC"
echo "rustflags: $CADENCE_RUSTFLAGS"
echo "cores:     ${CORES:-<none, not pinned>}"
echo "device:    ${DEVICE:-<unset — set CADENCE_DEVICE for a real disk number>}"
echo

# ── Optional privileged tuning (opt-in, your machine) ───────────────────────────
if [[ "$TUNE" == "1" ]]; then
  echo "== tuning (sudo) =="
  sudo cpupower frequency-set -g performance || echo "  (cpupower unavailable)"
  if [[ -w /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
    echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo >/dev/null || true
  fi
  command -v lstopo >/dev/null && lstopo --of console > topology.txt && echo "  topology.txt written"
  echo
fi

echo "== build (release) =="
cargo build --release -p bench -p persist
echo

drop_caches() {
  if [[ -w /proc/sys/vm/drop_caches ]] || command -v sudo >/dev/null; then
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
  fi
}

run_tail() {
  echo "== Test T: tail latency =="
  if [[ -n "$CORES" ]]; then
    taskset -c "$CORES" ./target/release/tail_bench
  else
    echo "  (no CORES set — running unpinned; results indicative only)"
    ./target/release/tail_bench
  fi
  echo "  → tail_bench_results.json"
  echo
}

run_disk() {
  echo "== Test D: disk write =="
  drop_caches
  ./target/release/persist_verify
  echo "  → persist_verify_results.json"
  echo
}

case "$TEST" in
  tail) run_tail ;;
  disk) run_disk ;;
  both) run_tail; run_disk ;;
  *) echo "TEST must be tail|disk|both" >&2; exit 2 ;;
esac

echo "Done. Commit the *_results.json files alongside the code (BENCHMARKING.md §9)."
