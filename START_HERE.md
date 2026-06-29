# START HERE — Cadence

> Formerly "Microsecond Message Bus."

Paste this into Cursor Pro, Codex Pro, or Claude Code Pro:

I am building `Cadence`, a lock-free low-latency pub/sub message bus in
Rust, inside a local repo that must remain interchangeable between Cursor
Pro, Codex Pro, and Claude Code Pro.

Before editing, read:
- `../../AGENTS.md`
- `../../REPO_STANDARD.md`
- `../../TOOL_SWITCHING_GUIDE.md`
- `PROJECT_PLAN.md`
- `TASKS.md`
- `HANDOFF.md`
- `BENCHMARKING.md` (once it exists / once Phase 3 begins)

Then:
1. Summarize the current project goal.
2. Identify the current phase and next unchecked task.
3. List files you expect to change.
4. Implement only the next task/phase.
5. Add or update tests/verification.
6. Update `HANDOFF.md`.
7. Stop and summarize changed files.

Do not build future phases. Do not assume memory from another AI tool. The
local repo is the source of truth. In particular: the project name
(Cadence) and the Phase 4 optimization choice (hand-written ring buffer +
core pinning, not io_uring-first, not AF_XDP/DPDK) are locked decisions
documented in `HANDOFF.md` — don't silently revisit them.
