# Direct I/O Performance Testing Notes

## Purpose
This doc summarizes how to interpret direct-vs-buffered results from `benches/io_patterns.rs`, why regime differences may look muted, and what to change for stronger signal.

## Current Findings (from `results/`)
1. Buffered wins most read-heavy and mixed no-fsync paths.
2. Direct I/O consistently wins `Random Write durability immediate-fsync data-fsync` (about 20-22%).
3. WAL-only benchmarks are mostly neutral, expected since WAL remains buffered.

## Why This Happens
1. Buffered path benefits from OS page cache on cache-friendly patterns.
2. Direct path bypasses page cache, so it loses when reuse is high and durability pressure is low.
3. Direct path in current implementation also pays extra per-op overhead (aligned buffer alloc/copy).

## Workload/Regime Caveats

### Truly regime-independent phase
1. Phase 5 (Concurrent I/O stress) does not scale with `working_set`.
2. It uses fixed block counts (`num_threads * 100` or `blocks_per_file = 100`) independent of regime.

### Weakly regime-sensitive phases
1. Phase 1 (seq/rand): `working_set` changes index range, but touched pages per run are capped by fixed `phase1_ops`.
2. Phase 4 (mixed): same issue with fixed `mixed_ops`.
3. Phase 6 (durability): same issue with fixed `durability_ops`.
4. Random/index sequences are pre-generated once and reused across iterations in several phases, reducing regime perturbation.

Net: direct-vs-buffered deltas are valid for this harness shape, but hot/pressure/thrash conclusions are weaker than they appear.

## Is `buffer_pool` Better for Direct-vs-Buffered?
No, not as primary evidence.
1. `buffer_pool` mainly measures replacement/latching/pin-unpin behavior.
2. Many cases are hit-heavy and keep I/O mode effects secondary.
3. Use `buffer_pool` as secondary context, not primary direct-vs-buffered proof.

## What Is Cache-Adverse?
1. One-pass scan over dataset much larger than RAM.
2. Random access with reuse distance beyond effective page-cache capacity.
3. Multi-stream scans on different files causing cache interference.
4. Read-mostly workload plus sustained heavy writes (writeback pressure).
5. Frequent durability barriers (`fsync`/`fdatasync`) where page cache adds overhead.

## How To Get Cache-Adverse Signal Without 64GB+ Files Every Run
1. Reuse-distance method:
   - Moderate file sizes (for example 8-16 GiB), but low temporal locality per timed run.
   - Re-randomize per iteration instead of reusing fixed sequences.
2. Multi-file interference:
   - 2-4 concurrent workers on different files; aggregate footprint exceeds effective cache.
3. Eviction controls between iterations (Linux):
   - `posix_fadvise(..., DONTNEED)` or controlled cache drop in dedicated runs.
4. Fewer iterations, more ops/iteration for big-I/O tests.
5. Keep durability stress cases; they are where direct wins are most plausible.

## Recommended Testing Strategy
1. Keep `io_patterns` as primary direct-vs-buffered harness.
2. Make regime effects real by:
   - Scaling touched unique pages with regime,
   - Re-randomizing access sequences each iteration,
   - Making concurrent phase regime-aware,
   - Optionally adding cache-eviction variants.
3. Keep `buffer_pool` as secondary system-level context.

## Decision Guidance
1. Do not generalize direct-I/O default decisions from cache-friendly microbenchmarks.
2. Prioritize decisions based on:
   - durability-heavy workloads,
   - cache-adverse workloads,
   - target hardware/filesystem.
3. Promote direct I/O defaults only if wins are broad on real workload shapes.

## Concrete Changes Proposed
1. Make Phase 5 regime-aware (`benches/io_patterns.rs`):
   - Replace fixed concurrent footprints (`num_threads * 100`, `blocks_per_file = 100`) with values derived from `working_set_blocks`.
   - Keep configuration profile-driven; avoid adding new per-phase CLI knobs.

2. Stop reusing identical random/index sequences across iterations (`benches/io_patterns.rs`):
   - Affected phases: Phase 1 random, Phase 4 mixed, Phase 6 durability.
   - Generate sequences per iteration (or deterministic per-iteration seeds).

3. Scale touched unique pages per iteration with regime (`benches/io_patterns.rs`):
   - Derive unique-touch targets internally from regime/profile constants.
   - Ensure `hot/pressure/thrash` materially changes reuse distance, not just modulo range.

4. Add explicit cache-adverse variants (`benches/io_patterns.rs`):
   - New cases: one-pass sequential scan, low-locality random read/write, multi-stream scan interference.
   - Keep existing benchmarks; do not replace current cases.

5. Add optional cache-hygiene controls for Linux:
   - Prefer file-scoped eviction (`posix_fadvise(..., DONTNEED)`) between iterations for selected runs.
   - Keep this as an optional targeted benchmark variant, not a separate matrix profile.

6. Extend matrix runner profiles (`scripts/run_regime_matrix.py`):
   - Keep `capped` as quick gate.
   - Keep `heavy` as pre-merge/signoff profile.

7. Update benchmark docs (`benches/README.md` and this doc):
   - Keep CLI surface minimal; put tuning in profile presets.
   - Document profile intent:
     - `capped` = quick guardrail
     - `heavy` = decision-grade signal.

## Minimal Profiling Toolkit (Buffered vs Direct)
Goal: collect enough signal to explain behavior differences without over-instrumenting.

### Minimum tools
1. `iostat -x 1`
   - Device-level throughput/latency/utilization (`rkB/s`, `wkB/s`, `await`, `aqu-sz`, `%util`).
2. `pidstat -d -u -r -h 1 -p <bench_pid>`
   - Per-process I/O, CPU, memory/faults.
3. `vmstat 1`
   - System memory/writeback pressure (`wa`, `bi`, `bo`) to interpret buffered-path effects.
4. Benchmark run metadata (`run_manifest.json` + compare markdown headers)
   - Exact commands, iterations, ops, profile, cgroup context, features.

### Why `iostat` alone is not enough
1. `iostat` is device-aggregate only; it does not attribute I/O to your benchmark PID.
2. Buffered I/O can be served from page cache (app busy, disk quiet), then flushed later by kernel threads.
3. Journal/writeback activity can dominate `await/%util` while not appearing as direct app syscalls.

### Shared-device caveat (even on benchmark-only machines)
1. Kernel flusher threads and filesystem journal I/O are still separate actors.
2. This I/O is workload-induced, but attribution differs from process-issued I/O.
3. Pairing device + process + memory/writeback views avoids false conclusions.

### Capture pattern
1. Start samplers before launching the benchmark:
   - `iostat -x 1 > iostat.log`
   - `vmstat 1 > vmstat.log`
2. Launch benchmark, grab PID, then:
   - `pidstat -d -u -r -h 1 -p <pid> > pidstat.log`
3. Stop samplers immediately after benchmark exit.
4. Store logs under the same run directory as result JSON/markdown artifacts.

### Profiling workflow (recommended)
1. Profile one benchmark case at a time (single benchmark row + single regime).
2. For each case, run buffered and direct as two separate runs with identical args.
3. Capture `iostat`/`pidstat`/`vmstat` only during that run window.
4. Repeat each buffered/direct pair 3-5 times; compare medians instead of single runs.
5. Move to the next benchmark case only after collecting a complete pair set.

### Case selection order
1. Phase 6 durability rows first (`data-fsync`, `data-nosync`).
2. Cache-adverse rows next (one-pass, low-locality, multi-stream).
3. A few simple Phase 1 rows as control/sanity checks.
