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

## Compact Chart Deck Plan (Direct vs Buffered)
Goal: produce a small, shareable chart set that emphasizes decision-relevant direct-vs-buffered behavior without adding framework complexity.

### Scope
1. Use existing regime-matrix JSON artifacts (`buffered_<regime>.json`, `direct_<regime>.json`).
2. Build charts from JSON only (do not parse markdown reports).
3. Start with fixed benchmark allowlist (no dynamic categorization in v1).

### Benchmarks to chart (11 rows)
1. Core read/write path:
   - `Sequential Read`
   - `Random Read`
   - `Sequential Write`
   - `Random Write`
2. Durability path:
   - `Random Write durability immediate-fsync data-nosync`
   - `Random Write durability immediate-fsync data-fsync`
3. Cache-adverse path:
   - `One-pass Seq Scan`
   - `Low-locality Rand Read`
   - `One-pass Seq Scan+Evict`
   - `Low-locality Rand Read+Evict`
   - `Multi-stream Scan`

### Chart structure
1. Per-regime grouped bar charts:
   - Y-axis: latency (ms, lower is better)
   - X-axis: benchmark names
   - Two bars per benchmark: `buffered`, `direct`

### Output location
1. Write chart images under `docs/benchmarks/charts/io_mode/`.
2. Keep naming deterministic, for example:
   - `io_mode_hot.png`
   - `io_mode_pressure.png`
   - `io_mode_thrash.png`

### Rendering command
1. Generate charts from a completed regime-matrix results directory:
   ```bash
   uv run python scripts/bench/generate_io_mode_charts.py \
     results/regime_capped_YYYYMMDD_HHMMSS
   ```

### Non-goals (v1)
1. No thread/concurrency sweep charts.
2. No WAL-focused charts (WAL path remains buffered by design).
3. No CLI knob expansion beyond selecting input results directory.

## Experiment: 1 GiB cgroup Memory Limit (Linux)
Goal: evaluate direct-vs-buffered behavior under explicit memory pressure without creating very large datasets.

### Why
1. Machine RAM is large (for example 64 GiB), so capped working sets may stay cache-friendly.
2. A cgroup memory cap creates a smaller effective memory budget (including page cache) for the benchmark process.
3. This should make `pressure`/`thrash` regimes more representative of constrained deployments.

### Setup (one-time per session)
```bash
sudo mkdir -p /sys/fs/cgroup/simpledb-io
echo $((1024*1024*1024)) | sudo tee /sys/fs/cgroup/simpledb-io/memory.max
echo 0 | sudo tee /sys/fs/cgroup/simpledb-io/memory.swap.max
```

### Run benchmark matrix inside cgroup
```bash
sudo systemd-run --scope -p MemoryMax=1G -p MemorySwapMax=0 \
  python3 scripts/run_regime_matrix.py 10 results/regime_capped_$(date +%Y%m%d_%H%M%S)_1g \
  --profile capped
```

If faster turnaround is needed, pin explicit ops:
```bash
sudo systemd-run --scope -p MemoryMax=1G -p MemorySwapMax=0 \
  python3 scripts/run_regime_matrix.py 3 results/regime_capped_$(date +%Y%m%d_%H%M%S)_1g_quick \
  --profile capped --phase1-ops 1000 --mixed-ops 500 --durability-ops 1000
```

### Generate compact charts
```bash
uv run python scripts/bench/generate_io_mode_charts.py \
  results/regime_capped_YYYYMMDD_HHMMSS_1g
```

### Compare against uncapped baseline
1. Use the same command/ops/iterations with and without cgroup limit.
2. Compare `compare_hot.md`, `compare_pressure.md`, `compare_thrash.md` pairs.
3. Focus on:
   - Phase 6 durability rows (`data-fsync`, `data-nosync`)
   - Phase 7/8 cache-adverse rows
   - Read-heavy Phase 1 rows for gap narrowing under pressure.

### Expected behavior
1. `hot` may remain similar.
2. `pressure` and `thrash` should slow down and show higher variance.
3. Direct may become more competitive, but broad wins are not guaranteed.

### Cleanup
```bash
sudo rmdir /sys/fs/cgroup/simpledb-io 2>/dev/null || true
```
