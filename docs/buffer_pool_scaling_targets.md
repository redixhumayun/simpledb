# Buffer Pool Scaling Target

Primary workload: `Phase5/Concurrent Pin` with `replacement_clock`.

Why:

- threads touch disjoint resident blocks
- pool size `4096` avoids eviction noise
- it isolates shared resident-hit overhead inside the buffer pool

## Harnesses

Use two harnesses:

- Criterion bench in [benches/buffer_pool.rs](/home/ci/worktree-buffer-pool-opt/benches/buffer_pool.rs:420)
  - use for scaling numbers and charts
  - use to answer: did throughput improve?
- standalone profiling binary in [profile_buffer_pool_pin.rs](/home/ci/worktree-buffer-pool-opt/src/bin/profile_buffer_pool_pin.rs:1)
  - use for `perf` / flamegraphs
  - use to answer: where is hit-path time going?

## Scaling Runs

```bash
SIMPLEDB_BENCH_BUFFERS=4096 cargo bench --bench buffer_pool \
  --no-default-features --features replacement_clock --features page-4k \
  -- --output-format bencher --noplot "Phase5/Concurrent Pin"
```

Caveat:

- treat `128+` threads cautiously; `PIN_TOTAL_OPS=10_000` makes per-thread work too small there

## Scaling Charts

```bash
uv run python scripts/bench/run_buffer_pool.py \
  --platform <platform-key> \
  --title "<title>" \
  --environment "<environment>" \
  --num-buffers 4096 \
  --policies replacement_clock \
  --page-size page-4k

uv run python scripts/bench/generate_scaling_charts.py --platform <platform-key>
```

Outputs:

- raw benchmark JSON: `docs/benchmarks/replacement_policies/raw/<platform-key>/`
- charts: `docs/benchmarks/charts/<platform-key>/`

Keep at minimum:

- `metadata.json`
- `replacement_clock.json`
- `pin_scaling.png`

## Profiling Runs

Build:

```bash
cargo build --release --bin profile_buffer_pool_pin \
  --no-default-features --features replacement_clock --features page-4k
```

Record:

```bash
OUT=results/profiles/buffer_pool_clock_scaling_$(date +%Y%m%d)
mkdir -p "$OUT"

perf record -e cpu-clock -F 199 -g -o "$OUT/pin_profile_bin_t4_perf.data" -- \
  target/release/profile_buffer_pool_pin \
  --threads 4 --duration-secs 20 --buffers 4096 --blocks-per-thread 10

perf record -e cpu-clock -F 199 -g -o "$OUT/pin_profile_bin_t8_perf.data" -- \
  target/release/profile_buffer_pool_pin \
  --threads 8 --duration-secs 20 --buffers 4096 --blocks-per-thread 10
```

Report:

```bash
perf script -i "$OUT/pin_profile_bin_t4_perf.data" | /home/ci/FlameGraph/stackcollapse-perf.pl > "$OUT/pin_profile_bin_t4_perf.folded"
/home/ci/FlameGraph/flamegraph.pl "$OUT/pin_profile_bin_t4_perf.folded" > "$OUT/pin_profile_bin_t4_flamegraph.svg"
perf report --stdio -i "$OUT/pin_profile_bin_t4_perf.data" > "$OUT/pin_profile_bin_t4_perf_report.txt"

perf script -i "$OUT/pin_profile_bin_t8_perf.data" | /home/ci/FlameGraph/stackcollapse-perf.pl > "$OUT/pin_profile_bin_t8_perf.folded"
/home/ci/FlameGraph/flamegraph.pl "$OUT/pin_profile_bin_t8_perf.folded" > "$OUT/pin_profile_bin_t8_flamegraph.svg"
perf report --stdio -i "$OUT/pin_profile_bin_t8_perf.data" > "$OUT/pin_profile_bin_t8_perf_report.txt"
```

Write summary:

- `results/profiles/buffer_pool_clock_scaling_<date>/SUMMARY.md`

## Current Design

Current direction:

- global directory with OCC-style frame validation
- no block-latch participation on the resident-hit path
- hot per-frame state split out of `FrameMeta`

Current state in code:

- directory
  - global `Mutex<HashMap<BlockId, DirectoryEntry>>`
  - `DirectoryEntry::{Installing, Resident { frame_idx, generation }}`
- hot per-frame state
  - atomic `pin_count`
  - atomic control word for `generation/loading/evicting`
  - atomic `ref_bit` for Clock/SIEVE
- cold per-frame state
  - `FrameMeta` still owns:
    - resident `BlockId`
    - flush/writeback state
    - list links for LRU/SIEVE
- hit bookkeeping
  - Clock and SIEVE record hits through `ref_bit`
  - LRU now records hit-time promotion again
  - `pin_fast()` now rolls back speculative pins if LRU hit bookkeeping would block

## Completed

- removed `latch_shards`
- removed per-block latch usage on resident hits
- removed `resident_shards`
- added directory `Installing` / `Resident { frame_idx, generation }` protocol
- added frame-local OCC validation for resident hits
- split hot hit-path state out of `FrameMeta`
- restored policy-specific hit semantics
  - `pin()` performs blocking hit bookkeeping
  - `pin_fast()` performs nonblocking hit bookkeeping or returns `Contended`
- added standalone profiling binary for resident-hit profiling

## Pending

- remove remaining `FrameMeta` lock pressure from `0 -> 1` and `1 -> 0` pin transitions
  - clean-slack accounting still lives under `FrameMeta`
  - dirty-queue eligibility still lives under `FrameMeta`
- remove blunt `notify_all()` behavior on every last unpin
- rerun scaling and profiling with the new implementation
  - determine whether the next bottleneck is:
    - global directory mutex
    - `FrameMeta` work on last-pin transitions
    - waiter wakeups
- if directory becomes the next bottleneck:
  - shard it first
  - then consider `Mutex -> RwLock`
- if miss/eviction contention becomes important later:
  - consider partitioned Clock hands / eviction domains

## Immediate Next Step

Highest-value next step:

- make `0 -> 1` and `1 -> 0` pin transitions cheaper by shrinking the amount of work that still requires `FrameMeta`
