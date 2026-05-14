# Buffer Pool Scaling Handoff

Primary workload: `Phase5/Concurrent Pin` with `replacement_clock`.

This workload isolates resident-hit scalability:

- threads touch disjoint resident blocks
- `SIMPLEDB_BENCH_BUFFERS=4096` avoids eviction noise
- each worker repeatedly pins/unpins already-resident pages
- measured directory access is read-only after warmup

Use this workload to answer: "How much shared overhead remains in the resident-hit path?"

## Current Status

The branch has moved the resident-hit path through three major improvements:

- replaced the old resident/latch tables with a directory plus frame-local OCC validation
- fixed last-unpin wakeups so resident hits no longer call `notify_all`
- replaced the global directory mutex with a custom 64-shard `HashMap` directory

The current directory is logically:

```rust
BlockId -> DirectoryEntry
```

with:

```rust
DirectoryEntry::Installing
DirectoryEntry::Resident { frame_idx, generation }
```

Frame-local state still stores the reverse relation, current flush state, and replacement metadata.

## Current Design

Directory:

- `ShardedDirectory`
- 64 shards
- each shard is `Mutex<HashMap<BlockId, DirectoryEntry>>`
- shard selected by `BlockId` hash
- resident hit touches one shard
- miss/install/evict may touch old and new block shards at different points

Resident hit path:

- lookup `BlockId -> Resident { frame_idx, generation }`
- atomically validate frame generation/loading/evicting state
- atomically increment `pin_count`
- update policy hit bookkeeping
- return frame

Fast pin path:

- resident-only
- uses nonblocking shard lock
- returns `NotResident` for real misses
- returns `Contended` only when it would wait
- this distinction is required by B-tree latch-crabbing retry logic

Unpin path:

- atomically decrement `pin_count`
- last pin still enters `FrameMeta`
- dirty flush eligibility and clean-slack accounting still require `FrameMeta`
- free-buffer waiters are woken with `notify_one` only on exhausted-to-available transition

## Completed Work

- removed `latch_shards`
- removed resident-hit block latch usage
- removed `resident_shards`
- added directory `Installing` / `Resident { frame_idx, generation }` protocol
- added frame-local OCC validation for resident hits
- split hot hit-path state out of `FrameMeta`
- restored policy-specific hit semantics
- added nonblocking `pin_fast()` rollback for LRU hit bookkeeping contention
- fixed `pin_fast()` miss semantics so absent directory entries return `NotResident`
- added opt-in profiling counters behind `SIMPLEDB_BUFFER_POOL_PROFILE_LOCKS=1`
- changed last-unpin wakeup from unconditional `notify_all` to targeted `notify_one`
- tested DashMap as an A/B experiment
- replaced DashMap with custom std-only sharded directory
- generated pre-opt vs custom scaling chart:
  - `docs/benchmarks/charts/linux-dir-custom-20260514/pin_scaling_preopt_vs_custom.png`
  - `docs/benchmarks/charts/linux-dir-custom-20260514/pin_scaling_preopt_vs_custom.md`

## Validation So Far

Full required test matrix passed before the custom sharding work:

- `cargo build`
- `cargo test --no-default-features --features replacement_lru --features page-4k`
- `cargo test --no-default-features --features replacement_clock --features page-4k`
- `cargo test --no-default-features --features replacement_sieve --features page-4k`
- `cargo test --no-default-features --features replacement_lru --features page-4k --features direct-io`

After custom sharding, focused validation passed:

- `cargo fmt -- --check`
- `cargo build --release --bin profile_buffer_pool_pin --no-default-features --features replacement_clock --features page-4k`
- `cargo test --no-default-features --features replacement_lru --features page-4k planner_tests::test_planner_index_updates`

Before merging custom sharding, rerun the full test matrix.

## Performance Snapshot

Criterion `Phase5/Concurrent Pin`, `replacement_clock`, `page-4k`, `SIMPLEDB_BENCH_BUFFERS=4096`:

| Threads | Pre-opt baseline | Custom sharded | Speedup |
|---:|---:|---:|---:|
| 1 | 0.861M ops/s | 5.067M ops/s | 5.89x |
| 2 | 1.483M ops/s | 7.416M ops/s | 5.00x |
| 4 | 2.579M ops/s | 8.199M ops/s | 3.18x |
| 8 | 2.708M ops/s | 9.005M ops/s | 3.33x |
| 16 | 2.676M ops/s | 8.946M ops/s | 3.34x |
| 32 | 2.614M ops/s | 7.581M ops/s | 2.90x |
| 64 | 2.571M ops/s | 6.223M ops/s | 2.42x |
| 128 | 2.306M ops/s | 4.579M ops/s | 1.99x |
| 256 | 1.787M ops/s | 3.157M ops/s | 1.77x |

Interpretation:

- absolute throughput is much higher across the full range
- 8 threads already uses all logical CPUs on the current machine
- this machine is 4 physical cores / 8 hardware threads
- lack of improvement from 8t to 16t is expected because 16t is oversubscribed
- pre-opt had a gentler high-thread dropoff because earlier serialization capped throughput sooner

## Profiling Snapshot

Latest custom-sharded profiling run:

- `results/profiles/buffer_pool_clock_scaling_20260514-custom-sharded-ab-58888d9/SUMMARY.md`

Top costs after custom sharding:

| Threads | Top areas |
|---:|---|
| 4 | `BufferFrame::lock_meta`, `BufferManager::pin`, `ShardedDirectory::lock_shard`, `BufferManager::unpin` |
| 8 | `BufferFrame::lock_meta`, `BufferManager::pin`, `BufferManager::unpin`, `ShardedDirectory::lock_shard` |
| 16 | `BufferFrame::lock_meta`, `BufferManager::pin`, `BufferManager::unpin`, `ShardedDirectory::lock_shard` |

Directory futex/syscall contention is no longer the dominant profile shape.

The next bottleneck is `FrameMeta` work on pin/unpin. It is mostly uncontended in this disjoint-frame workload, but it is still paid on every operation.

## How To Reproduce Scaling

Use Criterion for throughput:

```bash
SIMPLEDB_BENCH_BUFFERS=4096 cargo bench --bench buffer_pool \
  --no-default-features --features replacement_clock --features page-4k \
  -- --output-format bencher --noplot "Phase5/Concurrent Pin"
```

Generate stock scaling charts from raw JSON:

```bash
uv run python scripts/bench/run_buffer_pool.py \
  --platform <platform-key> \
  --title "<title>" \
  --environment "<environment>" \
  --num-buffers 4096 \
  --policies replacement_clock \
  --page-size page-4k \
  --skip-text

uv run python scripts/bench/generate_scaling_charts.py --platform <platform-key>
```

Caveat:

- treat `128+` threads cautiously; `PIN_TOTAL_OPS=10_000` makes per-thread work very small

## How To Reproduce Profiling

Build:

```bash
cargo build --release --bin profile_buffer_pool_pin \
  --no-default-features --features replacement_clock --features page-4k
```

Record:

```bash
OUT=results/profiles/buffer_pool_clock_scaling_<run-key>
mkdir -p "$OUT"

for t in 4 8 16; do
  SIMPLEDB_BUFFER_POOL_PROFILE_LOCKS=1 perf record -e cpu-clock -F 199 -g \
    -o "$OUT/pin_profile_bin_t${t}_perf.data" -- \
    target/release/profile_buffer_pool_pin \
    --threads "$t" --duration-secs 20 --buffers 4096 --blocks-per-thread 10 \
    > "$OUT/pin_profile_bin_t${t}.log" \
    2> "$OUT/pin_profile_bin_t${t}_perf.stderr"
done
```

Report:

```bash
for t in 4 8 16; do
  perf script -i "$OUT/pin_profile_bin_t${t}_perf.data" \
    | /home/ci/FlameGraph/stackcollapse-perf.pl \
    > "$OUT/pin_profile_bin_t${t}_perf.folded"

  /home/ci/FlameGraph/flamegraph.pl \
    "$OUT/pin_profile_bin_t${t}_perf.folded" \
    > "$OUT/pin_profile_bin_t${t}_flamegraph.svg"

  perf report --stdio -i "$OUT/pin_profile_bin_t${t}_perf.data" \
    > "$OUT/pin_profile_bin_t${t}_perf_report.txt"
done
```

Write:

- `$OUT/SUMMARY.md`

## Remaining Work

Immediate:

- rerun the full required test matrix after custom sharding
- run `cargo clippy -- -D warnings`
- decide whether to keep or remove profiling counters after the next optimization pass

Next optimization target:

- remove `FrameMeta` from the uncontended resident-hit pin/unpin fast path

Likely approach:

- avoid locking `FrameMeta` for ordinary `0 -> 1` clean resident hits where possible
- avoid locking `FrameMeta` for ordinary non-dirty last unpins where possible
- keep dirty/writeback/flush eligibility under explicit metadata coordination
- preserve `num_available` and clean-slack accounting invariants

Risks:

- dirty frames still need correct queueing when the last pin drops
- eviction must not claim frames in writeback/loading/evicting states
- `num_available` must not diverge from claimable frame reality
- B-tree fast-pin callers rely on exact `NotResident` vs `Contended` behavior

Later:

- profile mixed miss/eviction workloads, not only resident hits
- evaluate whether directory shard count should be configurable or derived from CPU count
- consider partitioned replacement policy state only if eviction paths become hot
