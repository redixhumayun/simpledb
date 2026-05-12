# Buffer Pool Scaling Target

Primary workload: `Phase5/Concurrent Pin` with `replacement_clock`.

Why:

- threads touch disjoint resident blocks
- pool size `4096` avoids eviction noise
- it isolates shared hit-path overhead inside the buffer pool

Use two harnesses:

- Criterion bench in [benches/buffer_pool.rs](/home/ci/worktree-buffer-pool-opt/benches/buffer_pool.rs:420)
  - use this for scaling numbers and charts
  - keep using this to compare throughput as the implementation changes
- standalone profiling binary in [profile_buffer_pool_pin.rs](/home/ci/worktree-buffer-pool-opt/src/bin/profile_buffer_pool_pin.rs:1)
  - use this for `perf` / flamegraphs
  - it avoids Criterion’s per-iteration barriers, so attribution is cleaner

## Scaling Runs

Use the Criterion harness for scaling data:

```bash
SIMPLEDB_BENCH_BUFFERS=4096 cargo bench --bench buffer_pool \
  --no-default-features --features replacement_clock --features page-4k \
  -- --output-format bencher --noplot "Phase5/Concurrent Pin"
```

Important caveat:

- treat `128+` threads cautiously; `PIN_TOTAL_OPS=10_000` makes per-thread work too small there

## Scaling Charts

Canonical chart pipeline:

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

At minimum, keep:

- `metadata.json`
- `replacement_clock.json`
- `pin_scaling.png`

## Profiling Runs

Build the standalone profiling binary:

```bash
cargo build --release --bin profile_buffer_pool_pin \
  --no-default-features --features replacement_clock --features page-4k
```

Record `perf` for the current scaling points of interest:

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

Generate flamegraphs and text reports:

```bash
perf script -i "$OUT/pin_profile_bin_t4_perf.data" | /home/ci/FlameGraph/stackcollapse-perf.pl > "$OUT/pin_profile_bin_t4_perf.folded"
/home/ci/FlameGraph/flamegraph.pl "$OUT/pin_profile_bin_t4_perf.folded" > "$OUT/pin_profile_bin_t4_flamegraph.svg"
perf report --stdio -i "$OUT/pin_profile_bin_t4_perf.data" > "$OUT/pin_profile_bin_t4_perf_report.txt"

perf script -i "$OUT/pin_profile_bin_t8_perf.data" | /home/ci/FlameGraph/stackcollapse-perf.pl > "$OUT/pin_profile_bin_t8_perf.folded"
/home/ci/FlameGraph/flamegraph.pl "$OUT/pin_profile_bin_t8_perf.folded" > "$OUT/pin_profile_bin_t8_flamegraph.svg"
perf report --stdio -i "$OUT/pin_profile_bin_t8_perf.data" > "$OUT/pin_profile_bin_t8_perf_report.txt"
```

Write a short summary in the same directory:

- `results/profiles/buffer_pool_clock_scaling_<date>/SUMMARY.md`

## Current Working Rule

- use the Criterion harness to answer “did throughput improve?”
- use the standalone profiling binary to answer “where is the hit-path time going?”

## Paths To Design

Discuss the buffer-pool protocol by path, not as one monolithic algorithm:

- Resident hit path
  - block already resident
  - no eviction
  - no I/O
  - primary scaling path

- Resident hit under same-page contention
  - multiple threads hit the same resident frame/block
  - defines what local serialization remains acceptable

- Miss / install path
  - block not resident
  - only one installer should win for a given block

- Eviction path
  - victim selection
  - pinned/writeback frames must be skipped
  - Clock hand logic belongs here

- Dirty / writeback path
  - mark dirty
  - background flush
  - writeback completion / requeue rules

- Unpin / waiter wakeup path
  - pin count drops
  - decide whether any waiter actually needs waking

- Directory publish / unpublish path
  - install/remove `BlockId -> frame`
  - generation/version validation rules live here

- Fast-path failure / retry path
  - resident lookup races with install/eviction
  - decide retry vs wait vs fallback

## Planned Progression

Current direction:

- move frame residency/pin state toward atomic words
- use directory lookup plus OCC-style frame validation
- remove block-latch participation from the resident-hit path

First implementation goal:

- get the protocol correct before optimizing the directory
- start with a single global directory
- allow the directory to return `(frame_idx, residency_generation)`
- validate frame identity/state after lookup
- retry on stale lookup races

Why start this way:

- easier to reason about correctness
- isolates the benefit of removing the current layered serialization points
- lets us measure how much improvement comes from the protocol itself

After correctness:

- rerun the existing scaling benchmark and profiling workflow
- if the global directory becomes the next bottleneck, shard it
- if reader serialization still matters, switch directory access from `Mutex` to `RwLock`
- if needed later, push further toward less locked directory reads

Working assumption:

- first milestone is not the final design
- first milestone is a correctness-first OCC protocol with fewer common-path serialization points

## Milestone 1 State

State to maintain:

- directory state
  - one global `Mutex<HashMap<BlockId, DirEntry>>`
  - `DirEntry` should support:
    - `Installing`
    - `Resident { frame_idx, generation }`
  - `Absent` is represented by no entry

- per-frame hot state
  - do not assume every field must live in one atomic word
  - split by update frequency to avoid unnecessary cache-line ping-pong
  - likely split:
    - hit-path atomic:
      - `pin_count`
      - `ref_bit`
    - residency/control atomic:
      - `loading`
      - `evicting`
      - `generation`
      - maybe `writeback`
  - goal:
    - hits mostly update hit-path state
    - install/evict mostly update residency/control state

- per-frame cold state
  - metadata not needed on every hit
  - likely includes:
    - current `BlockId`
    - flush metadata that does not need to live in the hot atomic word
  - can remain behind a small per-frame lock in the first milestone

- page bytes
  - keep `RwLock<Page>` as-is initially

- global/background state
  - keep existing dirty queue / flusher coordination initially
  - keep `num_available` / `clean_unpinned` initially

What should go away:

- `latch_shards`
- per-block latch usage on resident hits
- `resident_shards`
- `frame.meta` as the hit-path lock for residency validation, pin count, and `ref_bit`

What can stay in milestone 1:

- global directory `Mutex`
- per-frame cold metadata lock
- existing flusher machinery
- existing page latch

## State Machines

Define the protocol in terms of directory state and frame state.

### Directory States

- `Absent`
  - no entry for the block

- `Installing`
  - exactly one thread owns installation for this block
  - other missers must not install the same block again

- `Resident { frame_idx, generation }`
  - block is published as resident in a specific frame/generation
  - hit path must still validate frame-local state before pin succeeds

### Frame Residency / Control States

Think of each frame as moving through these coarse states:

- `Free`
  - frame has no published resident block

- `ResidentStable`
  - frame is resident and pinnable
  - hit path may validate and increment pin count

- `Evicting`
  - frame has been claimed for reuse
  - new hits must not pin it

- `Loading`
  - frame is being assigned a new block
  - new hits must not pin it

- `Writeback`
  - frame is in writeback for the current residency
  - eviction must not reuse it yet

`pin_count` is separate from these coarse states:

- `pin_count > 0` means not evictable
- `pin_count == 0` is required before eviction claim may succeed

### Allowed Transitions

Directory:

- `Absent -> Installing`
  - miss thread claims install ownership

- `Installing -> Resident { frame_idx, generation }`
  - installer publishes completed residency

- `Resident { ... } -> Absent`
  - old mapping is unpublished during reuse

Frame:

- `Free -> Loading`
  - installer claims free frame for a new block

- `ResidentStable -> Evicting`
  - eviction CAS succeeds only if frame is currently eligible

- `Evicting -> Loading`
  - old residency is gone; frame is now being filled with a new block

- `Loading -> ResidentStable`
  - new block is loaded and residency generation is ready to publish/use

- `ResidentStable -> Writeback`
  - flush protocol claims current residency for writeback

- `Writeback -> ResidentStable`
  - writeback completes and frame remains resident

Pin transitions:

- `ResidentStable + validate -> pin_count++`
- `unpin -> pin_count--`

### Core Invariants

- directory uniqueness
  - at most one directory entry exists per `BlockId`

- install exclusivity
  - at most one installer may own `Installing` for a given `BlockId`

- residency identity
  - `(frame_idx, generation)` identifies one specific residency assignment
  - generation must change when a frame is reused for a different block

- hit safety
  - no hit may succeed based on directory lookup alone
  - successful pin requires frame-local validation against `generation` and control state

- pin safety
  - once pin succeeds, the frame may not be reused until pin count drops again

- eviction safety
  - a frame may be reused only after eviction claim succeeds
  - pinned or writeback frames are not eligible for reuse

- publish safety
  - `Resident { frame_idx, generation }` may be visible in directory only for the matching residency assignment
  - stale directory observations must be detectable and force retry

- miss safety
  - while directory is `Installing`, no second installer may choose another victim for the same block

- transient inconsistency is allowed
  - directory and frame do not need one atomic combined update
  - any transient mismatch must be detectable and safe to retry

## Milestone 1 Algorithms

### Hit

1. lock directory
2. lookup `BlockId`
3. if `Absent`, unlock and go to miss path
4. if `Installing`, unlock and go to install-wait path
5. if `Resident { frame_idx, generation }`, copy entry and unlock
6. read frame control state
7. if frame generation mismatches, retry
8. if `loading` or `evicting`, retry
9. increment `pin_count`
10. re-read frame control state
11. if generation/state changed after pin, undo pin and retry
12. set `ref_bit`
13. return frame

Why the double-check:

- eviction or reuse may race between validation and pin increment
- successful pin must be validated after the increment as well

### Miss / Install

1. lock directory
2. lookup `BlockId`
3. if `Resident`, unlock and retry as hit
4. if `Installing`, unlock and go to install-wait path
5. if `Absent`, insert `Installing`
6. unlock directory
7. find victim frame
8. claim victim by CAS to `evicting=1`
9. if claim fails, retry victim selection
10. if dirty, flush before reuse
11. unpublish old resident mapping if it still matches expected `(frame, generation)`
12. transition frame to `loading`, bump generation
13. load new block into frame
14. lock directory
15. replace `Installing` with `Resident { frame_idx, generation }`
16. unlock directory
17. clear `loading` / `evicting`, make frame stable
18. increment installer pin count
19. set `ref_bit`
20. return frame

### Eviction

1. select candidate from Clock domain
2. read frame control state
3. skip if `pin_count > 0`
4. skip if `writeback`
5. CAS claim `evicting=1`
6. if CAS fails, retry another candidate
7. if dirty, flush
8. unpublish old resident mapping
9. reuse frame for install

### Install-Wait

Milestone 1 policy:

1. see `Installing`
2. drop lock
3. short backoff or `yield_now`
4. retry lookup

Later, if needed:

- condvar / waiter queue
- more targeted wakeup path

### Unpin

1. decrement `pin_count`
2. if pin count reaches zero and a waiter exists, wake selectively
3. do not `notify_all()` on the common path
