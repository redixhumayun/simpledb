# Buffer Pool Perf Improvements (Dec 15, 2025)

## Observations from flamegraphs
- 1 thread (`flamegraph_pin_t1.svg`):
  - `BufferManager::pin` ~37% of samples.
  - `LatchTableGuard` drop/cleanup ~10.5%.
  - LRU `record_hit` ~6.5%.
  - Hashing (`BuildHasher::hash_one`) ~8.3%.
  - Futex/syscalls minimal → little contention; cost is hit-path bookkeeping.
- 256 threads (`flamegraph_pin_t256.svg`):
  - `BufferManager::pin` ~77%.
  - `LatchTableGuard` drop ~32.5%.
  - Futex/syscall stacks dominate (heavy mutex blocking).
  - LRU `evict_frame` ~7.2% (secondary; churn from working-set >> 12 frame pool).
  - Hashing negligible; contention is the limiter.

## Bottlenecks confirmed
- Global mutexes on the pin path (`latch_table`, `resident_table`, `num_available`, policy list) drive futex time at high thread counts.
- Latch table churn (create/prune per pin) is hot; cleanup in `Drop` scales poorly.
- Single LRU list mutex serializes hits; with 256 threads it is overshadowed by mutex wait time.
- Eviction work exists but is not primary; locking dominates.

## Concrete, crate-free fixes
1) **Shard hot maps** (`latch_table`, `resident_table`)  
   - `const SHARDS: usize = 16;`  
   - Replace each global map with `[Mutex<HashMap<...>>; SHARDS]`.  
   - Hash `BlockId` to a shard (simple FNV64, power-of-two mask).  
   - Lock only the shard on lookup/insert; store `Weak` frames as before.  
   - Make `num_available` an `AtomicUsize` to remove another mutex.

2) **Stop pruning latch entries on the hot path**  
   - In `LatchTableGuard::drop`, drop the `Arc::strong_count` check and map removal.  
   - Keep one `Arc<Mutex<()>>` per `BlockId` (stable per-block latch objects).  
   - Optional later: background sweep to remove entries with `strong_count == 1`, but never in the pin fast path.

3) **Reduce policy lock contention**  
   - Easiest: run clock by default for MT workloads (single lightweight hand mutex).  
   - If keeping LRU: shard the intrusive list—`SHARDS` lists, frames assigned by `index % SHARDS`; `record_hit`/`on_frame_assigned` touch one shard; `evict_frame` round-robins shards scanning their tails. Approximates global LRU but removes the single global list lock.

4) **Optional file I/O lock narrowing**  
   - Replace the single FS mutex with per-file mutexes (store `(File, Mutex<()>)` per filename); MT scans then contend only on the file they touch, not a global gate.

## Expected effect
- Sharding + no pruning should collapse futex/syscall stacks in the 256-thread flamegraph; `LatchTableGuard` drops should disappear from the top.  
- Atomic `num_available` removes a contended mutex pair in pin/unpin.  
- Clock or sharded LRU should prevent the policy mutex from serializing hit traffic.  
- Per-file locks help MT sequential scans; not critical for the pin microbench but useful for Phase 2 workloads.

## Implementation order
1) Remove latch pruning + make `num_available` atomic.  
2) Shard latch/resident tables.  
3) Switch default policy to clock (or shard LRU).  
4) Per-file mutex refactor if MT scans still flatline.
