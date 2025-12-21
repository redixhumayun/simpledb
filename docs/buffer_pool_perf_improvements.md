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

## Implemented (done)
1) **Shard hot maps** (`latch_table`, `resident_table`)  
   - `const SHARDS: usize = 16;`  
   - `BlockId` → shard via FNV-1a; lock only the shard.  
   - `num_available` is `AtomicUsize`.

2) **Stop pruning latch entries on the hot path**  
   - Latches persist; no Drop-based cleanup on the pin path.

3) **Bench harness fixes for MT profiling**  
   - MT pin/hotset no longer spawn threads inside timed loops.  
   - Pin/hotset pool size overridden to 4096 to avoid eviction noise.

## Next steps (candidate optimizations)
1) **Faster `BlockId` hashing**  
   - Replace SipHash or pre-hash `BlockId` to cut hash cost on the hit path.

2) **Avoid `BlockId` cloning on hot path**  
   - Intern filename (`Arc<str>`), store precomputed hash, or avoid clone in map lookups.

3) **Reduce per-frame meta lock traffic**  
   - Use atomics for `pins`/`ref_bit` on hit path where safe.

4) **Resident map read optimization**  
   - RwLock for read-dominant path, or split lock for lookup vs cleanup.

5) **Clock hand contention**  
   - Atomic hand or sharded clock if eviction shows up in MT workloads.

6) **Per-file I/O locks (optional)**  
   - Replace global FS mutex with per-file locks if MT scans still flatline.
