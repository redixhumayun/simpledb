# Buffer Pool Benchmark Plan

## Goals
- Measure buffer pool performance independent of replacement policy (FIFO/LRU/Clock/SIEVE)
- Establish baseline metrics for optimization work
- Keep benchmarks simple, stdlib-only using existing `benchmark_framework.rs`

## Implementation: `benches/buffer_pool.rs`

---

## Phase 1: Core Latency Benchmarks ✓ COMPLETE

### 1.1 Pin/Unpin Overhead (Cache Hit) ✓
- Pin same block repeatedly
- Measures: sync overhead (Mutex, atomic ops)
- **Actual: ~85ns** (much faster than expected due to no I/O)

### 1.2 Cold Pin (Cache Miss - Clean) ✓
- Pin new blocks in empty pool
- Measures: disk read + buffer assignment
- **Actual: ~3.5µs**

### 1.3 Dirty Eviction ✓
- Modify buffer, force eviction
- Measures: flush + read cost (includes INSERT overhead)
- **Actual: ~6.65ms** (includes SQL operation overhead)

---

## Phase 2: Access Pattern Benchmarks

### 2.1 Sequential Scan (working set > pool)
- Access blocks 0..N where N = pool_size * 10
- Tests: constant eviction pressure
- Metric: blocks/sec throughput

### 2.2 Repeated Access (working set < pool)
- Access same 10 blocks repeatedly
- Tests: ideal case (100% hit rate)
- Metric: blocks/sec throughput

### 2.3 Random Access (varying working set)
- Random blocks from range [0..K]
- Vary K: 10, 50, 100, 500 blocks
- Shows hit rate degradation as working set grows

### 2.4 Zipfian Distribution (80/20 rule)
- Access follows power law: 20% of blocks get 80% of accesses
- Most realistic model of real workloads (hot/cold data)
- Tests: how well policy keeps frequently-accessed pages resident
- Metric: throughput + hit rate on hot vs cold pages

---

## Phase 3: Pool Size Sensitivity

### 3.1 Fixed Workload, Varying Pool Size
- Same workload (e.g., random access 100 blocks)
- Test with pool sizes: 8, 16, 32, 64, 128, 256
- Metric: throughput vs memory usage

### 3.2 Memory Pressure
- Working set = pool_size + K (where K = 1, 5, 10)
- Shows performance cliff when pool too small

---

## Phase 4: Hit Rate Measurement

### 4.1 Instrumentation
Add to BufferManager:
```rust
pub struct BufferStats {
    hits: AtomicUsize,
    misses: AtomicUsize,
}

fn get_stats() -> (usize, usize)
fn reset_stats()
```

Track in `try_to_pin()`:
- Hit: `find_existing_buffer()` returns Some
- Miss: `choose_unpinned_buffer()` called

### 4.2 Hit Rate Benchmarks
Re-run Phase 2 benchmarks with stats enabled:
- Sequential: expect ~0% hit rate
- Repeated: expect ~90%+ hit rate
- Random: varies by working set size
- Zipfian: expect ~60-70% hit rate (depends on skew factor)

---

## Phase 5: Concurrent Access (Optional)

### 5.1 Multi-threaded Ping
- N threads pin/unpin different blocks
- Measures: lock contention overhead

### 5.2 Buffer Starvation
- Pin entire pool, then spawn threads requesting pins
- Measures: `cond.wait()` latency + throughput recovery

---

## Output Format

```
Buffer Pool Benchmarks (pool=64, block=4096, iterations=1000)
==============================================================
Operation              |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------
Pin/Unpin (hit)        |      1.2µs |      1.1µs |      0.3µs |     1000
Cold Pin (read)        |    234.5µs |    230.1µs |     23.4µs |      100
Dirty Eviction         |    456.7µs |    450.2µs |     34.5µs |      100
Sequential Scan        |    2145 blocks/sec
Repeated Access        |   45230 blocks/sec
Random (K=100)         |    8934 blocks/sec  [Hit rate: 34.2%]
Zipfian (80/20)        |   12456 blocks/sec  [Hit rate: 67.8%]

Pool Size Scaling:
  8 buffers:   1234 blocks/sec
 16 buffers:   2456 blocks/sec
 32 buffers:   4512 blocks/sec
 64 buffers:   7823 blocks/sec
128 buffers:   8234 blocks/sec  (diminishing returns)
```

---

## Dependencies

None. Uses existing:
- `benches/benchmark_framework.rs`
- `FileManager`, `BufferManager`, `LogManager` from `main.rs`

---

## Progress

- [x] Phase 1: Core Latency Benchmarks - COMPLETE
- [ ] Phase 2: Access Pattern Benchmarks
- [ ] Phase 3: Pool Size Sensitivity
- [ ] Phase 4: Hit Rate Measurement
- [ ] Phase 5: Concurrent Access (Optional)

## Next Steps

1. ~~Review plan~~ ✓
2. ~~Implement Phase 1 (3 benchmarks)~~ ✓
3. ~~Validate output format~~ ✓
4. Implement Phase 2 (access pattern benchmarks)
5. Iterate on remaining phases
