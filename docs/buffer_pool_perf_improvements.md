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

## Raw Benchmark Output (baseline / global latch_table + Drop cleanup)
Date: 2025-12-20
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k -- 100 12

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   810.00ns |   792.00ns |    23.00ns |      100
Cold Pin (miss)                                              |    15.62µs |     5.42µs |    94.49µs |      100
Dirty Eviction                                               |   323.93µs |     5.50µs |   970.51µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 258093.38 blocks/sec |        464.95µs
Seq Scan MT x2 (120 blocks)                                  | 232044.13 blocks/sec |        517.14µs
Seq Scan MT x4 (120 blocks)                                  | 189555.79 blocks/sec |        633.06µs
Seq Scan MT x8 (120 blocks)                                  | 143283.24 blocks/sec |        837.50µs
Seq Scan MT x16 (120 blocks)                                 | 114618.76 blocks/sec |          1.05ms
Seq Scan MT x32 (120 blocks)                                 |  97933.36 blocks/sec |          1.23ms
Seq Scan MT x64 (120 blocks)                                 |  82975.33 blocks/sec |          1.45ms
Seq Scan MT x128 (120 blocks)                                |  57229.85 blocks/sec |          2.10ms
Seq Scan MT x256 (120 blocks)                                |  26046.32 blocks/sec |          4.61ms
Repeated Access (1000 ops)                                   | 3689655.68 blocks/sec |        271.03µs
Repeated Access MT x2 (1000 ops)                             | 1687114.49 blocks/sec |        592.73µs
Repeated Access MT x4 (1000 ops)                             | 915579.04 blocks/sec |          1.09ms
Repeated Access MT x8 (1000 ops)                             | 639719.96 blocks/sec |          1.56ms
Repeated Access MT x16 (1000 ops)                            | 523651.78 blocks/sec |          1.91ms
Repeated Access MT x32 (1000 ops)                            | 482140.78 blocks/sec |          2.07ms
Repeated Access MT x64 (1000 ops)                            | 383260.56 blocks/sec |          2.61ms
Repeated Access MT x128 (1000 ops)                           | 463261.08 blocks/sec |          2.16ms
Repeated Access MT x256 (1000 ops)                           | 241643.19 blocks/sec |          4.14ms
Random (K=10, 500 ops)                                       | 3676767.97 blocks/sec |        135.99µs
Random (K=50, 500 ops)                                       | 633305.00 blocks/sec |        789.51µs
Random (K=100, 500 ops)                                      | 542474.68 blocks/sec |        921.70µs
Random MT x2 (K=10, 500 ops)                                 | 1717876.57 blocks/sec |        291.06µs
Random MT x4 (K=10, 500 ops)                                 | 945762.42 blocks/sec |        528.67µs
Random MT x8 (K=10, 500 ops)                                 | 717408.49 blocks/sec |        696.95µs
Random MT x16 (K=10, 500 ops)                                | 565977.72 blocks/sec |        883.43µs
Random MT x32 (K=10, 500 ops)                                | 361982.56 blocks/sec |          1.38ms
Random MT x64 (K=10, 500 ops)                                | 346745.93 blocks/sec |          1.44ms
Random MT x128 (K=10, 500 ops)                               | 103388.73 blocks/sec |          4.84ms
Random MT x256 (K=10, 500 ops)                               | 122876.33 blocks/sec |          4.07ms
Random MT x2 (K=50, 500 ops)                                 | 420847.44 blocks/sec |          1.19ms
Random MT x4 (K=50, 500 ops)                                 | 254875.25 blocks/sec |          1.96ms
Random MT x8 (K=50, 500 ops)                                 | 199338.67 blocks/sec |          2.51ms
Random MT x16 (K=50, 500 ops)                                | 132189.14 blocks/sec |          3.78ms
Random MT x32 (K=50, 500 ops)                                | 157830.18 blocks/sec |          3.17ms
Random MT x64 (K=50, 500 ops)                                | 147893.99 blocks/sec |          3.38ms
Random MT x128 (K=50, 500 ops)                               | 151893.83 blocks/sec |          3.29ms
Random MT x256 (K=50, 500 ops)                               |  85590.10 blocks/sec |          5.84ms
Random MT x2 (K=100, 500 ops)                                | 298360.87 blocks/sec |          1.68ms
Random MT x4 (K=100, 500 ops)                                | 166213.68 blocks/sec |          3.01ms
Random MT x8 (K=100, 500 ops)                                | 133333.62 blocks/sec |          3.75ms
Random MT x16 (K=100, 500 ops)                               | 107676.08 blocks/sec |          4.64ms
Random MT x32 (K=100, 500 ops)                               | 148095.18 blocks/sec |          3.38ms
Random MT x64 (K=100, 500 ops)                               | 126989.22 blocks/sec |          3.94ms
Random MT x128 (K=100, 500 ops)                              | 121796.21 blocks/sec |          4.11ms
Random MT x256 (K=100, 500 ops)                              |  97016.08 blocks/sec |          5.15ms
Zipfian (80/20, 500 ops)                                     | 943761.27 blocks/sec |        529.80µs
Zipfian MT x2 (80/20, 500 ops)                               | 768814.43 blocks/sec |        650.35µs
Zipfian MT x4 (80/20, 500 ops)                               | 384007.77 blocks/sec |          1.30ms
Zipfian MT x8 (80/20, 500 ops)                               | 333350.00 blocks/sec |          1.50ms
Zipfian MT x16 (80/20, 500 ops)                              | 363888.31 blocks/sec |          1.37ms
Zipfian MT x32 (80/20, 500 ops)                              | 324724.46 blocks/sec |          1.54ms
Zipfian MT x64 (80/20, 500 ops)                              | 244087.23 blocks/sec |          2.05ms
Zipfian MT x128 (80/20, 500 ops)                             | 153493.97 blocks/sec |          3.26ms
Zipfian MT x256 (80/20, 500 ops)                             |  50589.86 blocks/sec |          9.88ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     273920
                 16 |     405643
                 32 |     524419
                 64 |     688831
                128 |    3393212
                256 |    2269519


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3209346
       32 |          33 |    1960984
       32 |          37 |    1517722
       32 |          42 |    1367103
       32 |          52 |    1031966

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  74.1% (hits: 37837, misses: 13216)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  29.6% (hits: 15095, misses: 35907)
Random (K=100)       | Hit rate:  11.2% (hits: 5712, misses: 45292)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   2749834.12 ops/sec |          3.64ms
2 threads, 5000 ops/thread                                   |    970783.78 ops/sec |         10.30ms
4 threads, 2500 ops/thread                                   |    338522.57 ops/sec |         29.54ms
8 threads, 1250 ops/thread                                   |    174036.43 ops/sec |         57.46ms
16 threads, 625 ops/thread                                   |    159964.53 ops/sec |         62.51ms
32 threads, 312 ops/thread                                   |    153259.77 ops/sec |         65.25ms
64 threads, 156 ops/thread                                   |    142358.36 ops/sec |         70.25ms
128 threads, 78 ops/thread                                   |    138314.69 ops/sec |         72.30ms
256 threads, 39 ops/thread                                   |    130972.45 ops/sec |         76.35ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3625714.58 ops/sec |          2.76ms
2 threads, K=4, 5000 ops/thread                              |   1757896.52 ops/sec |          5.69ms
4 threads, K=4, 2500 ops/thread                              |   1018551.59 ops/sec |          9.82ms
8 threads, K=4, 1250 ops/thread                              |    668568.83 ops/sec |         14.96ms
16 threads, K=4, 625 ops/thread                              |    650165.13 ops/sec |         15.38ms
32 threads, K=4, 312 ops/thread                              |    503918.65 ops/sec |         19.84ms
64 threads, K=4, 156 ops/thread                              |    452880.50 ops/sec |         22.08ms
128 threads, K=4, 78 ops/thread                              |    438662.19 ops/sec |         22.80ms
256 threads, K=4, 39 ops/thread                              |    416461.28 ops/sec |         24.01ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   202.90ms

All benchmarks completed!
```

## Raw Benchmark Output (no_drop / global latch_table + NO Drop cleanup)
Date: 2025-12-20
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k -- 100 12

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   526.00ns |   541.00ns |    20.00ns |      100
Cold Pin (miss)                                              |     8.75µs |     5.29µs |    23.14µs |      100
Dirty Eviction                                               |   323.17µs |    11.65µs |   936.80µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 206777.83 blocks/sec |        580.33µs
Seq Scan MT x2 (120 blocks)                                  | 259015.35 blocks/sec |        463.29µs
Seq Scan MT x4 (120 blocks)                                  | 183289.50 blocks/sec |        654.70µs
Seq Scan MT x8 (120 blocks)                                  | 143756.13 blocks/sec |        834.75µs
Seq Scan MT x16 (120 blocks)                                 | 123531.52 blocks/sec |        971.41µs
Seq Scan MT x32 (120 blocks)                                 | 106580.74 blocks/sec |          1.13ms
Seq Scan MT x64 (120 blocks)                                 |  87355.19 blocks/sec |          1.37ms
Seq Scan MT x128 (120 blocks)                                |  58141.23 blocks/sec |          2.06ms
Seq Scan MT x256 (120 blocks)                                |  28579.78 blocks/sec |          4.20ms
Repeated Access (1000 ops)                                   | 6076109.35 blocks/sec |        164.58µs
Repeated Access MT x2 (1000 ops)                             | 3030927.59 blocks/sec |        329.93µs
Repeated Access MT x4 (1000 ops)                             | 1513330.93 blocks/sec |        660.79µs
Repeated Access MT x8 (1000 ops)                             | 930167.66 blocks/sec |          1.08ms
Repeated Access MT x16 (1000 ops)                            | 632259.52 blocks/sec |          1.58ms
Repeated Access MT x32 (1000 ops)                            | 538720.25 blocks/sec |          1.86ms
Repeated Access MT x64 (1000 ops)                            | 483483.71 blocks/sec |          2.07ms
Repeated Access MT x128 (1000 ops)                           | 468181.67 blocks/sec |          2.14ms
Repeated Access MT x256 (1000 ops)                           | 238540.12 blocks/sec |          4.19ms
Random (K=10, 500 ops)                                       | 6171544.24 blocks/sec |         81.02µs
Random (K=50, 500 ops)                                       | 668357.16 blocks/sec |        748.10µs
Random (K=100, 500 ops)                                      | 589116.89 blocks/sec |        848.73µs
Random MT x2 (K=10, 500 ops)                                 | 2468660.36 blocks/sec |        202.54µs
Random MT x4 (K=10, 500 ops)                                 | 1406544.93 blocks/sec |        355.48µs
Random MT x8 (K=10, 500 ops)                                 | 921888.40 blocks/sec |        542.37µs
Random MT x16 (K=10, 500 ops)                                | 710159.69 blocks/sec |        704.07µs
Random MT x32 (K=10, 500 ops)                                | 573317.57 blocks/sec |        872.12µs
Random MT x64 (K=10, 500 ops)                                | 475005.68 blocks/sec |          1.05ms
Random MT x128 (K=10, 500 ops)                               | 240734.95 blocks/sec |          2.08ms
Random MT x256 (K=10, 500 ops)                               | 119706.92 blocks/sec |          4.18ms
Random MT x2 (K=50, 500 ops)                                 | 437600.37 blocks/sec |          1.14ms
Random MT x4 (K=50, 500 ops)                                 | 252547.45 blocks/sec |          1.98ms
Random MT x8 (K=50, 500 ops)                                 | 205433.13 blocks/sec |          2.43ms
Random MT x16 (K=50, 500 ops)                                | 168143.58 blocks/sec |          2.97ms
Random MT x32 (K=50, 500 ops)                                | 198028.90 blocks/sec |          2.52ms
Random MT x64 (K=50, 500 ops)                                | 199286.95 blocks/sec |          2.51ms
Random MT x128 (K=50, 500 ops)                               | 168776.71 blocks/sec |          2.96ms
Random MT x256 (K=50, 500 ops)                               | 108755.37 blocks/sec |          4.60ms
Random MT x2 (K=100, 500 ops)                                | 392625.86 blocks/sec |          1.27ms
Random MT x4 (K=100, 500 ops)                                | 216096.79 blocks/sec |          2.31ms
Random MT x8 (K=100, 500 ops)                                | 170696.59 blocks/sec |          2.93ms
Random MT x16 (K=100, 500 ops)                               | 151352.97 blocks/sec |          3.30ms
Random MT x32 (K=100, 500 ops)                               | 162624.95 blocks/sec |          3.07ms
Random MT x64 (K=100, 500 ops)                               | 161242.37 blocks/sec |          3.10ms
Random MT x128 (K=100, 500 ops)                              | 144622.24 blocks/sec |          3.46ms
Random MT x256 (K=100, 500 ops)                              | 106501.22 blocks/sec |          4.69ms
Zipfian (80/20, 500 ops)                                     | 1606967.81 blocks/sec |        311.15µs
Zipfian MT x2 (80/20, 500 ops)                               | 1218492.82 blocks/sec |        410.34µs
Zipfian MT x4 (80/20, 500 ops)                               | 725544.09 blocks/sec |        689.14µs
Zipfian MT x8 (80/20, 500 ops)                               | 439040.54 blocks/sec |          1.14ms
Zipfian MT x16 (80/20, 500 ops)                              | 418394.64 blocks/sec |          1.20ms
Zipfian MT x32 (80/20, 500 ops)                              | 370128.28 blocks/sec |          1.35ms
Zipfian MT x64 (80/20, 500 ops)                              | 321045.01 blocks/sec |          1.56ms
Zipfian MT x128 (80/20, 500 ops)                             | 229581.59 blocks/sec |          2.18ms
Zipfian MT x256 (80/20, 500 ops)                             | 107208.12 blocks/sec |          4.66ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     552034
                 16 |     416561
                 32 |     732121
                 64 |    1223179
                128 |    5799320
                256 |    5880623


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    5878549
       32 |          33 |    3458030
       32 |          37 |    2588126
       32 |          42 |    1728752
       32 |          52 |    1148343

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  79.6% (hits: 40590, misses: 10422)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.6% (hits: 12034, misses: 38968)
Random (K=100)       | Hit rate:  12.4% (hits: 6324, misses: 44681)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   5002185.96 ops/sec |          2.00ms
2 threads, 5000 ops/thread                                   |   2186764.09 ops/sec |          4.57ms
4 threads, 2500 ops/thread                                   |    551160.51 ops/sec |         18.14ms
8 threads, 1250 ops/thread                                   |    228260.74 ops/sec |         43.81ms
16 threads, 625 ops/thread                                   |    197827.72 ops/sec |         50.55ms
32 threads, 312 ops/thread                                   |    176861.27 ops/sec |         56.54ms
64 threads, 156 ops/thread                                   |    160558.86 ops/sec |         62.28ms
128 threads, 78 ops/thread                                   |    143833.31 ops/sec |         69.52ms
256 threads, 39 ops/thread                                   |    124019.12 ops/sec |         80.63ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   5037440.78 ops/sec |          1.99ms
2 threads, K=4, 5000 ops/thread                              |   2676772.02 ops/sec |          3.74ms
4 threads, K=4, 2500 ops/thread                              |   1608228.21 ops/sec |          6.22ms
8 threads, K=4, 1250 ops/thread                              |    918127.96 ops/sec |         10.89ms
16 threads, K=4, 625 ops/thread                              |    597301.13 ops/sec |         16.74ms
32 threads, K=4, 312 ops/thread                              |    480152.07 ops/sec |         20.83ms
64 threads, K=4, 156 ops/thread                              |    421577.34 ops/sec |         23.72ms
128 threads, K=4, 78 ops/thread                              |    395674.66 ops/sec |         25.27ms
256 threads, K=4, 39 ops/thread                              |    372866.38 ops/sec |         26.82ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   202.30ms

All benchmarks completed!
```

## Raw Benchmark Output (sharded / sharded latch_table + NO Drop cleanup)
Date: 2025-12-20
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k -- 100 12

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |     1.09µs |     1.08µs |    21.00ns |      100
Cold Pin (miss)                                              |     8.49µs |     8.19µs |     1.68µs |      100
Dirty Eviction                                               |   425.93µs |     6.04µs |     1.61ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 224843.31 blocks/sec |        533.71µs
Seq Scan MT x2 (120 blocks)                                  | 246655.76 blocks/sec |        486.51µs
Seq Scan MT x4 (120 blocks)                                  | 192532.31 blocks/sec |        623.27µs
Seq Scan MT x8 (120 blocks)                                  | 148115.96 blocks/sec |        810.18µs
Seq Scan MT x16 (120 blocks)                                 | 130345.52 blocks/sec |        920.63µs
Seq Scan MT x32 (120 blocks)                                 | 113473.25 blocks/sec |          1.06ms
Seq Scan MT x64 (120 blocks)                                 |  92914.57 blocks/sec |          1.29ms
Seq Scan MT x128 (120 blocks)                                |  59429.04 blocks/sec |          2.02ms
Seq Scan MT x256 (120 blocks)                                |  30058.43 blocks/sec |          3.99ms
Repeated Access (1000 ops)                                   | 6258370.57 blocks/sec |        159.79µs
Repeated Access MT x2 (1000 ops)                             | 4888015.56 blocks/sec |        204.58µs
Repeated Access MT x4 (1000 ops)                             | 2582577.93 blocks/sec |        387.21µs
Repeated Access MT x8 (1000 ops)                             | 2169319.74 blocks/sec |        460.97µs
Repeated Access MT x16 (1000 ops)                            | 1811672.97 blocks/sec |        551.98µs
Repeated Access MT x32 (1000 ops)                            | 1631087.22 blocks/sec |        613.09µs
Repeated Access MT x64 (1000 ops)                            | 1028299.84 blocks/sec |        972.48µs
Repeated Access MT x128 (1000 ops)                           | 520977.69 blocks/sec |          1.92ms
Repeated Access MT x256 (1000 ops)                           | 250262.34 blocks/sec |          4.00ms
Random (K=10, 500 ops)                                       | 5709391.95 blocks/sec |         87.58µs
Random (K=50, 500 ops)                                       | 665802.90 blocks/sec |        750.97µs
Random (K=100, 500 ops)                                      | 590265.11 blocks/sec |        847.08µs
Random MT x2 (K=10, 500 ops)                                 | 3797862.56 blocks/sec |        131.65µs
Random MT x4 (K=10, 500 ops)                                 | 2356112.23 blocks/sec |        212.21µs
Random MT x8 (K=10, 500 ops)                                 | 1931195.37 blocks/sec |        258.91µs
Random MT x16 (K=10, 500 ops)                                | 1669722.26 blocks/sec |        299.45µs
Random MT x32 (K=10, 500 ops)                                | 1055671.91 blocks/sec |        473.63µs
Random MT x64 (K=10, 500 ops)                                | 532556.23 blocks/sec |        938.87µs
Random MT x128 (K=10, 500 ops)                               | 259506.77 blocks/sec |          1.93ms
Random MT x256 (K=10, 500 ops)                               | 124834.06 blocks/sec |          4.01ms
Random MT x2 (K=50, 500 ops)                                 | 462316.58 blocks/sec |          1.08ms
Random MT x4 (K=50, 500 ops)                                 | 260922.61 blocks/sec |          1.92ms
Random MT x8 (K=50, 500 ops)                                 | 216591.52 blocks/sec |          2.31ms
Random MT x16 (K=50, 500 ops)                                | 198023.49 blocks/sec |          2.52ms
Random MT x32 (K=50, 500 ops)                                | 194519.76 blocks/sec |          2.57ms
Random MT x64 (K=50, 500 ops)                                | 212382.58 blocks/sec |          2.35ms
Random MT x128 (K=50, 500 ops)                               | 145930.86 blocks/sec |          3.43ms
Random MT x256 (K=50, 500 ops)                               |  99469.89 blocks/sec |          5.03ms
Random MT x2 (K=100, 500 ops)                                | 309320.57 blocks/sec |          1.62ms
Random MT x4 (K=100, 500 ops)                                | 191109.22 blocks/sec |          2.62ms
Random MT x8 (K=100, 500 ops)                                | 177654.16 blocks/sec |          2.81ms
Random MT x16 (K=100, 500 ops)                               | 167324.36 blocks/sec |          2.99ms
Random MT x32 (K=100, 500 ops)                               | 173329.15 blocks/sec |          2.88ms
Random MT x64 (K=100, 500 ops)                               | 133752.75 blocks/sec |          3.74ms
Random MT x128 (K=100, 500 ops)                              | 148989.02 blocks/sec |          3.36ms
Random MT x256 (K=100, 500 ops)                              | 103884.02 blocks/sec |          4.81ms
Zipfian (80/20, 500 ops)                                     | 1381673.48 blocks/sec |        361.88µs
Zipfian MT x2 (80/20, 500 ops)                               | 842986.94 blocks/sec |        593.13µs
Zipfian MT x4 (80/20, 500 ops)                               | 594128.35 blocks/sec |        841.57µs
Zipfian MT x8 (80/20, 500 ops)                               | 611131.14 blocks/sec |        818.16µs
Zipfian MT x16 (80/20, 500 ops)                              | 545062.50 blocks/sec |        917.33µs
Zipfian MT x32 (80/20, 500 ops)                              | 414655.59 blocks/sec |          1.21ms
Zipfian MT x64 (80/20, 500 ops)                              | 348138.99 blocks/sec |          1.44ms
Zipfian MT x128 (80/20, 500 ops)                             | 231599.10 blocks/sec |          2.16ms
Zipfian MT x256 (80/20, 500 ops)                             | 113488.16 blocks/sec |          4.41ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     528166
                 16 |     538236
                 32 |     401340
                 64 |    1192387
                128 |    5670477
                256 |    5691714


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    5868131
       32 |          33 |    3898392
       32 |          37 |    2222973
       32 |          42 |    1515969
       32 |          52 |    1048253

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  76.7% (hits: 39160, misses: 11893)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.2% (hits: 11319, misses: 39682)
Random (K=100)       | Hit rate:  13.2% (hits: 6730, misses: 44271)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   5556456.94 ops/sec |          1.80ms
2 threads, 5000 ops/thread                                   |   3229774.91 ops/sec |          3.10ms
4 threads, 2500 ops/thread                                   |    619217.78 ops/sec |         16.15ms
8 threads, 1250 ops/thread                                   |    349044.22 ops/sec |         28.65ms
16 threads, 625 ops/thread                                   |    267787.17 ops/sec |         37.34ms
32 threads, 312 ops/thread                                   |    214181.40 ops/sec |         46.69ms
64 threads, 156 ops/thread                                   |    171851.55 ops/sec |         58.19ms
128 threads, 78 ops/thread                                   |    153927.52 ops/sec |         64.97ms
256 threads, 39 ops/thread                                   |    128220.94 ops/sec |         77.99ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   5064758.00 ops/sec |          1.97ms
2 threads, K=4, 5000 ops/thread                              |   4727097.57 ops/sec |          2.12ms
4 threads, K=4, 2500 ops/thread                              |   2497934.83 ops/sec |          4.00ms
8 threads, K=4, 1250 ops/thread                              |   1727426.59 ops/sec |          5.79ms
16 threads, K=4, 625 ops/thread                              |   1388423.96 ops/sec |          7.20ms
32 threads, K=4, 312 ops/thread                              |   1270729.57 ops/sec |          7.87ms
64 threads, K=4, 156 ops/thread                              |   1128840.17 ops/sec |          8.86ms
128 threads, K=4, 78 ops/thread                              |   1095246.95 ops/sec |          9.13ms
256 threads, K=4, 39 ops/thread                              |    953526.73 ops/sec |         10.49ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   198.19ms

All benchmarks completed!
```

# Linux Results (x86_64, Dec 20, 2025)
Note: earlier raw benchmark data above is macOS (aarch64). Sections below are Linux.

## Raw Benchmark Output (baseline / global latch_table + Drop cleanup) — Linux
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 10 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   858.00ns |   856.00ns |     8.00ns |       10
Cold Pin (miss)                                              |     5.13µs |     3.83µs |     2.98µs |       10
Dirty Eviction                                               |     5.04ms |     5.02ms |   216.36µs |       10

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 272071.83 blocks/sec |        441.06µs
Seq Scan MT x2 (120 blocks)                                  | 239934.74 blocks/sec |        500.14µs
Seq Scan MT x4 (120 blocks)                                  | 183761.32 blocks/sec |        653.02µs
Seq Scan MT x8 (120 blocks)                                  | 134508.49 blocks/sec |        892.14µs
Seq Scan MT x16 (120 blocks)                                 | 129286.66 blocks/sec |        928.17µs
Seq Scan MT x32 (120 blocks)                                 | 103853.75 blocks/sec |          1.16ms
Seq Scan MT x64 (120 blocks)                                 |  70094.03 blocks/sec |          1.71ms
Seq Scan MT x128 (120 blocks)                                |  32634.56 blocks/sec |          3.68ms
Seq Scan MT x256 (120 blocks)                                |  16275.52 blocks/sec |          7.37ms
Repeated Access (1000 ops)                                   | 1263074.40 blocks/sec |        791.72µs
Repeated Access MT x2 (1000 ops)                             | 1262921.26 blocks/sec |        791.82µs
Repeated Access MT x4 (1000 ops)                             | 1233928.09 blocks/sec |        810.42µs
Repeated Access MT x8 (1000 ops)                             | 989843.22 blocks/sec |          1.01ms
Repeated Access MT x16 (1000 ops)                            | 974886.91 blocks/sec |          1.03ms
Repeated Access MT x32 (1000 ops)                            | 857975.09 blocks/sec |          1.17ms
Repeated Access MT x64 (1000 ops)                            | 469414.59 blocks/sec |          2.13ms
Repeated Access MT x128 (1000 ops)                           | 269468.64 blocks/sec |          3.71ms
Repeated Access MT x256 (1000 ops)                           | 138902.55 blocks/sec |          7.20ms
Random (K=10, 500 ops)                                       | 1200984.81 blocks/sec |        416.33µs
Random (K=50, 500 ops)                                       | 324004.38 blocks/sec |          1.54ms
Random (K=100, 500 ops)                                      | 254022.83 blocks/sec |          1.97ms
Random MT x2 (K=10, 500 ops)                                 | 1101018.44 blocks/sec |        454.13µs
Random MT x4 (K=10, 500 ops)                                 | 1104037.91 blocks/sec |        452.88µs
Random MT x8 (K=10, 500 ops)                                 | 955873.08 blocks/sec |        523.08µs
Random MT x16 (K=10, 500 ops)                                | 923828.49 blocks/sec |        541.23µs
Random MT x32 (K=10, 500 ops)                                | 628585.29 blocks/sec |        795.44µs
Random MT x64 (K=10, 500 ops)                                | 301174.34 blocks/sec |          1.66ms
Random MT x128 (K=10, 500 ops)                               | 133343.57 blocks/sec |          3.75ms
Random MT x256 (K=10, 500 ops)                               |  68038.97 blocks/sec |          7.35ms
Random MT x2 (K=50, 500 ops)                                 | 330511.43 blocks/sec |          1.51ms
Random MT x4 (K=50, 500 ops)                                 | 246140.03 blocks/sec |          2.03ms
Random MT x8 (K=50, 500 ops)                                 | 195377.52 blocks/sec |          2.56ms
Random MT x16 (K=50, 500 ops)                                | 213871.63 blocks/sec |          2.34ms
Random MT x32 (K=50, 500 ops)                                | 221265.96 blocks/sec |          2.26ms
Random MT x64 (K=50, 500 ops)                                | 179377.54 blocks/sec |          2.79ms
Random MT x128 (K=50, 500 ops)                               | 123558.54 blocks/sec |          4.05ms
Random MT x256 (K=50, 500 ops)                               |  64756.04 blocks/sec |          7.72ms
Random MT x2 (K=100, 500 ops)                                | 299461.27 blocks/sec |          1.67ms
Random MT x4 (K=100, 500 ops)                                | 213973.04 blocks/sec |          2.34ms
Random MT x8 (K=100, 500 ops)                                | 161053.47 blocks/sec |          3.10ms
Random MT x16 (K=100, 500 ops)                               | 169509.05 blocks/sec |          2.95ms
Random MT x32 (K=100, 500 ops)                               | 172074.33 blocks/sec |          2.91ms
Random MT x64 (K=100, 500 ops)                               | 151217.85 blocks/sec |          3.31ms
Random MT x128 (K=100, 500 ops)                              | 114129.45 blocks/sec |          4.38ms
Random MT x256 (K=100, 500 ops)                              |  66144.57 blocks/sec |          7.56ms
Zipfian (80/20, 500 ops)                                     | 579571.72 blocks/sec |        862.71µs
Zipfian MT x2 (80/20, 500 ops)                               | 667009.95 blocks/sec |        749.61µs
Zipfian MT x4 (80/20, 500 ops)                               | 795690.54 blocks/sec |        628.39µs
Zipfian MT x8 (80/20, 500 ops)                               | 619037.89 blocks/sec |        807.71µs
Zipfian MT x16 (80/20, 500 ops)                              | 527575.86 blocks/sec |        947.73µs
Zipfian MT x32 (80/20, 500 ops)                              | 420603.87 blocks/sec |          1.19ms
Zipfian MT x64 (80/20, 500 ops)                              | 254181.54 blocks/sec |          1.97ms
Zipfian MT x128 (80/20, 500 ops)                             | 139187.10 blocks/sec |          3.59ms
Zipfian MT x256 (80/20, 500 ops)                             |  65769.22 blocks/sec |          7.60ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     282280
                 16 |     298861
                 32 |     337158
                 64 |     541658
                128 |    1255852
                256 |    1254614


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1262489
       32 |          33 |    1130636
       32 |          37 |     838517
       32 |          42 |     625123
       32 |          52 |     503168

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 1560)
Repeated Access      | Hit rate:  99.9% (hits: 11990, misses: 10)
Zipfian (80/20)      | Hit rate:  70.3% (hits: 4243, misses: 1791)
Random (K=10)        | Hit rate:  99.8% (hits: 5990, misses: 10)
Random (K=50)        | Hit rate:  27.0% (hits: 1621, misses: 4383)
Random (K=100)       | Hit rate:  10.5% (hits: 637, misses: 5410)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1198479.18 ops/sec |          8.34ms
2 threads, 5000 ops/thread                                   |    298423.08 ops/sec |         33.51ms
4 threads, 2500 ops/thread                                   |    188572.24 ops/sec |         53.03ms
8 threads, 1250 ops/thread                                   |    142613.48 ops/sec |         70.12ms
16 threads, 625 ops/thread                                   |    142579.13 ops/sec |         70.14ms
32 threads, 312 ops/thread                                   |    146360.31 ops/sec |         68.32ms
64 threads, 156 ops/thread                                   |    152894.00 ops/sec |         65.40ms
128 threads, 78 ops/thread                                   |    142329.71 ops/sec |         70.26ms
256 threads, 39 ops/thread                                   |    131015.86 ops/sec |         76.33ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1173258.90 ops/sec |          8.52ms
2 threads, K=4, 5000 ops/thread                              |   1463447.04 ops/sec |          6.83ms
4 threads, K=4, 2500 ops/thread                              |   1338419.15 ops/sec |          7.47ms
8 threads, K=4, 1250 ops/thread                              |   1034543.51 ops/sec |          9.67ms
16 threads, K=4, 625 ops/thread                              |   1060985.67 ops/sec |          9.43ms
32 threads, K=4, 312 ops/thread                              |   1151549.70 ops/sec |          8.68ms
64 threads, K=4, 156 ops/thread                              |   1136155.26 ops/sec |          8.80ms
128 threads, K=4, 78 ops/thread                              |    979900.19 ops/sec |         10.21ms
256 threads, K=4, 39 ops/thread                              |    863904.61 ops/sec |         11.58ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   173.89ms

All benchmarks completed!
```

## Raw Benchmark Output (no_drop / global latch_table + NO Drop cleanup) — Linux
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 10 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   717.00ns |   715.00ns |     8.00ns |       10
Cold Pin (miss)                                              |     4.60µs |     4.02µs |     1.37µs |       10
Dirty Eviction                                               |     5.04ms |     5.04ms |   226.41µs |       10

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 272333.01 blocks/sec |        440.64µs
Seq Scan MT x2 (120 blocks)                                  | 241010.32 blocks/sec |        497.90µs
Seq Scan MT x4 (120 blocks)                                  | 184266.70 blocks/sec |        651.23µs
Seq Scan MT x8 (120 blocks)                                  | 134432.69 blocks/sec |        892.64µs
Seq Scan MT x16 (120 blocks)                                 | 125484.42 blocks/sec |        956.29µs
Seq Scan MT x32 (120 blocks)                                 | 102628.75 blocks/sec |          1.17ms
Seq Scan MT x64 (120 blocks)                                 |  67540.91 blocks/sec |          1.78ms
Seq Scan MT x128 (120 blocks)                                |  32922.55 blocks/sec |          3.64ms
Seq Scan MT x256 (120 blocks)                                |  16139.95 blocks/sec |          7.43ms
Repeated Access (1000 ops)                                   | 1356388.32 blocks/sec |        737.25µs
Repeated Access MT x2 (1000 ops)                             | 1441564.04 blocks/sec |        693.69µs
Repeated Access MT x4 (1000 ops)                             | 2126320.44 blocks/sec |        470.30µs
Repeated Access MT x8 (1000 ops)                             | 1790010.31 blocks/sec |        558.66µs
Repeated Access MT x16 (1000 ops)                            | 1668078.97 blocks/sec |        599.49µs
Repeated Access MT x32 (1000 ops)                            | 1094120.63 blocks/sec |        913.98µs
Repeated Access MT x64 (1000 ops)                            | 570248.32 blocks/sec |          1.75ms
Repeated Access MT x128 (1000 ops)                           | 268752.40 blocks/sec |          3.72ms
Repeated Access MT x256 (1000 ops)                           | 129787.49 blocks/sec |          7.70ms
Random (K=10, 500 ops)                                       | 1403532.41 blocks/sec |        356.24µs
Random (K=50, 500 ops)                                       | 326654.77 blocks/sec |          1.53ms
Random (K=100, 500 ops)                                      | 189647.31 blocks/sec |          2.64ms
Random MT x2 (K=10, 500 ops)                                 | 1297797.90 blocks/sec |        385.27µs
Random MT x4 (K=10, 500 ops)                                 | 1786550.14 blocks/sec |        279.87µs
Random MT x8 (K=10, 500 ops)                                 | 1596740.10 blocks/sec |        313.14µs
Random MT x16 (K=10, 500 ops)                                | 1305115.01 blocks/sec |        383.11µs
Random MT x32 (K=10, 500 ops)                                | 676280.67 blocks/sec |        739.34µs
Random MT x64 (K=10, 500 ops)                                | 302029.40 blocks/sec |          1.66ms
Random MT x128 (K=10, 500 ops)                               | 130813.79 blocks/sec |          3.82ms
Random MT x256 (K=10, 500 ops)                               |  67646.73 blocks/sec |          7.39ms
Random MT x2 (K=50, 500 ops)                                 | 345314.12 blocks/sec |          1.45ms
Random MT x4 (K=50, 500 ops)                                 | 242678.98 blocks/sec |          2.06ms
Random MT x8 (K=50, 500 ops)                                 | 193621.41 blocks/sec |          2.58ms
Random MT x16 (K=50, 500 ops)                                | 217940.89 blocks/sec |          2.29ms
Random MT x32 (K=50, 500 ops)                                | 216553.71 blocks/sec |          2.31ms
Random MT x64 (K=50, 500 ops)                                | 178799.72 blocks/sec |          2.80ms
Random MT x128 (K=50, 500 ops)                               | 121036.24 blocks/sec |          4.13ms
Random MT x256 (K=50, 500 ops)                               |  65991.66 blocks/sec |          7.58ms
Random MT x2 (K=100, 500 ops)                                | 297620.64 blocks/sec |          1.68ms
Random MT x4 (K=100, 500 ops)                                | 213237.53 blocks/sec |          2.34ms
Random MT x8 (K=100, 500 ops)                                | 163809.82 blocks/sec |          3.05ms
Random MT x16 (K=100, 500 ops)                               | 167208.59 blocks/sec |          2.99ms
Random MT x32 (K=100, 500 ops)                               | 172684.67 blocks/sec |          2.90ms
Random MT x64 (K=100, 500 ops)                               | 150044.89 blocks/sec |          3.33ms
Random MT x128 (K=100, 500 ops)                              | 116481.80 blocks/sec |          4.29ms
Random MT x256 (K=100, 500 ops)                              |  64712.85 blocks/sec |          7.73ms
Zipfian (80/20, 500 ops)                                     | 676869.34 blocks/sec |        738.70µs
Zipfian MT x2 (80/20, 500 ops)                               | 713869.63 blocks/sec |        700.41µs
Zipfian MT x4 (80/20, 500 ops)                               | 721398.71 blocks/sec |        693.10µs
Zipfian MT x8 (80/20, 500 ops)                               | 591014.45 blocks/sec |        846.00µs
Zipfian MT x16 (80/20, 500 ops)                              | 504880.17 blocks/sec |        990.33µs
Zipfian MT x32 (80/20, 500 ops)                              | 429404.23 blocks/sec |          1.16ms
Zipfian MT x64 (80/20, 500 ops)                              | 258605.62 blocks/sec |          1.93ms
Zipfian MT x128 (80/20, 500 ops)                             | 135765.71 blocks/sec |          3.68ms
Zipfian MT x256 (80/20, 500 ops)                             |  65288.39 blocks/sec |          7.66ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     253557
                 16 |     297244
                 32 |     354760
                 64 |     532203
                128 |    1404546
                256 |    1395899


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1366998
       32 |          33 |    1131124
       32 |          37 |     911499
       32 |          42 |     677046
       32 |          52 |     501653

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 1560)
Repeated Access      | Hit rate:  99.9% (hits: 11990, misses: 10)
Zipfian (80/20)      | Hit rate:  74.0% (hits: 4452, misses: 1563)
Random (K=10)        | Hit rate:  99.8% (hits: 5990, misses: 10)
Random (K=50)        | Hit rate:  23.6% (hits: 1427, misses: 4624)
Random (K=100)       | Hit rate:  12.2% (hits: 731, misses: 5272)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1327304.89 ops/sec |          7.53ms
2 threads, 5000 ops/thread                                   |    268621.14 ops/sec |         37.23ms
4 threads, 2500 ops/thread                                   |    187978.48 ops/sec |         53.20ms
8 threads, 1250 ops/thread                                   |    141614.94 ops/sec |         70.61ms
16 threads, 625 ops/thread                                   |    142623.87 ops/sec |         70.11ms
32 threads, 312 ops/thread                                   |    146706.79 ops/sec |         68.16ms
64 threads, 156 ops/thread                                   |    145975.05 ops/sec |         68.50ms
128 threads, 78 ops/thread                                   |    139961.44 ops/sec |         71.45ms
256 threads, 39 ops/thread                                   |    130276.63 ops/sec |         76.76ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1327726.61 ops/sec |          7.53ms
2 threads, K=4, 5000 ops/thread                              |   1826787.65 ops/sec |          5.47ms
4 threads, K=4, 2500 ops/thread                              |   2774080.54 ops/sec |          3.60ms
8 threads, K=4, 1250 ops/thread                              |   1829028.96 ops/sec |          5.47ms
16 threads, K=4, 625 ops/thread                              |   1702383.52 ops/sec |          5.87ms
32 threads, K=4, 312 ops/thread                              |   1614199.79 ops/sec |          6.20ms
64 threads, K=4, 156 ops/thread                              |   1518168.30 ops/sec |          6.59ms
128 threads, K=4, 78 ops/thread                              |   1484910.19 ops/sec |          6.73ms
256 threads, K=4, 39 ops/thread                              |   1026586.75 ops/sec |          9.74ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.32ms

All benchmarks completed!
```

## Raw Benchmark Output (sharded / sharded latch_table + NO Drop cleanup) — Linux
Command: cargo bench --bench buffer_pool --no-default-features --features replacement_clock,page-4k

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 10 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   718.00ns |   714.00ns |    15.00ns |       10
Cold Pin (miss)                                              |     9.75µs |     4.74µs |    10.03µs |       10
Dirty Eviction                                               |     5.02ms |     4.95ms |   284.69µs |       10

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 263859.20 blocks/sec |        454.79µs
Seq Scan MT x2 (120 blocks)                                  | 224157.03 blocks/sec |        535.34µs
Seq Scan MT x4 (120 blocks)                                  | 173450.01 blocks/sec |        691.84µs
Seq Scan MT x8 (120 blocks)                                  | 138401.12 blocks/sec |        867.05µs
Seq Scan MT x16 (120 blocks)                                 | 128143.93 blocks/sec |        936.45µs
Seq Scan MT x32 (120 blocks)                                 | 104016.51 blocks/sec |          1.15ms
Seq Scan MT x64 (120 blocks)                                 |  69369.70 blocks/sec |          1.73ms
Seq Scan MT x128 (120 blocks)                                |  29520.12 blocks/sec |          4.07ms
Seq Scan MT x256 (120 blocks)                                |  16244.20 blocks/sec |          7.39ms
Repeated Access (1000 ops)                                   | 1349955.79 blocks/sec |        740.77µs
Repeated Access MT x2 (1000 ops)                             | 1586392.56 blocks/sec |        630.36µs
Repeated Access MT x4 (1000 ops)                             | 2555838.69 blocks/sec |        391.26µs
Repeated Access MT x8 (1000 ops)                             | 2859880.17 blocks/sec |        349.67µs
Repeated Access MT x16 (1000 ops)                            | 2059897.71 blocks/sec |        485.46µs
Repeated Access MT x32 (1000 ops)                            | 977047.21 blocks/sec |          1.02ms
Repeated Access MT x64 (1000 ops)                            | 602453.55 blocks/sec |          1.66ms
Repeated Access MT x128 (1000 ops)                           | 270906.75 blocks/sec |          3.69ms
Repeated Access MT x256 (1000 ops)                           | 129295.20 blocks/sec |          7.73ms
Random (K=10, 500 ops)                                       | 1340119.70 blocks/sec |        373.10µs
Random (K=50, 500 ops)                                       | 321229.20 blocks/sec |          1.56ms
Random (K=100, 500 ops)                                      | 293420.69 blocks/sec |          1.70ms
Random MT x2 (K=10, 500 ops)                                 | 1405351.01 blocks/sec |        355.78µs
Random MT x4 (K=10, 500 ops)                                 | 2036328.09 blocks/sec |        245.54µs
Random MT x8 (K=10, 500 ops)                                 | 1820777.25 blocks/sec |        274.61µs
Random MT x16 (K=10, 500 ops)                                | 1459828.44 blocks/sec |        342.51µs
Random MT x32 (K=10, 500 ops)                                | 649793.69 blocks/sec |        769.48µs
Random MT x64 (K=10, 500 ops)                                | 295691.77 blocks/sec |          1.69ms
Random MT x128 (K=10, 500 ops)                               | 140938.61 blocks/sec |          3.55ms
Random MT x256 (K=10, 500 ops)                               |  65202.78 blocks/sec |          7.67ms
Random MT x2 (K=50, 500 ops)                                 | 349910.21 blocks/sec |          1.43ms
Random MT x4 (K=50, 500 ops)                                 | 246920.04 blocks/sec |          2.02ms
Random MT x8 (K=50, 500 ops)                                 | 193236.71 blocks/sec |          2.59ms
Random MT x16 (K=50, 500 ops)                                | 217173.56 blocks/sec |          2.30ms
Random MT x32 (K=50, 500 ops)                                | 224082.91 blocks/sec |          2.23ms
Random MT x64 (K=50, 500 ops)                                | 182131.05 blocks/sec |          2.75ms
Random MT x128 (K=50, 500 ops)                               | 120427.37 blocks/sec |          4.15ms
Random MT x256 (K=50, 500 ops)                               |  66235.77 blocks/sec |          7.55ms
Random MT x2 (K=100, 500 ops)                                | 303222.90 blocks/sec |          1.65ms
Random MT x4 (K=100, 500 ops)                                | 203465.01 blocks/sec |          2.46ms
Random MT x8 (K=100, 500 ops)                                | 164864.42 blocks/sec |          3.03ms
Random MT x16 (K=100, 500 ops)                               | 176069.17 blocks/sec |          2.84ms
Random MT x32 (K=100, 500 ops)                               | 173582.90 blocks/sec |          2.88ms
Random MT x64 (K=100, 500 ops)                               | 153411.32 blocks/sec |          3.26ms
Random MT x128 (K=100, 500 ops)                              | 112222.51 blocks/sec |          4.46ms
Random MT x256 (K=100, 500 ops)                              |  66358.50 blocks/sec |          7.53ms
Zipfian (80/20, 500 ops)                                     | 730242.56 blocks/sec |        684.70µs
Zipfian MT x2 (80/20, 500 ops)                               | 798824.13 blocks/sec |        625.92µs
Zipfian MT x4 (80/20, 500 ops)                               | 853112.75 blocks/sec |        586.09µs
Zipfian MT x8 (80/20, 500 ops)                               | 613755.99 blocks/sec |        814.66µs
Zipfian MT x16 (80/20, 500 ops)                              | 626770.63 blocks/sec |        797.74µs
Zipfian MT x32 (80/20, 500 ops)                              | 474891.08 blocks/sec |          1.05ms
Zipfian MT x64 (80/20, 500 ops)                              | 213207.07 blocks/sec |          2.35ms
Zipfian MT x128 (80/20, 500 ops)                             | 135585.35 blocks/sec |          3.69ms
Zipfian MT x256 (80/20, 500 ops)                             |  64966.90 blocks/sec |          7.70ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     287149
                 16 |     318757
                 32 |     351649
                 64 |     549165
                128 |    1354555
                256 |    1399294


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1400262
       32 |          33 |    1079270
       32 |          37 |     909552
       32 |          42 |     718591
       32 |          52 |     537271

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 1560)
Repeated Access      | Hit rate:  99.9% (hits: 11990, misses: 10)
Zipfian (80/20)      | Hit rate:  80.9% (hits: 4864, misses: 1148)
Random (K=10)        | Hit rate:  99.8% (hits: 5990, misses: 10)
Random (K=50)        | Hit rate:  22.9% (hits: 1380, misses: 4635)
Random (K=100)       | Hit rate:  10.4% (hits: 624, misses: 5380)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1325952.38 ops/sec |          7.54ms
2 threads, 5000 ops/thread                                   |    276002.86 ops/sec |         36.23ms
4 threads, 2500 ops/thread                                   |    190675.45 ops/sec |         52.45ms
8 threads, 1250 ops/thread                                   |    143907.43 ops/sec |         69.49ms
16 threads, 625 ops/thread                                   |    143315.26 ops/sec |         69.78ms
32 threads, 312 ops/thread                                   |    142612.70 ops/sec |         70.12ms
64 threads, 156 ops/thread                                   |    145413.90 ops/sec |         68.77ms
128 threads, 78 ops/thread                                   |    135483.14 ops/sec |         73.81ms
256 threads, 39 ops/thread                                   |    125271.32 ops/sec |         79.83ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1260050.00 ops/sec |          7.94ms
2 threads, K=4, 5000 ops/thread                              |   1971956.02 ops/sec |          5.07ms
4 threads, K=4, 2500 ops/thread                              |   3115099.82 ops/sec |          3.21ms
8 threads, K=4, 1250 ops/thread                              |   3052586.91 ops/sec |          3.28ms
16 threads, K=4, 625 ops/thread                              |   2726310.34 ops/sec |          3.67ms
32 threads, K=4, 312 ops/thread                              |   2528916.26 ops/sec |          3.95ms
64 threads, K=4, 156 ops/thread                              |   2234718.77 ops/sec |          4.47ms
128 threads, K=4, 78 ops/thread                              |   1609951.56 ops/sec |          6.21ms
256 threads, K=4, 39 ops/thread                              |    929293.76 ops/sec |         10.76ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.62ms

All benchmarks completed!
```
