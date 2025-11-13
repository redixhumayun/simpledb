# macOS Buffer Pool Benchmarks (Apple M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Master (first-unpinned policy)

```
➜  simpledb git:(master) cargo bench --bench buffer_pool -- 100 12
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-444470091e4ae742)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   319.00ns |   333.00ns |    29.00ns |      100
Cold Pin (miss)                                              |     4.95µs |     4.83µs |   560.00ns |      100
Dirty Eviction                                               |     3.16ms |     3.01ms |     1.16ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 158702.87 blocks/sec |        756.13µs
Seq Scan MT x4 (120 blocks)                                  | 150221.39 blocks/sec |        798.82µs
Repeated Access (1000 ops)                                   | 301815.91 blocks/sec |          3.31ms
Repeated Access MT x4 (1000 ops)                             | 267153.10 blocks/sec |          3.74ms
Random (K=10, 500 ops)                                       | 315631.86 blocks/sec |          1.58ms
Random (K=50, 500 ops)                                       | 302251.59 blocks/sec |          1.65ms
Random (K=100, 500 ops)                                      | 298682.21 blocks/sec |          1.67ms
Random MT x4 (K=10, 500 ops)                                 | 261105.06 blocks/sec |          1.91ms
Random MT x4 (K=50, 500 ops)                                 | 200011.84 blocks/sec |          2.50ms
Random MT x4 (K=100, 500 ops)                                | 189161.71 blocks/sec |          2.64ms
Zipfian (80/20, 500 ops)                                     | 323978.14 blocks/sec |          1.54ms
Zipfian MT x4 (80/20, 500 ops)                               | 275992.26 blocks/sec |          1.81ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     300504
                 16 |     291482
                 32 |     297365
                 64 |     301117
                128 |     298142
                256 |     302151


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |     308224
       32 |          33 |     304462
       32 |          37 |     305498
       32 |          42 |     304790
       32 |          52 |     303502

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate:   0.0% (hits: 0, misses: 102000)
Zipfian (80/20)      | Hit rate:   9.2% (hits: 4692, misses: 46308)
Random (K=10)        | Hit rate:  10.4% (hits: 5304, misses: 45696)
Random (K=50)        | Hit rate:   2.2% (hits: 1122, misses: 49878)
Random (K=100)       | Hit rate:   1.6% (hits: 816, misses: 50184)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    292333.60 ops/sec |          6.84ms
4 threads, 1000 ops/thread                                   |    207830.50 ops/sec |         19.25ms
8 threads, 1000 ops/thread                                   |    108304.43 ops/sec |         73.87ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |    645010.13 ops/sec |          6.20ms
8 threads, K=4, 1000 ops/thread                              |    680654.57 ops/sec |         11.75ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   202.40ms

All benchmarks completed!
➜  simpledb git:(master)
```

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_lru -- 100 12
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-595f081dbf33478a)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   290.00ns |   292.00ns |    23.00ns |      100
Cold Pin (miss)                                              |     2.61µs |     2.17µs |     1.07µs |      100
Dirty Eviction                                               |     3.49ms |     3.05ms |     1.24ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 242030.64 blocks/sec |        495.81µs
Seq Scan MT x4 (120 blocks)                                  | 191310.06 blocks/sec |        627.25µs
Repeated Access (1000 ops)                                   | 3559010.17 blocks/sec |        280.98µs
Repeated Access MT x4 (1000 ops)                             | 956221.32 blocks/sec |          1.05ms
Random (K=10, 500 ops)                                       | 3496454.60 blocks/sec |        143.00µs
Random (K=50, 500 ops)                                       | 620141.96 blocks/sec |        806.27µs
Random (K=100, 500 ops)                                      | 540369.96 blocks/sec |        925.29µs
Random MT x4 (K=10, 500 ops)                                 | 900205.07 blocks/sec |        555.43µs
Random MT x4 (K=50, 500 ops)                                 | 306987.46 blocks/sec |          1.63ms
Random MT x4 (K=100, 500 ops)                                | 282265.33 blocks/sec |          1.77ms
Zipfian (80/20, 500 ops)                                     | 1508081.81 blocks/sec |        331.55µs
Zipfian MT x4 (80/20, 500 ops)                               | 545058.34 blocks/sec |        917.33µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     487872
                 16 |     557914
                 32 |     728441
                 64 |    1176000
                128 |    3452204
                256 |    3532795


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3499073
       32 |          33 |    2982831
       32 |          37 |    1799176
       32 |          42 |    1377847
       32 |          52 |    1055327

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  77.4% (hits: 39468, misses: 11532)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.2% (hits: 11321, misses: 39679)
Random (K=100)       | Hit rate:  13.8% (hits: 7037, misses: 43963)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1250115.64 ops/sec |          1.60ms
4 threads, 1000 ops/thread                                   |    295595.10 ops/sec |         13.53ms
8 threads, 1000 ops/thread                                   |    225246.11 ops/sec |         35.52ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1016571.38 ops/sec |          3.93ms
8 threads, K=4, 1000 ops/thread                              |    506982.42 ops/sec |         15.78ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   202.22ms

All benchmarks completed!
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗
```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_clock -- 100 12
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-2ed923e46913521e)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   272.00ns |   291.00ns |    22.00ns |      100
Cold Pin (miss)                                              |     2.26µs |     1.92µs |   773.00ns |      100
Dirty Eviction                                               |     3.08ms |     3.01ms |   748.66µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 329211.78 blocks/sec |        364.51µs
Seq Scan MT x4 (120 blocks)                                  | 144708.73 blocks/sec |        829.25µs
Repeated Access (1000 ops)                                   | 3808029.61 blocks/sec |        262.60µs
Repeated Access MT x4 (1000 ops)                             | 1058854.30 blocks/sec |        944.42µs
Random (K=10, 500 ops)                                       | 3817026.99 blocks/sec |        130.99µs
Random (K=50, 500 ops)                                       | 604014.28 blocks/sec |        827.80µs
Random (K=100, 500 ops)                                      | 536939.85 blocks/sec |        931.20µs
Random MT x4 (K=10, 500 ops)                                 | 992806.13 blocks/sec |        503.62µs
Random MT x4 (K=50, 500 ops)                                 | 245634.46 blocks/sec |          2.04ms
Random MT x4 (K=100, 500 ops)                                | 208860.10 blocks/sec |          2.39ms
Zipfian (80/20, 500 ops)                                     | 1503112.95 blocks/sec |        332.64µs
Zipfian MT x4 (80/20, 500 ops)                               | 630098.28 blocks/sec |        793.53µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     497658
                 16 |     551827
                 32 |     690310
                 64 |    1038661
                128 |    3732457
                256 |    3878434


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3742431
       32 |          33 |     649617
       32 |          37 |    1865811
       32 |          42 |    1506532
       32 |          52 |    1112065

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  75.8% (hits: 38756, misses: 12349)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  26.7% (hits: 13669, misses: 37434)
Random (K=100)       | Hit rate:  14.0% (hits: 7138, misses: 43863)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1468244.44 ops/sec |          1.36ms
4 threads, 1000 ops/thread                                   |    297352.25 ops/sec |         13.45ms
8 threads, 1000 ops/thread                                   |    162103.91 ops/sec |         49.35ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1164624.30 ops/sec |          3.43ms
8 threads, K=4, 1000 ops/thread                              |    711339.12 ops/sec |         11.25ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   199.53ms

All benchmarks completed!
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗
```
