# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   284.00ns |   291.00ns |    29.00ns |      100
Cold Pin (miss)                                              |     2.42µs |     2.21µs |   806.00ns |      100
Dirty Eviction                                               |   421.85µs |     2.25µs |     1.44ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 233080.76 blocks/sec |        514.84µs
Seq Scan MT x4 (120 blocks)                                  |  66942.96 blocks/sec |          1.79ms
Seq Scan MT x16 (120 blocks)                                 |  69760.75 blocks/sec |          1.72ms
Repeated Access (1000 ops)                                   | 2131818.89 blocks/sec |        469.08µs
Repeated Access MT x4 (1000 ops)                             | 298007.52 blocks/sec |          3.36ms
Repeated Access MT x16 (1000 ops)                            | 116811.66 blocks/sec |          8.56ms
Random (K=10, 500 ops)                                       | 3216302.80 blocks/sec |        155.46µs
Random (K=50, 500 ops)                                       | 317271.57 blocks/sec |          1.58ms
Random (K=100, 500 ops)                                      | 324151.84 blocks/sec |          1.54ms
Random MT x4 (K=10, 500 ops)                                 | 187420.07 blocks/sec |          2.67ms
Random MT x16 (K=10, 500 ops)                                | 110022.64 blocks/sec |          4.54ms
Random MT x4 (K=50, 500 ops)                                 | 161493.54 blocks/sec |          3.10ms
Random MT x16 (K=50, 500 ops)                                | 103736.79 blocks/sec |          4.82ms
Random MT x4 (K=100, 500 ops)                                | 133274.41 blocks/sec |          3.75ms
Random MT x16 (K=100, 500 ops)                               |  97324.81 blocks/sec |          5.14ms
Zipfian (80/20, 500 ops)                                     | 1184943.63 blocks/sec |        421.96µs
Zipfian MT x4 (80/20, 500 ops)                               | 236657.60 blocks/sec |          2.11ms
Zipfian MT x16 (80/20, 500 ops)                              | 109317.50 blocks/sec |          4.57ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     389446
                 16 |     504013
                 32 |     615911
                 64 |     773843
                128 |    2931863
                256 |    2104714


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    2270880
       32 |          33 |    2215212
       32 |          37 |    1423338
       32 |          42 |    1080817
       32 |          52 |     531434

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  79.6% (hits: 40589, misses: 10411)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  20.2% (hits: 10301, misses: 40699)
Random (K=100)       | Hit rate:  12.0% (hits: 6118, misses: 44882)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    543478.41 ops/sec |          3.68ms
4 threads, 1000 ops/thread                                   |    183754.96 ops/sec |         21.77ms
8 threads, 1000 ops/thread                                   |    135070.57 ops/sec |         59.23ms
16 threads, 1000 ops/thread                                  |    124027.99 ops/sec |        129.00ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |    566279.23 ops/sec |          7.06ms
8 threads, K=4, 1000 ops/thread                              |    275250.45 ops/sec |         29.06ms
16 threads, K=4, 1000 ops/thread                             |    187872.36 ops/sec |         85.16ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   204.92ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-ebee752303b75e09)
```

## Replacement Clock (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   273.00ns |   291.00ns |    26.00ns |      100
Cold Pin (miss)                                              |     2.35µs |     2.21µs |   361.00ns |      100
Dirty Eviction                                               |   292.26µs |     2.29µs |   889.95µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 358592.17 blocks/sec |        334.64µs
Seq Scan MT x4 (120 blocks)                                  |  76475.11 blocks/sec |          1.57ms
Seq Scan MT x16 (120 blocks)                                 |  41600.65 blocks/sec |          2.88ms
Repeated Access (1000 ops)                                   | 3048250.76 blocks/sec |        328.06µs
Repeated Access MT x4 (1000 ops)                             | 357850.38 blocks/sec |          2.79ms
Repeated Access MT x16 (1000 ops)                            | 458085.83 blocks/sec |          2.18ms
Random (K=10, 500 ops)                                       | 3117634.59 blocks/sec |        160.38µs
Random (K=50, 500 ops)                                       | 389232.58 blocks/sec |          1.28ms
Random (K=100, 500 ops)                                      | 408610.90 blocks/sec |          1.22ms
Random MT x4 (K=10, 500 ops)                                 | 360150.37 blocks/sec |          1.39ms
Random MT x16 (K=10, 500 ops)                                | 449925.13 blocks/sec |          1.11ms
Random MT x4 (K=50, 500 ops)                                 | 100106.17 blocks/sec |          4.99ms
Random MT x16 (K=50, 500 ops)                                | 112698.23 blocks/sec |          4.44ms
Random MT x4 (K=100, 500 ops)                                | 116041.02 blocks/sec |          4.31ms
Random MT x16 (K=100, 500 ops)                               |  92985.89 blocks/sec |          5.38ms
Zipfian (80/20, 500 ops)                                     | 835854.83 blocks/sec |        598.19µs
Zipfian MT x4 (80/20, 500 ops)                               | 264108.83 blocks/sec |          1.89ms
Zipfian MT x16 (80/20, 500 ops)                              | 158671.73 blocks/sec |          3.15ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     231661
                 16 |     243233
                 32 |     292259
                 64 |     507439
                128 |    1979728
                256 |    2081010


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    2229406
       32 |          33 |    1441707
       32 |          37 |     961171
       32 |          42 |     605003
       32 |          52 |     518084

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  70.1% (hits: 35795, misses: 15257)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  25.8% (hits: 13153, misses: 37848)
Random (K=100)       | Hit rate:  11.4% (hits: 5814, misses: 45188)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    492919.58 ops/sec |          4.06ms
4 threads, 1000 ops/thread                                   |    247658.30 ops/sec |         16.15ms
8 threads, 1000 ops/thread                                   |    245714.23 ops/sec |         32.56ms
16 threads, 1000 ops/thread                                  |    198721.19 ops/sec |         80.51ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |    865884.08 ops/sec |          4.62ms
8 threads, K=4, 1000 ops/thread                              |    737186.82 ops/sec |         10.85ms
16 threads, K=4, 1000 ops/thread                             |    563106.52 ops/sec |         28.41ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   205.46ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-7e9b6dadb956fd52)
```

## Replacement SIEVE (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   278.00ns |   291.00ns |    19.00ns |      100
Cold Pin (miss)                                              |     2.35µs |     2.21µs |   660.00ns |      100
Dirty Eviction                                               |   306.15µs |     5.85µs |   904.20µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 438373.78 blocks/sec |        273.74µs
Seq Scan MT x4 (120 blocks)                                  | 141045.88 blocks/sec |        850.79µs
Seq Scan MT x16 (120 blocks)                                 | 107405.33 blocks/sec |          1.12ms
Repeated Access (1000 ops)                                   | 1665009.98 blocks/sec |        600.60µs
Repeated Access MT x4 (1000 ops)                             | 338300.14 blocks/sec |          2.96ms
Repeated Access MT x16 (1000 ops)                            | 345493.55 blocks/sec |          2.89ms
Random (K=10, 500 ops)                                       | 2486411.76 blocks/sec |        201.09µs
Random (K=50, 500 ops)                                       | 240607.18 blocks/sec |          2.08ms
Random (K=100, 500 ops)                                      | 268551.54 blocks/sec |          1.86ms
Random MT x4 (K=10, 500 ops)                                 | 224115.05 blocks/sec |          2.23ms
Random MT x16 (K=10, 500 ops)                                | 184463.93 blocks/sec |          2.71ms
Random MT x4 (K=50, 500 ops)                                 | 142643.34 blocks/sec |          3.51ms
Random MT x16 (K=50, 500 ops)                                | 107071.47 blocks/sec |          4.67ms
Random MT x4 (K=100, 500 ops)                                | 138874.46 blocks/sec |          3.60ms
Random MT x16 (K=100, 500 ops)                               | 105569.78 blocks/sec |          4.74ms
Zipfian (80/20, 500 ops)                                     | 480452.32 blocks/sec |          1.04ms
Zipfian MT x4 (80/20, 500 ops)                               | 195144.11 blocks/sec |          2.56ms
Zipfian MT x16 (80/20, 500 ops)                              | 166715.63 blocks/sec |          3.00ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     329080
                 16 |     232008
                 32 |     288808
                 64 |     448703
                128 |    2179219
                256 |    2529545


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1730140
       32 |          33 |    1348403
       32 |          37 |     838224
       32 |          42 |     615701
       32 |          52 |     503745

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  73.2% (hits: 38252, misses: 13970)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  20.6% (hits: 11218, misses: 43148)
Random (K=100)       | Hit rate:  11.0% (hits: 6016, misses: 48758)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    534204.87 ops/sec |          3.74ms
4 threads, 1000 ops/thread                                   |    212185.47 ops/sec |         18.85ms
8 threads, 1000 ops/thread                                   |    136861.24 ops/sec |         58.45ms
16 threads, 1000 ops/thread                                  |    126281.82 ops/sec |        126.70ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |    899675.40 ops/sec |          4.45ms
8 threads, K=4, 1000 ops/thread                              |    723947.50 ops/sec |         11.05ms
16 threads, K=4, 1000 ops/thread                             |    576252.77 ops/sec |         27.77ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   205.76ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-32828b2194705bfe)
```
