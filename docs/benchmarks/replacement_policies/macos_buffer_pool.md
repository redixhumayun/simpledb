# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_lru -- 100 12
   Compiling simpledb v0.1.0 (/Users/zaidhumayun/Desktop/Development.nosync/databases/worktree-replacement-policy)
    Finished `bench` profile [optimized + debuginfo] target(s) in 4.72s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-595f081dbf33478a)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   918.00ns |   834.00ns |   682.00ns |      100
Cold Pin (miss)                                              |     7.03µs |     5.00µs |     8.10µs |      100
Dirty Eviction                                               |     4.79ms |     3.88ms |     2.86ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 157524.35 blocks/sec |        761.79µs
Seq Scan MT x4 (120 blocks)                                  | 129231.11 blocks/sec |        928.57µs
Repeated Access (1000 ops)                                   | 3530836.56 blocks/sec |        283.22µs
Repeated Access MT x4 (1000 ops)                             | 948025.45 blocks/sec |          1.05ms
Random (K=10, 500 ops)                                       | 3500493.57 blocks/sec |        142.84µs
Random (K=50, 500 ops)                                       | 571241.20 blocks/sec |        875.29µs
Random (K=100, 500 ops)                                      | 461704.39 blocks/sec |          1.08ms
Random MT x4 (K=10, 500 ops)                                 | 803205.11 blocks/sec |        622.51µs
Random MT x4 (K=50, 500 ops)                                 | 283673.14 blocks/sec |          1.76ms
Random MT x4 (K=100, 500 ops)                                | 267496.26 blocks/sec |          1.87ms
Zipfian (80/20, 500 ops)                                     | 1741438.22 blocks/sec |        287.12µs
Zipfian MT x4 (80/20, 500 ops)                               | 487346.06 blocks/sec |          1.03ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     449090
                 16 |     561824
                 32 |     688338
                 64 |    1130544
                128 |    3579047
                256 |    3569109


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3567632
       32 |          33 |    2811722
       32 |          37 |    1935419
       32 |          42 |    1324928
       32 |          52 |    1081481

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  81.8% (hits: 41715, misses: 9285)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.6% (hits: 11524, misses: 39476)
Random (K=100)       | Hit rate:  14.6% (hits: 7445, misses: 43555)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1338457.84 ops/sec |          1.49ms
4 threads, 1000 ops/thread                                   |    267386.01 ops/sec |         14.96ms
8 threads, 1000 ops/thread                                   |    189329.08 ops/sec |         42.25ms
16 threads, 1000 ops/thread                                  |    103851.42 ops/sec |        154.07ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1009215.91 ops/sec |          3.96ms
8 threads, K=4, 1000 ops/thread                              |    501245.03 ops/sec |         15.96ms
16 threads, K=4, 1000 ops/thread                             |    334846.14 ops/sec |         47.78ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   201.76ms

All benchmarks completed!

```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_clock -- 100 12
   Compiling simpledb v0.1.0 (/Users/zaidhumayun/Desktop/Development.nosync/databases/worktree-replacement-policy)
    Finished `bench` profile [optimized + debuginfo] target(s) in 4.34s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-2ed923e46913521e)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   973.00ns |   833.00ns |   956.00ns |      100
Cold Pin (miss)                                              |     7.38µs |     4.88µs |     9.99µs |      100
Dirty Eviction                                               |     3.19ms |     3.02ms |   881.31µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 250515.64 blocks/sec |        479.01µs
Seq Scan MT x4 (120 blocks)                                  | 149881.22 blocks/sec |        800.63µs
Repeated Access (1000 ops)                                   | 3792490.11 blocks/sec |        263.68µs
Repeated Access MT x4 (1000 ops)                             | 1040688.85 blocks/sec |        960.90µs
Random (K=10, 500 ops)                                       | 3797920.26 blocks/sec |        131.65µs
Random (K=50, 500 ops)                                       | 600825.77 blocks/sec |        832.19µs
Random (K=100, 500 ops)                                      | 530123.75 blocks/sec |        943.18µs
Random MT x4 (K=10, 500 ops)                                 | 984833.56 blocks/sec |        507.70µs
Random MT x4 (K=50, 500 ops)                                 | 244544.94 blocks/sec |          2.04ms
Random MT x4 (K=100, 500 ops)                                | 207721.08 blocks/sec |          2.41ms
Zipfian (80/20, 500 ops)                                     | 1532008.25 blocks/sec |        326.37µs
Zipfian MT x4 (80/20, 500 ops)                               | 544178.60 blocks/sec |        918.82µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     492132
                 16 |     576842
                 32 |     708032
                 64 |    1288686
                128 |    3868861
                256 |    3849352


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3877863
       32 |          33 |    2642469
       32 |          37 |    1924402
       32 |          42 |    1401848
       32 |          52 |    1063373

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  76.1% (hits: 38918, misses: 12237)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  25.8% (hits: 13157, misses: 37844)
Random (K=100)       | Hit rate:  10.4% (hits: 5304, misses: 45700)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1501241.15 ops/sec |          1.33ms
4 threads, 1000 ops/thread                                   |    323502.71 ops/sec |         12.36ms
8 threads, 1000 ops/thread                                   |    160235.55 ops/sec |         49.93ms
16 threads, 1000 ops/thread                                  |    135162.23 ops/sec |        118.38ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1087798.38 ops/sec |          3.68ms
8 threads, K=4, 1000 ops/thread                              |    703756.20 ops/sec |         11.37ms
16 threads, K=4, 1000 ops/thread                             |    538364.49 ops/sec |         29.72ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   201.98ms

All benchmarks completed!

```

## Replacement SIEVE (`--no-default-features --features replacement_sieve`)

```
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_sieve -- 100 12
   Compiling simpledb v0.1.0 (/Users/zaidhumayun/Desktop/Development.nosync/databases/worktree-replacement-policy)
    Finished `bench` profile [optimized + debuginfo] target(s) in 4.54s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-83da990292f15006)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |     1.32µs |     1.33µs |    24.00ns |      100
Cold Pin (miss)                                              |    10.14µs |     7.67µs |     7.48µs |      100
Dirty Eviction                                               |     3.66ms |     3.20ms |     1.17ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 282319.06 blocks/sec |        425.05µs
Seq Scan MT x4 (120 blocks)                                  | 172428.91 blocks/sec |        695.94µs
Repeated Access (1000 ops)                                   | 3798858.06 blocks/sec |        263.24µs
Repeated Access MT x4 (1000 ops)                             | 1068048.58 blocks/sec |        936.29µs
Random (K=10, 500 ops)                                       | 3772588.37 blocks/sec |        132.54µs
Random (K=50, 500 ops)                                       | 580767.98 blocks/sec |        860.93µs
Random (K=100, 500 ops)                                      | 499095.64 blocks/sec |          1.00ms
Random MT x4 (K=10, 500 ops)                                 | 952696.70 blocks/sec |        524.83µs
Random MT x4 (K=50, 500 ops)                                 | 269577.53 blocks/sec |          1.85ms
Random MT x4 (K=100, 500 ops)                                | 270751.33 blocks/sec |          1.85ms
Zipfian (80/20, 500 ops)                                     | 1260677.94 blocks/sec |        396.61µs
Zipfian MT x4 (80/20, 500 ops)                               | 547297.45 blocks/sec |        913.58µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     474507
                 16 |     561871
                 32 |     668126
                 64 |    1161087
                128 |    3853654
                256 |    3822513


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3857281
       32 |          33 |    3129166
       32 |          37 |    1925462
       32 |          42 |    1476695
       32 |          52 |    1117461

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  69.8% (hits: 36513, misses: 15813)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  21.4% (hits: 11624, misses: 42742)
Random (K=100)       | Hit rate:  13.1% (hits: 7139, misses: 47533)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1294717.10 ops/sec |          1.54ms
4 threads, 1000 ops/thread                                   |    259400.45 ops/sec |         15.42ms
8 threads, 1000 ops/thread                                   |    190970.03 ops/sec |         41.89ms
16 threads, 1000 ops/thread                                  |    101593.91 ops/sec |        157.49ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1099422.72 ops/sec |          3.64ms
8 threads, K=4, 1000 ops/thread                              |    714614.05 ops/sec |         11.19ms
16 threads, K=4, 1000 ops/thread                             |    554275.66 ops/sec |         28.87ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   201.24ms

All benchmarks completed!
➜  worktree-replacement-policy git:(feature/replacement-policy) ✗
```
