# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 5 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   266.00ns |   250.00ns |    22.00ns |        5
Cold Pin (miss)                                              |     2.50µs |     2.46µs |   513.00ns |        5
Dirty Eviction                                               |     3.20ms |     3.03ms |   426.32µs |        5

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 409010.50 blocks/sec |        293.39µs
Seq Scan MT x4 (120 blocks)                                  | 153777.15 blocks/sec |        780.35µs
Seq Scan MT x16 (120 blocks)                                 |  77561.17 blocks/sec |          1.55ms
Seq Scan MT x64 (120 blocks)                                 |  52412.25 blocks/sec |          2.29ms
Seq Scan MT x128 (120 blocks)                                |  49428.32 blocks/sec |          2.43ms
Seq Scan MT x256 (120 blocks)                                |  27041.14 blocks/sec |          4.44ms
Repeated Access (1000 ops)                                   | 3508464.17 blocks/sec |        285.03µs
Repeated Access MT x4 (1000 ops)                             | 650741.72 blocks/sec |          1.54ms
Repeated Access MT x16 (1000 ops)                            | 319317.12 blocks/sec |          3.13ms
Repeated Access MT x64 (1000 ops)                            | 255787.19 blocks/sec |          3.91ms
Repeated Access MT x128 (1000 ops)                           | 249893.30 blocks/sec |          4.00ms
Repeated Access MT x256 (1000 ops)                           | 185760.25 blocks/sec |          5.38ms
Random (K=10, 500 ops)                                       | 3410059.68 blocks/sec |        146.63µs
Random (K=50, 500 ops)                                       | 586178.15 blocks/sec |        852.98µs
Random (K=100, 500 ops)                                      | 526741.06 blocks/sec |        949.23µs
Random MT x4 (K=10, 500 ops)                                 | 922296.52 blocks/sec |        542.13µs
Random MT x16 (K=10, 500 ops)                                | 284732.69 blocks/sec |          1.76ms
Random MT x64 (K=10, 500 ops)                                | 362557.66 blocks/sec |          1.38ms
Random MT x128 (K=10, 500 ops)                               | 178405.77 blocks/sec |          2.80ms
Random MT x256 (K=10, 500 ops)                               | 124710.05 blocks/sec |          4.01ms
Random MT x4 (K=50, 500 ops)                                 | 305957.11 blocks/sec |          1.63ms
Random MT x16 (K=50, 500 ops)                                | 152989.83 blocks/sec |          3.27ms
Random MT x64 (K=50, 500 ops)                                |  82845.13 blocks/sec |          6.04ms
Random MT x128 (K=50, 500 ops)                               |  71477.85 blocks/sec |          7.00ms
Random MT x256 (K=50, 500 ops)                               | 103015.98 blocks/sec |          4.85ms
Random MT x4 (K=100, 500 ops)                                | 284469.45 blocks/sec |          1.76ms
Random MT x16 (K=100, 500 ops)                               | 133584.19 blocks/sec |          3.74ms
Random MT x64 (K=100, 500 ops)                               |  78238.90 blocks/sec |          6.39ms
Random MT x128 (K=100, 500 ops)                              |  58259.22 blocks/sec |          8.58ms
Random MT x256 (K=100, 500 ops)                              |  54952.75 blocks/sec |          9.10ms
Zipfian (80/20, 500 ops)                                     | 1629816.52 blocks/sec |        306.78µs
Zipfian MT x4 (80/20, 500 ops)                               | 535972.89 blocks/sec |        932.88µs
Zipfian MT x16 (80/20, 500 ops)                              | 221910.76 blocks/sec |          2.25ms
Zipfian MT x64 (80/20, 500 ops)                              | 133242.73 blocks/sec |          3.75ms
Zipfian MT x128 (80/20, 500 ops)                             | 130298.29 blocks/sec |          3.84ms
Zipfian MT x256 (80/20, 500 ops)                             | 126053.87 blocks/sec |          3.97ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     489652
                 16 |     564823
                 32 |     716042
                 64 |    1129050
                128 |    3459825
                256 |    3579124


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3484733
       32 |          33 |    2852400
       32 |          37 |    1878936
       32 |          42 |    1328198
       32 |          52 |    1028402

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 840)
Repeated Access      | Hit rate:  99.9% (hits: 6990, misses: 10)
Zipfian (80/20)      | Hit rate:  78.0% (hits: 2731, misses: 769)
Random (K=10)        | Hit rate:  99.7% (hits: 3490, misses: 10)
Random (K=50)        | Hit rate:  23.2% (hits: 811, misses: 2689)
Random (K=100)       | Hit rate:  11.6% (hits: 406, misses: 3094)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1092041.04 ops/sec |          1.83ms
4 threads, 1000 ops/thread                                   |    320604.66 ops/sec |         12.48ms
8 threads, 1000 ops/thread                                   |    217043.03 ops/sec |         36.86ms
16 threads, 1000 ops/thread                                  |    104419.89 ops/sec |        153.23ms
64 threads, 1000 ops/thread                                  |     61969.53 ops/sec |           1.03s
128 threads, 1000 ops/thread                                 |     37593.59 ops/sec |           3.40s
256 threads, 1000 ops/thread                                 |     26227.37 ops/sec |           9.76s

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1016174.19 ops/sec |          3.94ms
8 threads, K=4, 1000 ops/thread                              |    505316.12 ops/sec |         15.83ms
16 threads, K=4, 1000 ops/thread                             |    336751.78 ops/sec |         47.51ms
64 threads, K=4, 1000 ops/thread                             |    235751.36 ops/sec |        271.47ms
128 threads, K=4, 1000 ops/thread                            |    225270.05 ops/sec |        568.21ms
256 threads, K=4, 1000 ops/thread                            |    214840.04 ops/sec |           1.19s

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   202.59ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-595f081dbf33478a)
```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 5 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   275.00ns |   291.00ns |    22.00ns |        5
Cold Pin (miss)                                              |     3.50µs |     2.79µs |     1.76µs |        5
Dirty Eviction                                               |     2.99ms |     2.97ms |    58.86µs |        5

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 299887.54 blocks/sec |        400.15µs
Seq Scan MT x4 (120 blocks)                                  | 139934.84 blocks/sec |        857.54µs
Seq Scan MT x16 (120 blocks)                                 | 111188.33 blocks/sec |          1.08ms
Seq Scan MT x64 (120 blocks)                                 |  79311.37 blocks/sec |          1.51ms
Seq Scan MT x128 (120 blocks)                                |  58117.23 blocks/sec |          2.06ms
Seq Scan MT x256 (120 blocks)                                |  29720.88 blocks/sec |          4.04ms
Repeated Access (1000 ops)                                   | 3784896.75 blocks/sec |        264.21µs
Repeated Access MT x4 (1000 ops)                             | 996943.37 blocks/sec |          1.00ms
Repeated Access MT x16 (1000 ops)                            | 552303.10 blocks/sec |          1.81ms
Repeated Access MT x64 (1000 ops)                            | 586003.31 blocks/sec |          1.71ms
Repeated Access MT x128 (1000 ops)                           | 503858.80 blocks/sec |          1.98ms
Repeated Access MT x256 (1000 ops)                           | 246886.70 blocks/sec |          4.05ms
Random (K=10, 500 ops)                                       | 3895871.16 blocks/sec |        128.34µs
Random (K=50, 500 ops)                                       | 588143.27 blocks/sec |        850.13µs
Random (K=100, 500 ops)                                      | 529586.40 blocks/sec |        944.13µs
Random MT x4 (K=10, 500 ops)                                 | 1100110.01 blocks/sec |        454.50µs
Random MT x16 (K=10, 500 ops)                                | 610240.57 blocks/sec |        819.35µs
Random MT x64 (K=10, 500 ops)                                | 531218.09 blocks/sec |        941.23µs
Random MT x128 (K=10, 500 ops)                               | 278147.40 blocks/sec |          1.80ms
Random MT x256 (K=10, 500 ops)                               | 127109.22 blocks/sec |          3.93ms
Random MT x4 (K=50, 500 ops)                                 | 221664.81 blocks/sec |          2.26ms
Random MT x16 (K=50, 500 ops)                                | 179593.44 blocks/sec |          2.78ms
Random MT x64 (K=50, 500 ops)                                | 191030.51 blocks/sec |          2.62ms
Random MT x128 (K=50, 500 ops)                               | 175140.06 blocks/sec |          2.85ms
Random MT x256 (K=50, 500 ops)                               | 114771.93 blocks/sec |          4.36ms
Random MT x4 (K=100, 500 ops)                                | 210820.75 blocks/sec |          2.37ms
Random MT x16 (K=100, 500 ops)                               | 153642.12 blocks/sec |          3.25ms
Random MT x64 (K=100, 500 ops)                               | 149638.64 blocks/sec |          3.34ms
Random MT x128 (K=100, 500 ops)                              | 147921.73 blocks/sec |          3.38ms
Random MT x256 (K=100, 500 ops)                              | 108712.20 blocks/sec |          4.60ms
Zipfian (80/20, 500 ops)                                     | 1573772.14 blocks/sec |        317.71µs
Zipfian MT x4 (80/20, 500 ops)                               | 559519.44 blocks/sec |        893.62µs
Zipfian MT x16 (80/20, 500 ops)                              | 399268.22 blocks/sec |          1.25ms
Zipfian MT x64 (80/20, 500 ops)                              | 290146.73 blocks/sec |          1.72ms
Zipfian MT x128 (80/20, 500 ops)                             | 227694.58 blocks/sec |          2.20ms
Zipfian MT x256 (80/20, 500 ops)                             | 115755.93 blocks/sec |          4.32ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     492692
                 16 |     559472
                 32 |     733102
                 64 |    1146657
                128 |    3771706
                256 |    3862256


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3666361
       32 |          33 |    3226327
       32 |          37 |    2090450
       32 |          42 |    1474491
       32 |          52 |    1022374

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 910)
Repeated Access      | Hit rate:  99.9% (hits: 6990, misses: 10)
Zipfian (80/20)      | Hit rate:  78.5% (hits: 2758, misses: 754)
Random (K=10)        | Hit rate:  99.7% (hits: 3490, misses: 10)
Random (K=50)        | Hit rate:  22.0% (hits: 774, misses: 2743)
Random (K=100)       | Hit rate:   9.6% (hits: 336, misses: 3165)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1517067.39 ops/sec |          1.32ms
4 threads, 1000 ops/thread                                   |    317262.66 ops/sec |         12.61ms
8 threads, 1000 ops/thread                                   |    163242.38 ops/sec |         49.01ms
16 threads, 1000 ops/thread                                  |    149282.73 ops/sec |        107.18ms
64 threads, 1000 ops/thread                                  |    140667.59 ops/sec |        454.97ms
128 threads, 1000 ops/thread                                 |    138044.12 ops/sec |        927.24ms
256 threads, 1000 ops/thread                                 |    123440.75 ops/sec |           2.07s

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1031355.53 ops/sec |          3.88ms
8 threads, K=4, 1000 ops/thread                              |    693746.42 ops/sec |         11.53ms
16 threads, K=4, 1000 ops/thread                             |    528606.13 ops/sec |         30.27ms
64 threads, K=4, 1000 ops/thread                             |    447771.04 ops/sec |        142.93ms
128 threads, K=4, 1000 ops/thread                            |    455414.48 ops/sec |        281.06ms
256 threads, K=4, 1000 ops/thread                            |    436780.74 ops/sec |        586.11ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   201.14ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-2ed923e46913521e)
```

## Replacement SIEVE (`--no-default-features --features replacement_sieve`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 5 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   275.00ns |   291.00ns |    22.00ns |        5
Cold Pin (miss)                                              |     3.01µs |     2.50µs |     1.53µs |        5
Dirty Eviction                                               |     3.41ms |     3.04ms |   899.46µs |        5

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 290065.26 blocks/sec |        413.70µs
Seq Scan MT x4 (120 blocks)                                  | 139255.56 blocks/sec |        861.73µs
Seq Scan MT x16 (120 blocks)                                 |  97227.00 blocks/sec |          1.23ms
Seq Scan MT x64 (120 blocks)                                 |  90771.01 blocks/sec |          1.32ms
Seq Scan MT x128 (120 blocks)                                |  59876.77 blocks/sec |          2.00ms
Seq Scan MT x256 (120 blocks)                                |  31895.10 blocks/sec |          3.76ms
Repeated Access (1000 ops)                                   | 3672986.65 blocks/sec |        272.26µs
Repeated Access MT x4 (1000 ops)                             | 904016.09 blocks/sec |          1.11ms
Repeated Access MT x16 (1000 ops)                            | 567067.66 blocks/sec |          1.76ms
Repeated Access MT x64 (1000 ops)                            | 796881.96 blocks/sec |          1.25ms
Repeated Access MT x128 (1000 ops)                           | 500644.83 blocks/sec |          2.00ms
Repeated Access MT x256 (1000 ops)                           | 239472.72 blocks/sec |          4.18ms
Random (K=10, 500 ops)                                       | 3871976.95 blocks/sec |        129.13µs
Random (K=50, 500 ops)                                       | 586281.25 blocks/sec |        852.83µs
Random (K=100, 500 ops)                                      | 520034.86 blocks/sec |        961.47µs
Random MT x4 (K=10, 500 ops)                                 | 1114722.81 blocks/sec |        448.54µs
Random MT x16 (K=10, 500 ops)                                | 556302.86 blocks/sec |        898.79µs
Random MT x64 (K=10, 500 ops)                                | 587578.59 blocks/sec |        850.95µs
Random MT x128 (K=10, 500 ops)                               | 282042.42 blocks/sec |          1.77ms
Random MT x256 (K=10, 500 ops)                               | 132770.10 blocks/sec |          3.77ms
Random MT x4 (K=50, 500 ops)                                 | 324678.91 blocks/sec |          1.54ms
Random MT x16 (K=50, 500 ops)                                | 169299.27 blocks/sec |          2.95ms
Random MT x64 (K=50, 500 ops)                                |  92441.23 blocks/sec |          5.41ms
Random MT x128 (K=50, 500 ops)                               |  91548.68 blocks/sec |          5.46ms
Random MT x256 (K=50, 500 ops)                               | 116549.00 blocks/sec |          4.29ms
Random MT x4 (K=100, 500 ops)                                | 284974.82 blocks/sec |          1.75ms
Random MT x16 (K=100, 500 ops)                               | 144959.43 blocks/sec |          3.45ms
Random MT x64 (K=100, 500 ops)                               |  83666.84 blocks/sec |          5.98ms
Random MT x128 (K=100, 500 ops)                              |  68760.42 blocks/sec |          7.27ms
Random MT x256 (K=100, 500 ops)                              |  76809.25 blocks/sec |          6.51ms
Zipfian (80/20, 500 ops)                                     | 1204772.83 blocks/sec |        415.02µs
Zipfian MT x4 (80/20, 500 ops)                               | 518618.40 blocks/sec |        964.10µs
Zipfian MT x16 (80/20, 500 ops)                              | 326244.69 blocks/sec |          1.53ms
Zipfian MT x64 (80/20, 500 ops)                              | 232414.91 blocks/sec |          2.15ms
Zipfian MT x128 (80/20, 500 ops)                             | 243390.96 blocks/sec |          2.05ms
Zipfian MT x256 (80/20, 500 ops)                             | 121480.38 blocks/sec |          4.12ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     478835
                 16 |     554216
                 32 |     701025
                 64 |    1142794
                128 |    3796767
                256 |    3898149


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3702579
       32 |          33 |    2910784
       32 |          37 |    1783434
       32 |          42 |    1358696
       32 |          52 |    1068091

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 910)
Repeated Access      | Hit rate:  99.9% (hits: 6990, misses: 10)
Zipfian (80/20)      | Hit rate:  68.5% (hits: 2467, misses: 1132)
Random (K=10)        | Hit rate:  99.7% (hits: 3490, misses: 10)
Random (K=50)        | Hit rate:  25.4% (hits: 946, misses: 2774)
Random (K=100)       | Hit rate:  10.8% (hits: 406, misses: 3353)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1158508.26 ops/sec |          1.73ms
4 threads, 1000 ops/thread                                   |    304649.91 ops/sec |         13.13ms
8 threads, 1000 ops/thread                                   |    208738.99 ops/sec |         38.33ms
16 threads, 1000 ops/thread                                  |    104922.19 ops/sec |        152.49ms
64 threads, 1000 ops/thread                                  |     55421.48 ops/sec |           1.15s
128 threads, 1000 ops/thread                                 |     34885.21 ops/sec |           3.67s
256 threads, 1000 ops/thread                                 |     22433.01 ops/sec |          11.41s

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1064854.14 ops/sec |          3.76ms
8 threads, K=4, 1000 ops/thread                              |    709426.90 ops/sec |         11.28ms
16 threads, K=4, 1000 ops/thread                             |    534321.69 ops/sec |         29.94ms
64 threads, K=4, 1000 ops/thread                             |    392325.84 ops/sec |        163.13ms
128 threads, K=4, 1000 ops/thread                            |    395190.90 ops/sec |        323.89ms
256 threads, K=4, 1000 ops/thread                            |    393776.33 ops/sec |        650.12ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   205.70ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-83da990292f15006)
```
