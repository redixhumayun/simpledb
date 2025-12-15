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
Pin/Unpin (hit)                                              |   265.00ns |   250.00ns |    21.00ns |      100
Cold Pin (miss)                                              |     2.21µs |     2.17µs |   161.00ns |      100
Dirty Eviction                                               |   342.02µs |     2.90µs |     1.09ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 359399.80 blocks/sec |        333.89µs
Seq Scan MT x2 (120 blocks)                                  | 331227.28 blocks/sec |        362.29µs
Seq Scan MT x4 (120 blocks)                                  | 251291.53 blocks/sec |        477.53µs
Seq Scan MT x8 (120 blocks)                                  | 180739.50 blocks/sec |        663.94µs
Seq Scan MT x16 (120 blocks)                                 | 121073.68 blocks/sec |        991.13µs
Seq Scan MT x32 (120 blocks)                                 |  97469.29 blocks/sec |          1.23ms
Seq Scan MT x64 (120 blocks)                                 |  83091.62 blocks/sec |          1.44ms
Seq Scan MT x128 (120 blocks)                                |  60183.77 blocks/sec |          1.99ms
Seq Scan MT x256 (120 blocks)                                |  29411.48 blocks/sec |          4.08ms
Repeated Access (1000 ops)                                   | 3549409.56 blocks/sec |        281.74µs
Repeated Access MT x2 (1000 ops)                             | 1556279.74 blocks/sec |        642.56µs
Repeated Access MT x4 (1000 ops)                             | 906263.37 blocks/sec |          1.10ms
Repeated Access MT x8 (1000 ops)                             | 467541.84 blocks/sec |          2.14ms
Repeated Access MT x16 (1000 ops)                            | 301676.78 blocks/sec |          3.31ms
Repeated Access MT x32 (1000 ops)                            | 203845.92 blocks/sec |          4.91ms
Repeated Access MT x64 (1000 ops)                            | 157316.93 blocks/sec |          6.36ms
Repeated Access MT x128 (1000 ops)                           | 172643.47 blocks/sec |          5.79ms
Repeated Access MT x256 (1000 ops)                           | 148341.78 blocks/sec |          6.74ms
Random (K=10, 500 ops)                                       | 3487893.52 blocks/sec |        143.35µs
Random (K=50, 500 ops)                                       | 653819.55 blocks/sec |        764.74µs
Random (K=100, 500 ops)                                      | 581102.09 blocks/sec |        860.43µs
Random MT x2 (K=10, 500 ops)                                 | 1607536.13 blocks/sec |        311.04µs
Random MT x4 (K=10, 500 ops)                                 | 869436.69 blocks/sec |        575.09µs
Random MT x8 (K=10, 500 ops)                                 | 492410.96 blocks/sec |          1.02ms
Random MT x16 (K=10, 500 ops)                                | 285853.78 blocks/sec |          1.75ms
Random MT x32 (K=10, 500 ops)                                | 207283.61 blocks/sec |          2.41ms
Random MT x64 (K=10, 500 ops)                                | 199437.59 blocks/sec |          2.51ms
Random MT x128 (K=10, 500 ops)                               | 164852.68 blocks/sec |          3.03ms
Random MT x256 (K=10, 500 ops)                               | 118030.31 blocks/sec |          4.24ms
Random MT x2 (K=50, 500 ops)                                 | 454283.62 blocks/sec |          1.10ms
Random MT x4 (K=50, 500 ops)                                 | 317408.73 blocks/sec |          1.58ms
Random MT x8 (K=50, 500 ops)                                 | 209489.98 blocks/sec |          2.39ms
Random MT x16 (K=50, 500 ops)                                | 136782.48 blocks/sec |          3.66ms
Random MT x32 (K=50, 500 ops)                                | 101805.15 blocks/sec |          4.91ms
Random MT x64 (K=50, 500 ops)                                |  85512.23 blocks/sec |          5.85ms
Random MT x128 (K=50, 500 ops)                               |  72274.07 blocks/sec |          6.92ms
Random MT x256 (K=50, 500 ops)                               |  76464.81 blocks/sec |          6.54ms
Random MT x2 (K=100, 500 ops)                                | 413178.41 blocks/sec |          1.21ms
Random MT x4 (K=100, 500 ops)                                | 297366.82 blocks/sec |          1.68ms
Random MT x8 (K=100, 500 ops)                                | 199179.38 blocks/sec |          2.51ms
Random MT x16 (K=100, 500 ops)                               | 131699.38 blocks/sec |          3.80ms
Random MT x32 (K=100, 500 ops)                               |  98816.06 blocks/sec |          5.06ms
Random MT x64 (K=100, 500 ops)                               |  81341.17 blocks/sec |          6.15ms
Random MT x128 (K=100, 500 ops)                              |  62822.52 blocks/sec |          7.96ms
Random MT x256 (K=100, 500 ops)                              |  60516.03 blocks/sec |          8.26ms
Zipfian (80/20, 500 ops)                                     | 1726096.16 blocks/sec |        289.67µs
Zipfian MT x2 (80/20, 500 ops)                               | 1160868.52 blocks/sec |        430.71µs
Zipfian MT x4 (80/20, 500 ops)                               | 508860.79 blocks/sec |        982.59µs
Zipfian MT x8 (80/20, 500 ops)                               | 295441.57 blocks/sec |          1.69ms
Zipfian MT x16 (80/20, 500 ops)                              | 234903.68 blocks/sec |          2.13ms
Zipfian MT x32 (80/20, 500 ops)                              | 162955.73 blocks/sec |          3.07ms
Zipfian MT x64 (80/20, 500 ops)                              | 143767.52 blocks/sec |          3.48ms
Zipfian MT x128 (80/20, 500 ops)                             | 135493.64 blocks/sec |          3.69ms
Zipfian MT x256 (80/20, 500 ops)                             | 116216.85 blocks/sec |          4.30ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     551882
                 16 |     580442
                 32 |     707289
                 64 |    1006200
                128 |    3482306
                256 |    3490645


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3514197
       32 |          33 |    3212810
       32 |          37 |    1858978
       32 |          42 |    1419301
       32 |          52 |     972398

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  74.2% (hits: 37837, misses: 13163)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  28.0% (hits: 14277, misses: 36723)
Random (K=100)       | Hit rate:  11.6% (hits: 5915, misses: 45085)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3376328.63 ops/sec |          2.96ms
2 threads, 5000 ops/thread                                   |   1248657.38 ops/sec |          8.01ms
4 threads, 2500 ops/thread                                   |    305276.76 ops/sec |         32.76ms
8 threads, 1250 ops/thread                                   |    238287.58 ops/sec |         41.97ms
16 threads, 625 ops/thread                                   |    111963.28 ops/sec |         89.31ms
32 threads, 312 ops/thread                                   |     86227.88 ops/sec |        115.97ms
64 threads, 156 ops/thread                                   |     62411.15 ops/sec |        160.23ms
128 threads, 78 ops/thread                                   |     42798.24 ops/sec |        233.65ms
256 threads, 39 ops/thread                                   |     30825.58 ops/sec |        324.41ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3359957.64 ops/sec |          2.98ms
2 threads, K=4, 5000 ops/thread                              |   1581087.41 ops/sec |          6.32ms
4 threads, K=4, 2500 ops/thread                              |    964088.66 ops/sec |         10.37ms
8 threads, K=4, 1250 ops/thread                              |    473342.94 ops/sec |         21.13ms
16 threads, K=4, 625 ops/thread                              |    320581.92 ops/sec |         31.19ms
32 threads, K=4, 312 ops/thread                              |    255898.45 ops/sec |         39.08ms
64 threads, K=4, 156 ops/thread                              |    230548.37 ops/sec |         43.37ms
128 threads, K=4, 78 ops/thread                              |    217909.45 ops/sec |         45.89ms
256 threads, K=4, 39 ops/thread                              |    210510.64 ops/sec |         47.50ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   200.80ms

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
Pin/Unpin (hit)                                              |   270.00ns |   270.00ns |    21.00ns |      100
Cold Pin (miss)                                              |     2.26µs |     2.17µs |   193.00ns |      100
Dirty Eviction                                               |   302.96µs |     2.88µs |   904.85µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 380276.33 blocks/sec |        315.56µs
Seq Scan MT x2 (120 blocks)                                  | 319885.69 blocks/sec |        375.13µs
Seq Scan MT x4 (120 blocks)                                  | 196867.19 blocks/sec |        609.55µs
Seq Scan MT x8 (120 blocks)                                  | 147165.95 blocks/sec |        815.41µs
Seq Scan MT x16 (120 blocks)                                 | 125723.57 blocks/sec |        954.48µs
Seq Scan MT x32 (120 blocks)                                 | 111756.19 blocks/sec |          1.07ms
Seq Scan MT x64 (120 blocks)                                 |  91359.94 blocks/sec |          1.31ms
Seq Scan MT x128 (120 blocks)                                |  61032.24 blocks/sec |          1.97ms
Seq Scan MT x256 (120 blocks)                                |  30029.54 blocks/sec |          4.00ms
Repeated Access (1000 ops)                                   | 3826154.83 blocks/sec |        261.36µs
Repeated Access MT x2 (1000 ops)                             | 1770522.57 blocks/sec |        564.81µs
Repeated Access MT x4 (1000 ops)                             | 1017770.27 blocks/sec |        982.54µs
Repeated Access MT x8 (1000 ops)                             | 687847.79 blocks/sec |          1.45ms
Repeated Access MT x16 (1000 ops)                            | 538650.60 blocks/sec |          1.86ms
Repeated Access MT x32 (1000 ops)                            | 485667.00 blocks/sec |          2.06ms
Repeated Access MT x64 (1000 ops)                            | 456631.01 blocks/sec |          2.19ms
Repeated Access MT x128 (1000 ops)                           | 487303.07 blocks/sec |          2.05ms
Repeated Access MT x256 (1000 ops)                           | 275540.09 blocks/sec |          3.63ms
Random (K=10, 500 ops)                                       | 3864823.92 blocks/sec |        129.37µs
Random (K=50, 500 ops)                                       | 639498.22 blocks/sec |        781.86µs
Random (K=100, 500 ops)                                      | 558722.89 blocks/sec |        894.90µs
Random MT x2 (K=10, 500 ops)                                 | 1885476.18 blocks/sec |        265.19µs
Random MT x4 (K=10, 500 ops)                                 | 1089203.60 blocks/sec |        459.05µs
Random MT x8 (K=10, 500 ops)                                 | 745542.03 blocks/sec |        670.65µs
Random MT x16 (K=10, 500 ops)                                | 569071.64 blocks/sec |        878.62µs
Random MT x32 (K=10, 500 ops)                                | 515860.12 blocks/sec |        969.26µs
Random MT x64 (K=10, 500 ops)                                | 488069.63 blocks/sec |          1.02ms
Random MT x128 (K=10, 500 ops)                               | 254489.58 blocks/sec |          1.96ms
Random MT x256 (K=10, 500 ops)                               | 124171.00 blocks/sec |          4.03ms
Random MT x2 (K=50, 500 ops)                                 | 458978.77 blocks/sec |          1.09ms
Random MT x4 (K=50, 500 ops)                                 | 259844.87 blocks/sec |          1.92ms
Random MT x8 (K=50, 500 ops)                                 | 201922.71 blocks/sec |          2.48ms
Random MT x16 (K=50, 500 ops)                                | 190608.35 blocks/sec |          2.62ms
Random MT x32 (K=50, 500 ops)                                | 193747.46 blocks/sec |          2.58ms
Random MT x64 (K=50, 500 ops)                                | 192750.65 blocks/sec |          2.59ms
Random MT x128 (K=50, 500 ops)                               | 168861.53 blocks/sec |          2.96ms
Random MT x256 (K=50, 500 ops)                               | 108795.86 blocks/sec |          4.60ms
Random MT x2 (K=100, 500 ops)                                | 414704.77 blocks/sec |          1.21ms
Random MT x4 (K=100, 500 ops)                                | 225515.96 blocks/sec |          2.22ms
Random MT x8 (K=100, 500 ops)                                | 174288.48 blocks/sec |          2.87ms
Random MT x16 (K=100, 500 ops)                               | 163644.53 blocks/sec |          3.06ms
Random MT x32 (K=100, 500 ops)                               | 161115.23 blocks/sec |          3.10ms
Random MT x64 (K=100, 500 ops)                               | 160755.06 blocks/sec |          3.11ms
Random MT x128 (K=100, 500 ops)                              | 146427.29 blocks/sec |          3.41ms
Random MT x256 (K=100, 500 ops)                              | 106451.81 blocks/sec |          4.70ms
Zipfian (80/20, 500 ops)                                     | 1402792.12 blocks/sec |        356.43µs
Zipfian MT x2 (80/20, 500 ops)                               | 1096493.63 blocks/sec |        456.00µs
Zipfian MT x4 (80/20, 500 ops)                               | 586820.02 blocks/sec |        852.05µs
Zipfian MT x8 (80/20, 500 ops)                               | 415991.03 blocks/sec |          1.20ms
Zipfian MT x16 (80/20, 500 ops)                              | 346211.48 blocks/sec |          1.44ms
Zipfian MT x32 (80/20, 500 ops)                              | 331403.02 blocks/sec |          1.51ms
Zipfian MT x64 (80/20, 500 ops)                              | 295312.62 blocks/sec |          1.69ms
Zipfian MT x128 (80/20, 500 ops)                             | 239938.77 blocks/sec |          2.08ms
Zipfian MT x256 (80/20, 500 ops)                             | 121542.98 blocks/sec |          4.11ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     542070
                 16 |     599628
                 32 |     704077
                 64 |    1100655
                128 |    3735469
                256 |    3758918


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3875338
       32 |          33 |    3029826
       32 |          37 |    1927310
       32 |          42 |    1438216
       32 |          52 |    1047050

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  73.2% (hits: 37324, misses: 13677)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.6% (hits: 12034, misses: 38967)
Random (K=100)       | Hit rate:  12.4% (hits: 6358, misses: 44713)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3593018.76 ops/sec |          2.78ms
2 threads, 5000 ops/thread                                   |   1432513.91 ops/sec |          6.98ms
4 threads, 2500 ops/thread                                   |    336149.80 ops/sec |         29.75ms
8 threads, 1250 ops/thread                                   |    181819.85 ops/sec |         55.00ms
16 threads, 625 ops/thread                                   |    158629.18 ops/sec |         63.04ms
32 threads, 312 ops/thread                                   |    153273.75 ops/sec |         65.24ms
64 threads, 156 ops/thread                                   |    148917.88 ops/sec |         67.15ms
128 threads, 78 ops/thread                                   |    142626.37 ops/sec |         70.11ms
256 threads, 39 ops/thread                                   |    136051.43 ops/sec |         73.50ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3680229.29 ops/sec |          2.72ms
2 threads, K=4, 5000 ops/thread                              |   1792951.08 ops/sec |          5.58ms
4 threads, K=4, 2500 ops/thread                              |   1023773.45 ops/sec |          9.77ms
8 threads, K=4, 1250 ops/thread                              |    674190.68 ops/sec |         14.83ms
16 threads, K=4, 625 ops/thread                              |    519251.43 ops/sec |         19.26ms
32 threads, K=4, 312 ops/thread                              |    446564.99 ops/sec |         22.39ms
64 threads, K=4, 156 ops/thread                              |    411777.60 ops/sec |         24.28ms
128 threads, K=4, 78 ops/thread                              |    390667.09 ops/sec |         25.60ms
256 threads, K=4, 39 ops/thread                              |    380435.68 ops/sec |         26.29ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   203.68ms

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
Pin/Unpin (hit)                                              |   274.00ns |   291.00ns |    21.00ns |      100
Cold Pin (miss)                                              |     2.61µs |     2.38µs |   947.00ns |      100
Dirty Eviction                                               |   302.00µs |     2.88µs |   902.01µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 467688.57 blocks/sec |        256.58µs
Seq Scan MT x2 (120 blocks)                                  | 314381.38 blocks/sec |        381.70µs
Seq Scan MT x4 (120 blocks)                                  | 246361.55 blocks/sec |        487.09µs
Seq Scan MT x8 (120 blocks)                                  | 186223.50 blocks/sec |        644.39µs
Seq Scan MT x16 (120 blocks)                                 | 119449.81 blocks/sec |          1.00ms
Seq Scan MT x32 (120 blocks)                                 |  94171.42 blocks/sec |          1.27ms
Seq Scan MT x64 (120 blocks)                                 |  75792.41 blocks/sec |          1.58ms
Seq Scan MT x128 (120 blocks)                                |  56011.92 blocks/sec |          2.14ms
Seq Scan MT x256 (120 blocks)                                |  29757.41 blocks/sec |          4.03ms
Repeated Access (1000 ops)                                   | 3744392.77 blocks/sec |        267.07µs
Repeated Access MT x2 (1000 ops)                             | 1692872.33 blocks/sec |        590.71µs
Repeated Access MT x4 (1000 ops)                             | 915711.50 blocks/sec |          1.09ms
Repeated Access MT x8 (1000 ops)                             | 655723.95 blocks/sec |          1.53ms
Repeated Access MT x16 (1000 ops)                            | 518422.40 blocks/sec |          1.93ms
Repeated Access MT x32 (1000 ops)                            | 461812.28 blocks/sec |          2.17ms
Repeated Access MT x64 (1000 ops)                            | 447183.79 blocks/sec |          2.24ms
Repeated Access MT x128 (1000 ops)                           | 475753.24 blocks/sec |          2.10ms
Repeated Access MT x256 (1000 ops)                           | 270134.84 blocks/sec |          3.70ms
Random (K=10, 500 ops)                                       | 3817726.47 blocks/sec |        130.97µs
Random (K=50, 500 ops)                                       | 644964.06 blocks/sec |        775.24µs
Random (K=100, 500 ops)                                      | 563969.99 blocks/sec |        886.57µs
Random MT x2 (K=10, 500 ops)                                 | 1858687.69 blocks/sec |        269.01µs
Random MT x4 (K=10, 500 ops)                                 | 1053836.28 blocks/sec |        474.46µs
Random MT x8 (K=10, 500 ops)                                 | 749527.42 blocks/sec |        667.09µs
Random MT x16 (K=10, 500 ops)                                | 573105.31 blocks/sec |        872.44µs
Random MT x32 (K=10, 500 ops)                                | 529152.60 blocks/sec |        944.91µs
Random MT x64 (K=10, 500 ops)                                | 465888.14 blocks/sec |          1.07ms
Random MT x128 (K=10, 500 ops)                               | 252626.30 blocks/sec |          1.98ms
Random MT x256 (K=10, 500 ops)                               | 124246.85 blocks/sec |          4.02ms
Random MT x2 (K=50, 500 ops)                                 | 466745.33 blocks/sec |          1.07ms
Random MT x4 (K=50, 500 ops)                                 | 324708.01 blocks/sec |          1.54ms
Random MT x8 (K=50, 500 ops)                                 | 220494.23 blocks/sec |          2.27ms
Random MT x16 (K=50, 500 ops)                                | 168472.41 blocks/sec |          2.97ms
Random MT x32 (K=50, 500 ops)                                | 120045.80 blocks/sec |          4.17ms
Random MT x64 (K=50, 500 ops)                                | 103936.99 blocks/sec |          4.81ms
Random MT x128 (K=50, 500 ops)                               |  90978.70 blocks/sec |          5.50ms
Random MT x256 (K=50, 500 ops)                               | 108715.86 blocks/sec |          4.60ms
Random MT x2 (K=100, 500 ops)                                | 405449.90 blocks/sec |          1.23ms
Random MT x4 (K=100, 500 ops)                                | 285793.49 blocks/sec |          1.75ms
Random MT x8 (K=100, 500 ops)                                | 203444.89 blocks/sec |          2.46ms
Random MT x16 (K=100, 500 ops)                               | 142478.68 blocks/sec |          3.51ms
Random MT x32 (K=100, 500 ops)                               | 105783.23 blocks/sec |          4.73ms
Random MT x64 (K=100, 500 ops)                               |  88630.86 blocks/sec |          5.64ms
Random MT x128 (K=100, 500 ops)                              |  72886.25 blocks/sec |          6.86ms
Random MT x256 (K=100, 500 ops)                              |  67247.49 blocks/sec |          7.44ms
Zipfian (80/20, 500 ops)                                     | 1312766.66 blocks/sec |        380.88µs
Zipfian MT x2 (80/20, 500 ops)                               | 893385.20 blocks/sec |        559.67µs
Zipfian MT x4 (80/20, 500 ops)                               | 549576.17 blocks/sec |        909.79µs
Zipfian MT x8 (80/20, 500 ops)                               | 381477.29 blocks/sec |          1.31ms
Zipfian MT x16 (80/20, 500 ops)                              | 331586.32 blocks/sec |          1.51ms
Zipfian MT x32 (80/20, 500 ops)                              | 271448.95 blocks/sec |          1.84ms
Zipfian MT x64 (80/20, 500 ops)                              | 253843.70 blocks/sec |          1.97ms
Zipfian MT x128 (80/20, 500 ops)                             | 233904.23 blocks/sec |          2.14ms
Zipfian MT x256 (80/20, 500 ops)                             | 123467.61 blocks/sec |          4.05ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     530771
                 16 |     581719
                 32 |     687750
                 64 |    1108249
                128 |    3727949
                256 |    3767273


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3855139
       32 |          33 |    3103836
       32 |          37 |    1865658
       32 |          42 |    1427397
       32 |          52 |    1023535

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  70.8% (hits: 36991, misses: 15284)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  19.4% (hits: 10555, misses: 43862)
Random (K=100)       | Hit rate:   8.5% (hits: 4690, misses: 50186)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3616112.09 ops/sec |          2.77ms
2 threads, 5000 ops/thread                                   |   1412091.31 ops/sec |          7.08ms
4 threads, 2500 ops/thread                                   |    320872.65 ops/sec |         31.17ms
8 threads, 1250 ops/thread                                   |    234577.46 ops/sec |         42.63ms
16 threads, 625 ops/thread                                   |    111003.94 ops/sec |         90.09ms
32 threads, 312 ops/thread                                   |     80948.54 ops/sec |        123.54ms
64 threads, 156 ops/thread                                   |     56577.28 ops/sec |        176.75ms
128 threads, 78 ops/thread                                   |     38754.51 ops/sec |        258.03ms
256 threads, 39 ops/thread                                   |     26579.45 ops/sec |        376.23ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3585294.70 ops/sec |          2.79ms
2 threads, K=4, 5000 ops/thread                              |   1743985.08 ops/sec |          5.73ms
4 threads, K=4, 2500 ops/thread                              |   1018666.87 ops/sec |          9.82ms
8 threads, K=4, 1250 ops/thread                              |    646069.83 ops/sec |         15.48ms
16 threads, K=4, 625 ops/thread                              |    495190.54 ops/sec |         20.19ms
32 threads, K=4, 312 ops/thread                              |    435175.38 ops/sec |         22.98ms
64 threads, K=4, 156 ops/thread                              |    409359.45 ops/sec |         24.43ms
128 threads, K=4, 78 ops/thread                              |    393970.24 ops/sec |         25.38ms
256 threads, K=4, 39 ops/thread                              |    385027.72 ops/sec |         25.97ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   200.69ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-32828b2194705bfe)
```
