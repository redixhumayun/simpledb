# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`
Note: Pin/Hotset benchmarks use 4096 buffers regardless of `num_buffers`.

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
Pin/Unpin (hit)                                              |   279.00ns |   291.00ns |    23.00ns |      100
Cold Pin (miss)                                              |     2.24µs |     2.21µs |   179.00ns |      100
Dirty Eviction                                               |   313.17µs |     2.92µs |   939.70µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 379481.44 blocks/sec |        316.22µs
Seq Scan MT x2 (120 blocks)                                  | 359881.60 blocks/sec |        333.44µs
Seq Scan MT x4 (120 blocks)                                  | 278381.58 blocks/sec |        431.06µs
Seq Scan MT x8 (120 blocks)                                  | 195471.89 blocks/sec |        613.90µs
Seq Scan MT x16 (120 blocks)                                 | 125151.49 blocks/sec |        958.84µs
Seq Scan MT x32 (120 blocks)                                 |  96488.39 blocks/sec |          1.24ms
Seq Scan MT x64 (120 blocks)                                 |  68798.90 blocks/sec |          1.74ms
Seq Scan MT x128 (120 blocks)                                |  46039.60 blocks/sec |          2.61ms
Seq Scan MT x256 (120 blocks)                                |  44535.41 blocks/sec |          2.69ms
Repeated Access (1000 ops)                                   | 3436839.48 blocks/sec |        290.97µs
Repeated Access MT x2 (1000 ops)                             | 1599900.17 blocks/sec |        625.04µs
Repeated Access MT x4 (1000 ops)                             | 959594.36 blocks/sec |          1.04ms
Repeated Access MT x8 (1000 ops)                             | 472331.75 blocks/sec |          2.12ms
Repeated Access MT x16 (1000 ops)                            | 297456.69 blocks/sec |          3.36ms
Repeated Access MT x32 (1000 ops)                            | 205069.10 blocks/sec |          4.88ms
Repeated Access MT x64 (1000 ops)                            | 167121.91 blocks/sec |          5.98ms
Repeated Access MT x128 (1000 ops)                           | 140627.40 blocks/sec |          7.11ms
Repeated Access MT x256 (1000 ops)                           | 126490.13 blocks/sec |          7.91ms
Random (K=10, 500 ops)                                       | 3453825.80 blocks/sec |        144.77µs
Random (K=50, 500 ops)                                       | 635705.28 blocks/sec |        786.53µs
Random (K=100, 500 ops)                                      | 562960.36 blocks/sec |        888.16µs
Random MT x2 (K=10, 500 ops)                                 | 1695685.50 blocks/sec |        294.87µs
Random MT x4 (K=10, 500 ops)                                 | 976301.26 blocks/sec |        512.14µs
Random MT x8 (K=10, 500 ops)                                 | 481362.14 blocks/sec |          1.04ms
Random MT x16 (K=10, 500 ops)                                | 302313.36 blocks/sec |          1.65ms
Random MT x32 (K=10, 500 ops)                                | 190381.76 blocks/sec |          2.63ms
Random MT x64 (K=10, 500 ops)                                | 156079.09 blocks/sec |          3.20ms
Random MT x128 (K=10, 500 ops)                               | 129201.90 blocks/sec |          3.87ms
Random MT x256 (K=10, 500 ops)                               | 116110.52 blocks/sec |          4.31ms
Random MT x2 (K=50, 500 ops)                                 | 473257.18 blocks/sec |          1.06ms
Random MT x4 (K=50, 500 ops)                                 | 336155.02 blocks/sec |          1.49ms
Random MT x8 (K=50, 500 ops)                                 | 218695.20 blocks/sec |          2.29ms
Random MT x16 (K=50, 500 ops)                                | 138030.53 blocks/sec |          3.62ms
Random MT x32 (K=50, 500 ops)                                | 103328.54 blocks/sec |          4.84ms
Random MT x64 (K=50, 500 ops)                                |  86071.41 blocks/sec |          5.81ms
Random MT x128 (K=50, 500 ops)                               |  69005.35 blocks/sec |          7.25ms
Random MT x256 (K=50, 500 ops)                               |  65894.36 blocks/sec |          7.59ms
Random MT x2 (K=100, 500 ops)                                | 408306.59 blocks/sec |          1.22ms
Random MT x4 (K=100, 500 ops)                                | 304085.76 blocks/sec |          1.64ms
Random MT x8 (K=100, 500 ops)                                | 207241.43 blocks/sec |          2.41ms
Random MT x16 (K=100, 500 ops)                               | 119229.47 blocks/sec |          4.19ms
Random MT x32 (K=100, 500 ops)                               | 100092.00 blocks/sec |          5.00ms
Random MT x64 (K=100, 500 ops)                               |  77757.97 blocks/sec |          6.43ms
Random MT x128 (K=100, 500 ops)                              |  58024.98 blocks/sec |          8.62ms
Random MT x256 (K=100, 500 ops)                              |  48025.66 blocks/sec |         10.41ms
Zipfian (80/20, 500 ops)                                     | 1758130.47 blocks/sec |        284.39µs
Zipfian MT x2 (80/20, 500 ops)                               | 1143934.40 blocks/sec |        437.09µs
Zipfian MT x4 (80/20, 500 ops)                               | 574086.43 blocks/sec |        870.95µs
Zipfian MT x8 (80/20, 500 ops)                               | 317684.01 blocks/sec |          1.57ms
Zipfian MT x16 (80/20, 500 ops)                              | 174956.82 blocks/sec |          2.86ms
Zipfian MT x32 (80/20, 500 ops)                              | 171534.30 blocks/sec |          2.91ms
Zipfian MT x64 (80/20, 500 ops)                              | 147381.94 blocks/sec |          3.39ms
Zipfian MT x128 (80/20, 500 ops)                             | 121370.54 blocks/sec |          4.12ms
Zipfian MT x256 (80/20, 500 ops)                             |  98966.90 blocks/sec |          5.05ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     548507
                 16 |     607135
                 32 |     667075
                 64 |    1081144
                128 |    3280991
                256 |    3394825


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3434302
       32 |          33 |    2963472
       32 |          37 |    1989630
       32 |          42 |    1367503
       32 |          52 |    1038573

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  85.8% (hits: 43751, misses: 7249)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.8% (hits: 11627, misses: 39373)
Random (K=100)       | Hit rate:  11.0% (hits: 5610, misses: 45390)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3364181.76 ops/sec |          2.97ms
2 threads, 5000 ops/thread                                   |   1567219.29 ops/sec |          6.38ms
4 threads, 2500 ops/thread                                   |    910401.97 ops/sec |         10.98ms
8 threads, 1250 ops/thread                                   |    582203.02 ops/sec |         17.18ms
16 threads, 625 ops/thread                                   |    343370.03 ops/sec |         29.12ms
32 threads, 312 ops/thread                                   |    243225.78 ops/sec |         41.11ms
64 threads, 156 ops/thread                                   |    221681.61 ops/sec |         45.11ms
128 threads, 78 ops/thread                                   |    206284.49 ops/sec |         48.48ms
256 threads, 39 ops/thread                                   |    189852.12 ops/sec |         52.67ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3416057.73 ops/sec |          2.93ms
2 threads, K=4, 5000 ops/thread                              |   1574356.38 ops/sec |          6.35ms
4 threads, K=4, 2500 ops/thread                              |   1000349.32 ops/sec |         10.00ms
8 threads, K=4, 1250 ops/thread                              |    489585.56 ops/sec |         20.43ms
16 threads, K=4, 625 ops/thread                              |    329252.86 ops/sec |         30.37ms
32 threads, K=4, 312 ops/thread                              |    260660.05 ops/sec |         38.36ms
64 threads, K=4, 156 ops/thread                              |    232589.24 ops/sec |         42.99ms
128 threads, K=4, 78 ops/thread                              |    218701.18 ops/sec |         45.72ms
256 threads, K=4, 39 ops/thread                              |    217443.62 ops/sec |         45.99ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   199.94ms

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
Pin/Unpin (hit)                                              |   274.00ns |   291.00ns |    22.00ns |      100
Cold Pin (miss)                                              |     2.24µs |     2.17µs |   196.00ns |      100
Dirty Eviction                                               |   784.90µs |     4.25µs |     2.41ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 304436.40 blocks/sec |        394.17µs
Seq Scan MT x2 (120 blocks)                                  | 272519.79 blocks/sec |        440.34µs
Seq Scan MT x4 (120 blocks)                                  | 210470.56 blocks/sec |        570.15µs
Seq Scan MT x8 (120 blocks)                                  | 156062.03 blocks/sec |        768.93µs
Seq Scan MT x16 (120 blocks)                                 | 134826.31 blocks/sec |        890.03µs
Seq Scan MT x32 (120 blocks)                                 | 123951.19 blocks/sec |        968.12µs
Seq Scan MT x64 (120 blocks)                                 | 102657.28 blocks/sec |          1.17ms
Seq Scan MT x128 (120 blocks)                                |  85036.36 blocks/sec |          1.41ms
Seq Scan MT x256 (120 blocks)                                |  56891.75 blocks/sec |          2.11ms
Repeated Access (1000 ops)                                   | 3677768.62 blocks/sec |        271.90µs
Repeated Access MT x2 (1000 ops)                             | 1712897.78 blocks/sec |        583.81µs
Repeated Access MT x4 (1000 ops)                             | 959970.20 blocks/sec |          1.04ms
Repeated Access MT x8 (1000 ops)                             | 651776.19 blocks/sec |          1.53ms
Repeated Access MT x16 (1000 ops)                            | 505917.21 blocks/sec |          1.98ms
Repeated Access MT x32 (1000 ops)                            | 454325.93 blocks/sec |          2.20ms
Repeated Access MT x64 (1000 ops)                            | 400021.60 blocks/sec |          2.50ms
Repeated Access MT x128 (1000 ops)                           | 358081.67 blocks/sec |          2.79ms
Repeated Access MT x256 (1000 ops)                           | 299046.82 blocks/sec |          3.34ms
Random (K=10, 500 ops)                                       | 3639725.42 blocks/sec |        137.37µs
Random (K=50, 500 ops)                                       | 553352.60 blocks/sec |        903.58µs
Random (K=100, 500 ops)                                      | 556281.81 blocks/sec |        898.83µs
Random MT x2 (K=10, 500 ops)                                 | 1907872.65 blocks/sec |        262.07µs
Random MT x4 (K=10, 500 ops)                                 | 1045148.32 blocks/sec |        478.40µs
Random MT x8 (K=10, 500 ops)                                 | 700692.28 blocks/sec |        713.58µs
Random MT x16 (K=10, 500 ops)                                | 532989.38 blocks/sec |        938.11µs
Random MT x32 (K=10, 500 ops)                                | 466054.46 blocks/sec |          1.07ms
Random MT x64 (K=10, 500 ops)                                | 412420.79 blocks/sec |          1.21ms
Random MT x128 (K=10, 500 ops)                               | 318951.80 blocks/sec |          1.57ms
Random MT x256 (K=10, 500 ops)                               | 238713.28 blocks/sec |          2.09ms
Random MT x2 (K=50, 500 ops)                                 | 472817.71 blocks/sec |          1.06ms
Random MT x4 (K=50, 500 ops)                                 | 260852.92 blocks/sec |          1.92ms
Random MT x8 (K=50, 500 ops)                                 | 207311.03 blocks/sec |          2.41ms
Random MT x16 (K=50, 500 ops)                                | 195339.59 blocks/sec |          2.56ms
Random MT x32 (K=50, 500 ops)                                | 197859.48 blocks/sec |          2.53ms
Random MT x64 (K=50, 500 ops)                                | 197027.49 blocks/sec |          2.54ms
Random MT x128 (K=50, 500 ops)                               | 178884.60 blocks/sec |          2.80ms
Random MT x256 (K=50, 500 ops)                               | 157500.36 blocks/sec |          3.17ms
Random MT x2 (K=100, 500 ops)                                | 416776.77 blocks/sec |          1.20ms
Random MT x4 (K=100, 500 ops)                                | 230773.67 blocks/sec |          2.17ms
Random MT x8 (K=100, 500 ops)                                | 180191.54 blocks/sec |          2.77ms
Random MT x16 (K=100, 500 ops)                               | 164664.40 blocks/sec |          3.04ms
Random MT x32 (K=100, 500 ops)                               | 165461.99 blocks/sec |          3.02ms
Random MT x64 (K=100, 500 ops)                               | 164395.16 blocks/sec |          3.04ms
Random MT x128 (K=100, 500 ops)                              | 162017.20 blocks/sec |          3.09ms
Random MT x256 (K=100, 500 ops)                              | 136883.28 blocks/sec |          3.65ms
Zipfian (80/20, 500 ops)                                     | 1467911.46 blocks/sec |        340.62µs
Zipfian MT x2 (80/20, 500 ops)                               | 1163388.63 blocks/sec |        429.78µs
Zipfian MT x4 (80/20, 500 ops)                               | 601718.75 blocks/sec |        830.95µs
Zipfian MT x8 (80/20, 500 ops)                               | 499097.13 blocks/sec |          1.00ms
Zipfian MT x16 (80/20, 500 ops)                              | 371443.71 blocks/sec |          1.35ms
Zipfian MT x32 (80/20, 500 ops)                              | 343511.58 blocks/sec |          1.46ms
Zipfian MT x64 (80/20, 500 ops)                              | 300884.72 blocks/sec |          1.66ms
Zipfian MT x128 (80/20, 500 ops)                             | 244175.92 blocks/sec |          2.05ms
Zipfian MT x256 (80/20, 500 ops)                             | 192015.83 blocks/sec |          2.60ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     557959
                 16 |     596600
                 32 |     694453
                 64 |    1139573
                128 |    3562548
                256 |    3590020


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3641634
       32 |          33 |    2521865
       32 |          37 |    2066756
       32 |          42 |    1400521
       32 |          52 |    1031525

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  74.2% (hits: 37942, misses: 13161)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  25.8% (hits: 13154, misses: 37848)
Random (K=100)       | Hit rate:  12.3% (hits: 6270, misses: 44734)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3507330.85 ops/sec |          2.85ms
2 threads, 5000 ops/thread                                   |   1773461.25 ops/sec |          5.64ms
4 threads, 2500 ops/thread                                   |   1146278.42 ops/sec |          8.72ms
8 threads, 1250 ops/thread                                   |    826504.00 ops/sec |         12.10ms
16 threads, 625 ops/thread                                   |    700413.28 ops/sec |         14.28ms
32 threads, 312 ops/thread                                   |    676358.07 ops/sec |         14.79ms
64 threads, 156 ops/thread                                   |    656034.06 ops/sec |         15.24ms
128 threads, 78 ops/thread                                   |    634925.71 ops/sec |         15.75ms
256 threads, 39 ops/thread                                   |    603419.57 ops/sec |         16.57ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3656927.54 ops/sec |          2.73ms
2 threads, K=4, 5000 ops/thread                              |   1704515.72 ops/sec |          5.87ms
4 threads, K=4, 2500 ops/thread                              |   1076129.49 ops/sec |          9.29ms
8 threads, K=4, 1250 ops/thread                              |    694159.60 ops/sec |         14.41ms
16 threads, K=4, 625 ops/thread                              |    579406.91 ops/sec |         17.26ms
32 threads, K=4, 312 ops/thread                              |    547562.66 ops/sec |         18.26ms
64 threads, K=4, 156 ops/thread                              |    464719.91 ops/sec |         21.52ms
128 threads, K=4, 78 ops/thread                              |    404167.58 ops/sec |         24.74ms
256 threads, K=4, 39 ops/thread                              |    396260.60 ops/sec |         25.24ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   200.91ms

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
Pin/Unpin (hit)                                              |   293.00ns |   292.00ns |    19.00ns |      100
Cold Pin (miss)                                              |     2.05µs |     2.00µs |   188.00ns |      100
Dirty Eviction                                               |   313.73µs |     2.92µs |   941.89µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 379614.69 blocks/sec |        316.11µs
Seq Scan MT x2 (120 blocks)                                  | 359864.33 blocks/sec |        333.46µs
Seq Scan MT x4 (120 blocks)                                  | 269900.07 blocks/sec |        444.61µs
Seq Scan MT x8 (120 blocks)                                  | 193098.34 blocks/sec |        621.45µs
Seq Scan MT x16 (120 blocks)                                 | 119797.90 blocks/sec |          1.00ms
Seq Scan MT x32 (120 blocks)                                 |  91610.61 blocks/sec |          1.31ms
Seq Scan MT x64 (120 blocks)                                 |  69205.79 blocks/sec |          1.73ms
Seq Scan MT x128 (120 blocks)                                |  42312.97 blocks/sec |          2.84ms
Seq Scan MT x256 (120 blocks)                                |  36993.27 blocks/sec |          3.24ms
Repeated Access (1000 ops)                                   | 3495855.66 blocks/sec |        286.05µs
Repeated Access MT x2 (1000 ops)                             | 1752660.98 blocks/sec |        570.56µs
Repeated Access MT x4 (1000 ops)                             | 975028.54 blocks/sec |          1.03ms
Repeated Access MT x8 (1000 ops)                             | 639545.26 blocks/sec |          1.56ms
Repeated Access MT x16 (1000 ops)                            | 511264.96 blocks/sec |          1.96ms
Repeated Access MT x32 (1000 ops)                            | 462476.93 blocks/sec |          2.16ms
Repeated Access MT x64 (1000 ops)                            | 398720.43 blocks/sec |          2.51ms
Repeated Access MT x128 (1000 ops)                           | 347229.34 blocks/sec |          2.88ms
Repeated Access MT x256 (1000 ops)                           | 330021.35 blocks/sec |          3.03ms
Random (K=10, 500 ops)                                       | 3496576.85 blocks/sec |        143.00µs
Random (K=50, 500 ops)                                       | 624604.94 blocks/sec |        800.51µs
Random (K=100, 500 ops)                                      | 549229.65 blocks/sec |        910.37µs
Random MT x2 (K=10, 500 ops)                                 | 1926938.21 blocks/sec |        259.48µs
Random MT x4 (K=10, 500 ops)                                 | 1016582.49 blocks/sec |        491.84µs
Random MT x8 (K=10, 500 ops)                                 | 683803.70 blocks/sec |        731.20µs
Random MT x16 (K=10, 500 ops)                                | 525893.97 blocks/sec |        950.76µs
Random MT x32 (K=10, 500 ops)                                | 439000.45 blocks/sec |          1.14ms
Random MT x64 (K=10, 500 ops)                                | 380690.18 blocks/sec |          1.31ms
Random MT x128 (K=10, 500 ops)                               | 298790.73 blocks/sec |          1.67ms
Random MT x256 (K=10, 500 ops)                               | 188504.11 blocks/sec |          2.65ms
Random MT x2 (K=50, 500 ops)                                 | 476899.47 blocks/sec |          1.05ms
Random MT x4 (K=50, 500 ops)                                 | 337419.03 blocks/sec |          1.48ms
Random MT x8 (K=50, 500 ops)                                 | 229028.02 blocks/sec |          2.18ms
Random MT x16 (K=50, 500 ops)                                | 170216.76 blocks/sec |          2.94ms
Random MT x32 (K=50, 500 ops)                                | 117870.40 blocks/sec |          4.24ms
Random MT x64 (K=50, 500 ops)                                |  98472.82 blocks/sec |          5.08ms
Random MT x128 (K=50, 500 ops)                               |  87563.50 blocks/sec |          5.71ms
Random MT x256 (K=50, 500 ops)                               |  82212.29 blocks/sec |          6.08ms
Random MT x2 (K=100, 500 ops)                                | 430250.39 blocks/sec |          1.16ms
Random MT x4 (K=100, 500 ops)                                | 305897.02 blocks/sec |          1.63ms
Random MT x8 (K=100, 500 ops)                                | 208504.05 blocks/sec |          2.40ms
Random MT x16 (K=100, 500 ops)                               | 143976.79 blocks/sec |          3.47ms
Random MT x32 (K=100, 500 ops)                               | 106400.21 blocks/sec |          4.70ms
Random MT x64 (K=100, 500 ops)                               |  85266.74 blocks/sec |          5.86ms
Random MT x128 (K=100, 500 ops)                              |  70739.17 blocks/sec |          7.07ms
Random MT x256 (K=100, 500 ops)                              |  63103.45 blocks/sec |          7.92ms
Zipfian (80/20, 500 ops)                                     | 1377698.91 blocks/sec |        362.92µs
Zipfian MT x2 (80/20, 500 ops)                               | 1137638.31 blocks/sec |        439.51µs
Zipfian MT x4 (80/20, 500 ops)                               | 585462.95 blocks/sec |        854.03µs
Zipfian MT x8 (80/20, 500 ops)                               | 430699.21 blocks/sec |          1.16ms
Zipfian MT x16 (80/20, 500 ops)                              | 331674.74 blocks/sec |          1.51ms
Zipfian MT x32 (80/20, 500 ops)                              | 269011.73 blocks/sec |          1.86ms
Zipfian MT x64 (80/20, 500 ops)                              | 230958.08 blocks/sec |          2.16ms
Zipfian MT x128 (80/20, 500 ops)                             | 198190.36 blocks/sec |          2.52ms
Zipfian MT x256 (80/20, 500 ops)                             | 165143.28 blocks/sec |          3.03ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     537771
                 16 |     554626
                 32 |     671290
                 64 |    1064423
                128 |    3511063
                256 |    3523981


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3530974
       32 |          33 |    2938739
       32 |          37 |    1812133
       32 |          42 |    1411237
       32 |          52 |    1010089

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  68.9% (hits: 36107, misses: 16269)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.5% (hits: 12242, misses: 42123)
Random (K=100)       | Hit rate:  10.4% (hits: 5711, misses: 49114)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3458000.00 ops/sec |          2.89ms
2 threads, 5000 ops/thread                                   |   1740361.70 ops/sec |          5.75ms
4 threads, 2500 ops/thread                                   |   1106012.52 ops/sec |          9.04ms
8 threads, 1250 ops/thread                                   |    766277.65 ops/sec |         13.05ms
16 threads, 625 ops/thread                                   |    650105.83 ops/sec |         15.38ms
32 threads, 312 ops/thread                                   |    656876.92 ops/sec |         15.22ms
64 threads, 156 ops/thread                                   |    626705.70 ops/sec |         15.96ms
128 threads, 78 ops/thread                                   |    609801.83 ops/sec |         16.40ms
256 threads, 39 ops/thread                                   |    584065.58 ops/sec |         17.12ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3483085.09 ops/sec |          2.87ms
2 threads, K=4, 5000 ops/thread                              |   1708090.13 ops/sec |          5.85ms
4 threads, K=4, 2500 ops/thread                              |   1044509.58 ops/sec |          9.57ms
8 threads, K=4, 1250 ops/thread                              |    670243.35 ops/sec |         14.92ms
16 threads, K=4, 625 ops/thread                              |    532332.58 ops/sec |         18.79ms
32 threads, K=4, 312 ops/thread                              |    465611.08 ops/sec |         21.48ms
64 threads, K=4, 156 ops/thread                              |    425784.99 ops/sec |         23.49ms
128 threads, K=4, 78 ops/thread                              |    393826.84 ops/sec |         25.39ms
256 threads, K=4, 39 ops/thread                              |    385893.17 ops/sec |         25.91ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   204.30ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-32828b2194705bfe)
```
