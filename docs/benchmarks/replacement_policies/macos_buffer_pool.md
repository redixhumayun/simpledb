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
Pin/Unpin (hit)                                              |   282.00ns |   291.00ns |    25.00ns |      100
Cold Pin (miss)                                              |     2.27µs |     2.21µs |   165.00ns |      100
Dirty Eviction                                               |   303.02µs |     2.88µs |   905.09µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 386225.90 blocks/sec |        310.70µs
Seq Scan MT x2 (120 blocks)                                  | 361724.70 blocks/sec |        331.74µs
Seq Scan MT x4 (120 blocks)                                  | 276206.85 blocks/sec |        434.46µs
Seq Scan MT x8 (120 blocks)                                  | 199153.27 blocks/sec |        602.55µs
Seq Scan MT x16 (120 blocks)                                 | 122262.84 blocks/sec |        981.49µs
Seq Scan MT x32 (120 blocks)                                 |  91876.30 blocks/sec |          1.31ms
Seq Scan MT x64 (120 blocks)                                 |  69082.52 blocks/sec |          1.74ms
Seq Scan MT x128 (120 blocks)                                |  55098.75 blocks/sec |          2.18ms
Seq Scan MT x256 (120 blocks)                                |  42579.13 blocks/sec |          2.82ms
Repeated Access (1000 ops)                                   | 3451108.15 blocks/sec |        289.76µs
Repeated Access MT x2 (1000 ops)                             | 1509531.94 blocks/sec |        662.46µs
Repeated Access MT x4 (1000 ops)                             | 923448.86 blocks/sec |          1.08ms
Repeated Access MT x8 (1000 ops)                             | 467926.23 blocks/sec |          2.14ms
Repeated Access MT x16 (1000 ops)                            | 302174.78 blocks/sec |          3.31ms
Repeated Access MT x32 (1000 ops)                            | 210573.35 blocks/sec |          4.75ms
Repeated Access MT x64 (1000 ops)                            | 164428.47 blocks/sec |          6.08ms
Repeated Access MT x128 (1000 ops)                           | 140997.61 blocks/sec |          7.09ms
Repeated Access MT x256 (1000 ops)                           | 134386.51 blocks/sec |          7.44ms
Random (K=10, 500 ops)                                       | 3446540.71 blocks/sec |        145.07µs
Random (K=50, 500 ops)                                       | 641213.89 blocks/sec |        779.77µs
Random (K=100, 500 ops)                                      | 554192.24 blocks/sec |        902.21µs
Random MT x2 (K=10, 500 ops)                                 | 1687695.35 blocks/sec |        296.26µs
Random MT x4 (K=10, 500 ops)                                 | 895245.53 blocks/sec |        558.51µs
Random MT x8 (K=10, 500 ops)                                 | 465079.07 blocks/sec |          1.08ms
Random MT x16 (K=10, 500 ops)                                | 304145.32 blocks/sec |          1.64ms
Random MT x32 (K=10, 500 ops)                                | 194016.45 blocks/sec |          2.58ms
Random MT x64 (K=10, 500 ops)                                | 154091.56 blocks/sec |          3.24ms
Random MT x128 (K=10, 500 ops)                               | 128094.18 blocks/sec |          3.90ms
Random MT x256 (K=10, 500 ops)                               | 116291.92 blocks/sec |          4.30ms
Random MT x2 (K=50, 500 ops)                                 | 485962.96 blocks/sec |          1.03ms
Random MT x4 (K=50, 500 ops)                                 | 329841.19 blocks/sec |          1.52ms
Random MT x8 (K=50, 500 ops)                                 | 222029.60 blocks/sec |          2.25ms
Random MT x16 (K=50, 500 ops)                                | 138866.28 blocks/sec |          3.60ms
Random MT x32 (K=50, 500 ops)                                | 101047.52 blocks/sec |          4.95ms
Random MT x64 (K=50, 500 ops)                                |  84149.63 blocks/sec |          5.94ms
Random MT x128 (K=50, 500 ops)                               |  70126.87 blocks/sec |          7.13ms
Random MT x256 (K=50, 500 ops)                               |  59323.09 blocks/sec |          8.43ms
Random MT x2 (K=100, 500 ops)                                | 419004.71 blocks/sec |          1.19ms
Random MT x4 (K=100, 500 ops)                                | 302836.12 blocks/sec |          1.65ms
Random MT x8 (K=100, 500 ops)                                | 206197.22 blocks/sec |          2.42ms
Random MT x16 (K=100, 500 ops)                               | 133311.04 blocks/sec |          3.75ms
Random MT x32 (K=100, 500 ops)                               |  94873.98 blocks/sec |          5.27ms
Random MT x64 (K=100, 500 ops)                               |  74180.92 blocks/sec |          6.74ms
Random MT x128 (K=100, 500 ops)                              |  56896.55 blocks/sec |          8.79ms
Random MT x256 (K=100, 500 ops)                              |  53070.45 blocks/sec |          9.42ms
Zipfian (80/20, 500 ops)                                     | 1724732.67 blocks/sec |        289.90µs
Zipfian MT x2 (80/20, 500 ops)                               | 1200304.40 blocks/sec |        416.56µs
Zipfian MT x4 (80/20, 500 ops)                               | 568921.42 blocks/sec |        878.86µs
Zipfian MT x8 (80/20, 500 ops)                               | 325759.64 blocks/sec |          1.53ms
Zipfian MT x16 (80/20, 500 ops)                              | 241477.77 blocks/sec |          2.07ms
Zipfian MT x32 (80/20, 500 ops)                              | 164947.31 blocks/sec |          3.03ms
Zipfian MT x64 (80/20, 500 ops)                              | 145251.55 blocks/sec |          3.44ms
Zipfian MT x128 (80/20, 500 ops)                             | 120887.26 blocks/sec |          4.14ms
Zipfian MT x256 (80/20, 500 ops)                             | 100308.61 blocks/sec |          4.98ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     544765
                 16 |     591736
                 32 |     635012
                 64 |     666686
                128 |    3297066
                256 |    3284029


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3349141
       32 |          33 |    2684448
       32 |          37 |    1683292
       32 |          42 |    1353012
       32 |          52 |     891727

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  80.4% (hits: 40996, misses: 10004)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  24.0% (hits: 12239, misses: 38761)
Random (K=100)       | Hit rate:  10.6% (hits: 5405, misses: 45595)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   2762713.32 ops/sec |          3.62ms
2 threads, 5000 ops/thread                                   |   1595995.46 ops/sec |          6.27ms
4 threads, 2500 ops/thread                                   |    921388.77 ops/sec |         10.85ms
8 threads, 1250 ops/thread                                   |    560120.22 ops/sec |         17.85ms
16 threads, 625 ops/thread                                   |    357565.05 ops/sec |         27.97ms
32 threads, 312 ops/thread                                   |    239345.37 ops/sec |         41.78ms
64 threads, 156 ops/thread                                   |    218216.01 ops/sec |         45.83ms
128 threads, 78 ops/thread                                   |    205989.82 ops/sec |         48.55ms
256 threads, 39 ops/thread                                   |    190824.06 ops/sec |         52.40ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3430071.64 ops/sec |          2.92ms
2 threads, K=4, 5000 ops/thread                              |   1499428.19 ops/sec |          6.67ms
4 threads, K=4, 2500 ops/thread                              |    967032.23 ops/sec |         10.34ms
8 threads, K=4, 1250 ops/thread                              |    462334.51 ops/sec |         21.63ms
16 threads, K=4, 625 ops/thread                              |    318482.70 ops/sec |         31.40ms
32 threads, K=4, 312 ops/thread                              |    258579.21 ops/sec |         38.67ms
64 threads, K=4, 156 ops/thread                              |    228609.42 ops/sec |         43.74ms
128 threads, K=4, 78 ops/thread                              |    223414.57 ops/sec |         44.76ms
256 threads, K=4, 39 ops/thread                              |    215000.84 ops/sec |         46.51ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   201.08ms

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
Pin/Unpin (hit)                                              |   269.00ns |   250.00ns |    20.00ns |      100
Cold Pin (miss)                                              |     2.22µs |     2.17µs |   175.00ns |      100
Dirty Eviction                                               |   313.16µs |     2.92µs |   939.66µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 402367.26 blocks/sec |        298.24µs
Seq Scan MT x2 (120 blocks)                                  | 350824.88 blocks/sec |        342.05µs
Seq Scan MT x4 (120 blocks)                                  | 216865.64 blocks/sec |        553.34µs
Seq Scan MT x8 (120 blocks)                                  | 155760.81 blocks/sec |        770.41µs
Seq Scan MT x16 (120 blocks)                                 | 138663.08 blocks/sec |        865.41µs
Seq Scan MT x32 (120 blocks)                                 | 122752.73 blocks/sec |        977.58µs
Seq Scan MT x64 (120 blocks)                                 | 102153.31 blocks/sec |          1.17ms
Seq Scan MT x128 (120 blocks)                                |  80672.00 blocks/sec |          1.49ms
Seq Scan MT x256 (120 blocks)                                |  56747.57 blocks/sec |          2.11ms
Repeated Access (1000 ops)                                   | 3649794.88 blocks/sec |        273.99µs
Repeated Access MT x2 (1000 ops)                             | 1702098.69 blocks/sec |        587.51µs
Repeated Access MT x4 (1000 ops)                             | 1009499.39 blocks/sec |        990.59µs
Repeated Access MT x8 (1000 ops)                             | 666345.49 blocks/sec |          1.50ms
Repeated Access MT x16 (1000 ops)                            | 508479.92 blocks/sec |          1.97ms
Repeated Access MT x32 (1000 ops)                            | 463217.30 blocks/sec |          2.16ms
Repeated Access MT x64 (1000 ops)                            | 414018.16 blocks/sec |          2.42ms
Repeated Access MT x128 (1000 ops)                           | 347011.61 blocks/sec |          2.88ms
Repeated Access MT x256 (1000 ops)                           | 316346.58 blocks/sec |          3.16ms
Random (K=10, 500 ops)                                       | 3679121.72 blocks/sec |        135.90µs
Random (K=50, 500 ops)                                       | 622190.81 blocks/sec |        803.61µs
Random (K=100, 500 ops)                                      | 569573.39 blocks/sec |        877.85µs
Random MT x2 (K=10, 500 ops)                                 | 1911877.73 blocks/sec |        261.52µs
Random MT x4 (K=10, 500 ops)                                 | 1077298.31 blocks/sec |        464.12µs
Random MT x8 (K=10, 500 ops)                                 | 688082.55 blocks/sec |        726.66µs
Random MT x16 (K=10, 500 ops)                                | 535571.02 blocks/sec |        933.58µs
Random MT x32 (K=10, 500 ops)                                | 448278.12 blocks/sec |          1.12ms
Random MT x64 (K=10, 500 ops)                                | 379099.77 blocks/sec |          1.32ms
Random MT x128 (K=10, 500 ops)                               | 268694.27 blocks/sec |          1.86ms
Random MT x256 (K=10, 500 ops)                               | 218876.62 blocks/sec |          2.28ms
Random MT x2 (K=50, 500 ops)                                 | 428615.39 blocks/sec |          1.17ms
Random MT x4 (K=50, 500 ops)                                 | 272304.31 blocks/sec |          1.84ms
Random MT x8 (K=50, 500 ops)                                 | 211040.01 blocks/sec |          2.37ms
Random MT x16 (K=50, 500 ops)                                | 194736.51 blocks/sec |          2.57ms
Random MT x32 (K=50, 500 ops)                                | 201355.12 blocks/sec |          2.48ms
Random MT x64 (K=50, 500 ops)                                | 197147.28 blocks/sec |          2.54ms
Random MT x128 (K=50, 500 ops)                               | 181534.13 blocks/sec |          2.75ms
Random MT x256 (K=50, 500 ops)                               | 155126.79 blocks/sec |          3.22ms
Random MT x2 (K=100, 500 ops)                                | 398255.64 blocks/sec |          1.26ms
Random MT x4 (K=100, 500 ops)                                | 224621.18 blocks/sec |          2.23ms
Random MT x8 (K=100, 500 ops)                                | 175840.19 blocks/sec |          2.84ms
Random MT x16 (K=100, 500 ops)                               | 167970.45 blocks/sec |          2.98ms
Random MT x32 (K=100, 500 ops)                               | 165763.97 blocks/sec |          3.02ms
Random MT x64 (K=100, 500 ops)                               | 166808.40 blocks/sec |          3.00ms
Random MT x128 (K=100, 500 ops)                              | 158299.88 blocks/sec |          3.16ms
Random MT x256 (K=100, 500 ops)                              | 141826.91 blocks/sec |          3.53ms
Zipfian (80/20, 500 ops)                                     | 1573059.16 blocks/sec |        317.85µs
Zipfian MT x2 (80/20, 500 ops)                               | 1165520.18 blocks/sec |        428.99µs
Zipfian MT x4 (80/20, 500 ops)                               | 634104.15 blocks/sec |        788.51µs
Zipfian MT x8 (80/20, 500 ops)                               | 447806.38 blocks/sec |          1.12ms
Zipfian MT x16 (80/20, 500 ops)                              | 387544.33 blocks/sec |          1.29ms
Zipfian MT x32 (80/20, 500 ops)                              | 331846.66 blocks/sec |          1.51ms
Zipfian MT x64 (80/20, 500 ops)                              | 284464.11 blocks/sec |          1.76ms
Zipfian MT x128 (80/20, 500 ops)                             | 251959.62 blocks/sec |          1.98ms
Zipfian MT x256 (80/20, 500 ops)                             | 190240.36 blocks/sec |          2.63ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     552413
                 16 |     612380
                 32 |     686084
                 64 |    1112954
                128 |    3507123
                256 |    3574492


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3661153
       32 |          33 |    2498488
       32 |          37 |    1892191
       32 |          42 |    1464562
       32 |          52 |    1052301

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  81.2% (hits: 41498, misses: 9605)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  24.8% (hits: 12647, misses: 38355)
Random (K=100)       | Hit rate:  10.8% (hits: 5507, misses: 45499)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3507735.61 ops/sec |          2.85ms
2 threads, 5000 ops/thread                                   |   1772580.10 ops/sec |          5.64ms
4 threads, 2500 ops/thread                                   |   1169694.75 ops/sec |          8.55ms
8 threads, 1250 ops/thread                                   |    823904.77 ops/sec |         12.14ms
16 threads, 625 ops/thread                                   |    694843.11 ops/sec |         14.39ms
32 threads, 312 ops/thread                                   |    675133.70 ops/sec |         14.81ms
64 threads, 156 ops/thread                                   |    658459.06 ops/sec |         15.19ms
128 threads, 78 ops/thread                                   |    639726.95 ops/sec |         15.63ms
256 threads, 39 ops/thread                                   |    625828.17 ops/sec |         15.98ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3622484.91 ops/sec |          2.76ms
2 threads, K=4, 5000 ops/thread                              |   1655097.81 ops/sec |          6.04ms
4 threads, K=4, 2500 ops/thread                              |   1038962.01 ops/sec |          9.62ms
8 threads, K=4, 1250 ops/thread                              |    690526.71 ops/sec |         14.48ms
16 threads, K=4, 625 ops/thread                              |    547942.53 ops/sec |         18.25ms
32 threads, K=4, 312 ops/thread                              |    476596.10 ops/sec |         20.98ms
64 threads, K=4, 156 ops/thread                              |    435111.02 ops/sec |         22.98ms
128 threads, K=4, 78 ops/thread                              |    403130.49 ops/sec |         24.81ms
256 threads, K=4, 39 ops/thread                              |    398695.07 ops/sec |         25.08ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   196.79ms

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
Pin/Unpin (hit)                                              |   272.00ns |   291.00ns |    23.00ns |      100
Cold Pin (miss)                                              |     2.26µs |     2.21µs |   203.00ns |      100
Dirty Eviction                                               |   292.65µs |     2.92µs |   880.76µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 382039.07 blocks/sec |        314.10µs
Seq Scan MT x2 (120 blocks)                                  | 353593.24 blocks/sec |        339.37µs
Seq Scan MT x4 (120 blocks)                                  | 279951.29 blocks/sec |        428.65µs
Seq Scan MT x8 (120 blocks)                                  | 200478.14 blocks/sec |        598.57µs
Seq Scan MT x16 (120 blocks)                                 | 118846.95 blocks/sec |          1.01ms
Seq Scan MT x32 (120 blocks)                                 |  93595.86 blocks/sec |          1.28ms
Seq Scan MT x64 (120 blocks)                                 |  65883.89 blocks/sec |          1.82ms
Seq Scan MT x128 (120 blocks)                                |  52920.81 blocks/sec |          2.27ms
Seq Scan MT x256 (120 blocks)                                |  44650.33 blocks/sec |          2.69ms
Repeated Access (1000 ops)                                   | 3717969.69 blocks/sec |        268.96µs
Repeated Access MT x2 (1000 ops)                             | 1757475.42 blocks/sec |        569.00µs
Repeated Access MT x4 (1000 ops)                             | 965157.80 blocks/sec |          1.04ms
Repeated Access MT x8 (1000 ops)                             | 649335.05 blocks/sec |          1.54ms
Repeated Access MT x16 (1000 ops)                            | 496707.57 blocks/sec |          2.01ms
Repeated Access MT x32 (1000 ops)                            | 455201.57 blocks/sec |          2.20ms
Repeated Access MT x64 (1000 ops)                            | 428978.53 blocks/sec |          2.33ms
Repeated Access MT x128 (1000 ops)                           | 366308.55 blocks/sec |          2.73ms
Repeated Access MT x256 (1000 ops)                           | 342707.17 blocks/sec |          2.92ms
Random (K=10, 500 ops)                                       | 3625053.47 blocks/sec |        137.93µs
Random (K=50, 500 ops)                                       | 639319.97 blocks/sec |        782.08µs
Random (K=100, 500 ops)                                      | 544246.12 blocks/sec |        918.70µs
Random MT x2 (K=10, 500 ops)                                 | 1993715.81 blocks/sec |        250.79µs
Random MT x4 (K=10, 500 ops)                                 | 1015282.03 blocks/sec |        492.47µs
Random MT x8 (K=10, 500 ops)                                 | 703499.06 blocks/sec |        710.73µs
Random MT x16 (K=10, 500 ops)                                | 534975.64 blocks/sec |        934.62µs
Random MT x32 (K=10, 500 ops)                                | 456834.61 blocks/sec |          1.09ms
Random MT x64 (K=10, 500 ops)                                | 401000.26 blocks/sec |          1.25ms
Random MT x128 (K=10, 500 ops)                               | 317602.48 blocks/sec |          1.57ms
Random MT x256 (K=10, 500 ops)                               | 229191.58 blocks/sec |          2.18ms
Random MT x2 (K=50, 500 ops)                                 | 472676.46 blocks/sec |          1.06ms
Random MT x4 (K=50, 500 ops)                                 | 336821.46 blocks/sec |          1.48ms
Random MT x8 (K=50, 500 ops)                                 | 230136.89 blocks/sec |          2.17ms
Random MT x16 (K=50, 500 ops)                                | 172270.39 blocks/sec |          2.90ms
Random MT x32 (K=50, 500 ops)                                | 125663.06 blocks/sec |          3.98ms
Random MT x64 (K=50, 500 ops)                                | 107564.31 blocks/sec |          4.65ms
Random MT x128 (K=50, 500 ops)                               |  92799.52 blocks/sec |          5.39ms
Random MT x256 (K=50, 500 ops)                               |  90063.27 blocks/sec |          5.55ms
Random MT x2 (K=100, 500 ops)                                | 419810.35 blocks/sec |          1.19ms
Random MT x4 (K=100, 500 ops)                                | 293267.69 blocks/sec |          1.70ms
Random MT x8 (K=100, 500 ops)                                | 208558.05 blocks/sec |          2.40ms
Random MT x16 (K=100, 500 ops)                               | 142635.41 blocks/sec |          3.51ms
Random MT x32 (K=100, 500 ops)                               | 106896.61 blocks/sec |          4.68ms
Random MT x64 (K=100, 500 ops)                               |  85384.79 blocks/sec |          5.86ms
Random MT x128 (K=100, 500 ops)                              |  64593.79 blocks/sec |          7.74ms
Random MT x256 (K=100, 500 ops)                              |  66605.32 blocks/sec |          7.51ms
Zipfian (80/20, 500 ops)                                     | 1379321.76 blocks/sec |        362.50µs
Zipfian MT x2 (80/20, 500 ops)                               | 972736.15 blocks/sec |        514.01µs
Zipfian MT x4 (80/20, 500 ops)                               | 537349.56 blocks/sec |        930.49µs
Zipfian MT x8 (80/20, 500 ops)                               | 412166.84 blocks/sec |          1.21ms
Zipfian MT x16 (80/20, 500 ops)                              | 317768.00 blocks/sec |          1.57ms
Zipfian MT x32 (80/20, 500 ops)                              | 280276.60 blocks/sec |          1.78ms
Zipfian MT x64 (80/20, 500 ops)                              | 211019.08 blocks/sec |          2.37ms
Zipfian MT x128 (80/20, 500 ops)                             | 192600.00 blocks/sec |          2.60ms
Zipfian MT x256 (80/20, 500 ops)                             | 169474.98 blocks/sec |          2.95ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     546739
                 16 |     573228
                 32 |     678429
                 64 |    1053208
                128 |    3564427
                256 |    3566894


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3635544
       32 |          33 |    2969156
       32 |          37 |    1837492
       32 |          42 |    1425505
       32 |          52 |    1083917

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  70.0% (hits: 36619, misses: 15707)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.7% (hits: 12849, misses: 41415)
Random (K=100)       | Hit rate:  10.0% (hits: 5457, misses: 49367)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   3486941.23 ops/sec |          2.87ms
2 threads, 5000 ops/thread                                   |   1766223.69 ops/sec |          5.66ms
4 threads, 2500 ops/thread                                   |   1104459.44 ops/sec |          9.05ms
8 threads, 1250 ops/thread                                   |    777257.69 ops/sec |         12.87ms
16 threads, 625 ops/thread                                   |    669840.68 ops/sec |         14.93ms
32 threads, 312 ops/thread                                   |    652291.38 ops/sec |         15.33ms
64 threads, 156 ops/thread                                   |    630194.81 ops/sec |         15.87ms
128 threads, 78 ops/thread                                   |    612032.60 ops/sec |         16.34ms
256 threads, 39 ops/thread                                   |    593483.01 ops/sec |         16.85ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   3635854.62 ops/sec |          2.75ms
2 threads, K=4, 5000 ops/thread                              |   1640108.66 ops/sec |          6.10ms
4 threads, K=4, 2500 ops/thread                              |   1004622.07 ops/sec |          9.95ms
8 threads, K=4, 1250 ops/thread                              |    655723.00 ops/sec |         15.25ms
16 threads, K=4, 625 ops/thread                              |    521145.91 ops/sec |         19.19ms
32 threads, K=4, 312 ops/thread                              |    454724.33 ops/sec |         21.99ms
64 threads, K=4, 156 ops/thread                              |    416365.69 ops/sec |         24.02ms
128 threads, K=4, 78 ops/thread                              |    392923.42 ops/sec |         25.45ms
256 threads, K=4, 39 ops/thread                              |    379728.32 ops/sec |         26.33ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   204.52ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-32828b2194705bfe)
```
