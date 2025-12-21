# Linux (i7-8650U, Ubuntu 6.8.0-86)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`
Note: Pin/Hotset benchmarks use 4096 buffers regardless of `num_buffers`.

## Replacement LRU (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   808.00ns |   810.00ns |     7.00ns |      100
Cold Pin (miss)                                              |     4.01µs |     3.84µs |   931.00ns |      100
Dirty Eviction                                               |   507.20µs |     4.26µs |     1.52ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 261526.22 blocks/sec |        458.85µs
Seq Scan MT x2 (120 blocks)                                  | 274043.30 blocks/sec |        437.89µs
Seq Scan MT x4 (120 blocks)                                  | 173138.44 blocks/sec |        693.09µs
Seq Scan MT x8 (120 blocks)                                  | 152470.79 blocks/sec |        787.04µs
Seq Scan MT x16 (120 blocks)                                 | 110123.77 blocks/sec |          1.09ms
Seq Scan MT x32 (120 blocks)                                 |  76770.37 blocks/sec |          1.56ms
Seq Scan MT x64 (120 blocks)                                 |  71213.74 blocks/sec |          1.69ms
Seq Scan MT x128 (120 blocks)                                |  63303.73 blocks/sec |          1.90ms
Seq Scan MT x256 (120 blocks)                                |  44033.50 blocks/sec |          2.73ms
Repeated Access (1000 ops)                                   | 1009371.00 blocks/sec |        990.72µs
Repeated Access MT x2 (1000 ops)                             | 1078056.70 blocks/sec |        927.60µs
Repeated Access MT x4 (1000 ops)                             | 1144071.82 blocks/sec |        874.07µs
Repeated Access MT x8 (1000 ops)                             | 759666.57 blocks/sec |          1.32ms
Repeated Access MT x16 (1000 ops)                            | 657897.33 blocks/sec |          1.52ms
Repeated Access MT x32 (1000 ops)                            | 589780.87 blocks/sec |          1.70ms
Repeated Access MT x64 (1000 ops)                            | 505678.77 blocks/sec |          1.98ms
Repeated Access MT x128 (1000 ops)                           | 456134.46 blocks/sec |          2.19ms
Repeated Access MT x256 (1000 ops)                           | 350212.77 blocks/sec |          2.86ms
Random (K=10, 500 ops)                                       | 955144.50 blocks/sec |        523.48µs
Random (K=50, 500 ops)                                       | 260899.47 blocks/sec |          1.92ms
Random (K=100, 500 ops)                                      | 237040.75 blocks/sec |          2.11ms
Random MT x2 (K=10, 500 ops)                                 | 1071926.25 blocks/sec |        466.45µs
Random MT x4 (K=10, 500 ops)                                 | 1066989.89 blocks/sec |        468.61µs
Random MT x8 (K=10, 500 ops)                                 | 808022.04 blocks/sec |        618.80µs
Random MT x16 (K=10, 500 ops)                                | 637378.69 blocks/sec |        784.46µs
Random MT x32 (K=10, 500 ops)                                | 555407.45 blocks/sec |        900.24µs
Random MT x64 (K=10, 500 ops)                                | 484763.40 blocks/sec |          1.03ms
Random MT x128 (K=10, 500 ops)                               | 362803.10 blocks/sec |          1.38ms
Random MT x256 (K=10, 500 ops)                               | 202224.96 blocks/sec |          2.47ms
Random MT x2 (K=50, 500 ops)                                 | 328572.54 blocks/sec |          1.52ms
Random MT x4 (K=50, 500 ops)                                 | 208502.92 blocks/sec |          2.40ms
Random MT x8 (K=50, 500 ops)                                 | 198097.94 blocks/sec |          2.52ms
Random MT x16 (K=50, 500 ops)                                | 138063.08 blocks/sec |          3.62ms
Random MT x32 (K=50, 500 ops)                                |  97702.28 blocks/sec |          5.12ms
Random MT x64 (K=50, 500 ops)                                |  89991.57 blocks/sec |          5.56ms
Random MT x128 (K=50, 500 ops)                               |  82750.67 blocks/sec |          6.04ms
Random MT x256 (K=50, 500 ops)                               |  75505.42 blocks/sec |          6.62ms
Random MT x2 (K=100, 500 ops)                                | 304006.99 blocks/sec |          1.64ms
Random MT x4 (K=100, 500 ops)                                | 216619.01 blocks/sec |          2.31ms
Random MT x8 (K=100, 500 ops)                                | 176557.16 blocks/sec |          2.83ms
Random MT x16 (K=100, 500 ops)                               | 122342.36 blocks/sec |          4.09ms
Random MT x32 (K=100, 500 ops)                               |  87585.23 blocks/sec |          5.71ms
Random MT x64 (K=100, 500 ops)                               |  84234.74 blocks/sec |          5.94ms
Random MT x128 (K=100, 500 ops)                              |  75832.45 blocks/sec |          6.59ms
Random MT x256 (K=100, 500 ops)                              |  69720.14 blocks/sec |          7.17ms
Zipfian (80/20, 500 ops)                                     | 635166.64 blocks/sec |        787.20µs
Zipfian MT x2 (80/20, 500 ops)                               | 896714.62 blocks/sec |        557.59µs
Zipfian MT x4 (80/20, 500 ops)                               | 749830.91 blocks/sec |        666.82µs
Zipfian MT x8 (80/20, 500 ops)                               | 491883.43 blocks/sec |          1.02ms
Zipfian MT x16 (80/20, 500 ops)                              | 434955.83 blocks/sec |          1.15ms
Zipfian MT x32 (80/20, 500 ops)                              | 365883.29 blocks/sec |          1.37ms
Zipfian MT x64 (80/20, 500 ops)                              | 296443.16 blocks/sec |          1.69ms
Zipfian MT x128 (80/20, 500 ops)                             | 233486.55 blocks/sec |          2.14ms
Zipfian MT x256 (80/20, 500 ops)                             | 178637.59 blocks/sec |          2.80ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     258053
                 16 |     273671
                 32 |     331289
                 64 |     456518
                128 |    1106173
                256 |    1126481


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1079711
       32 |          33 |    1022405
       32 |          37 |     695396
       32 |          42 |     548186
       32 |          52 |     425091

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  82.0% (hits: 41814, misses: 9186)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  27.6% (hits: 14074, misses: 36926)
Random (K=100)       | Hit rate:  12.2% (hits: 6222, misses: 44778)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1034660.40 ops/sec |          9.67ms
2 threads, 5000 ops/thread                                   |   1257565.04 ops/sec |          7.95ms
4 threads, 2500 ops/thread                                   |   1160589.90 ops/sec |          8.62ms
8 threads, 1250 ops/thread                                   |    793826.00 ops/sec |         12.60ms
16 threads, 625 ops/thread                                   |    572394.81 ops/sec |         17.47ms
32 threads, 312 ops/thread                                   |    571043.75 ops/sec |         17.51ms
64 threads, 156 ops/thread                                   |    567478.02 ops/sec |         17.62ms
128 threads, 78 ops/thread                                   |    553470.91 ops/sec |         18.07ms
256 threads, 39 ops/thread                                   |    524874.07 ops/sec |         19.05ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1017574.22 ops/sec |          9.83ms
2 threads, K=4, 5000 ops/thread                              |   1245984.35 ops/sec |          8.03ms
4 threads, K=4, 2500 ops/thread                              |   1102645.12 ops/sec |          9.07ms
8 threads, K=4, 1250 ops/thread                              |    854703.85 ops/sec |         11.70ms
16 threads, K=4, 625 ops/thread                              |    796771.93 ops/sec |         12.55ms
32 threads, K=4, 312 ops/thread                              |    781775.26 ops/sec |         12.79ms
64 threads, K=4, 156 ops/thread                              |    785665.69 ops/sec |         12.73ms
128 threads, K=4, 78 ops/thread                              |    773732.76 ops/sec |         12.92ms
256 threads, K=4, 39 ops/thread                              |    746260.92 ops/sec |         13.40ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   173.55ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-33185e61bb240555)
```

## Replacement Clock (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   986.00ns |   900.00ns |   853.00ns |      100
Cold Pin (miss)                                              |     4.40µs |     4.17µs |   955.00ns |      100
Dirty Eviction                                               |   502.66µs |     4.27µs |     1.50ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 268737.14 blocks/sec |        446.53µs
Seq Scan MT x2 (120 blocks)                                  | 273135.73 blocks/sec |        439.34µs
Seq Scan MT x4 (120 blocks)                                  | 176625.14 blocks/sec |        679.41µs
Seq Scan MT x8 (120 blocks)                                  | 141912.08 blocks/sec |        845.59µs
Seq Scan MT x16 (120 blocks)                                 | 132970.18 blocks/sec |        902.46µs
Seq Scan MT x32 (120 blocks)                                 | 120132.63 blocks/sec |        998.90µs
Seq Scan MT x64 (120 blocks)                                 | 101340.91 blocks/sec |          1.18ms
Seq Scan MT x128 (120 blocks)                                |  76606.00 blocks/sec |          1.57ms
Seq Scan MT x256 (120 blocks)                                |  49258.50 blocks/sec |          2.44ms
Repeated Access (1000 ops)                                   | 1123221.94 blocks/sec |        890.30µs
Repeated Access MT x2 (1000 ops)                             | 1280719.66 blocks/sec |        780.81µs
Repeated Access MT x4 (1000 ops)                             | 1324089.42 blocks/sec |        755.24µs
Repeated Access MT x8 (1000 ops)                             | 860198.03 blocks/sec |          1.16ms
Repeated Access MT x16 (1000 ops)                            | 842913.75 blocks/sec |          1.19ms
Repeated Access MT x32 (1000 ops)                            | 763100.72 blocks/sec |          1.31ms
Repeated Access MT x64 (1000 ops)                            | 664531.75 blocks/sec |          1.50ms
Repeated Access MT x128 (1000 ops)                           | 534021.44 blocks/sec |          1.87ms
Repeated Access MT x256 (1000 ops)                           | 368169.94 blocks/sec |          2.72ms
Random (K=10, 500 ops)                                       | 1088660.51 blocks/sec |        459.28µs
Random (K=50, 500 ops)                                       | 298490.89 blocks/sec |          1.68ms
Random (K=100, 500 ops)                                      | 284565.78 blocks/sec |          1.76ms
Random MT x2 (K=10, 500 ops)                                 | 1321199.44 blocks/sec |        378.44µs
Random MT x4 (K=10, 500 ops)                                 | 1333994.99 blocks/sec |        374.81µs
Random MT x8 (K=10, 500 ops)                                 | 958149.93 blocks/sec |        521.84µs
Random MT x16 (K=10, 500 ops)                                | 834712.00 blocks/sec |        599.01µs
Random MT x32 (K=10, 500 ops)                                | 731458.26 blocks/sec |        683.57µs
Random MT x64 (K=10, 500 ops)                                | 574823.64 blocks/sec |        869.83µs
Random MT x128 (K=10, 500 ops)                               | 423974.22 blocks/sec |          1.18ms
Random MT x256 (K=10, 500 ops)                               | 225811.42 blocks/sec |          2.21ms
Random MT x2 (K=50, 500 ops)                                 | 370666.63 blocks/sec |          1.35ms
Random MT x4 (K=50, 500 ops)                                 | 249205.41 blocks/sec |          2.01ms
Random MT x8 (K=50, 500 ops)                                 | 192333.51 blocks/sec |          2.60ms
Random MT x16 (K=50, 500 ops)                                | 201337.69 blocks/sec |          2.48ms
Random MT x32 (K=50, 500 ops)                                | 220010.38 blocks/sec |          2.27ms
Random MT x64 (K=50, 500 ops)                                | 241512.88 blocks/sec |          2.07ms
Random MT x128 (K=50, 500 ops)                               | 214531.05 blocks/sec |          2.33ms
Random MT x256 (K=50, 500 ops)                               | 152114.67 blocks/sec |          3.29ms
Random MT x2 (K=100, 500 ops)                                | 302194.23 blocks/sec |          1.65ms
Random MT x4 (K=100, 500 ops)                                | 199722.23 blocks/sec |          2.50ms
Random MT x8 (K=100, 500 ops)                                | 159853.70 blocks/sec |          3.13ms
Random MT x16 (K=100, 500 ops)                               | 159688.85 blocks/sec |          3.13ms
Random MT x32 (K=100, 500 ops)                               | 169106.82 blocks/sec |          2.96ms
Random MT x64 (K=100, 500 ops)                               | 174791.56 blocks/sec |          2.86ms
Random MT x128 (K=100, 500 ops)                              | 169248.55 blocks/sec |          2.95ms
Random MT x256 (K=100, 500 ops)                              | 136709.29 blocks/sec |          3.66ms
Zipfian (80/20, 500 ops)                                     | 571267.31 blocks/sec |        875.25µs
Zipfian MT x2 (80/20, 500 ops)                               | 721971.62 blocks/sec |        692.55µs
Zipfian MT x4 (80/20, 500 ops)                               | 805636.88 blocks/sec |        620.63µs
Zipfian MT x8 (80/20, 500 ops)                               | 795980.62 blocks/sec |        628.16µs
Zipfian MT x16 (80/20, 500 ops)                              | 532645.30 blocks/sec |        938.71µs
Zipfian MT x32 (80/20, 500 ops)                              | 529524.15 blocks/sec |        944.24µs
Zipfian MT x64 (80/20, 500 ops)                              | 414338.09 blocks/sec |          1.21ms
Zipfian MT x128 (80/20, 500 ops)                             | 306799.10 blocks/sec |          1.63ms
Zipfian MT x256 (80/20, 500 ops)                             | 194547.91 blocks/sec |          2.57ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     253929
                 16 |     280304
                 32 |     306667
                 64 |     459264
                128 |    1183813
                256 |    1149658


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1173921
       32 |          33 |     936384
       32 |          37 |     817706
       32 |          42 |     590375
       32 |          52 |     469120

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  71.7% (hits: 36617, misses: 14486)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.6% (hits: 12036, misses: 38967)
Random (K=100)       | Hit rate:  13.0% (hits: 6627, misses: 44476)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1184318.72 ops/sec |          8.44ms
2 threads, 5000 ops/thread                                   |   1839319.95 ops/sec |          5.44ms
4 threads, 2500 ops/thread                                   |   1241072.35 ops/sec |          8.06ms
8 threads, 1250 ops/thread                                   |    822960.94 ops/sec |         12.15ms
16 threads, 625 ops/thread                                   |    747839.79 ops/sec |         13.37ms
32 threads, 312 ops/thread                                   |    786302.18 ops/sec |         12.72ms
64 threads, 156 ops/thread                                   |    781118.80 ops/sec |         12.80ms
128 threads, 78 ops/thread                                   |    750773.63 ops/sec |         13.32ms
256 threads, 39 ops/thread                                   |    679129.00 ops/sec |         14.72ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1150239.58 ops/sec |          8.69ms
2 threads, K=4, 5000 ops/thread                              |   1270283.57 ops/sec |          7.87ms
4 threads, K=4, 2500 ops/thread                              |   1235396.53 ops/sec |          8.09ms
8 threads, K=4, 1250 ops/thread                              |    962821.79 ops/sec |         10.39ms
16 threads, K=4, 625 ops/thread                              |    985775.85 ops/sec |         10.14ms
32 threads, K=4, 312 ops/thread                              |   1094229.10 ops/sec |          9.14ms
64 threads, K=4, 156 ops/thread                              |   1122885.94 ops/sec |          8.91ms
128 threads, K=4, 78 ops/thread                              |   1019622.43 ops/sec |          9.81ms
256 threads, K=4, 39 ops/thread                              |    902360.94 ops/sec |         11.08ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.04ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-011ae9cbe4ac89c1)
```

## Replacement SIEVE (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   931.00ns |   930.00ns |     6.00ns |      100
Cold Pin (miss)                                              |     4.51µs |     4.31µs |     1.05µs |      100
Dirty Eviction                                               |   503.75µs |     4.25µs |     1.51ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 264638.94 blocks/sec |        453.45µs
Seq Scan MT x2 (120 blocks)                                  | 274789.44 blocks/sec |        436.70µs
Seq Scan MT x4 (120 blocks)                                  | 171224.33 blocks/sec |        700.84µs
Seq Scan MT x8 (120 blocks)                                  | 160292.59 blocks/sec |        748.63µs
Seq Scan MT x16 (120 blocks)                                 | 128765.31 blocks/sec |        931.93µs
Seq Scan MT x32 (120 blocks)                                 |  88681.96 blocks/sec |          1.35ms
Seq Scan MT x64 (120 blocks)                                 |  79023.38 blocks/sec |          1.52ms
Seq Scan MT x128 (120 blocks)                                |  66264.40 blocks/sec |          1.81ms
Seq Scan MT x256 (120 blocks)                                |  45759.26 blocks/sec |          2.62ms
Repeated Access (1000 ops)                                   | 1105860.73 blocks/sec |        904.27µs
Repeated Access MT x2 (1000 ops)                             | 1324077.15 blocks/sec |        755.24µs
Repeated Access MT x4 (1000 ops)                             | 1421686.40 blocks/sec |        703.39µs
Repeated Access MT x8 (1000 ops)                             | 919590.97 blocks/sec |          1.09ms
Repeated Access MT x16 (1000 ops)                            | 804197.91 blocks/sec |          1.24ms
Repeated Access MT x32 (1000 ops)                            | 747921.90 blocks/sec |          1.34ms
Repeated Access MT x64 (1000 ops)                            | 661994.31 blocks/sec |          1.51ms
Repeated Access MT x128 (1000 ops)                           | 513252.43 blocks/sec |          1.95ms
Repeated Access MT x256 (1000 ops)                           | 369913.19 blocks/sec |          2.70ms
Random (K=10, 500 ops)                                       | 1057883.13 blocks/sec |        472.64µs
Random (K=50, 500 ops)                                       | 289595.36 blocks/sec |          1.73ms
Random (K=100, 500 ops)                                      | 253750.95 blocks/sec |          1.97ms
Random MT x2 (K=10, 500 ops)                                 | 1354738.20 blocks/sec |        369.08µs
Random MT x4 (K=10, 500 ops)                                 | 1434856.10 blocks/sec |        348.47µs
Random MT x8 (K=10, 500 ops)                                 | 957315.23 blocks/sec |        522.29µs
Random MT x16 (K=10, 500 ops)                                | 826675.84 blocks/sec |        604.83µs
Random MT x32 (K=10, 500 ops)                                | 735967.31 blocks/sec |        679.38µs
Random MT x64 (K=10, 500 ops)                                | 579134.70 blocks/sec |        863.36µs
Random MT x128 (K=10, 500 ops)                               | 406781.21 blocks/sec |          1.23ms
Random MT x256 (K=10, 500 ops)                               | 221630.22 blocks/sec |          2.26ms
Random MT x2 (K=50, 500 ops)                                 | 351351.09 blocks/sec |          1.42ms
Random MT x4 (K=50, 500 ops)                                 | 255443.76 blocks/sec |          1.96ms
Random MT x8 (K=50, 500 ops)                                 | 202313.41 blocks/sec |          2.47ms
Random MT x16 (K=50, 500 ops)                                | 151162.94 blocks/sec |          3.31ms
Random MT x32 (K=50, 500 ops)                                | 101492.92 blocks/sec |          4.93ms
Random MT x64 (K=50, 500 ops)                                |  88445.69 blocks/sec |          5.65ms
Random MT x128 (K=50, 500 ops)                               |  81384.13 blocks/sec |          6.14ms
Random MT x256 (K=50, 500 ops)                               |  72872.52 blocks/sec |          6.86ms
Random MT x2 (K=100, 500 ops)                                | 299044.67 blocks/sec |          1.67ms
Random MT x4 (K=100, 500 ops)                                | 223144.25 blocks/sec |          2.24ms
Random MT x8 (K=100, 500 ops)                                | 179753.50 blocks/sec |          2.78ms
Random MT x16 (K=100, 500 ops)                               | 130059.98 blocks/sec |          3.84ms
Random MT x32 (K=100, 500 ops)                               |  85928.89 blocks/sec |          5.82ms
Random MT x64 (K=100, 500 ops)                               |  77625.90 blocks/sec |          6.44ms
Random MT x128 (K=100, 500 ops)                              |  72155.47 blocks/sec |          6.93ms
Random MT x256 (K=100, 500 ops)                              |  66847.32 blocks/sec |          7.48ms
Zipfian (80/20, 500 ops)                                     | 504924.53 blocks/sec |        990.25µs
Zipfian MT x2 (80/20, 500 ops)                               | 701979.02 blocks/sec |        712.27µs
Zipfian MT x4 (80/20, 500 ops)                               | 617855.28 blocks/sec |        809.25µs
Zipfian MT x8 (80/20, 500 ops)                               | 452332.32 blocks/sec |          1.11ms
Zipfian MT x16 (80/20, 500 ops)                              | 401257.06 blocks/sec |          1.25ms
Zipfian MT x32 (80/20, 500 ops)                              | 330042.15 blocks/sec |          1.51ms
Zipfian MT x64 (80/20, 500 ops)                              | 266820.36 blocks/sec |          1.87ms
Zipfian MT x128 (80/20, 500 ops)                             | 229878.72 blocks/sec |          2.18ms
Zipfian MT x256 (80/20, 500 ops)                             | 169624.64 blocks/sec |          2.95ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     247456
                 16 |     277959
                 32 |     311884
                 64 |     475206
                128 |    1182262
                256 |    1171687


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1182215
       32 |          33 |     986199
       32 |          37 |     729043
       32 |          42 |     616111
       32 |          52 |     467080

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  73.0% (hits: 38147, misses: 14077)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.1% (hits: 12034, misses: 42332)
Random (K=100)       | Hit rate:   8.9% (hits: 4895, misses: 49981)

Phase 5: Concurrent Access

Pin pool size override: 4096 buffers

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1178683.74 ops/sec |          8.48ms
2 threads, 5000 ops/thread                                   |   1912275.87 ops/sec |          5.23ms
4 threads, 2500 ops/thread                                   |   1222586.10 ops/sec |          8.18ms
8 threads, 1250 ops/thread                                   |    813460.69 ops/sec |         12.29ms
16 threads, 625 ops/thread                                   |    734264.10 ops/sec |         13.62ms
32 threads, 312 ops/thread                                   |    759660.74 ops/sec |         13.16ms
64 threads, 156 ops/thread                                   |    722486.71 ops/sec |         13.84ms
128 threads, 78 ops/thread                                   |    696728.60 ops/sec |         14.35ms
256 threads, 39 ops/thread                                   |    657725.59 ops/sec |         15.20ms

Hotset pool size override: 4096 buffers

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1144237.55 ops/sec |          8.74ms
2 threads, K=4, 5000 ops/thread                              |   1423785.48 ops/sec |          7.02ms
4 threads, K=4, 2500 ops/thread                              |   1320349.89 ops/sec |          7.57ms
8 threads, K=4, 1250 ops/thread                              |    951402.10 ops/sec |         10.51ms
16 threads, K=4, 625 ops/thread                              |    942083.26 ops/sec |         10.61ms
32 threads, K=4, 312 ops/thread                              |   1040682.57 ops/sec |          9.61ms
64 threads, K=4, 156 ops/thread                              |   1040322.59 ops/sec |          9.61ms
128 threads, K=4, 78 ops/thread                              |    980766.00 ops/sec |         10.20ms
256 threads, K=4, 39 ops/thread                              |    867657.80 ops/sec |         11.53ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.23ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-bb12b085f3a416ad)
```
