# Linux (i7-8650U, Ubuntu 6.8.0-86)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

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
Pin/Unpin (hit)                                              |   912.00ns |   909.00ns |     8.00ns |      100
Cold Pin (miss)                                              |     4.40µs |     4.20µs |     1.07µs |      100
Dirty Eviction                                               |   454.76µs |     4.12µs |     1.40ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 268809.98 blocks/sec |        446.41µs
Seq Scan MT x2 (120 blocks)                                  | 227526.54 blocks/sec |        527.41µs
Seq Scan MT x4 (120 blocks)                                  | 189226.99 blocks/sec |        634.16µs
Seq Scan MT x8 (120 blocks)                                  | 156597.65 blocks/sec |        766.30µs
Seq Scan MT x16 (120 blocks)                                 | 124638.16 blocks/sec |        962.79µs
Seq Scan MT x32 (120 blocks)                                 | 100419.75 blocks/sec |          1.19ms
Seq Scan MT x64 (120 blocks)                                 |  62890.48 blocks/sec |          1.91ms
Seq Scan MT x128 (120 blocks)                                |  31757.44 blocks/sec |          3.78ms
Seq Scan MT x256 (120 blocks)                                |  14897.75 blocks/sec |          8.05ms
Repeated Access (1000 ops)                                   | 1005546.60 blocks/sec |        994.48µs
Repeated Access MT x2 (1000 ops)                             | 1111011.12 blocks/sec |        900.08µs
Repeated Access MT x4 (1000 ops)                             | 1153736.55 blocks/sec |        866.75µs
Repeated Access MT x8 (1000 ops)                             | 773348.55 blocks/sec |          1.29ms
Repeated Access MT x16 (1000 ops)                            | 709416.08 blocks/sec |          1.41ms
Repeated Access MT x32 (1000 ops)                            | 681068.54 blocks/sec |          1.47ms
Repeated Access MT x64 (1000 ops)                            | 497212.87 blocks/sec |          2.01ms
Repeated Access MT x128 (1000 ops)                           | 248128.12 blocks/sec |          4.03ms
Repeated Access MT x256 (1000 ops)                           | 121876.95 blocks/sec |          8.20ms
Random (K=10, 500 ops)                                       | 1031357.39 blocks/sec |        484.80µs
Random (K=50, 500 ops)                                       | 279825.95 blocks/sec |          1.79ms
Random (K=100, 500 ops)                                      | 256396.19 blocks/sec |          1.95ms
Random MT x2 (K=10, 500 ops)                                 | 972726.69 blocks/sec |        514.02µs
Random MT x4 (K=10, 500 ops)                                 | 1121277.36 blocks/sec |        445.92µs
Random MT x8 (K=10, 500 ops)                                 | 850384.97 blocks/sec |        587.97µs
Random MT x16 (K=10, 500 ops)                                | 750874.77 blocks/sec |        665.89µs
Random MT x32 (K=10, 500 ops)                                | 543040.29 blocks/sec |        920.74µs
Random MT x64 (K=10, 500 ops)                                | 284184.88 blocks/sec |          1.76ms
Random MT x128 (K=10, 500 ops)                               | 127013.74 blocks/sec |          3.94ms
Random MT x256 (K=10, 500 ops)                               |  62556.76 blocks/sec |          7.99ms
Random MT x2 (K=50, 500 ops)                                 | 333209.82 blocks/sec |          1.50ms
Random MT x4 (K=50, 500 ops)                                 | 244885.33 blocks/sec |          2.04ms
Random MT x8 (K=50, 500 ops)                                 | 205500.17 blocks/sec |          2.43ms
Random MT x16 (K=50, 500 ops)                                | 152446.18 blocks/sec |          3.28ms
Random MT x32 (K=50, 500 ops)                                | 104722.53 blocks/sec |          4.77ms
Random MT x64 (K=50, 500 ops)                                |  91381.95 blocks/sec |          5.47ms
Random MT x128 (K=50, 500 ops)                               | 104990.14 blocks/sec |          4.76ms
Random MT x256 (K=50, 500 ops)                               |  59163.46 blocks/sec |          8.45ms
Random MT x2 (K=100, 500 ops)                                | 269952.16 blocks/sec |          1.85ms
Random MT x4 (K=100, 500 ops)                                | 217802.57 blocks/sec |          2.30ms
Random MT x8 (K=100, 500 ops)                                | 181670.53 blocks/sec |          2.75ms
Random MT x16 (K=100, 500 ops)                               | 127548.25 blocks/sec |          3.92ms
Random MT x32 (K=100, 500 ops)                               |  94237.19 blocks/sec |          5.31ms
Random MT x64 (K=100, 500 ops)                               |  80790.68 blocks/sec |          6.19ms
Random MT x128 (K=100, 500 ops)                              |  99364.03 blocks/sec |          5.03ms
Random MT x256 (K=100, 500 ops)                              |  59211.08 blocks/sec |          8.44ms
Zipfian (80/20, 500 ops)                                     | 667977.68 blocks/sec |        748.53µs
Zipfian MT x2 (80/20, 500 ops)                               | 689841.67 blocks/sec |        724.80µs
Zipfian MT x4 (80/20, 500 ops)                               | 708269.47 blocks/sec |        705.95µs
Zipfian MT x8 (80/20, 500 ops)                               | 503772.75 blocks/sec |        992.51µs
Zipfian MT x16 (80/20, 500 ops)                              | 445802.06 blocks/sec |          1.12ms
Zipfian MT x32 (80/20, 500 ops)                              | 325183.79 blocks/sec |          1.54ms
Zipfian MT x64 (80/20, 500 ops)                              | 238648.56 blocks/sec |          2.10ms
Zipfian MT x128 (80/20, 500 ops)                             | 122109.78 blocks/sec |          4.09ms
Zipfian MT x256 (80/20, 500 ops)                             |  60186.69 blocks/sec |          8.31ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     236351
                 16 |     257750
                 32 |     296123
                 64 |     470014
                128 |    1070112
                256 |    1070668


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1071917
       32 |          33 |     978843
       32 |          37 |     753958
       32 |          42 |     600248
       32 |          52 |     435251

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  79.2% (hits: 40387, misses: 10613)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  19.4% (hits: 9893, misses: 41107)
Random (K=100)       | Hit rate:  11.4% (hits: 5814, misses: 45186)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1116489.24 ops/sec |          8.96ms
2 threads, 5000 ops/thread                                   |    235189.69 ops/sec |         42.52ms
4 threads, 2500 ops/thread                                   |    204367.28 ops/sec |         48.93ms
8 threads, 1250 ops/thread                                   |    176525.41 ops/sec |         56.65ms
16 threads, 625 ops/thread                                   |    100704.82 ops/sec |         99.30ms
32 threads, 312 ops/thread                                   |     76220.77 ops/sec |        131.20ms
64 threads, 156 ops/thread                                   |     74726.95 ops/sec |        133.82ms
128 threads, 78 ops/thread                                   |     64382.32 ops/sec |        155.32ms
256 threads, 39 ops/thread                                   |     52747.57 ops/sec |        189.58ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1075679.89 ops/sec |          9.30ms
2 threads, K=4, 5000 ops/thread                              |   1177132.00 ops/sec |          8.50ms
4 threads, K=4, 2500 ops/thread                              |   1010716.63 ops/sec |          9.89ms
8 threads, K=4, 1250 ops/thread                              |    759045.26 ops/sec |         13.17ms
16 threads, K=4, 625 ops/thread                              |    700464.11 ops/sec |         14.28ms
32 threads, K=4, 312 ops/thread                              |    694718.52 ops/sec |         14.39ms
64 threads, K=4, 156 ops/thread                              |    670856.16 ops/sec |         14.91ms
128 threads, K=4, 78 ops/thread                              |    628725.04 ops/sec |         15.91ms
256 threads, K=4, 39 ops/thread                              |    632006.70 ops/sec |         15.82ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.83ms

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
Pin/Unpin (hit)                                              |     1.07µs |   926.00ns |     1.37µs |      100
Cold Pin (miss)                                              |     4.65µs |     4.30µs |     1.63µs |      100
Dirty Eviction                                               |   503.32µs |     4.21µs |     1.51ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 259348.99 blocks/sec |        462.70µs
Seq Scan MT x2 (120 blocks)                                  | 234116.19 blocks/sec |        512.57µs
Seq Scan MT x4 (120 blocks)                                  | 179099.67 blocks/sec |        670.02µs
Seq Scan MT x8 (120 blocks)                                  | 136140.71 blocks/sec |        881.44µs
Seq Scan MT x16 (120 blocks)                                 | 121922.10 blocks/sec |        984.24µs
Seq Scan MT x32 (120 blocks)                                 |  91483.50 blocks/sec |          1.31ms
Seq Scan MT x64 (120 blocks)                                 |  58402.20 blocks/sec |          2.05ms
Seq Scan MT x128 (120 blocks)                                |  28072.88 blocks/sec |          4.27ms
Seq Scan MT x256 (120 blocks)                                |  14038.49 blocks/sec |          8.55ms
Repeated Access (1000 ops)                                   | 1091551.72 blocks/sec |        916.13µs
Repeated Access MT x2 (1000 ops)                             | 1174936.26 blocks/sec |        851.11µs
Repeated Access MT x4 (1000 ops)                             | 1182565.67 blocks/sec |        845.62µs
Repeated Access MT x8 (1000 ops)                             | 851070.35 blocks/sec |          1.17ms
Repeated Access MT x16 (1000 ops)                            | 779764.18 blocks/sec |          1.28ms
Repeated Access MT x32 (1000 ops)                            | 667114.97 blocks/sec |          1.50ms
Repeated Access MT x64 (1000 ops)                            | 456238.72 blocks/sec |          2.19ms
Repeated Access MT x128 (1000 ops)                           | 236127.34 blocks/sec |          4.24ms
Repeated Access MT x256 (1000 ops)                           | 113679.92 blocks/sec |          8.80ms
Random (K=10, 500 ops)                                       | 1047895.09 blocks/sec |        477.15µs
Random (K=50, 500 ops)                                       | 282311.23 blocks/sec |          1.77ms
Random (K=100, 500 ops)                                      | 260192.25 blocks/sec |          1.92ms
Random MT x2 (K=10, 500 ops)                                 | 1125097.32 blocks/sec |        444.41µs
Random MT x4 (K=10, 500 ops)                                 | 1140635.84 blocks/sec |        438.35µs
Random MT x8 (K=10, 500 ops)                                 | 853229.22 blocks/sec |        586.01µs
Random MT x16 (K=10, 500 ops)                                | 784556.17 blocks/sec |        637.30µs
Random MT x32 (K=10, 500 ops)                                | 581715.97 blocks/sec |        859.53µs
Random MT x64 (K=10, 500 ops)                                | 280540.64 blocks/sec |          1.78ms
Random MT x128 (K=10, 500 ops)                               | 120088.47 blocks/sec |          4.16ms
Random MT x256 (K=10, 500 ops)                               |  58724.46 blocks/sec |          8.51ms
Random MT x2 (K=50, 500 ops)                                 | 308717.63 blocks/sec |          1.62ms
Random MT x4 (K=50, 500 ops)                                 | 224461.65 blocks/sec |          2.23ms
Random MT x8 (K=50, 500 ops)                                 | 179562.74 blocks/sec |          2.78ms
Random MT x16 (K=50, 500 ops)                                | 179684.38 blocks/sec |          2.78ms
Random MT x32 (K=50, 500 ops)                                | 180797.56 blocks/sec |          2.77ms
Random MT x64 (K=50, 500 ops)                                | 141768.88 blocks/sec |          3.53ms
Random MT x128 (K=50, 500 ops)                               | 100445.11 blocks/sec |          4.98ms
Random MT x256 (K=50, 500 ops)                               |  57806.21 blocks/sec |          8.65ms
Random MT x2 (K=100, 500 ops)                                | 262348.62 blocks/sec |          1.91ms
Random MT x4 (K=100, 500 ops)                                | 196135.43 blocks/sec |          2.55ms
Random MT x8 (K=100, 500 ops)                                | 147363.65 blocks/sec |          3.39ms
Random MT x16 (K=100, 500 ops)                               | 145047.06 blocks/sec |          3.45ms
Random MT x32 (K=100, 500 ops)                               | 142716.67 blocks/sec |          3.50ms
Random MT x64 (K=100, 500 ops)                               | 123086.53 blocks/sec |          4.06ms
Random MT x128 (K=100, 500 ops)                              |  95731.67 blocks/sec |          5.22ms
Random MT x256 (K=100, 500 ops)                              |  56362.11 blocks/sec |          8.87ms
Zipfian (80/20, 500 ops)                                     | 569485.16 blocks/sec |        877.99µs
Zipfian MT x2 (80/20, 500 ops)                               | 669164.88 blocks/sec |        747.20µs
Zipfian MT x4 (80/20, 500 ops)                               | 752866.92 blocks/sec |        664.13µs
Zipfian MT x8 (80/20, 500 ops)                               | 542957.14 blocks/sec |        920.88µs
Zipfian MT x16 (80/20, 500 ops)                              | 434216.62 blocks/sec |          1.15ms
Zipfian MT x32 (80/20, 500 ops)                              | 358344.33 blocks/sec |          1.40ms
Zipfian MT x64 (80/20, 500 ops)                              | 212274.29 blocks/sec |          2.36ms
Zipfian MT x128 (80/20, 500 ops)                             | 115130.55 blocks/sec |          4.34ms
Zipfian MT x256 (80/20, 500 ops)                             |  57490.33 blocks/sec |          8.70ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     245405
                 16 |     265389
                 32 |     311685
                 64 |     449628
                128 |    1152305
                256 |    1148056


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1165577
       32 |          33 |     918152
       32 |          37 |     737404
       32 |          42 |     586962
       32 |          52 |     480088

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  74.3% (hits: 37949, misses: 13153)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.5% (hits: 11521, misses: 39686)
Random (K=100)       | Hit rate:  12.6% (hits: 6423, misses: 44581)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1200657.77 ops/sec |          8.33ms
2 threads, 5000 ops/thread                                   |    260237.87 ops/sec |         38.43ms
4 threads, 2500 ops/thread                                   |    180902.41 ops/sec |         55.28ms
8 threads, 1250 ops/thread                                   |    123494.77 ops/sec |         80.98ms
16 threads, 625 ops/thread                                   |    123328.12 ops/sec |         81.08ms
32 threads, 312 ops/thread                                   |    125122.17 ops/sec |         79.92ms
64 threads, 156 ops/thread                                   |    126726.37 ops/sec |         78.91ms
128 threads, 78 ops/thread                                   |    121584.82 ops/sec |         82.25ms
256 threads, 39 ops/thread                                   |    113677.00 ops/sec |         87.97ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1166344.66 ops/sec |          8.57ms
2 threads, K=4, 5000 ops/thread                              |   1316797.69 ops/sec |          7.59ms
4 threads, K=4, 2500 ops/thread                              |   1130166.85 ops/sec |          8.85ms
8 threads, K=4, 1250 ops/thread                              |    867002.73 ops/sec |         11.53ms
16 threads, K=4, 625 ops/thread                              |    894488.31 ops/sec |         11.18ms
32 threads, K=4, 312 ops/thread                              |    964869.58 ops/sec |         10.36ms
64 threads, K=4, 156 ops/thread                              |    962756.07 ops/sec |         10.39ms
128 threads, K=4, 78 ops/thread                              |    826967.95 ops/sec |         12.09ms
256 threads, K=4, 39 ops/thread                              |    709535.32 ops/sec |         14.09ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.56ms

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
Pin/Unpin (hit)                                              |     1.01µs |   960.00ns |   479.00ns |      100
Cold Pin (miss)                                              |     4.63µs |     4.42µs |     1.13µs |      100
Dirty Eviction                                               |   504.89µs |     4.25µs |     1.51ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 266403.22 blocks/sec |        450.45µs
Seq Scan MT x2 (120 blocks)                                  | 223490.32 blocks/sec |        536.94µs
Seq Scan MT x4 (120 blocks)                                  | 182990.43 blocks/sec |        655.77µs
Seq Scan MT x8 (120 blocks)                                  | 156017.19 blocks/sec |        769.15µs
Seq Scan MT x16 (120 blocks)                                 | 133370.97 blocks/sec |        899.75µs
Seq Scan MT x32 (120 blocks)                                 |  99422.03 blocks/sec |          1.21ms
Seq Scan MT x64 (120 blocks)                                 |  56452.84 blocks/sec |          2.13ms
Seq Scan MT x128 (120 blocks)                                |  28493.96 blocks/sec |          4.21ms
Seq Scan MT x256 (120 blocks)                                |  13911.06 blocks/sec |          8.63ms
Repeated Access (1000 ops)                                   | 1091718.55 blocks/sec |        915.99µs
Repeated Access MT x2 (1000 ops)                             | 1245599.92 blocks/sec |        802.83µs
Repeated Access MT x4 (1000 ops)                             | 1165078.86 blocks/sec |        858.31µs
Repeated Access MT x8 (1000 ops)                             | 882499.59 blocks/sec |          1.13ms
Repeated Access MT x16 (1000 ops)                            | 746597.94 blocks/sec |          1.34ms
Repeated Access MT x32 (1000 ops)                            | 657702.62 blocks/sec |          1.52ms
Repeated Access MT x64 (1000 ops)                            | 449010.38 blocks/sec |          2.23ms
Repeated Access MT x128 (1000 ops)                           | 238439.73 blocks/sec |          4.19ms
Repeated Access MT x256 (1000 ops)                           | 114846.48 blocks/sec |          8.71ms
Random (K=10, 500 ops)                                       | 1081279.80 blocks/sec |        462.42µs
Random (K=50, 500 ops)                                       | 285672.82 blocks/sec |          1.75ms
Random (K=100, 500 ops)                                      | 267468.36 blocks/sec |          1.87ms
Random MT x2 (K=10, 500 ops)                                 | 1132877.46 blocks/sec |        441.35µs
Random MT x4 (K=10, 500 ops)                                 | 1148712.29 blocks/sec |        435.27µs
Random MT x8 (K=10, 500 ops)                                 | 911683.41 blocks/sec |        548.44µs
Random MT x16 (K=10, 500 ops)                                | 849236.71 blocks/sec |        588.76µs
Random MT x32 (K=10, 500 ops)                                | 602974.84 blocks/sec |        829.22µs
Random MT x64 (K=10, 500 ops)                                | 275568.48 blocks/sec |          1.81ms
Random MT x128 (K=10, 500 ops)                               | 122821.12 blocks/sec |          4.07ms
Random MT x256 (K=10, 500 ops)                               |  58086.72 blocks/sec |          8.61ms
Random MT x2 (K=50, 500 ops)                                 | 320968.35 blocks/sec |          1.56ms
Random MT x4 (K=50, 500 ops)                                 | 228460.62 blocks/sec |          2.19ms
Random MT x8 (K=50, 500 ops)                                 | 190506.45 blocks/sec |          2.62ms
Random MT x16 (K=50, 500 ops)                                | 143472.72 blocks/sec |          3.48ms
Random MT x32 (K=50, 500 ops)                                |  98756.44 blocks/sec |          5.06ms
Random MT x64 (K=50, 500 ops)                                |  86412.16 blocks/sec |          5.79ms
Random MT x128 (K=50, 500 ops)                               | 119381.77 blocks/sec |          4.19ms
Random MT x256 (K=50, 500 ops)                               |  60632.56 blocks/sec |          8.25ms
Random MT x2 (K=100, 500 ops)                                | 269698.07 blocks/sec |          1.85ms
Random MT x4 (K=100, 500 ops)                                | 205144.87 blocks/sec |          2.44ms
Random MT x8 (K=100, 500 ops)                                | 169080.69 blocks/sec |          2.96ms
Random MT x16 (K=100, 500 ops)                               | 123447.37 blocks/sec |          4.05ms
Random MT x32 (K=100, 500 ops)                               |  86765.56 blocks/sec |          5.76ms
Random MT x64 (K=100, 500 ops)                               |  76000.06 blocks/sec |          6.58ms
Random MT x128 (K=100, 500 ops)                              |  94914.50 blocks/sec |          5.27ms
Random MT x256 (K=100, 500 ops)                              |  56509.23 blocks/sec |          8.85ms
Zipfian (80/20, 500 ops)                                     | 509782.73 blocks/sec |        980.81µs
Zipfian MT x2 (80/20, 500 ops)                               | 614637.22 blocks/sec |        813.49µs
Zipfian MT x4 (80/20, 500 ops)                               | 609556.88 blocks/sec |        820.27µs
Zipfian MT x8 (80/20, 500 ops)                               | 450887.93 blocks/sec |          1.11ms
Zipfian MT x16 (80/20, 500 ops)                              | 349149.82 blocks/sec |          1.43ms
Zipfian MT x32 (80/20, 500 ops)                              | 287022.29 blocks/sec |          1.74ms
Zipfian MT x64 (80/20, 500 ops)                              | 208275.97 blocks/sec |          2.40ms
Zipfian MT x128 (80/20, 500 ops)                             | 116573.49 blocks/sec |          4.29ms
Zipfian MT x256 (80/20, 500 ops)                             |  58025.86 blocks/sec |          8.62ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     252649
                 16 |     281411
                 32 |     328019
                 64 |     499228
                128 |    1194894
                256 |    1192319


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1198443
       32 |          33 |    1043103
       32 |          37 |     742256
       32 |          42 |     606517
       32 |          52 |     449375

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  71.3% (hits: 37262, misses: 14996)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.3% (hits: 12646, misses: 41618)
Random (K=100)       | Hit rate:  10.8% (hits: 5914, misses: 48860)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1212938.07 ops/sec |          8.24ms
2 threads, 5000 ops/thread                                   |    246445.68 ops/sec |         40.58ms
4 threads, 2500 ops/thread                                   |    191906.09 ops/sec |         52.11ms
8 threads, 1250 ops/thread                                   |    166246.00 ops/sec |         60.15ms
16 threads, 625 ops/thread                                   |     97678.00 ops/sec |        102.38ms
32 threads, 312 ops/thread                                   |     73719.60 ops/sec |        135.65ms
64 threads, 156 ops/thread                                   |     67693.94 ops/sec |        147.72ms
128 threads, 78 ops/thread                                   |     55441.63 ops/sec |        180.37ms
256 threads, 39 ops/thread                                   |     41255.72 ops/sec |        242.39ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1146701.80 ops/sec |          8.72ms
2 threads, K=4, 5000 ops/thread                              |   1386816.75 ops/sec |          7.21ms
4 threads, K=4, 2500 ops/thread                              |   1121853.23 ops/sec |          8.91ms
8 threads, K=4, 1250 ops/thread                              |    872955.70 ops/sec |         11.46ms
16 threads, K=4, 625 ops/thread                              |    898652.08 ops/sec |         11.13ms
32 threads, K=4, 312 ops/thread                              |    969540.43 ops/sec |         10.31ms
64 threads, K=4, 156 ops/thread                              |    968767.23 ops/sec |         10.32ms
128 threads, K=4, 78 ops/thread                              |    832256.60 ops/sec |         12.02ms
256 threads, K=4, 39 ops/thread                              |    721264.41 ops/sec |         13.86ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.17ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-bb12b085f3a416ad)
```
