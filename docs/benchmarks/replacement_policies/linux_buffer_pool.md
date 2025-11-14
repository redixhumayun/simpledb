# Linux (i7-8650U, Ubuntu 6.8.0-86)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   818.00ns |   817.00ns |     5.00ns |      100
Cold Pin (miss)                                              |     4.58µs |     3.58µs |     3.75µs |      100
Dirty Eviction                                               |     4.99ms |     5.00ms |   327.00µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 244963.45 blocks/sec |        489.87µs
Seq Scan MT x4 (120 blocks)                                  | 178636.29 blocks/sec |        671.76µs
Seq Scan MT x16 (120 blocks)                                 | 130283.96 blocks/sec |        921.07µs
Repeated Access (1000 ops)                                   | 967967.07 blocks/sec |          1.03ms
Repeated Access MT x4 (1000 ops)                             | 1132891.58 blocks/sec |        882.70µs
Repeated Access MT x16 (1000 ops)                            | 700905.92 blocks/sec |          1.43ms
Random (K=10, 500 ops)                                       | 1059762.10 blocks/sec |        471.80µs
Random (K=50, 500 ops)                                       | 272744.73 blocks/sec |          1.83ms
Random (K=100, 500 ops)                                      | 245099.36 blocks/sec |          2.04ms
Random MT x4 (K=10, 500 ops)                                 | 1116233.38 blocks/sec |        447.94µs
Random MT x16 (K=10, 500 ops)                                | 776800.39 blocks/sec |        643.67µs
Random MT x4 (K=50, 500 ops)                                 | 238794.45 blocks/sec |          2.09ms
Random MT x16 (K=50, 500 ops)                                | 154742.28 blocks/sec |          3.23ms
Random MT x4 (K=100, 500 ops)                                | 209498.41 blocks/sec |          2.39ms
Random MT x16 (K=100, 500 ops)                               | 126867.20 blocks/sec |          3.94ms
Zipfian (80/20, 500 ops)                                     | 639051.14 blocks/sec |        782.41µs
Zipfian MT x4 (80/20, 500 ops)                               | 679901.17 blocks/sec |        735.40µs
Zipfian MT x16 (80/20, 500 ops)                              | 458849.88 blocks/sec |          1.09ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     219347
                 16 |     261950
                 32 |     303412
                 64 |     493876
                128 |    1037086
                256 |    1074192


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1082792
       32 |          33 |     988652
       32 |          37 |     714449
       32 |          42 |     596578
       32 |          52 |     442429

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  80.4% (hits: 40996, misses: 10004)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  25.0% (hits: 12749, misses: 38251)
Random (K=100)       | Hit rate:  11.6% (hits: 5916, misses: 45084)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    207304.35 ops/sec |          9.65ms
4 threads, 1000 ops/thread                                   |    191824.40 ops/sec |         20.85ms
8 threads, 1000 ops/thread                                   |    169727.84 ops/sec |         47.13ms
16 threads, 1000 ops/thread                                  |    100523.60 ops/sec |        159.17ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |    987472.67 ops/sec |          4.05ms
8 threads, K=4, 1000 ops/thread                              |    776077.87 ops/sec |         10.31ms
16 threads, K=4, 1000 ops/thread                             |    737459.15 ops/sec |         21.70ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.79ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-5808dadef433eec1)
```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   797.00ns |   797.00ns |     4.00ns |      100
Cold Pin (miss)                                              |     4.12µs |     3.52µs |     1.36µs |      100
Dirty Eviction                                               |     5.00ms |     4.96ms |   286.87µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 249106.85 blocks/sec |        481.72µs
Seq Scan MT x4 (120 blocks)                                  | 169125.33 blocks/sec |        709.53µs
Seq Scan MT x16 (120 blocks)                                 | 121891.64 blocks/sec |        984.48µs
Repeated Access (1000 ops)                                   | 1122702.39 blocks/sec |        890.71µs
Repeated Access MT x4 (1000 ops)                             | 1189998.77 blocks/sec |        840.34µs
Repeated Access MT x16 (1000 ops)                            | 789588.80 blocks/sec |          1.27ms
Random (K=10, 500 ops)                                       | 1152559.72 blocks/sec |        433.82µs
Random (K=50, 500 ops)                                       | 278163.81 blocks/sec |          1.80ms
Random (K=100, 500 ops)                                      | 255566.62 blocks/sec |          1.96ms
Random MT x4 (K=10, 500 ops)                                 | 1164567.34 blocks/sec |        429.34µs
Random MT x16 (K=10, 500 ops)                                | 841736.67 blocks/sec |        594.01µs
Random MT x4 (K=50, 500 ops)                                 | 220248.40 blocks/sec |          2.27ms
Random MT x16 (K=50, 500 ops)                                | 192745.22 blocks/sec |          2.59ms
Random MT x4 (K=100, 500 ops)                                | 198704.45 blocks/sec |          2.52ms
Random MT x16 (K=100, 500 ops)                               | 155565.27 blocks/sec |          3.21ms
Zipfian (80/20, 500 ops)                                     | 567930.77 blocks/sec |        880.39µs
Zipfian MT x4 (80/20, 500 ops)                               | 704171.80 blocks/sec |        710.05µs
Zipfian MT x16 (80/20, 500 ops)                              | 424394.18 blocks/sec |          1.18ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     232683
                 16 |     274975
                 32 |     333161
                 64 |     489268
                128 |    1170796
                256 |    1165767


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1181123
       32 |          33 |     948338
       32 |          37 |     761407
       32 |          42 |     641376
       32 |          52 |     491894

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  75.6% (hits: 38650, misses: 12456)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  26.0% (hits: 13257, misses: 37745)
Random (K=100)       | Hit rate:  10.6% (hits: 5406, misses: 45598)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    217423.92 ops/sec |          9.20ms
4 threads, 1000 ops/thread                                   |    184298.77 ops/sec |         21.70ms
8 threads, 1000 ops/thread                                   |    132424.05 ops/sec |         60.41ms
16 threads, 1000 ops/thread                                  |    123916.40 ops/sec |        129.12ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1043933.14 ops/sec |          3.83ms
8 threads, K=4, 1000 ops/thread                              |    845002.64 ops/sec |          9.47ms
16 threads, K=4, 1000 ops/thread                             |    888587.07 ops/sec |         18.01ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   174.90ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-eddfe38e510ad5d3)
```

## Replacement SIEVE (`--no-default-features --features replacement_sieve`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   985.00ns |   985.00ns |     5.00ns |      100
Cold Pin (miss)                                              |     5.10µs |     4.37µs |     1.69µs |      100
Dirty Eviction                                               |     5.00ms |     5.01ms |   246.81µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 249159.09 blocks/sec |        481.62µs
Seq Scan MT x4 (120 blocks)                                  | 181197.72 blocks/sec |        662.26µs
Seq Scan MT x16 (120 blocks)                                 | 136206.07 blocks/sec |        881.02µs
Repeated Access (1000 ops)                                   | 1053654.18 blocks/sec |        949.08µs
Repeated Access MT x4 (1000 ops)                             | 1196203.73 blocks/sec |        835.98µs
Repeated Access MT x16 (1000 ops)                            | 822699.26 blocks/sec |          1.22ms
Random (K=10, 500 ops)                                       | 1218098.01 blocks/sec |        410.48µs
Random (K=50, 500 ops)                                       | 269333.14 blocks/sec |          1.86ms
Random (K=100, 500 ops)                                      | 242574.43 blocks/sec |          2.06ms
Random MT x4 (K=10, 500 ops)                                 | 1181954.86 blocks/sec |        423.03µs
Random MT x16 (K=10, 500 ops)                                | 845773.25 blocks/sec |        591.18µs
Random MT x4 (K=50, 500 ops)                                 | 238860.39 blocks/sec |          2.09ms
Random MT x16 (K=50, 500 ops)                                | 163722.61 blocks/sec |          3.05ms
Random MT x4 (K=100, 500 ops)                                | 211965.27 blocks/sec |          2.36ms
Random MT x16 (K=100, 500 ops)                               | 137982.34 blocks/sec |          3.62ms
Zipfian (80/20, 500 ops)                                     | 477527.55 blocks/sec |          1.05ms
Zipfian MT x4 (80/20, 500 ops)                               | 582906.16 blocks/sec |        857.77µs
Zipfian MT x16 (80/20, 500 ops)                              | 414872.34 blocks/sec |          1.21ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     209428
                 16 |     254415
                 32 |     313748
                 64 |     483268
                128 |    1127927
                256 |    1131770


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1141235
       32 |          33 |     998399
       32 |          37 |     716359
       32 |          42 |     591008
       32 |          52 |     446880

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  62.5% (hits: 32945, misses: 19737)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.3% (hits: 12646, misses: 41618)
Random (K=100)       | Hit rate:  11.9% (hits: 6528, misses: 48245)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    216776.39 ops/sec |          9.23ms
4 threads, 1000 ops/thread                                   |    199351.45 ops/sec |         20.07ms
8 threads, 1000 ops/thread                                   |    169283.42 ops/sec |         47.26ms
16 threads, 1000 ops/thread                                  |    101886.77 ops/sec |        157.04ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1069095.94 ops/sec |          3.74ms
8 threads, K=4, 1000 ops/thread                              |    857300.14 ops/sec |          9.33ms
16 threads, K=4, 1000 ops/thread                             |    892197.60 ops/sec |         17.93ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.13ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-73fb01e136c64d8a)
```
