# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   275.00ns |   291.00ns |    26.00ns |      100
Cold Pin (miss)                                              |     2.39µs |     2.06µs |   688.00ns |      100
Dirty Eviction                                               |     3.80ms |     3.07ms |     1.93ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 200304.13 blocks/sec |        599.09µs
Seq Scan MT x4 (120 blocks)                                  | 194520.68 blocks/sec |        616.90µs
Seq Scan MT x16 (120 blocks)                                 | 115331.16 blocks/sec |          1.04ms
Repeated Access (1000 ops)                                   | 3566143.04 blocks/sec |        280.42µs
Repeated Access MT x4 (1000 ops)                             | 942492.86 blocks/sec |          1.06ms
Repeated Access MT x16 (1000 ops)                            | 306684.21 blocks/sec |          3.26ms
Random (K=10, 500 ops)                                       | 3526963.64 blocks/sec |        141.77µs
Random (K=50, 500 ops)                                       | 601301.22 blocks/sec |        831.53µs
Random (K=100, 500 ops)                                      | 535583.07 blocks/sec |        933.56µs
Random MT x4 (K=10, 500 ops)                                 | 916197.27 blocks/sec |        545.73µs
Random MT x16 (K=10, 500 ops)                                | 287041.90 blocks/sec |          1.74ms
Random MT x4 (K=50, 500 ops)                                 | 213250.90 blocks/sec |          2.34ms
Random MT x16 (K=50, 500 ops)                                | 126550.14 blocks/sec |          3.95ms
Random MT x4 (K=100, 500 ops)                                | 265569.55 blocks/sec |          1.88ms
Random MT x16 (K=100, 500 ops)                               | 116163.18 blocks/sec |          4.30ms
Zipfian (80/20, 500 ops)                                     | 1513303.45 blocks/sec |        330.40µs
Zipfian MT x4 (80/20, 500 ops)                               | 539475.00 blocks/sec |        926.83µs
Zipfian MT x16 (80/20, 500 ops)                              | 237819.82 blocks/sec |          2.10ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     500631
                 16 |     577423
                 32 |     735360
                 64 |    1166086
                128 |    3522714
                256 |    3546124


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3527561
       32 |          33 |    2893000
       32 |          37 |    1729505
       32 |          42 |    1452846
       32 |          52 |    1068981

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  79.6% (hits: 40589, misses: 10411)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  21.4% (hits: 10914, misses: 40086)
Random (K=100)       | Hit rate:  13.2% (hits: 6731, misses: 44269)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1243130.15 ops/sec |          1.61ms
4 threads, 1000 ops/thread                                   |    295647.43 ops/sec |         13.53ms
8 threads, 1000 ops/thread                                   |    185532.43 ops/sec |         43.12ms
16 threads, 1000 ops/thread                                  |    100164.02 ops/sec |        159.74ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1034344.37 ops/sec |          3.87ms
8 threads, K=4, 1000 ops/thread                              |    516540.73 ops/sec |         15.49ms
16 threads, K=4, 1000 ops/thread                             |    336741.94 ops/sec |         47.51ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   205.02ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-595f081dbf33478a)
```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   266.00ns |   250.00ns |    26.00ns |      100
Cold Pin (miss)                                              |     2.27µs |     1.96µs |   758.00ns |      100
Dirty Eviction                                               |     3.16ms |     3.01ms |   687.84µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 192499.27 blocks/sec |        623.38µs
Seq Scan MT x4 (120 blocks)                                  | 160047.37 blocks/sec |        749.78µs
Seq Scan MT x16 (120 blocks)                                 | 116938.55 blocks/sec |          1.03ms
Repeated Access (1000 ops)                                   | 3829598.20 blocks/sec |        261.12µs
Repeated Access MT x4 (1000 ops)                             | 1028334.74 blocks/sec |        972.45µs
Repeated Access MT x16 (1000 ops)                            | 568193.76 blocks/sec |          1.76ms
Random (K=10, 500 ops)                                       | 3846716.06 blocks/sec |        129.98µs
Random (K=50, 500 ops)                                       | 624964.85 blocks/sec |        800.05µs
Random (K=100, 500 ops)                                      | 546648.23 blocks/sec |        914.67µs
Random MT x4 (K=10, 500 ops)                                 | 1003741.95 blocks/sec |        498.14µs
Random MT x16 (K=10, 500 ops)                                | 586674.74 blocks/sec |        852.26µs
Random MT x4 (K=50, 500 ops)                                 | 217183.85 blocks/sec |          2.30ms
Random MT x16 (K=50, 500 ops)                                | 184703.25 blocks/sec |          2.71ms
Random MT x4 (K=100, 500 ops)                                | 211873.56 blocks/sec |          2.36ms
Random MT x16 (K=100, 500 ops)                               | 152102.91 blocks/sec |          3.29ms
Zipfian (80/20, 500 ops)                                     | 1517851.45 blocks/sec |        329.41µs
Zipfian MT x4 (80/20, 500 ops)                               | 549016.16 blocks/sec |        910.72µs
Zipfian MT x16 (80/20, 500 ops)                              | 394160.12 blocks/sec |          1.27ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     483166
                 16 |     575640
                 32 |     751590
                 64 |    1238298
                128 |    3871108
                256 |    3886000


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3899335
       32 |          33 |    2651999
       32 |          37 |    2160527
       32 |          42 |    1527305
       32 |          52 |    1139059

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  73.6% (hits: 37528, misses: 13474)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  24.2% (hits: 12342, misses: 38762)
Random (K=100)       | Hit rate:  10.6% (hits: 5406, misses: 45595)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1524449.50 ops/sec |          1.31ms
4 threads, 1000 ops/thread                                   |    311785.04 ops/sec |         12.83ms
8 threads, 1000 ops/thread                                   |    151912.67 ops/sec |         52.66ms
16 threads, 1000 ops/thread                                  |    137918.94 ops/sec |        116.01ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1122190.25 ops/sec |          3.56ms
8 threads, K=4, 1000 ops/thread                              |    716089.36 ops/sec |         11.17ms
16 threads, K=4, 1000 ops/thread                             |    535460.34 ops/sec |         29.88ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   205.41ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-2ed923e46913521e)
```

## Replacement SIEVE (`--no-default-features --features replacement_sieve`)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   274.00ns |   291.00ns |    24.00ns |      100
Cold Pin (miss)                                              |     2.41µs |     2.04µs |   814.00ns |      100
Dirty Eviction                                               |     3.20ms |     3.04ms |   853.72µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 203932.50 blocks/sec |        588.43µs
Seq Scan MT x4 (120 blocks)                                  | 201817.70 blocks/sec |        594.60µs
Seq Scan MT x16 (120 blocks)                                 | 111845.46 blocks/sec |          1.07ms
Repeated Access (1000 ops)                                   | 3815541.46 blocks/sec |        262.09µs
Repeated Access MT x4 (1000 ops)                             | 972055.35 blocks/sec |          1.03ms
Repeated Access MT x16 (1000 ops)                            | 565370.71 blocks/sec |          1.77ms
Random (K=10, 500 ops)                                       | 3666253.60 blocks/sec |        136.38µs
Random (K=50, 500 ops)                                       | 591015.15 blocks/sec |        846.00µs
Random (K=100, 500 ops)                                      | 519800.77 blocks/sec |        961.91µs
Random MT x4 (K=10, 500 ops)                                 | 1024709.85 blocks/sec |        487.94µs
Random MT x16 (K=10, 500 ops)                                | 570963.97 blocks/sec |        875.71µs
Random MT x4 (K=50, 500 ops)                                 | 281440.10 blocks/sec |          1.78ms
Random MT x16 (K=50, 500 ops)                                | 164464.70 blocks/sec |          3.04ms
Random MT x4 (K=100, 500 ops)                                | 274905.93 blocks/sec |          1.82ms
Random MT x16 (K=100, 500 ops)                               | 133240.53 blocks/sec |          3.75ms
Zipfian (80/20, 500 ops)                                     | 1297222.13 blocks/sec |        385.44µs
Zipfian MT x4 (80/20, 500 ops)                               | 518807.28 blocks/sec |        963.75µs
Zipfian MT x16 (80/20, 500 ops)                              | 322162.46 blocks/sec |          1.55ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     475395
                 16 |     569141
                 32 |     680137
                 64 |    1129010
                128 |    3875008
                256 |    3878916


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    3890990
       32 |          33 |    3132636
       32 |          37 |    1926315
       32 |          42 |    1478559
       32 |          52 |    1085774

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  67.1% (hits: 35173, misses: 17256)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  21.2% (hits: 11518, misses: 42849)
Random (K=100)       | Hit rate:  12.5% (hits: 6834, misses: 47940)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |   1365985.86 ops/sec |          1.46ms
4 threads, 1000 ops/thread                                   |    301520.05 ops/sec |         13.27ms
8 threads, 1000 ops/thread                                   |    189182.50 ops/sec |         42.29ms
16 threads, 1000 ops/thread                                  |    101077.98 ops/sec |        158.29ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1114876.61 ops/sec |          3.59ms
8 threads, K=4, 1000 ops/thread                              |    713411.53 ops/sec |         11.21ms
16 threads, K=4, 1000 ops/thread                             |    528242.90 ops/sec |         30.29ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   203.91ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-83da990292f15006)
```
