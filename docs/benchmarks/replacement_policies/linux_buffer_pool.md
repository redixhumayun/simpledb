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
Pin/Unpin (hit)                                              |   837.00ns |   836.00ns |     6.00ns |      100
Cold Pin (miss)                                              |     4.42µs |     4.02µs |     2.01µs |      100
Dirty Eviction                                               |   452.47µs |     4.36µs |     1.40ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 241270.05 blocks/sec |        497.37µs
Seq Scan MT x4 (120 blocks)                                  | 182544.49 blocks/sec |        657.37µs
Seq Scan MT x16 (120 blocks)                                 | 116018.70 blocks/sec |          1.03ms
Repeated Access (1000 ops)                                   | 968559.59 blocks/sec |          1.03ms
Repeated Access MT x4 (1000 ops)                             | 1147940.88 blocks/sec |        871.13µs
Repeated Access MT x16 (1000 ops)                            | 684140.53 blocks/sec |          1.46ms
Random (K=10, 500 ops)                                       | 1091293.23 blocks/sec |        458.17µs
Random (K=50, 500 ops)                                       | 283161.46 blocks/sec |          1.77ms
Random (K=100, 500 ops)                                      | 254796.54 blocks/sec |          1.96ms
Random MT x4 (K=10, 500 ops)                                 | 1109520.80 blocks/sec |        450.65µs
Random MT x16 (K=10, 500 ops)                                | 779193.66 blocks/sec |        641.69µs
Random MT x4 (K=50, 500 ops)                                 | 223990.25 blocks/sec |          2.23ms
Random MT x16 (K=50, 500 ops)                                | 138130.74 blocks/sec |          3.62ms
Random MT x4 (K=100, 500 ops)                                | 206723.13 blocks/sec |          2.42ms
Random MT x16 (K=100, 500 ops)                               | 117259.47 blocks/sec |          4.26ms
Zipfian (80/20, 500 ops)                                     | 476356.97 blocks/sec |          1.05ms
Zipfian MT x4 (80/20, 500 ops)                               | 568716.28 blocks/sec |        879.17µs
Zipfian MT x16 (80/20, 500 ops)                              | 411739.86 blocks/sec |          1.21ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     246509
                 16 |     266461
                 32 |     299802
                 64 |     433068
                128 |    1047816
                256 |    1051704


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1070444
       32 |          33 |     910473
       32 |          37 |     732476
       32 |          42 |     535248
       32 |          52 |     383945

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  81.2% (hits: 41405, misses: 9595)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  18.8% (hits: 9587, misses: 41413)
Random (K=100)       | Hit rate:  10.6% (hits: 5406, misses: 45594)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    218817.02 ops/sec |          9.14ms
4 threads, 1000 ops/thread                                   |    196410.83 ops/sec |         20.37ms
8 threads, 1000 ops/thread                                   |    170027.34 ops/sec |         47.05ms
16 threads, 1000 ops/thread                                  |     98751.98 ops/sec |        162.02ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1029927.11 ops/sec |          3.88ms
8 threads, K=4, 1000 ops/thread                              |    800751.83 ops/sec |          9.99ms
16 threads, K=4, 1000 ops/thread                             |    753298.83 ops/sec |         21.24ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.57ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-04ba11714cd0931e)
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
Pin/Unpin (hit)                                              |     1.11µs |   808.00ns |     2.94µs |      100
Cold Pin (miss)                                              |     4.19µs |     3.95µs |     1.51µs |      100
Dirty Eviction                                               |   504.53µs |     4.31µs |     1.54ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 249760.13 blocks/sec |        480.46µs
Seq Scan MT x4 (120 blocks)                                  | 172438.32 blocks/sec |        695.90µs
Seq Scan MT x16 (120 blocks)                                 | 118783.54 blocks/sec |          1.01ms
Repeated Access (1000 ops)                                   | 1023300.55 blocks/sec |        977.23µs
Repeated Access MT x4 (1000 ops)                             | 1160884.69 blocks/sec |        861.41µs
Repeated Access MT x16 (1000 ops)                            | 807527.45 blocks/sec |          1.24ms
Random (K=10, 500 ops)                                       | 1170880.67 blocks/sec |        427.03µs
Random (K=50, 500 ops)                                       | 287163.89 blocks/sec |          1.74ms
Random (K=100, 500 ops)                                      | 255869.39 blocks/sec |          1.95ms
Random MT x4 (K=10, 500 ops)                                 | 1140615.02 blocks/sec |        438.36µs
Random MT x16 (K=10, 500 ops)                                | 878864.37 blocks/sec |        568.92µs
Random MT x4 (K=50, 500 ops)                                 | 217852.77 blocks/sec |          2.30ms
Random MT x16 (K=50, 500 ops)                                | 185040.51 blocks/sec |          2.70ms
Random MT x4 (K=100, 500 ops)                                | 203618.21 blocks/sec |          2.46ms
Random MT x16 (K=100, 500 ops)                               | 151598.21 blocks/sec |          3.30ms
Zipfian (80/20, 500 ops)                                     | 610201.10 blocks/sec |        819.40µs
Zipfian MT x4 (80/20, 500 ops)                               | 691401.32 blocks/sec |        723.17µs
Zipfian MT x16 (80/20, 500 ops)                              | 457755.57 blocks/sec |          1.09ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     250454
                 16 |     267277
                 32 |     315210
                 64 |     500692
                128 |    1183768
                256 |    1181734


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1190884
       32 |          33 |     936279
       32 |          37 |     795768
       32 |          42 |     605357
       32 |          52 |     466040

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  79.1% (hits: 40487, misses: 10718)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.4% (hits: 11423, misses: 39578)
Random (K=100)       | Hit rate:  11.8% (hits: 6019, misses: 44983)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    253871.09 ops/sec |          7.88ms
4 threads, 1000 ops/thread                                   |    186826.29 ops/sec |         21.41ms
8 threads, 1000 ops/thread                                   |    130216.61 ops/sec |         61.44ms
16 threads, 1000 ops/thread                                  |    122604.39 ops/sec |        130.50ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1124882.48 ops/sec |          3.56ms
8 threads, K=4, 1000 ops/thread                              |    891370.86 ops/sec |          8.97ms
16 threads, K=4, 1000 ops/thread                             |    920802.42 ops/sec |         17.38ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.23ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-831160f8f5b9694a)
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
Pin/Unpin (hit)                                              |     1.00µs |     1.00µs |    12.00ns |      100
Cold Pin (miss)                                              |     5.24µs |     4.74µs |     2.19µs |      100
Dirty Eviction                                               |   455.48µs |     4.38µs |     1.41ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 244891.46 blocks/sec |        490.01µs
Seq Scan MT x4 (120 blocks)                                  | 178245.18 blocks/sec |        673.23µs
Seq Scan MT x16 (120 blocks)                                 | 120185.69 blocks/sec |        998.46µs
Repeated Access (1000 ops)                                   | 1030325.57 blocks/sec |        970.57µs
Repeated Access MT x4 (1000 ops)                             | 1213609.91 blocks/sec |        823.99µs
Repeated Access MT x16 (1000 ops)                            | 799276.18 blocks/sec |          1.25ms
Random (K=10, 500 ops)                                       | 1071852.72 blocks/sec |        466.48µs
Random (K=50, 500 ops)                                       | 255509.94 blocks/sec |          1.96ms
Random (K=100, 500 ops)                                      | 241485.00 blocks/sec |          2.07ms
Random MT x4 (K=10, 500 ops)                                 | 1209511.60 blocks/sec |        413.39µs
Random MT x16 (K=10, 500 ops)                                | 838848.76 blocks/sec |        596.06µs
Random MT x4 (K=50, 500 ops)                                 | 235451.12 blocks/sec |          2.12ms
Random MT x16 (K=50, 500 ops)                                | 150247.34 blocks/sec |          3.33ms
Random MT x4 (K=100, 500 ops)                                | 193877.28 blocks/sec |          2.58ms
Random MT x16 (K=100, 500 ops)                               | 119380.26 blocks/sec |          4.19ms
Zipfian (80/20, 500 ops)                                     | 476603.08 blocks/sec |          1.05ms
Zipfian MT x4 (80/20, 500 ops)                               | 521471.05 blocks/sec |        958.83µs
Zipfian MT x16 (80/20, 500 ops)                              | 367862.10 blocks/sec |          1.36ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     224999
                 16 |     239751
                 32 |     290976
                 64 |     436613
                128 |    1122299
                256 |    1123214


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1125267
       32 |          33 |     981229
       32 |          37 |     686835
       32 |          42 |     553931
       32 |          52 |     429869

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  67.9% (hits: 35594, misses: 16834)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.3% (hits: 12646, misses: 41720)
Random (K=100)       | Hit rate:  12.1% (hits: 6629, misses: 48111)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    232278.46 ops/sec |          8.61ms
4 threads, 1000 ops/thread                                   |    197062.17 ops/sec |         20.30ms
8 threads, 1000 ops/thread                                   |    163666.64 ops/sec |         48.88ms
16 threads, 1000 ops/thread                                  |     92131.55 ops/sec |        173.66ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1071646.54 ops/sec |          3.73ms
8 threads, K=4, 1000 ops/thread                              |    863020.80 ops/sec |          9.27ms
16 threads, K=4, 1000 ops/thread                             |    895454.66 ops/sec |         17.87ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.13ms

All benchmarks completed!
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-46f7dfc99e749e33)
```
