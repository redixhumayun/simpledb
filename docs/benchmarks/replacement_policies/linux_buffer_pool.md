# Linux Buffer Pool Benchmarks (Dell XPS 13-9370, Ubuntu 6.8.0-86)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Master (first-unpinned policy)

```
➜  simpledb git:(master) ✗ cargo bench --bench buffer_pool -- 100 12
   Compiling simpledb v0.1.0 (/home/zaid-humayun/Desktop/Development/simpledb)
    Finished `bench` profile [optimized + debuginfo] target(s) in 5.73s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-4fba10c72d7e463d)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   829.00ns |   829.00ns |     5.00ns |      100
Cold Pin (miss)                                              |     6.41µs |     6.22µs |     1.22µs |      100
Dirty Eviction                                               |     5.00ms |     4.97ms |   234.04µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 163392.46 blocks/sec |        734.43µs
Seq Scan MT x4 (120 blocks)                                  | 120744.02 blocks/sec |        993.84µs
Repeated Access (1000 ops)                                   | 162066.10 blocks/sec |          6.17ms
Repeated Access MT x4 (1000 ops)                             | 175440.54 blocks/sec |          5.70ms
Random (K=10, 500 ops)                                       | 175008.75 blocks/sec |          2.86ms
Random (K=50, 500 ops)                                       | 165206.79 blocks/sec |          3.03ms
Random (K=100, 500 ops)                                      | 163452.22 blocks/sec |          3.06ms
Random MT x4 (K=10, 500 ops)                                 | 176986.45 blocks/sec |          2.83ms
Random MT x4 (K=50, 500 ops)                                 | 130607.84 blocks/sec |          3.83ms
Random MT x4 (K=100, 500 ops)                                | 128069.57 blocks/sec |          3.90ms
Zipfian (80/20, 500 ops)                                     | 175094.12 blocks/sec |          2.86ms
Zipfian MT x4 (80/20, 500 ops)                               | 174244.50 blocks/sec |          2.87ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     164032
                 16 |     163163
                 32 |     163438
                 64 |     160276
                128 |     163017
                256 |     163808


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |     167556
       32 |          33 |     166894
       32 |          37 |     166741
       32 |          42 |     166924
       32 |          52 |     168506

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate:   0.0% (hits: 0, misses: 102000)
Zipfian (80/20)      | Hit rate:   9.0% (hits: 4590, misses: 46410)
Random (K=10)        | Hit rate:  10.4% (hits: 5303, misses: 45697)
Random (K=50)        | Hit rate:   2.8% (hits: 1428, misses: 49572)
Random (K=100)       | Hit rate:   0.6% (hits: 306, misses: 50694)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    144767.32 ops/sec |         13.82ms
4 threads, 1000 ops/thread                                   |    138062.32 ops/sec |         28.97ms
8 threads, 1000 ops/thread                                   |    126000.63 ops/sec |         63.49ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1299457.77 ops/sec |          3.08ms
8 threads, K=4, 1000 ops/thread                              |    997291.48 ops/sec |          8.02ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.04ms

All benchmarks completed!
➜  simpledb git:(master) ✗
```

## Replacement LRU (`--no-default-features --features replacement_lru`)

```
➜  simpledb git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_lru -- 100 12
   Compiling simpledb v0.1.0 (/home/zaid-humayun/Desktop/Development/simpledb)
    Finished `bench` profile [optimized + debuginfo] target(s) in 5.82s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-5808dadef433eec1)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   804.00ns |   804.00ns |     5.00ns |      100
Cold Pin (miss)                                              |     4.11µs |     3.54µs |     1.59µs |      100
Dirty Eviction                                               |     5.00ms |     4.97ms |   267.75µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 251317.85 blocks/sec |        477.48µs
Seq Scan MT x4 (120 blocks)                                  | 182483.69 blocks/sec |        657.59µs
Repeated Access (1000 ops)                                   | 1184803.24 blocks/sec |        844.02µs
Repeated Access MT x4 (1000 ops)                             | 1233203.76 blocks/sec |        810.90µs
Random (K=10, 500 ops)                                       | 1197797.01 blocks/sec |        417.43µs
Random (K=50, 500 ops)                                       | 314310.82 blocks/sec |          1.59ms
Random (K=100, 500 ops)                                      | 273779.08 blocks/sec |          1.83ms
Random MT x4 (K=10, 500 ops)                                 | 1114983.82 blocks/sec |        448.44µs
Random MT x4 (K=50, 500 ops)                                 | 245147.55 blocks/sec |          2.04ms
Random MT x4 (K=100, 500 ops)                                | 213497.57 blocks/sec |          2.34ms
Zipfian (80/20, 500 ops)                                     | 694831.43 blocks/sec |        719.60µs
Zipfian MT x4 (80/20, 500 ops)                               | 701149.18 blocks/sec |        713.12µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     262290
                 16 |     297675
                 32 |     358691
                 64 |     543959
                128 |    1192134
                256 |    1192191


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1199783
       32 |          33 |    1035173
       32 |          37 |     824333
       32 |          42 |     650619
       32 |          52 |     505888

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  81.4% (hits: 41509, misses: 9491)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  23.0% (hits: 11729, misses: 39271)
Random (K=100)       | Hit rate:  10.2% (hits: 5202, misses: 45798)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    220074.47 ops/sec |          9.09ms
4 threads, 1000 ops/thread                                   |    200436.74 ops/sec |         19.96ms
8 threads, 1000 ops/thread                                   |    184006.53 ops/sec |         43.48ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1159099.44 ops/sec |          3.45ms
8 threads, K=4, 1000 ops/thread                              |    890711.48 ops/sec |          8.98ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.66ms

All benchmarks completed!
➜  simpledb git:(feature/replacement-policy) ✗
```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
➜  simpledb git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_clock -- 100 12
   Compiling simpledb v0.1.0 (/home/zaid-humayun/Desktop/Development/simpledb)
    Finished `bench` profile [optimized + debuginfo] target(s) in 5.92s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-eddfe38e510ad5d3)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   793.00ns |   793.00ns |     4.00ns |      100
Cold Pin (miss)                                              |     4.57µs |     3.52µs |     3.29µs |      100
Dirty Eviction                                               |     5.00ms |     5.01ms |   267.27µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 255109.63 blocks/sec |        470.39µs
Seq Scan MT x4 (120 blocks)                                  | 160438.32 blocks/sec |        747.95µs
Repeated Access (1000 ops)                                   | 1254840.55 blocks/sec |        796.91µs
Repeated Access MT x4 (1000 ops)                             | 1213761.63 blocks/sec |        823.89µs
Random (K=10, 500 ops)                                       | 1254516.26 blocks/sec |        398.56µs
Random (K=50, 500 ops)                                       | 306807.38 blocks/sec |          1.63ms
Random (K=100, 500 ops)                                      | 275374.83 blocks/sec |          1.82ms
Random MT x4 (K=10, 500 ops)                                 | 1155604.45 blocks/sec |        432.67µs
Random MT x4 (K=50, 500 ops)                                 | 233206.67 blocks/sec |          2.14ms
Random MT x4 (K=100, 500 ops)                                | 195212.38 blocks/sec |          2.56ms
Zipfian (80/20, 500 ops)                                     | 668402.73 blocks/sec |        748.05µs
Zipfian MT x4 (80/20, 500 ops)                               | 649870.55 blocks/sec |        769.38µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     248640
                 16 |     288294
                 32 |     357343
                 64 |     526737
                128 |    1223891
                256 |    1223840


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1256594
       32 |          33 |    1086031
       32 |          37 |     792139
       32 |          42 |     608953
       32 |          52 |     489518

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  76.3% (hits: 39062, misses: 12144)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  24.0% (hits: 12240, misses: 38761)
Random (K=100)       | Hit rate:  13.2% (hits: 6731, misses: 44270)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    224692.52 ops/sec |          8.90ms
4 threads, 1000 ops/thread                                   |    187702.06 ops/sec |         21.31ms
8 threads, 1000 ops/thread                                   |    137431.59 ops/sec |         58.21ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1170568.86 ops/sec |          3.42ms
8 threads, K=4, 1000 ops/thread                              |    931987.73 ops/sec |          8.58ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   176.29ms

All benchmarks completed!
➜  simpledb git:(feature/replacement-policy) ✗
```
