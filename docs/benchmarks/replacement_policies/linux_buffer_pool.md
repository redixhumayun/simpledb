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
    Finished `bench` profile [optimized + debuginfo] target(s) in 5.77s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-5808dadef433eec1)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |     1.09µs |   807.00ns |     2.07µs |      100
Cold Pin (miss)                                              |     4.49µs |     3.60µs |     2.70µs |      100
Dirty Eviction                                               |     5.00ms |     5.00ms |   195.62µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 255045.65 blocks/sec |        470.50µs
Seq Scan MT x4 (120 blocks)                                  | 179092.72 blocks/sec |        670.04µs
Repeated Access (1000 ops)                                   | 1180315.64 blocks/sec |        847.23µs
Repeated Access MT x4 (1000 ops)                             | 1211664.45 blocks/sec |        825.31µs
Random (K=10, 500 ops)                                       | 1200569.55 blocks/sec |        416.47µs
Random (K=50, 500 ops)                                       | 308195.47 blocks/sec |          1.62ms
Random (K=100, 500 ops)                                      | 278633.96 blocks/sec |          1.79ms
Random MT x4 (K=10, 500 ops)                                 | 1103114.75 blocks/sec |        453.26µs
Random MT x4 (K=50, 500 ops)                                 | 236433.34 blocks/sec |          2.11ms
Random MT x4 (K=100, 500 ops)                                | 214873.45 blocks/sec |          2.33ms
Zipfian (80/20, 500 ops)                                     | 756024.76 blocks/sec |        661.35µs
Zipfian MT x4 (80/20, 500 ops)                               | 753173.12 blocks/sec |        663.86µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     249495
                 16 |     296370
                 32 |     362285
                 64 |     556503
                128 |    1164657
                256 |    1184192


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1196539
       32 |          33 |    1091808
       32 |          37 |     768630
       32 |          42 |     690920
       32 |          52 |     506121

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 12240)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  76.4% (hits: 38956, misses: 12044)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  22.2% (hits: 11318, misses: 39682)
Random (K=100)       | Hit rate:  13.4% (hits: 6834, misses: 44166)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    219529.06 ops/sec |          9.11ms
4 threads, 1000 ops/thread                                   |    199440.43 ops/sec |         20.06ms
8 threads, 1000 ops/thread                                   |    183893.56 ops/sec |         43.50ms
16 threads, 1000 ops/thread                                  |    117081.52 ops/sec |        136.66ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1042650.94 ops/sec |          3.84ms
8 threads, K=4, 1000 ops/thread                              |    809715.53 ops/sec |          9.88ms
16 threads, K=4, 1000 ops/thread                             |    755601.89 ops/sec |         21.18ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.04ms

All benchmarks completed!
➜  simpledb git:(feature/replacement-policy) ✗

```

## Replacement Clock (`--no-default-features --features replacement_clock`)

```
➜  simpledb git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_clock -- 100 12
   Compiling simpledb v0.1.0 (/home/zaid-humayun/Desktop/Development/simpledb)
    Finished `bench` profile [optimized + debuginfo] target(s) in 6.38s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-eddfe38e510ad5d3)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   800.00ns |   799.00ns |     4.00ns |      100
Cold Pin (miss)                                              |     4.11µs |     3.51µs |     1.67µs |      100
Dirty Eviction                                               |     5.00ms |     5.02ms |   322.45µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 252703.40 blocks/sec |        474.87µs
Seq Scan MT x4 (120 blocks)                                  | 171162.29 blocks/sec |        701.09µs
Repeated Access (1000 ops)                                   | 1189079.02 blocks/sec |        840.99µs
Repeated Access MT x4 (1000 ops)                             | 1260333.16 blocks/sec |        793.44µs
Random (K=10, 500 ops)                                       | 1137671.96 blocks/sec |        439.49µs
Random (K=50, 500 ops)                                       | 296557.62 blocks/sec |          1.69ms
Random (K=100, 500 ops)                                      | 255207.77 blocks/sec |          1.96ms
Random MT x4 (K=10, 500 ops)                                 | 1171975.72 blocks/sec |        426.63µs
Random MT x4 (K=50, 500 ops)                                 | 228965.93 blocks/sec |          2.18ms
Random MT x4 (K=100, 500 ops)                                | 199184.38 blocks/sec |          2.51ms
Zipfian (80/20, 500 ops)                                     | 565845.78 blocks/sec |        883.63µs
Zipfian MT x4 (80/20, 500 ops)                               | 611397.92 blocks/sec |        817.80µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     234647
                 16 |     275098
                 32 |     344784
                 64 |     501227
                128 |    1183440
                256 |    1172355


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1179891
       32 |          33 |     976963
       32 |          37 |     815337
       32 |          42 |     615319
       32 |          52 |     487166

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  76.9% (hits: 39321, misses: 11782)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  21.4% (hits: 10912, misses: 40089)
Random (K=100)       | Hit rate:  12.0% (hits: 6121, misses: 44881)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    213233.46 ops/sec |          9.38ms
4 threads, 1000 ops/thread                                   |    181174.66 ops/sec |         22.08ms
8 threads, 1000 ops/thread                                   |    125316.33 ops/sec |         63.84ms
16 threads, 1000 ops/thread                                  |    122137.09 ops/sec |        131.00ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1047005.85 ops/sec |          3.82ms
8 threads, K=4, 1000 ops/thread                              |    858723.92 ops/sec |          9.32ms
16 threads, K=4, 1000 ops/thread                             |    909117.51 ops/sec |         17.60ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.71ms

All benchmarks completed!
➜  simpledb git:(feature/replacement-policy) ✗

```

## Replacement SIEVE (`--no-default-features --features replacement_sieve`)

```
➜  simpledb git:(feature/replacement-policy) ✗ cargo bench --bench buffer_pool --no-default-features --features replacement_sieve -- 100 12
   Compiling simpledb v0.1.0 (/home/zaid-humayun/Desktop/Development/simpledb)
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.97s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-73fb01e136c64d8a)
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 12 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |     1.06µs |   797.00ns |     2.46µs |      100
Cold Pin (miss)                                              |     4.05µs |     3.55µs |     1.16µs |      100
Dirty Eviction                                               |     5.01ms |     4.99ms |   366.46µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (120 blocks)                                 | 250409.52 blocks/sec |        479.22µs
Seq Scan MT x4 (120 blocks)                                  | 177838.37 blocks/sec |        674.77µs
Repeated Access (1000 ops)                                   | 1223945.27 blocks/sec |        817.03µs
Repeated Access MT x4 (1000 ops)                             | 1252831.40 blocks/sec |        798.19µs
Random (K=10, 500 ops)                                       | 1204244.72 blocks/sec |        415.20µs
Random (K=50, 500 ops)                                       | 299839.83 blocks/sec |          1.67ms
Random (K=100, 500 ops)                                      | 276103.52 blocks/sec |          1.81ms
Random MT x4 (K=10, 500 ops)                                 | 1168216.11 blocks/sec |        428.00µs
Random MT x4 (K=50, 500 ops)                                 | 243883.64 blocks/sec |          2.05ms
Random MT x4 (K=100, 500 ops)                                | 206600.39 blocks/sec |          2.42ms
Zipfian (80/20, 500 ops)                                     | 536580.29 blocks/sec |        931.83µs
Zipfian MT x4 (80/20, 500 ops)                               | 557310.46 blocks/sec |        897.17µs

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     237001
                 16 |     274510
                 32 |     361197
                 64 |     529458
                128 |    1214574
                256 |    1225652


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1238132
       32 |          33 |    1088014
       32 |          37 |     787738
       32 |          42 |     652966
       32 |          52 |     500500

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 13260)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  68.1% (hits: 35706, misses: 16721)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  19.7% (hits: 10708, misses: 43760)
Random (K=100)       | Hit rate:  12.0% (hits: 6562, misses: 48178)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
2 threads, 1000 ops/thread                                   |    217037.06 ops/sec |          9.22ms
4 threads, 1000 ops/thread                                   |    198688.61 ops/sec |         20.13ms
8 threads, 1000 ops/thread                                   |    181551.67 ops/sec |         44.06ms
16 threads, 1000 ops/thread                                  |    102941.29 ops/sec |        155.43ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
4 threads, K=4, 1000 ops/thread                              |   1075836.23 ops/sec |          3.72ms
8 threads, K=4, 1000 ops/thread                              |    859092.60 ops/sec |          9.31ms
16 threads, K=4, 1000 ops/thread                             |    902816.18 ops/sec |         17.72ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:   175.87ms

All benchmarks completed!
➜  simpledb git:(feature/replacement-policy) ✗

```
