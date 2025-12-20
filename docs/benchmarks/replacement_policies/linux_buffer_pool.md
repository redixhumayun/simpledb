# Linux (i7-8650U, Ubuntu 6.8.0-86)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   879.00ns |   877.00ns |     8.00ns |      100
Cold Pin (miss)                                              |     4.94µs |     4.79µs |     1.01µs |      100
Dirty Eviction                                               |     5.00ms |     5.01ms |   217.84µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 209418.65 blocks/sec |        195.59ms
Seq Scan MT x2 (40960 blocks)                                | 189180.88 blocks/sec |        216.51ms
Seq Scan MT x4 (40960 blocks)                                | 162876.51 blocks/sec |        251.48ms
Seq Scan MT x8 (40960 blocks)                                | 145840.86 blocks/sec |        280.85ms
Seq Scan MT x16 (40960 blocks)                               | 148346.33 blocks/sec |        276.11ms
Seq Scan MT x32 (40960 blocks)                               | 146319.23 blocks/sec |        279.94ms
Seq Scan MT x64 (40960 blocks)                               | 143581.98 blocks/sec |        285.27ms
Seq Scan MT x128 (40960 blocks)                              | 138979.12 blocks/sec |        294.72ms
Seq Scan MT x256 (40960 blocks)                              | 136402.09 blocks/sec |        300.29ms
Repeated Access (1000 ops)                                   | 1249242.65 blocks/sec |        800.49µs
Repeated Access MT x2 (1000 ops)                             | 1306928.16 blocks/sec |        765.15µs
Repeated Access MT x4 (1000 ops)                             | 720748.25 blocks/sec |          1.39ms
Repeated Access MT x8 (1000 ops)                             | 561054.20 blocks/sec |          1.78ms
Repeated Access MT x16 (1000 ops)                            | 483198.93 blocks/sec |          2.07ms
Repeated Access MT x32 (1000 ops)                            | 431538.56 blocks/sec |          2.32ms
Repeated Access MT x64 (1000 ops)                            | 378225.27 blocks/sec |          2.64ms
Repeated Access MT x128 (1000 ops)                           | 326053.96 blocks/sec |          3.07ms
Repeated Access MT x256 (1000 ops)                           | 295999.07 blocks/sec |          3.38ms
Random (K=10, 500 ops)                                       | 1051312.46 blocks/sec |        475.60µs
Random (K=50, 500 ops)                                       | 1156890.56 blocks/sec |        432.19µs
Random (K=100, 500 ops)                                      | 1200036.48 blocks/sec |        416.65µs
Random MT x2 (K=10, 500 ops)                                 | 1175212.42 blocks/sec |        425.46µs
Random MT x4 (K=10, 500 ops)                                 | 734964.46 blocks/sec |        680.31µs
Random MT x8 (K=10, 500 ops)                                 | 638769.47 blocks/sec |        782.76µs
Random MT x16 (K=10, 500 ops)                                | 520946.21 blocks/sec |        959.79µs
Random MT x32 (K=10, 500 ops)                                | 432732.96 blocks/sec |          1.16ms
Random MT x64 (K=10, 500 ops)                                | 348862.12 blocks/sec |          1.43ms
Random MT x128 (K=10, 500 ops)                               | 297906.14 blocks/sec |          1.68ms
Random MT x256 (K=10, 500 ops)                               | 190880.20 blocks/sec |          2.62ms
Random MT x2 (K=50, 500 ops)                                 | 1051009.71 blocks/sec |        475.73µs
Random MT x4 (K=50, 500 ops)                                 | 627744.81 blocks/sec |        796.50µs
Random MT x8 (K=50, 500 ops)                                 | 598714.20 blocks/sec |        835.12µs
Random MT x16 (K=50, 500 ops)                                | 492745.31 blocks/sec |          1.01ms
Random MT x32 (K=50, 500 ops)                                | 426298.16 blocks/sec |          1.17ms
Random MT x64 (K=50, 500 ops)                                | 352581.71 blocks/sec |          1.42ms
Random MT x128 (K=50, 500 ops)                               | 271905.67 blocks/sec |          1.84ms
Random MT x256 (K=50, 500 ops)                               | 188945.19 blocks/sec |          2.65ms
Random MT x2 (K=100, 500 ops)                                | 1095739.11 blocks/sec |        456.31µs
Random MT x4 (K=100, 500 ops)                                | 608902.15 blocks/sec |        821.15µs
Random MT x8 (K=100, 500 ops)                                | 631290.81 blocks/sec |        792.03µs
Random MT x16 (K=100, 500 ops)                               | 521939.19 blocks/sec |        957.97µs
Random MT x32 (K=100, 500 ops)                               | 440848.30 blocks/sec |          1.13ms
Random MT x64 (K=100, 500 ops)                               | 349368.66 blocks/sec |          1.43ms
Random MT x128 (K=100, 500 ops)                              | 280159.92 blocks/sec |          1.78ms
Random MT x256 (K=100, 500 ops)                              | 185278.24 blocks/sec |          2.70ms
Zipfian (80/20, 500 ops)                                     | 1139868.73 blocks/sec |        438.65µs
Zipfian MT x2 (80/20, 500 ops)                               | 1168068.74 blocks/sec |        428.06µs
Zipfian MT x4 (80/20, 500 ops)                               | 661773.95 blocks/sec |        755.55µs
Zipfian MT x8 (80/20, 500 ops)                               | 770018.56 blocks/sec |        649.34µs
Zipfian MT x16 (80/20, 500 ops)                              | 660499.36 blocks/sec |        757.00µs
Zipfian MT x32 (80/20, 500 ops)                              | 542401.12 blocks/sec |        921.83µs
Zipfian MT x64 (80/20, 500 ops)                              | 423984.28 blocks/sec |          1.18ms
Zipfian MT x128 (80/20, 500 ops)                             | 300951.01 blocks/sec |          1.66ms
Zipfian MT x256 (80/20, 500 ops)                             | 196198.15 blocks/sec |          2.55ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     254749
                 16 |     288939
                 32 |     334232
                 64 |     489918
                128 |    1239351
                256 |    1205328


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1252674
       32 |          33 |    1109735
       32 |          37 |     825377
       32 |          42 |     623570
       32 |          52 |     468242

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4177920)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50531, misses: 469)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50900, misses: 100)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1276379.57 ops/sec |          7.83ms
2 threads, 5000 ops/thread                                   |   1373634.30 ops/sec |          7.28ms
4 threads, 2500 ops/thread                                   |    710622.20 ops/sec |         14.07ms
8 threads, 1250 ops/thread                                   |    751432.29 ops/sec |         13.31ms
16 threads, 625 ops/thread                                   |    721941.80 ops/sec |         13.85ms
32 threads, 312 ops/thread                                   |    705582.26 ops/sec |         14.17ms
64 threads, 156 ops/thread                                   |    611959.41 ops/sec |         16.34ms
128 threads, 78 ops/thread                                   |    581426.85 ops/sec |         17.20ms
256 threads, 39 ops/thread                                   |    542247.54 ops/sec |         18.44ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1220634.43 ops/sec |          8.19ms
2 threads, K=4, 5000 ops/thread                              |   1405246.94 ops/sec |          7.12ms
4 threads, K=4, 2500 ops/thread                              |    774307.13 ops/sec |         12.91ms
8 threads, K=4, 1250 ops/thread                              |    611188.74 ops/sec |         16.36ms
16 threads, K=4, 625 ops/thread                              |    565026.98 ops/sec |         17.70ms
32 threads, K=4, 312 ops/thread                              |    543460.78 ops/sec |         18.40ms
64 threads, K=4, 156 ops/thread                              |    532977.50 ops/sec |         18.76ms
128 threads, K=4, 78 ops/thread                              |    520117.74 ops/sec |         19.23ms
256 threads, K=4, 39 ops/thread                              |    501975.73 ops/sec |         19.92ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     42.53s

All benchmarks completed!
warning: struct `LatchTableGuard` is never constructed
  --> src/buffer_manager/baseline.rs:24:8
   |
24 | struct LatchTableGuard<'a> {
   |        ^^^^^^^^^^^^^^^
   |
   = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: associated items `new` and `lock` are never used
  --> src/buffer_manager/baseline.rs:31:12
   |
30 | impl<'a> LatchTableGuard<'a> {
   | ---------------------------- associated items in this implementation
31 |     pub fn new(table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>, block_id: &BlockId) -> Self {
   |            ^^^
...
46 |     fn lock(&'a self) -> MutexGuard<'a, ()> {
   |        ^^^^

warning: struct `BufferManager` is never constructed
  --> src/buffer_manager/baseline.rs:67:12
   |
67 | pub struct BufferManager {
   |            ^^^^^^^^^^^^^

warning: multiple associated items are never used
   --> src/buffer_manager/baseline.rs:80:11
    |
 79 | impl BufferManager {
    | ------------------ associated items in this implementation
 80 |     const MAX_TIME: u64 = 10;
    |           ^^^^^^^^
 81 |
 82 |     pub fn new(
    |            ^^^
...
111 |     pub fn enable_stats(&self) {
    |            ^^^^^^^^^^^^
...
115 |     pub fn get_stats(&self) -> Option<(usize, usize)> {
    |            ^^^^^^^^^
...
119 |     pub fn stats(&self) -> Option<&Arc<BufferStats>> {
    |            ^^^^^
...
123 |     pub fn reset_stats(&self) {
    |            ^^^^^^^^^^^
...
129 |     pub fn available(&self) -> usize {
    |            ^^^^^^^^^
...
133 |     pub fn file_manager(&self) -> SharedFS {
    |            ^^^^^^^^^^^^
...
137 |     pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
    |            ^^^^^^^^^^^
...
141 |     pub(crate) fn flush_all(&self, txn_num: usize) {
    |                   ^^^^^^^^^
...
150 |     pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
    |            ^^^
...
174 |     fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
    |        ^^^^^^^^^^
...
239 |     pub fn unpin(&self, frame: Arc<BufferFrame>) {
    |            ^^^^^
...
248 |     fn evict_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
    |        ^^^^^^^^^^^
...
252 |     fn record_hit<'a>(
    |        ^^^^^^^^^^

warning: `simpledb` (lib) generated 4 warnings
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-33185e61bb240555)
```

## Replacement Clock (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   812.00ns |   812.00ns |     3.00ns |      100
Cold Pin (miss)                                              |     5.07µs |     4.54µs |     2.18µs |      100
Dirty Eviction                                               |     5.06ms |     5.01ms |   540.90µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 210748.89 blocks/sec |        194.35ms
Seq Scan MT x2 (40960 blocks)                                | 194166.59 blocks/sec |        210.95ms
Seq Scan MT x4 (40960 blocks)                                | 161897.31 blocks/sec |        253.00ms
Seq Scan MT x8 (40960 blocks)                                | 144210.10 blocks/sec |        284.03ms
Seq Scan MT x16 (40960 blocks)                               | 147161.38 blocks/sec |        278.33ms
Seq Scan MT x32 (40960 blocks)                               | 145405.29 blocks/sec |        281.70ms
Seq Scan MT x64 (40960 blocks)                               | 141604.59 blocks/sec |        289.26ms
Seq Scan MT x128 (40960 blocks)                              | 136819.36 blocks/sec |        299.37ms
Seq Scan MT x256 (40960 blocks)                              | 131649.46 blocks/sec |        311.13ms
Repeated Access (1000 ops)                                   | 1310628.01 blocks/sec |        762.99µs
Repeated Access MT x2 (1000 ops)                             | 1974821.03 blocks/sec |        506.38µs
Repeated Access MT x4 (1000 ops)                             | 3481882.03 blocks/sec |        287.20µs
Repeated Access MT x8 (1000 ops)                             | 3042787.68 blocks/sec |        328.65µs
Repeated Access MT x16 (1000 ops)                            | 2914304.87 blocks/sec |        343.14µs
Repeated Access MT x32 (1000 ops)                            | 2189065.18 blocks/sec |        456.82µs
Repeated Access MT x64 (1000 ops)                            | 1453467.25 blocks/sec |        688.01µs
Repeated Access MT x128 (1000 ops)                           | 738972.68 blocks/sec |          1.35ms
Repeated Access MT x256 (1000 ops)                           | 393281.03 blocks/sec |          2.54ms
Random (K=10, 500 ops)                                       | 1158013.22 blocks/sec |        431.77µs
Random (K=50, 500 ops)                                       | 1266242.73 blocks/sec |        394.87µs
Random (K=100, 500 ops)                                      | 1333216.01 blocks/sec |        375.03µs
Random MT x2 (K=10, 500 ops)                                 | 1809686.16 blocks/sec |        276.29µs
Random MT x4 (K=10, 500 ops)                                 | 2297519.60 blocks/sec |        217.63µs
Random MT x8 (K=10, 500 ops)                                 | 2816250.89 blocks/sec |        177.54µs
Random MT x16 (K=10, 500 ops)                                | 2210716.67 blocks/sec |        226.17µs
Random MT x32 (K=10, 500 ops)                                | 1596067.29 blocks/sec |        313.27µs
Random MT x64 (K=10, 500 ops)                                | 929490.69 blocks/sec |        537.93µs
Random MT x128 (K=10, 500 ops)                               | 436425.47 blocks/sec |          1.15ms
Random MT x256 (K=10, 500 ops)                               | 204100.38 blocks/sec |          2.45ms
Random MT x2 (K=50, 500 ops)                                 | 1653761.81 blocks/sec |        302.34µs
Random MT x4 (K=50, 500 ops)                                 | 3206772.70 blocks/sec |        155.92µs
Random MT x8 (K=50, 500 ops)                                 | 2506579.77 blocks/sec |        199.48µs
Random MT x16 (K=50, 500 ops)                                | 2158195.75 blocks/sec |        231.68µs
Random MT x32 (K=50, 500 ops)                                | 1516222.06 blocks/sec |        329.77µs
Random MT x64 (K=50, 500 ops)                                | 844141.15 blocks/sec |        592.32µs
Random MT x128 (K=50, 500 ops)                               | 411460.66 blocks/sec |          1.22ms
Random MT x256 (K=50, 500 ops)                               | 206878.72 blocks/sec |          2.42ms
Random MT x2 (K=100, 500 ops)                                | 1671078.31 blocks/sec |        299.21µs
Random MT x4 (K=100, 500 ops)                                | 2121520.71 blocks/sec |        235.68µs
Random MT x8 (K=100, 500 ops)                                | 2561921.65 blocks/sec |        195.17µs
Random MT x16 (K=100, 500 ops)                               | 2219539.05 blocks/sec |        225.27µs
Random MT x32 (K=100, 500 ops)                               | 1557249.15 blocks/sec |        321.08µs
Random MT x64 (K=100, 500 ops)                               | 848704.79 blocks/sec |        589.13µs
Random MT x128 (K=100, 500 ops)                              | 433555.83 blocks/sec |          1.15ms
Random MT x256 (K=100, 500 ops)                              | 204231.85 blocks/sec |          2.45ms
Zipfian (80/20, 500 ops)                                     | 1225487.19 blocks/sec |        408.00µs
Zipfian MT x2 (80/20, 500 ops)                               | 1952598.71 blocks/sec |        256.07µs
Zipfian MT x4 (80/20, 500 ops)                               | 2229922.89 blocks/sec |        224.22µs
Zipfian MT x8 (80/20, 500 ops)                               | 2647912.39 blocks/sec |        188.83µs
Zipfian MT x16 (80/20, 500 ops)                              | 2352210.61 blocks/sec |        212.57µs
Zipfian MT x32 (80/20, 500 ops)                              | 1645169.78 blocks/sec |        303.92µs
Zipfian MT x64 (80/20, 500 ops)                              | 938235.93 blocks/sec |        532.92µs
Zipfian MT x128 (80/20, 500 ops)                             | 488028.18 blocks/sec |          1.02ms
Zipfian MT x256 (80/20, 500 ops)                             | 216166.21 blocks/sec |          2.31ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     257637
                 16 |     283457
                 32 |     336114
                 64 |     484986
                128 |    1334878
                256 |    1337589


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1338172
       32 |          33 |    1030677
       32 |          37 |     833368
       32 |          42 |     614322
       32 |          52 |     490315

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4178940)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50530, misses: 470)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50901, misses: 99)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1331922.03 ops/sec |          7.51ms
2 threads, 5000 ops/thread                                   |   2340733.01 ops/sec |          4.27ms
4 threads, 2500 ops/thread                                   |   4079141.88 ops/sec |          2.45ms
8 threads, 1250 ops/thread                                   |   3664358.00 ops/sec |          2.73ms
16 threads, 625 ops/thread                                   |   4046944.56 ops/sec |          2.47ms
32 threads, 312 ops/thread                                   |   3835814.85 ops/sec |          2.61ms
64 threads, 156 ops/thread                                   |   3552948.17 ops/sec |          2.81ms
128 threads, 78 ops/thread                                   |   3063946.09 ops/sec |          3.26ms
256 threads, 39 ops/thread                                   |   2356146.64 ops/sec |          4.24ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1262267.03 ops/sec |          7.92ms
2 threads, K=4, 5000 ops/thread                              |   1975766.04 ops/sec |          5.06ms
4 threads, K=4, 2500 ops/thread                              |   3238892.62 ops/sec |          3.09ms
8 threads, K=4, 1250 ops/thread                              |   3224346.13 ops/sec |          3.10ms
16 threads, K=4, 625 ops/thread                              |   3090449.74 ops/sec |          3.24ms
32 threads, K=4, 312 ops/thread                              |   2893803.21 ops/sec |          3.46ms
64 threads, K=4, 156 ops/thread                              |   2721109.91 ops/sec |          3.67ms
128 threads, K=4, 78 ops/thread                              |   2472724.61 ops/sec |          4.04ms
256 threads, K=4, 39 ops/thread                              |   2061615.50 ops/sec |          4.85ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     42.48s

All benchmarks completed!
warning: struct `LatchTableGuard` is never constructed
  --> src/buffer_manager/baseline.rs:24:8
   |
24 | struct LatchTableGuard<'a> {
   |        ^^^^^^^^^^^^^^^
   |
   = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: associated items `new` and `lock` are never used
  --> src/buffer_manager/baseline.rs:31:12
   |
30 | impl<'a> LatchTableGuard<'a> {
   | ---------------------------- associated items in this implementation
31 |     pub fn new(table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>, block_id: &BlockId) -> Self {
   |            ^^^
...
46 |     fn lock(&'a self) -> MutexGuard<'a, ()> {
   |        ^^^^

warning: struct `BufferManager` is never constructed
  --> src/buffer_manager/baseline.rs:67:12
   |
67 | pub struct BufferManager {
   |            ^^^^^^^^^^^^^

warning: multiple associated items are never used
   --> src/buffer_manager/baseline.rs:80:11
    |
 79 | impl BufferManager {
    | ------------------ associated items in this implementation
 80 |     const MAX_TIME: u64 = 10;
    |           ^^^^^^^^
 81 |
 82 |     pub fn new(
    |            ^^^
...
111 |     pub fn enable_stats(&self) {
    |            ^^^^^^^^^^^^
...
115 |     pub fn get_stats(&self) -> Option<(usize, usize)> {
    |            ^^^^^^^^^
...
119 |     pub fn stats(&self) -> Option<&Arc<BufferStats>> {
    |            ^^^^^
...
123 |     pub fn reset_stats(&self) {
    |            ^^^^^^^^^^^
...
129 |     pub fn available(&self) -> usize {
    |            ^^^^^^^^^
...
133 |     pub fn file_manager(&self) -> SharedFS {
    |            ^^^^^^^^^^^^
...
137 |     pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
    |            ^^^^^^^^^^^
...
141 |     pub(crate) fn flush_all(&self, txn_num: usize) {
    |                   ^^^^^^^^^
...
150 |     pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
    |            ^^^
...
174 |     fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
    |        ^^^^^^^^^^
...
239 |     pub fn unpin(&self, frame: Arc<BufferFrame>) {
    |            ^^^^^
...
248 |     fn evict_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
    |        ^^^^^^^^^^^
...
252 |     fn record_hit<'a>(
    |        ^^^^^^^^^^

warning: `simpledb` (lib) generated 4 warnings
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-011ae9cbe4ac89c1)
```

## Replacement SIEVE (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: linux (x86_64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   945.00ns |   709.00ns |     1.86µs |      100
Cold Pin (miss)                                              |     4.11µs |     3.97µs |   710.00ns |      100
Dirty Eviction                                               |     5.00ms |     5.00ms |   175.68µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 208388.85 blocks/sec |        196.56ms
Seq Scan MT x2 (40960 blocks)                                | 191436.51 blocks/sec |        213.96ms
Seq Scan MT x4 (40960 blocks)                                | 158473.08 blocks/sec |        258.47ms
Seq Scan MT x8 (40960 blocks)                                | 142329.33 blocks/sec |        287.78ms
Seq Scan MT x16 (40960 blocks)                               | 146004.66 blocks/sec |        280.54ms
Seq Scan MT x32 (40960 blocks)                               | 144028.86 blocks/sec |        284.39ms
Seq Scan MT x64 (40960 blocks)                               | 141025.79 blocks/sec |        290.44ms
Seq Scan MT x128 (40960 blocks)                              | 136904.96 blocks/sec |        299.19ms
Seq Scan MT x256 (40960 blocks)                              | 134965.79 blocks/sec |        303.48ms
Repeated Access (1000 ops)                                   | 1367039.05 blocks/sec |        731.51µs
Repeated Access MT x2 (1000 ops)                             | 1989293.62 blocks/sec |        502.69µs
Repeated Access MT x4 (1000 ops)                             | 2771649.35 blocks/sec |        360.80µs
Repeated Access MT x8 (1000 ops)                             | 2561849.45 blocks/sec |        390.34µs
Repeated Access MT x16 (1000 ops)                            | 2906461.35 blocks/sec |        344.06µs
Repeated Access MT x32 (1000 ops)                            | 2190753.27 blocks/sec |        456.46µs
Repeated Access MT x64 (1000 ops)                            | 1377205.59 blocks/sec |        726.11µs
Repeated Access MT x128 (1000 ops)                           | 751404.00 blocks/sec |          1.33ms
Repeated Access MT x256 (1000 ops)                           | 393411.77 blocks/sec |          2.54ms
Random (K=10, 500 ops)                                       | 1147826.25 blocks/sec |        435.61µs
Random (K=50, 500 ops)                                       | 1240968.85 blocks/sec |        402.91µs
Random (K=100, 500 ops)                                      | 1325138.68 blocks/sec |        377.32µs
Random MT x2 (K=10, 500 ops)                                 | 1780284.42 blocks/sec |        280.85µs
Random MT x4 (K=10, 500 ops)                                 | 3113247.49 blocks/sec |        160.60µs
Random MT x8 (K=10, 500 ops)                                 | 2825369.56 blocks/sec |        176.97µs
Random MT x16 (K=10, 500 ops)                                | 2130824.07 blocks/sec |        234.65µs
Random MT x32 (K=10, 500 ops)                                | 1502191.70 blocks/sec |        332.85µs
Random MT x64 (K=10, 500 ops)                                | 870910.21 blocks/sec |        574.11µs
Random MT x128 (K=10, 500 ops)                               | 413546.12 blocks/sec |          1.21ms
Random MT x256 (K=10, 500 ops)                               | 203924.82 blocks/sec |          2.45ms
Random MT x2 (K=50, 500 ops)                                 | 1659916.34 blocks/sec |        301.22µs
Random MT x4 (K=50, 500 ops)                                 | 2048491.90 blocks/sec |        244.08µs
Random MT x8 (K=50, 500 ops)                                 | 2540921.54 blocks/sec |        196.78µs
Random MT x16 (K=50, 500 ops)                                | 2141217.58 blocks/sec |        233.51µs
Random MT x32 (K=50, 500 ops)                                | 1475910.19 blocks/sec |        338.77µs
Random MT x64 (K=50, 500 ops)                                | 843009.68 blocks/sec |        593.11µs
Random MT x128 (K=50, 500 ops)                               | 401145.03 blocks/sec |          1.25ms
Random MT x256 (K=50, 500 ops)                               | 201972.71 blocks/sec |          2.48ms
Random MT x2 (K=100, 500 ops)                                | 1661057.83 blocks/sec |        301.01µs
Random MT x4 (K=100, 500 ops)                                | 3208295.37 blocks/sec |        155.85µs
Random MT x8 (K=100, 500 ops)                                | 2597537.53 blocks/sec |        192.49µs
Random MT x16 (K=100, 500 ops)                               | 2149659.28 blocks/sec |        232.60µs
Random MT x32 (K=100, 500 ops)                               | 1470964.63 blocks/sec |        339.91µs
Random MT x64 (K=100, 500 ops)                               | 817522.45 blocks/sec |        611.60µs
Random MT x128 (K=100, 500 ops)                              | 409063.20 blocks/sec |          1.22ms
Random MT x256 (K=100, 500 ops)                              | 200007.76 blocks/sec |          2.50ms
Zipfian (80/20, 500 ops)                                     | 1239765.73 blocks/sec |        403.30µs
Zipfian MT x2 (80/20, 500 ops)                               | 1912652.96 blocks/sec |        261.42µs
Zipfian MT x4 (80/20, 500 ops)                               | 3718135.58 blocks/sec |        134.48µs
Zipfian MT x8 (80/20, 500 ops)                               | 2720481.42 blocks/sec |        183.79µs
Zipfian MT x16 (80/20, 500 ops)                              | 2312160.11 blocks/sec |        216.25µs
Zipfian MT x32 (80/20, 500 ops)                              | 1621765.39 blocks/sec |        308.31µs
Zipfian MT x64 (80/20, 500 ops)                              | 921534.98 blocks/sec |        542.57µs
Zipfian MT x128 (80/20, 500 ops)                             | 470110.82 blocks/sec |          1.06ms
Zipfian MT x256 (80/20, 500 ops)                             | 216038.71 blocks/sec |          2.31ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     269118
                 16 |     295523
                 32 |     329514
                 64 |     532624
                128 |    1295229
                256 |    1389545


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    1397722
       32 |          33 |    1213893
       32 |          37 |     838015
       32 |          42 |     654359
       32 |          52 |     512977

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4178940)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50538, misses: 462)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50900, misses: 100)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   1351268.81 ops/sec |          7.40ms
2 threads, 5000 ops/thread                                   |   2257345.52 ops/sec |          4.43ms
4 threads, 2500 ops/thread                                   |   4075144.03 ops/sec |          2.45ms
8 threads, 1250 ops/thread                                   |   3601137.96 ops/sec |          2.78ms
16 threads, 625 ops/thread                                   |   4042064.96 ops/sec |          2.47ms
32 threads, 312 ops/thread                                   |   3834186.76 ops/sec |          2.61ms
64 threads, 156 ops/thread                                   |   3498728.74 ops/sec |          2.86ms
128 threads, 78 ops/thread                                   |   3038212.51 ops/sec |          3.29ms
256 threads, 39 ops/thread                                   |   2371392.84 ops/sec |          4.22ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   1274845.14 ops/sec |          7.84ms
2 threads, K=4, 5000 ops/thread                              |   2022926.64 ops/sec |          4.94ms
4 threads, K=4, 2500 ops/thread                              |   3418670.18 ops/sec |          2.93ms
8 threads, K=4, 1250 ops/thread                              |   3230039.89 ops/sec |          3.10ms
16 threads, K=4, 625 ops/thread                              |   3058142.32 ops/sec |          3.27ms
32 threads, K=4, 312 ops/thread                              |   2950796.07 ops/sec |          3.39ms
64 threads, K=4, 156 ops/thread                              |   2750794.77 ops/sec |          3.64ms
128 threads, K=4, 78 ops/thread                              |   2462235.46 ops/sec |          4.06ms
256 threads, K=4, 39 ops/thread                              |   1921783.42 ops/sec |          5.20ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     42.51s

All benchmarks completed!
warning: struct `LatchTableGuard` is never constructed
  --> src/buffer_manager/baseline.rs:24:8
   |
24 | struct LatchTableGuard<'a> {
   |        ^^^^^^^^^^^^^^^
   |
   = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: associated items `new` and `lock` are never used
  --> src/buffer_manager/baseline.rs:31:12
   |
30 | impl<'a> LatchTableGuard<'a> {
   | ---------------------------- associated items in this implementation
31 |     pub fn new(table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>, block_id: &BlockId) -> Self {
   |            ^^^
...
46 |     fn lock(&'a self) -> MutexGuard<'a, ()> {
   |        ^^^^

warning: struct `BufferManager` is never constructed
  --> src/buffer_manager/baseline.rs:67:12
   |
67 | pub struct BufferManager {
   |            ^^^^^^^^^^^^^

warning: multiple associated items are never used
   --> src/buffer_manager/baseline.rs:80:11
    |
 79 | impl BufferManager {
    | ------------------ associated items in this implementation
 80 |     const MAX_TIME: u64 = 10;
    |           ^^^^^^^^
 81 |
 82 |     pub fn new(
    |            ^^^
...
111 |     pub fn enable_stats(&self) {
    |            ^^^^^^^^^^^^
...
115 |     pub fn get_stats(&self) -> Option<(usize, usize)> {
    |            ^^^^^^^^^
...
119 |     pub fn stats(&self) -> Option<&Arc<BufferStats>> {
    |            ^^^^^
...
123 |     pub fn reset_stats(&self) {
    |            ^^^^^^^^^^^
...
129 |     pub fn available(&self) -> usize {
    |            ^^^^^^^^^
...
133 |     pub fn file_manager(&self) -> SharedFS {
    |            ^^^^^^^^^^^^
...
137 |     pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
    |            ^^^^^^^^^^^
...
141 |     pub(crate) fn flush_all(&self, txn_num: usize) {
    |                   ^^^^^^^^^
...
150 |     pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
    |            ^^^
...
174 |     fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
    |        ^^^^^^^^^^
...
239 |     pub fn unpin(&self, frame: Arc<BufferFrame>) {
    |            ^^^^^
...
248 |     fn evict_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
    |        ^^^^^^^^^^^
...
252 |     fn record_hit<'a>(
    |        ^^^^^^^^^^

warning: `simpledb` (lib) generated 4 warnings
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.01s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-bb12b085f3a416ad)
```
