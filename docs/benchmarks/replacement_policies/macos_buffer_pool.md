# macOS (M1 Pro, macOS Sequoia)

Command template: `cargo bench --bench buffer_pool -- <iterations> <num_buffers>`

## Replacement LRU (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   172.00ns |   167.00ns |    17.00ns |      100
Cold Pin (miss)                                              |     2.28µs |     2.10µs |   540.00ns |      100
Dirty Eviction                                               |     3.22ms |     3.03ms |   683.63µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 426316.97 blocks/sec |         96.08ms
Seq Scan MT x2 (40960 blocks)                                | 339645.64 blocks/sec |        120.60ms
Seq Scan MT x4 (40960 blocks)                                | 218497.02 blocks/sec |        187.46ms
Seq Scan MT x8 (40960 blocks)                                | 168889.01 blocks/sec |        242.53ms
Seq Scan MT x16 (40960 blocks)                               | 165956.50 blocks/sec |        246.81ms
Seq Scan MT x32 (40960 blocks)                               | 164408.07 blocks/sec |        249.14ms
Seq Scan MT x64 (40960 blocks)                               | 162872.79 blocks/sec |        251.48ms
Seq Scan MT x128 (40960 blocks)                              | 157266.18 blocks/sec |        260.45ms
Seq Scan MT x256 (40960 blocks)                              | 150381.51 blocks/sec |        272.37ms
Repeated Access (1000 ops)                                   | 5403244.11 blocks/sec |        185.07µs
Repeated Access MT x2 (1000 ops)                             | 2432728.96 blocks/sec |        411.06µs
Repeated Access MT x4 (1000 ops)                             | 1332777.12 blocks/sec |        750.31µs
Repeated Access MT x8 (1000 ops)                             | 589338.74 blocks/sec |          1.70ms
Repeated Access MT x16 (1000 ops)                            | 347513.75 blocks/sec |          2.88ms
Repeated Access MT x32 (1000 ops)                            | 230440.13 blocks/sec |          4.34ms
Repeated Access MT x64 (1000 ops)                            | 174096.31 blocks/sec |          5.74ms
Repeated Access MT x128 (1000 ops)                           | 132975.31 blocks/sec |          7.52ms
Repeated Access MT x256 (1000 ops)                           | 136484.39 blocks/sec |          7.33ms
Random (K=10, 500 ops)                                       | 4853897.68 blocks/sec |        103.01µs
Random (K=50, 500 ops)                                       | 4911060.69 blocks/sec |        101.81µs
Random (K=100, 500 ops)                                      | 4863718.60 blocks/sec |        102.80µs
Random MT x2 (K=10, 500 ops)                                 | 2050785.66 blocks/sec |        243.81µs
Random MT x4 (K=10, 500 ops)                                 | 1103487.24 blocks/sec |        453.11µs
Random MT x8 (K=10, 500 ops)                                 | 582593.73 blocks/sec |        858.23µs
Random MT x16 (K=10, 500 ops)                                | 303548.73 blocks/sec |          1.65ms
Random MT x32 (K=10, 500 ops)                                | 232717.79 blocks/sec |          2.15ms
Random MT x64 (K=10, 500 ops)                                | 156990.55 blocks/sec |          3.18ms
Random MT x128 (K=10, 500 ops)                               | 137195.03 blocks/sec |          3.64ms
Random MT x256 (K=10, 500 ops)                               | 118509.33 blocks/sec |          4.22ms
Random MT x2 (K=50, 500 ops)                                 | 2378415.40 blocks/sec |        210.22µs
Random MT x4 (K=50, 500 ops)                                 | 922710.11 blocks/sec |        541.88µs
Random MT x8 (K=50, 500 ops)                                 | 512795.27 blocks/sec |        975.05µs
Random MT x16 (K=50, 500 ops)                                | 314072.72 blocks/sec |          1.59ms
Random MT x32 (K=50, 500 ops)                                | 205904.09 blocks/sec |          2.43ms
Random MT x64 (K=50, 500 ops)                                | 161232.64 blocks/sec |          3.10ms
Random MT x128 (K=50, 500 ops)                               | 138674.91 blocks/sec |          3.61ms
Random MT x256 (K=50, 500 ops)                               | 115579.09 blocks/sec |          4.33ms
Random MT x2 (K=100, 500 ops)                                | 2463126.99 blocks/sec |        202.99µs
Random MT x4 (K=100, 500 ops)                                | 801468.29 blocks/sec |        623.86µs
Random MT x8 (K=100, 500 ops)                                | 480050.54 blocks/sec |          1.04ms
Random MT x16 (K=100, 500 ops)                               | 307855.04 blocks/sec |          1.62ms
Random MT x32 (K=100, 500 ops)                               | 213844.10 blocks/sec |          2.34ms
Random MT x64 (K=100, 500 ops)                               | 170022.63 blocks/sec |          2.94ms
Random MT x128 (K=100, 500 ops)                              | 132853.29 blocks/sec |          3.76ms
Random MT x256 (K=100, 500 ops)                              | 130102.22 blocks/sec |          3.84ms
Zipfian (80/20, 500 ops)                                     | 5204375.84 blocks/sec |         96.07µs
Zipfian MT x2 (80/20, 500 ops)                               | 2930952.62 blocks/sec |        170.59µs
Zipfian MT x4 (80/20, 500 ops)                               | 829699.25 blocks/sec |        602.63µs
Zipfian MT x8 (80/20, 500 ops)                               | 455438.94 blocks/sec |          1.10ms
Zipfian MT x16 (80/20, 500 ops)                              | 296489.39 blocks/sec |          1.69ms
Zipfian MT x32 (80/20, 500 ops)                              | 223453.30 blocks/sec |          2.24ms
Zipfian MT x64 (80/20, 500 ops)                              | 193578.84 blocks/sec |          2.58ms
Zipfian MT x128 (80/20, 500 ops)                             | 167777.30 blocks/sec |          2.98ms
Zipfian MT x256 (80/20, 500 ops)                             | 155284.52 blocks/sec |          3.22ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     560947
                 16 |     543104
                 32 |     699036
                 64 |    1115039
                128 |    4796209
                256 |    4843366


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    4930869
       32 |          33 |    4118039
       32 |          37 |    2089331
       32 |          42 |    1590189
       32 |          52 |    1157539

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4177920)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50519, misses: 481)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50900, misses: 100)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   5154546.18 ops/sec |          1.94ms
2 threads, 5000 ops/thread                                   |   2789445.41 ops/sec |          3.58ms
4 threads, 2500 ops/thread                                   |   1264714.96 ops/sec |          7.91ms
8 threads, 1250 ops/thread                                   |    829148.00 ops/sec |         12.06ms
16 threads, 625 ops/thread                                   |    481006.17 ops/sec |         20.79ms
32 threads, 312 ops/thread                                   |    271387.83 ops/sec |         36.85ms
64 threads, 156 ops/thread                                   |    234459.00 ops/sec |         42.65ms
128 threads, 78 ops/thread                                   |    214313.33 ops/sec |         46.66ms
256 threads, 39 ops/thread                                   |    198758.09 ops/sec |         50.31ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   4388773.34 ops/sec |          2.28ms
2 threads, K=4, 5000 ops/thread                              |   2089066.08 ops/sec |          4.79ms
4 threads, K=4, 2500 ops/thread                              |   1342837.04 ops/sec |          7.45ms
8 threads, K=4, 1250 ops/thread                              |    631802.98 ops/sec |         15.83ms
16 threads, K=4, 625 ops/thread                              |    399009.88 ops/sec |         25.06ms
32 threads, K=4, 312 ops/thread                              |    300867.39 ops/sec |         33.24ms
64 threads, K=4, 156 ops/thread                              |    260891.71 ops/sec |         38.33ms
128 threads, K=4, 78 ops/thread                              |    238281.59 ops/sec |         41.97ms
256 threads, K=4, 39 ops/thread                              |    235397.62 ops/sec |         42.48ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     48.32s

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
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-ebee752303b75e09)
```

## Replacement Clock (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   156.00ns |   166.00ns |    17.00ns |      100
Cold Pin (miss)                                              |     2.00µs |     1.92µs |   364.00ns |      100
Dirty Eviction                                               |     3.19ms |     3.01ms |   792.03µs |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 413396.66 blocks/sec |         99.08ms
Seq Scan MT x2 (40960 blocks)                                | 363260.64 blocks/sec |        112.76ms
Seq Scan MT x4 (40960 blocks)                                | 240586.42 blocks/sec |        170.25ms
Seq Scan MT x8 (40960 blocks)                                | 173424.88 blocks/sec |        236.18ms
Seq Scan MT x16 (40960 blocks)                               | 166159.89 blocks/sec |        246.51ms
Seq Scan MT x32 (40960 blocks)                               | 167566.32 blocks/sec |        244.44ms
Seq Scan MT x64 (40960 blocks)                               | 164593.52 blocks/sec |        248.86ms
Seq Scan MT x128 (40960 blocks)                              | 160293.63 blocks/sec |        255.53ms
Seq Scan MT x256 (40960 blocks)                              | 153436.02 blocks/sec |        266.95ms
Repeated Access (1000 ops)                                   | 6284881.09 blocks/sec |        159.11µs
Repeated Access MT x2 (1000 ops)                             | 6187697.62 blocks/sec |        161.61µs
Repeated Access MT x4 (1000 ops)                             | 3001966.29 blocks/sec |        333.12µs
Repeated Access MT x8 (1000 ops)                             | 2286702.14 blocks/sec |        437.31µs
Repeated Access MT x16 (1000 ops)                            | 1793204.12 blocks/sec |        557.66µs
Repeated Access MT x32 (1000 ops)                            | 1421654.07 blocks/sec |        703.41µs
Repeated Access MT x64 (1000 ops)                            | 1111754.69 blocks/sec |        899.48µs
Repeated Access MT x128 (1000 ops)                           | 811474.90 blocks/sec |          1.23ms
Repeated Access MT x256 (1000 ops)                           | 566979.24 blocks/sec |          1.76ms
Random (K=10, 500 ops)                                       | 5732893.05 blocks/sec |         87.22µs
Random (K=50, 500 ops)                                       | 5518398.34 blocks/sec |         90.61µs
Random (K=100, 500 ops)                                      | 5982865.07 blocks/sec |         83.57µs
Random MT x2 (K=10, 500 ops)                                 | 4647574.43 blocks/sec |        107.58µs
Random MT x4 (K=10, 500 ops)                                 | 2756795.50 blocks/sec |        181.37µs
Random MT x8 (K=10, 500 ops)                                 | 2154178.89 blocks/sec |        232.11µs
Random MT x16 (K=10, 500 ops)                                | 1621376.22 blocks/sec |        308.38µs
Random MT x32 (K=10, 500 ops)                                | 1188515.14 blocks/sec |        420.69µs
Random MT x64 (K=10, 500 ops)                                | 850069.79 blocks/sec |        588.19µs
Random MT x128 (K=10, 500 ops)                               | 541698.90 blocks/sec |        923.02µs
Random MT x256 (K=10, 500 ops)                               | 302245.75 blocks/sec |          1.65ms
Random MT x2 (K=50, 500 ops)                                 | 5309490.18 blocks/sec |         94.17µs
Random MT x4 (K=50, 500 ops)                                 | 2969791.28 blocks/sec |        168.36µs
Random MT x8 (K=50, 500 ops)                                 | 2547614.92 blocks/sec |        196.26µs
Random MT x16 (K=50, 500 ops)                                | 1983064.63 blocks/sec |        252.14µs
Random MT x32 (K=50, 500 ops)                                | 1466963.97 blocks/sec |        340.84µs
Random MT x64 (K=50, 500 ops)                                | 977209.52 blocks/sec |        511.66µs
Random MT x128 (K=50, 500 ops)                               | 539177.15 blocks/sec |        927.34µs
Random MT x256 (K=50, 500 ops)                               | 220779.60 blocks/sec |          2.26ms
Random MT x2 (K=100, 500 ops)                                | 5480593.22 blocks/sec |         91.23µs
Random MT x4 (K=100, 500 ops)                                | 3066224.31 blocks/sec |        163.07µs
Random MT x8 (K=100, 500 ops)                                | 2687247.40 blocks/sec |        186.06µs
Random MT x16 (K=100, 500 ops)                               | 2103845.83 blocks/sec |        237.66µs
Random MT x32 (K=100, 500 ops)                               | 1586546.09 blocks/sec |        315.15µs
Random MT x64 (K=100, 500 ops)                               | 1057328.34 blocks/sec |        472.89µs
Random MT x128 (K=100, 500 ops)                              | 626573.48 blocks/sec |        797.99µs
Random MT x256 (K=100, 500 ops)                              | 229904.72 blocks/sec |          2.17ms
Zipfian (80/20, 500 ops)                                     | 6131433.41 blocks/sec |         81.55µs
Zipfian MT x2 (80/20, 500 ops)                               | 5945091.14 blocks/sec |         84.10µs
Zipfian MT x4 (80/20, 500 ops)                               | 3386570.22 blocks/sec |        147.64µs
Zipfian MT x8 (80/20, 500 ops)                               | 2655252.62 blocks/sec |        188.31µs
Zipfian MT x16 (80/20, 500 ops)                              | 2105467.06 blocks/sec |        237.48µs
Zipfian MT x32 (80/20, 500 ops)                              | 1519964.74 blocks/sec |        328.96µs
Zipfian MT x64 (80/20, 500 ops)                              | 1011347.32 blocks/sec |        494.39µs
Zipfian MT x128 (80/20, 500 ops)                             | 568184.40 blocks/sec |        880.00µs
Zipfian MT x256 (80/20, 500 ops)                             | 245038.70 blocks/sec |          2.04ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     573261
                 16 |     617146
                 32 |     748650
                 64 |    1234419
                128 |    5508610
                256 |    5662707


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    5651953
       32 |          33 |    3407689
       32 |          37 |    2362045
       32 |          42 |    1572713
       32 |          52 |    1163627

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4178940)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50533, misses: 467)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50900, misses: 100)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   5878051.59 ops/sec |          1.70ms
2 threads, 5000 ops/thread                                   |   8113537.60 ops/sec |          1.23ms
4 threads, 2500 ops/thread                                   |   5592265.67 ops/sec |          1.79ms
8 threads, 1250 ops/thread                                   |   4993301.49 ops/sec |          2.00ms
16 threads, 625 ops/thread                                   |   4630339.19 ops/sec |          2.16ms
32 threads, 312 ops/thread                                   |   4331967.32 ops/sec |          2.31ms
64 threads, 156 ops/thread                                   |   4335977.56 ops/sec |          2.31ms
128 threads, 78 ops/thread                                   |   3721005.00 ops/sec |          2.69ms
256 threads, 39 ops/thread                                   |   2895467.26 ops/sec |          3.45ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   5057186.67 ops/sec |          1.98ms
2 threads, K=4, 5000 ops/thread                              |   5836955.16 ops/sec |          1.71ms
4 threads, K=4, 2500 ops/thread                              |   2570536.82 ops/sec |          3.89ms
8 threads, K=4, 1250 ops/thread                              |   1728296.87 ops/sec |          5.79ms
16 threads, K=4, 625 ops/thread                              |   1327630.19 ops/sec |          7.53ms
32 threads, K=4, 312 ops/thread                              |   1197811.93 ops/sec |          8.35ms
64 threads, K=4, 156 ops/thread                              |   1129425.24 ops/sec |          8.85ms
128 threads, K=4, 78 ops/thread                              |   1036632.41 ops/sec |          9.65ms
256 threads, K=4, 39 ops/thread                              |    954817.01 ops/sec |         10.47ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     48.19s

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
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-7e9b6dadb956fd52)
```

## Replacement SIEVE (4KB pages)

```
SimpleDB Buffer Pool Benchmark Suite
====================================
Running benchmarks with 100 iterations per operation
Pool size: 4096 buffers, Block size: 4096 bytes
Environment: macos (aarch64)

Phase 1: Core Latency Benchmarks
Operation                                                    |       Mean |     Median |     StdDev |    Iters
------------------------------------------------------------------------------------------------------------------------
Pin/Unpin (hit)                                              |   166.00ns |   167.00ns |    29.00ns |      100
Cold Pin (miss)                                              |     2.22µs |     2.00µs |   461.00ns |      100
Dirty Eviction                                               |     3.28ms |     3.04ms |     1.14ms |      100

Phase 2: Access Pattern Benchmarks
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
Sequential Scan (40960 blocks)                               | 403257.49 blocks/sec |        101.57ms
Seq Scan MT x2 (40960 blocks)                                | 310156.60 blocks/sec |        132.06ms
Seq Scan MT x4 (40960 blocks)                                | 207861.58 blocks/sec |        197.05ms
Seq Scan MT x8 (40960 blocks)                                | 166570.31 blocks/sec |        245.90ms
Seq Scan MT x16 (40960 blocks)                               | 162600.82 blocks/sec |        251.91ms
Seq Scan MT x32 (40960 blocks)                               | 161397.91 blocks/sec |        253.78ms
Seq Scan MT x64 (40960 blocks)                               | 160839.92 blocks/sec |        254.66ms
Seq Scan MT x128 (40960 blocks)                              | 156873.43 blocks/sec |        261.10ms
Seq Scan MT x256 (40960 blocks)                              | 151848.54 blocks/sec |        269.74ms
Repeated Access (1000 ops)                                   | 6255590.93 blocks/sec |        159.86µs
Repeated Access MT x2 (1000 ops)                             | 5848774.10 blocks/sec |        170.98µs
Repeated Access MT x4 (1000 ops)                             | 2928514.95 blocks/sec |        341.47µs
Repeated Access MT x8 (1000 ops)                             | 2197357.90 blocks/sec |        455.09µs
Repeated Access MT x16 (1000 ops)                            | 1745746.05 blocks/sec |        572.82µs
Repeated Access MT x32 (1000 ops)                            | 1386814.45 blocks/sec |        721.08µs
Repeated Access MT x64 (1000 ops)                            | 1096459.97 blocks/sec |        912.03µs
Repeated Access MT x128 (1000 ops)                           | 784569.10 blocks/sec |          1.27ms
Repeated Access MT x256 (1000 ops)                           | 391565.37 blocks/sec |          2.55ms
Random (K=10, 500 ops)                                       | 5672922.01 blocks/sec |         88.14µs
Random (K=50, 500 ops)                                       | 5587278.88 blocks/sec |         89.49µs
Random (K=100, 500 ops)                                      | 5965305.78 blocks/sec |         83.82µs
Random MT x2 (K=10, 500 ops)                                 | 4729876.74 blocks/sec |        105.71µs
Random MT x4 (K=10, 500 ops)                                 | 2760417.82 blocks/sec |        181.13µs
Random MT x8 (K=10, 500 ops)                                 | 2022261.05 blocks/sec |        247.25µs
Random MT x16 (K=10, 500 ops)                                | 1575790.81 blocks/sec |        317.30µs
Random MT x32 (K=10, 500 ops)                                | 1191494.63 blocks/sec |        419.64µs
Random MT x64 (K=10, 500 ops)                                | 808216.65 blocks/sec |        618.65µs
Random MT x128 (K=10, 500 ops)                               | 570122.18 blocks/sec |        877.01µs
Random MT x256 (K=10, 500 ops)                               | 232481.04 blocks/sec |          2.15ms
Random MT x2 (K=50, 500 ops)                                 | 5532993.24 blocks/sec |         90.37µs
Random MT x4 (K=50, 500 ops)                                 | 2991074.63 blocks/sec |        167.16µs
Random MT x8 (K=50, 500 ops)                                 | 2506026.99 blocks/sec |        199.52µs
Random MT x16 (K=50, 500 ops)                                | 2035540.54 blocks/sec |        245.64µs
Random MT x32 (K=50, 500 ops)                                | 1471471.12 blocks/sec |        339.80µs
Random MT x64 (K=50, 500 ops)                                | 1022559.71 blocks/sec |        488.97µs
Random MT x128 (K=50, 500 ops)                               | 617409.72 blocks/sec |        809.84µs
Random MT x256 (K=50, 500 ops)                               | 307312.19 blocks/sec |          1.63ms
Random MT x2 (K=100, 500 ops)                                | 5436437.18 blocks/sec |         91.97µs
Random MT x4 (K=100, 500 ops)                                | 2960191.35 blocks/sec |        168.91µs
Random MT x8 (K=100, 500 ops)                                | 2580525.29 blocks/sec |        193.76µs
Random MT x16 (K=100, 500 ops)                               | 2038819.12 blocks/sec |        245.24µs
Random MT x32 (K=100, 500 ops)                               | 1522380.52 blocks/sec |        328.43µs
Random MT x64 (K=100, 500 ops)                               | 1032434.98 blocks/sec |        484.29µs
Random MT x128 (K=100, 500 ops)                              | 627107.08 blocks/sec |        797.31µs
Random MT x256 (K=100, 500 ops)                              | 244532.38 blocks/sec |          2.04ms
Zipfian (80/20, 500 ops)                                     | 6146508.17 blocks/sec |         81.35µs
Zipfian MT x2 (80/20, 500 ops)                               | 5906883.88 blocks/sec |         84.65µs
Zipfian MT x4 (80/20, 500 ops)                               | 3355997.50 blocks/sec |        148.99µs
Zipfian MT x8 (80/20, 500 ops)                               | 2617225.53 blocks/sec |        191.04µs
Zipfian MT x16 (80/20, 500 ops)                              | 2050373.58 blocks/sec |        243.86µs
Zipfian MT x32 (80/20, 500 ops)                              | 1533314.32 blocks/sec |        326.09µs
Zipfian MT x64 (80/20, 500 ops)                              | 1057279.16 blocks/sec |        472.91µs
Zipfian MT x128 (80/20, 500 ops)                             | 566743.06 blocks/sec |        882.23µs
Zipfian MT x256 (80/20, 500 ops)                             | 212889.89 blocks/sec |          2.35ms

Phase 3A: Pool Size Sensitivity

Fixed workload: Random access to 100 blocks
Pool Size (buffers) | Throughput (blocks/sec)
--------------------------------------------------
                  8 |     568335
                 16 |     589753
                 32 |     717727
                 64 |    1213916
                128 |    5575877
                256 |    5607141


Phase 3B: Memory Pressure Test
Memory Pressure Test: Working set = pool_size + K
Pool Size | Working Set | Throughput (blocks/sec)
------------------------------------------------------------
       32 |          32 |    5610413
       32 |          33 |    4179938
       32 |          37 |    2235786
       32 |          42 |    1584575
       32 |          52 |    1107908

Phase 4: Hit Rate Measurement
Operation            | Hit Rate & Statistics
----------------------------------------------------------------------
Sequential Scan      | Hit rate:   0.0% (hits: 0, misses: 4178940)
Repeated Access      | Hit rate: 100.0% (hits: 101990, misses: 10)
Zipfian (80/20)      | Hit rate:  99.1% (hits: 50529, misses: 471)
Random (K=10)        | Hit rate: 100.0% (hits: 50990, misses: 10)
Random (K=50)        | Hit rate:  99.9% (hits: 50950, misses: 50)
Random (K=100)       | Hit rate:  99.8% (hits: 50901, misses: 99)

Phase 5: Concurrent Access

Multi-threaded Pin/Unpin (lock contention)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, 10000 ops/thread                                  |   5905122.40 ops/sec |          1.69ms
2 threads, 5000 ops/thread                                   |   9575939.11 ops/sec |          1.04ms
4 threads, 2500 ops/thread                                   |   5895871.83 ops/sec |          1.70ms
8 threads, 1250 ops/thread                                   |   4957297.84 ops/sec |          2.02ms
16 threads, 625 ops/thread                                   |   4486968.94 ops/sec |          2.23ms
32 threads, 312 ops/thread                                   |   4640039.64 ops/sec |          2.16ms
64 threads, 156 ops/thread                                   |   4328801.94 ops/sec |          2.31ms
128 threads, 78 ops/thread                                   |   3686372.29 ops/sec |          2.71ms
256 threads, 39 ops/thread                                   |   2821187.23 ops/sec |          3.54ms

Hot-set Contention (shared buffers)
Operation                                                    |           Throughput |   Mean Duration
------------------------------------------------------------------------------------------------------------------------
1 threads, K=4, 10000 ops/thread                             |   5060513.62 ops/sec |          1.98ms
2 threads, K=4, 5000 ops/thread                              |   6348766.97 ops/sec |          1.58ms
4 threads, K=4, 2500 ops/thread                              |   2456115.36 ops/sec |          4.07ms
8 threads, K=4, 1250 ops/thread                              |   1682993.39 ops/sec |          5.94ms
16 threads, K=4, 625 ops/thread                              |   1343930.31 ops/sec |          7.44ms
32 threads, K=4, 312 ops/thread                              |   1282095.83 ops/sec |          7.80ms
64 threads, K=4, 156 ops/thread                              |   1173267.43 ops/sec |          8.52ms
128 threads, K=4, 78 ops/thread                              |   1037660.86 ops/sec |          9.64ms
256 threads, K=4, 39 ops/thread                              |   1015725.15 ops/sec |          9.85ms

Buffer Starvation (cond.wait() latency):
----------------------------------------------------------------------
Starved 4 threads | Pool recovery time:     48.66s

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
    Finished `bench` profile [optimized + debuginfo] target(s) in 0.00s
     Running benches/buffer_pool.rs (target/release/deps/buffer_pool-32828b2194705bfe)
```
