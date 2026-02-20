## To-do list

### Transactions
1. [Implement deadlock detection strategy](https://github.com/redixhumayun/simpledb/issues/6) - Add wait-for or wait-die strategy
2. [Enforce transaction guard lifetimes via session API](https://github.com/redixhumayun/simpledb/issues/63) - Introduce transaction sessions backed by RwLock to make guard scoping compile-time enforced and eliminate commit/flush deadlocks

### Storage
1. [Redesign Page format with integrated bitmap and ID table](https://github.com/redixhumayun/simpledb/issues/18) - Comprehensive page layout redesign
2. [Expand type system with additional data types and NULL support](https://github.com/redixhumayun/simpledb/issues/33) - Add 7 new types (boolean, bigint, float, decimal, date, timestamp, blob) with NULL bitmap
3. [Implement direct I/O](https://github.com/redixhumayun/simpledb/issues/12) - Eliminate double-buffering with OS page cache
4. [Implement torn write protection](https://github.com/redixhumayun/simpledb/issues/25) - Protect against partial page writes during crashes via checksums, double-write buffer, or full-page WAL logging
5. [Typed page layouts for B-Tree nodes](https://github.com/redixhumayun/simpledb/issues/61) - Give index pages dedicated headers/slot structures instead of reusing heap metadata
6. [Integrate WAL logging with typed page views](https://github.com/redixhumayun/simpledb/issues/62) - Add write-ahead logging to view-based mutations (HeapPageViewMut, BTreePageViewMut) for crash recovery and durability
7. [Implement dense variable-length heap tuples](https://github.com/redixhumayun/simpledb/issues/64) - Switch heap tuple encoding to dense payloads with dynamic offsets to eliminate wasted space from fixed slot sizes
8. [WAL page reclaim/reuse semantics for non-fresh page repurpose](https://github.com/redixhumayun/simpledb/issues/69) - Define and enforce correct undo/recovery behavior when reused pages are reformatted (beyond `*FormatFresh`)

### Buffer Management
1. [Implement ReadHandle and WriteHandle for type-safe buffer access](https://github.com/redixhumayun/simpledb/issues/29) - Compile-time enforcement of read/write access and integrated lock management
2. [Remove redundant Mutex wrapper from BufferManager](https://github.com/redixhumayun/simpledb/issues/26) - BufferManager has interior mutability, outer Mutex is unnecessary
3. [Replace Mutex<Buffer> with RwLock<Buffer> for concurrent reads](https://github.com/redixhumayun/simpledb/issues/27) - Enable true concurrent reads when multiple transactions hold shared locks (requires profiling first)
4. [Implement LRU replacement policy for buffer pool](https://github.com/redixhumayun/simpledb/issues/17) - Replace naive first-available selection with cache-aware algorithm
5. [Remove global lock from BufferManager](https://github.com/redixhumayun/simpledb/issues/38) - Implement stdlib residency tracking to preserve concurrency without coarse locking
6. [Investigate scan-resistant buffer pool behavior](https://github.com/redixhumayun/simpledb/issues/55) - Research and prototype scan-resistant policies (ring buffers, 2Q, LRU-2, cold sublists) to prevent sequential scans from evicting hot OLTP working set

### Iterator Design
1. [Value-Based vs Zero-Copy Scans](https://github.com/redixhumayun/simpledb/issues/10) - Overhaul Scan trait to separate concerns and improve API

### BTree
1. [Support range scans for BTree](https://github.com/redixhumayun/simpledb/issues/11) - Enable efficient range queries

### Query Engine & CLI
1. [Add EXPLAIN command for query plan visualization](https://github.com/redixhumayun/simpledb/issues/19) - Show query execution plans for educational insight
2. [Implement cost-based query optimizer](https://github.com/redixhumayun/simpledb/issues/20) - Replace heuristic optimizer with statistics-driven cost-based optimization
3. [Implement intra-query parallelism for parallel table scans](https://github.com/redixhumayun/simpledb/issues/32) - Enable single queries to leverage multiple CPU cores through parallel table scans
4. [Hash Join Optimization From DuckDB](https://github.com/redixhumayun/simpledb/issues/34) - Implement hash join operator with DuckDB-style optimizations for equi-join queries

### Performance & Benchmarking
1. [Implement buffer pool performance benchmarks](https://github.com/redixhumayun/simpledb/issues/15) - Buffer pool effectiveness measurement (completed in PR #36)
2. [Implement I/O performance benchmarks](https://github.com/redixhumayun/simpledb/issues/37) - Raw disk performance measurement at FileManager layer
3. [Simplify Arc/Mutex usage and clarify multi-threading boundaries](https://github.com/redixhumayun/simpledb/issues/31) - Remove unnecessary synchronization overhead and improve code clarity
4. [Tracking: Performance Alignment Plan](https://github.com/redixhumayun/simpledb/issues/46) - Stabilize benchmarking infrastructure, add instrumentation for latency/queue metrics, implement admission control and background maintenance
5. [Evaluate planner plan ownership and reduce Arc usage](https://github.com/redixhumayun/simpledb/issues/50) - Audit Arc<dyn Plan> usage and prototype alternatives (Rc, owned structs) to clarify single-thread semantics and reduce overhead
6. [Add async I/O queue-depth benchmarks for direct vs buffered](https://github.com/redixhumayun/simpledb/issues/77) - Introduce async-capable I/O benchmarking path (qd=1/4/16/32) to evaluate direct-I/O competitiveness with in-flight concurrency
