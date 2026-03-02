## To-do list

### Transactions
1. [Implement deadlock detection strategy](https://github.com/redixhumayun/simpledb/issues/6) - Add wait-for or wait-die strategy
2. [Enforce transaction guard lifetimes via session API](https://github.com/redixhumayun/simpledb/issues/63) - Introduce transaction sessions backed by RwLock to make guard scoping compile-time enforced and eliminate commit/flush deadlocks

### Storage
1. [Expand type system with additional data types and NULL support](https://github.com/redixhumayun/simpledb/issues/33) - Add 7 new types (boolean, bigint, float, decimal, date, timestamp, blob) with NULL bitmap
2. [Implement torn write protection](https://github.com/redixhumayun/simpledb/issues/25) - Protect against partial page writes during crashes via checksums, double-write buffer, or full-page WAL logging
3. [WAL page reclaim/reuse semantics for non-fresh page repurpose](https://github.com/redixhumayun/simpledb/issues/69) - Define and enforce correct undo/recovery behavior when reused pages are reformatted (beyond `*FormatFresh`)

### Buffer Management
1. [Implement ReadHandle and WriteHandle for type-safe buffer access](https://github.com/redixhumayun/simpledb/issues/29) - Compile-time enforcement of read/write access and integrated lock management
2. [Investigate scan-resistant buffer pool behavior](https://github.com/redixhumayun/simpledb/issues/55) - Research and prototype scan-resistant policies (ring buffers, 2Q, LRU-2, cold sublists) to prevent sequential scans from evicting hot OLTP working set

### Iterator Design
1. [Value-Based vs Zero-Copy Scans](https://github.com/redixhumayun/simpledb/issues/10) - Overhaul Scan trait to separate concerns and improve API

### BTree
1. [B-tree concurrency: logical locks and latch crabbing](https://github.com/redixhumayun/simpledb/issues/83) - Add table-S/X logical locks at index entry points and latch crabbing for traversal to give index operations correct 2PL protection
2. [Fix u32::MAX table_id placeholder causing false lock conflicts in ChunkScan](https://github.com/redixhumayun/simpledb/issues/84) - Replace u32::MAX sentinel with stable per-bucket IDs to avoid cross-table row lock collisions

### Query Engine & CLI
1. [Add EXPLAIN command for query plan visualization](https://github.com/redixhumayun/simpledb/issues/19) - Show query execution plans for educational insight
2. [Implement cost-based query optimizer](https://github.com/redixhumayun/simpledb/issues/20) - Replace heuristic optimizer with statistics-driven cost-based optimization
3. [Implement intra-query parallelism for parallel table scans](https://github.com/redixhumayun/simpledb/issues/32) - Enable single queries to leverage multiple CPU cores through parallel table scans
4. [Hash Join Optimization From DuckDB](https://github.com/redixhumayun/simpledb/issues/34) - Implement hash join operator with DuckDB-style optimizations for equi-join queries

### Performance & Benchmarking
1. [Tracking: Performance Alignment Plan](https://github.com/redixhumayun/simpledb/issues/46) - Stabilize benchmarking infrastructure, add instrumentation for latency/queue metrics, implement admission control and background maintenance
2. [Evaluate planner plan ownership and reduce Arc usage](https://github.com/redixhumayun/simpledb/issues/50) - Audit Arc<dyn Plan> usage and prototype alternatives (Rc, owned structs) to clarify single-thread semantics and reduce overhead
