## To-do list

### Transactions
1. [Implement deadlock detection strategy](https://github.com/redixhumayun/simpledb/issues/6) - Add wait-for or wait-die strategy

### Storage
1. [Redesign Page format with integrated bitmap and ID table](https://github.com/redixhumayun/simpledb/issues/18) - Comprehensive page layout redesign
2. [Expand type system with additional data types and NULL support](https://github.com/redixhumayun/simpledb/issues/33) - Add 7 new types (boolean, bigint, float, decimal, date, timestamp, blob) with NULL bitmap
3. [Implement direct I/O](https://github.com/redixhumayun/simpledb/issues/12) - Eliminate double-buffering with OS page cache

### Buffer Management
1. [Implement ReadHandle and WriteHandle for type-safe buffer access](https://github.com/redixhumayun/simpledb/issues/29) - Compile-time enforcement of read/write access and integrated lock management
2. [Remove redundant Mutex wrapper from BufferManager](https://github.com/redixhumayun/simpledb/issues/26) - BufferManager has interior mutability, outer Mutex is unnecessary
3. [Replace Mutex<Buffer> with RwLock<Buffer> for concurrent reads](https://github.com/redixhumayun/simpledb/issues/27) - Enable true concurrent reads when multiple transactions hold shared locks (requires profiling first)
4. [Implement LRU replacement policy for buffer pool](https://github.com/redixhumayun/simpledb/issues/17) - Replace naive first-available selection with cache-aware algorithm
5. [Remove global lock from BufferManager](https://github.com/redixhumayun/simpledb/issues/38) - Implement stdlib residency tracking to preserve concurrency without coarse locking

### Iterator Design
1. [Value-Based vs Zero-Copy Scans](https://github.com/redixhumayun/simpledb/issues/10) - Overhaul Scan trait to separate concerns and improve API

### BTree
1. [Support range scans for BTree](https://github.com/redixhumayun/simpledb/issues/11) - Enable efficient range queries

### Query Engine & CLI
1. [Add EXPLAIN command for query plan visualization](https://github.com/redixhumayun/simpledb/issues/19) - Show query execution plans for educational insight
2. [Implement cost-based query optimizer](https://github.com/redixhumayun/simpledb/issues/20) - Replace heuristic optimizer with statistics-driven cost-based optimization
3. [Implement intra-query parallelism for parallel table scans](https://github.com/redixhumayun/simpledb/issues/32) - Enable single queries to leverage multiple CPU cores through parallel table scans

### Performance & Benchmarking
1. [Implement buffer pool performance benchmarks](https://github.com/redixhumayun/simpledb/issues/15) - Buffer pool effectiveness measurement (completed in PR #36)
2. [Implement I/O performance benchmarks](https://github.com/redixhumayun/simpledb/issues/37) - Raw disk performance measurement at FileManager layer
3. [Simplify Arc/Mutex usage and clarify multi-threading boundaries](https://github.com/redixhumayun/simpledb/issues/31) - Remove unnecessary synchronization overhead and improve code clarity