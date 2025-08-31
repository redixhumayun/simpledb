## To-do list

### Transactions
1. [Implement deadlock detection strategy](https://github.com/redixhumayun/simpledb/issues/6) - Add wait-for or wait-die strategy
2. [Add file synchronization for transaction durability](https://github.com/redixhumayun/simpledb/issues/13) - Fix critical durability flaw with fsync operations

### Storage
1. [Redesign Page format with integrated bitmap and ID table](https://github.com/redixhumayun/simpledb/issues/18) - Comprehensive page layout redesign (supersedes #7, #8)
2. [Implement direct I/O](https://github.com/redixhumayun/simpledb/issues/12) - Eliminate double-buffering with OS page cache

### Buffer Management
1. [Convert manual pin/unpin to RAII Buffer Guard](https://github.com/redixhumayun/simpledb/issues/9) - Eliminate memory leaks and double-unpin errors
2. [Implement LRU replacement policy for buffer pool](https://github.com/redixhumayun/simpledb/issues/17) - Replace naive first-available selection with cache-aware algorithm

### Iterator Design  
1. [Value-Based vs Zero-Copy Scans](https://github.com/redixhumayun/simpledb/issues/10) - Overhaul Scan trait to separate concerns and improve API

### BTree
1. [Support range scans for BTree](https://github.com/redixhumayun/simpledb/issues/11) - Enable efficient range queries

### Performance & Benchmarking
1. [Set up Criterion.rs benchmarking framework](https://github.com/redixhumayun/simpledb/issues/14) - Basic CRUD operation benchmarks
2. [Implement buffer pool and I/O performance benchmarks](https://github.com/redixhumayun/simpledb/issues/15) - Storage layer performance measurement
3. [Add basic CI benchmark execution](https://github.com/redixhumayun/simpledb/issues/16) - Automated benchmark runs with output