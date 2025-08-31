## To-do list

### Transactions
1. [Implement deadlock detection strategy](https://github.com/redixhumayun/simpledb/issues/6) - Add wait-for or wait-die strategy

### Storage
1. [Store bitmap for presence checking](https://github.com/redixhumayun/simpledb/issues/7) - Improve record presence check performance
2. [Implement ID table for variable length strings](https://github.com/redixhumayun/simpledb/issues/8) - Better offset management similar to B-tree pages
3. [Implement direct I/O](https://github.com/redixhumayun/simpledb/issues/12) - Eliminate double-buffering with OS page cache

### Buffer Management
1. [Convert manual pin/unpin to RAII Buffer Guard](https://github.com/redixhumayun/simpledb/issues/9) - Eliminate memory leaks and double-unpin errors

### Iterator Design  
1. [Value-Based vs Zero-Copy Scans](https://github.com/redixhumayun/simpledb/issues/10) - Overhaul Scan trait to separate concerns and improve API

### BTree
1. [Support range scans for BTree](https://github.com/redixhumayun/simpledb/issues/11) - Enable efficient range queries