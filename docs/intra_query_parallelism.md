# Intra-Query Parallelism Implementation Guide

## Overview

This document outlines the design and implementation of **intra-query parallelism** for SimpleDB - the ability to execute a single query using multiple threads concurrently, particularly for parallel table scans.

## Motivation

Currently, SimpleDB supports **inter-transaction parallelism** where multiple transactions can run concurrently, but each transaction's query execution is single-threaded. This means:
- Large table scans are sequential, even with available CPU cores
- A single query cannot leverage multiple threads
- Query performance is limited by single-thread throughput

**Goal:** Enable a single transaction to spawn multiple worker threads to scan different partitions of a table concurrently, improving query performance on multi-core systems.

---

## Current Architecture: Inter-Transaction Parallelism

### Threading Model

```
Thread 1              Thread 2              Thread 3
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Txn(1)    │      │   Txn(2)    │      │   Txn(3)    │
│             │      │             │      │             │
│  Query:     │      │  Query:     │      │  Query:     │
│  SELECT *   │      │  INSERT     │      │  SELECT *   │
│  FROM emp   │      │  INTO dept  │      │  FROM dept  │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       │                    │                    │
    ┌──▼────────────────────▼────────────────────▼──┐
    │         Query Execution (Pull-Based)          │
    │                                                │
    │  TablePlan::open() → TableScan → Iterator     │
    │                                                │
    │  Single-threaded pull:                        │
    │  while let Some(row) = scan.next() {          │
    │      process(row)                             │
    │  }                                             │
    └────────────────────────────────────────────────┘
```

### Key Characteristics

- **Multi-threading boundary:** At the Transaction level
- **Each transaction:** Single-threaded query execution
- **Synchronization:** Transactions coordinate through shared resources (FileManager, LogManager, BufferManager, LockTable)
- **Scans:** Sequential iteration through blocks (0 → 1 → 2 → ... → N)

---

## Proposed Architecture: Intra-Query Parallelism

### Model 1: Static Partition-Based Parallel Scan

A single transaction spawns multiple worker threads to scan different partitions of the same table.

```
                    ┌───────────────┐
                    │   Txn(1)      │
                    │   Main Thread │
                    └───────┬───────┘
                            │
                    Creates Parallel Plan
                            │
              ┌─────────────┼─────────────┐
              │             │             │
         Worker 1       Worker 2     Worker 3
         Thread          Thread       Thread
              │             │             │
    ┌─────────▼──┐   ┌─────▼──────┐  ┌──▼─────────┐
    │ TableScan  │   │ TableScan  │  │ TableScan  │
    │ Partition1 │   │ Partition2 │  │ Partition3 │
    │            │   │            │  │            │
    │ Blocks     │   │ Blocks     │  │ Blocks     │
    │ 0-99       │   │ 100-199    │  │ 200-299    │
    └─────┬──────┘   └─────┬──────┘  └──┬─────────┘
          │                │            │
          │  Each worker scans its range sequentially
          │                │            │
    ┌─────▼────────────────▼────────────▼─────┐
    │          Merge/Coordinator              │
    │        (Collect results from            │
    │         all partitions)                 │
    └─────────────────┬───────────────────────┘
                      │
                  Results to
                 Parent Operator
```

### Table Partitioning Strategy

```
Table: emp.tbl (300 blocks total)

┌──────────────────────────────────────────────────────────────┐
│                     emp.tbl                                   │
├──────────────────┬──────────────────┬──────────────────────┤
│  Partition 1     │  Partition 2     │  Partition 3         │
│  Blocks 0-99     │  Blocks 100-199  │  Blocks 200-299      │
│                  │                  │                      │
│  Worker 1        │  Worker 2        │  Worker 3            │
│  scans here      │  scans here      │  scans here          │
└──────────────────┴──────────────────┴──────────────────────┘
```

**Benefits:**
- Simple to implement
- Predictable partition sizes
- No coordination during scan

**Drawbacks:**
- Load imbalance if data is skewed
- Workers may finish at different times

---

### Model 2: Morsel-Driven Parallelism

Workers dynamically pull "morsels" (small chunks) from a shared work queue.

```
                    ┌─────────────────┐
                    │  Work Queue     │
                    │  (Mutex)        │
                    │                 │
                    │ ┌──────────┐    │
                    │ │Blocks    │    │
                    │ │0-9       │    │
                    │ ├──────────┤    │
                    │ │Blocks    │    │
                    │ │10-19     │    │
                    │ ├──────────┤    │
                    │ │Blocks    │    │
                    │ │20-29     │    │
                    │ └──────────┘    │
                    └─────────────────┘
                             ▲
                             │ Pull next morsel
          ┌──────────────────┼──────────────────┐
          │                  │                  │
     ┌────▼────┐       ┌─────▼────┐       ┌────▼────┐
     │Worker 1 │       │Worker 2  │       │Worker 3 │
     │         │       │          │       │         │
     │Process  │       │Process   │       │Process  │
     │10 blocks│       │10 blocks │       │10 blocks│
     └─────────┘       └──────────┘       └─────────┘
```

**Benefits:**
- Better load balancing (fast workers get more work)
- Handles skewed data distribution
- Workers finish at similar times

**Drawbacks:**
- Contention on work queue
- More complex coordinator logic

---

### Model 3: Exchange Operators (Volcano-Style)

Insert "Exchange" operators in query plan to parallelize between operators.

```
Query: SELECT * FROM emp WHERE salary > 50000

Serial Plan:
┌──────────────┐
│  SelectPlan  │
│  (salary>5k) │
└──────┬───────┘
       │
┌──────▼───────┐
│  TablePlan   │
│  (emp)       │
└──────────────┘

Parallel Plan with Exchange:
┌──────────────────────────────────┐
│      SelectPlan                  │
│      (salary>50k)                │
│      Main Thread                 │
└───────────┬──────────────────────┘
            │
┌───────────▼──────────────────────┐
│  EXCHANGE (Gather)               │
│  Collects from all workers       │
└───────────┬──────────────────────┘
            │
    ┌───────┼───────┐
    │       │       │
┌───▼──┐ ┌──▼──┐ ┌─▼───┐
│Select│ │Select│ │Select│
│Worker│ │Worker│ │Worker│
│  1   │ │  2  │ │  3  │
└───┬──┘ └──┬──┘ └─┬───┘
    │       │      │
┌───▼──────────────▼─────────┐
│  EXCHANGE (Distribute)     │
│  Partitions data to workers│
└───────────┬────────────────┘
            │
    ┌───────┼───────┐
    │       │       │
┌───▼──┐ ┌──▼──┐ ┌─▼───┐
│Scan  │ │Scan │ │Scan │
│Part 1│ │Part2│ │Part3│
└──────┘ └─────┘ └─────┘
```

**Benefits:**
- Parallelizes entire pipeline
- Can parallelize joins, aggregations, etc.
- Industry-standard approach

**Drawbacks:**
- Most complex to implement
- Requires rethinking entire query execution model

---

## Required Architectural Changes

### 1. Make Transaction Thread-Safe for Sharing

Currently, `Transaction` contains single-threaded components:

```rust
pub struct Transaction {
    file_manager: SharedFS,                  // ✓ Already Arc<Mutex>
    log_manager: Arc<Mutex<LogManager>>,     // ✓ Already Arc<Mutex>
    buffer_manager: Arc<Mutex<BufferManager>>,// ✓ Already Arc<Mutex>
    recovery_manager: RecoveryManager,       // ✗ NOT thread-safe
    concurrency_manager: ConcurrencyManager, // ✓ Uses Arc<LockTable>
    buffer_list: BufferList,                 // ✗ Uses RefCell (single-threaded)
    tx_id: TransactionID,
}
```

**Problem: BufferList uses RefCell**

```rust
struct BufferList {
    buffers: RefCell<HashMap<BlockId, HashMapValue>>,  // Panics with multiple threads!
    buffer_manager: Arc<Mutex<BufferManager>>,
    txn_committed: Cell<bool>,
}
```

When multiple workers try to pin different blocks:

```
Worker 1: pin(block 5)   ──┐
Worker 2: pin(block 105) ──┼─→ Both try to borrow_mut() RefCell
Worker 3: pin(block 205) ──┘    PANIC: already borrowed!
```

**Solution: Replace RefCell with Mutex**

```rust
struct BufferList {
    buffers: Mutex<HashMap<BlockId, HashMapValue>>,  // ✓ Thread-safe
    buffer_manager: Arc<Mutex<BufferManager>>,
    txn_committed: AtomicBool,  // Cell → Atomic
}
```

**Alternative: Use DashMap (lock-free concurrent HashMap)**

```rust
struct BufferList {
    buffers: DashMap<BlockId, HashMapValue>,  // Lock-free!
    buffer_manager: Arc<Mutex<BufferManager>>,
    txn_committed: AtomicBool,
}
```

### 2. Make RecoveryManager Thread-Safe (For Updates)

Currently, `RecoveryManager` is owned directly by `Transaction`:

```rust
struct RecoveryManager {
    tx_id: usize,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}
```

For **read-only queries**, this is fine (workers don't modify data).

For **parallel updates** (INSERT/UPDATE/DELETE), we need:

```rust
pub struct Transaction {
    // ...
    recovery_manager: Arc<Mutex<RecoveryManager>>,  // Wrap in Arc<Mutex>
}
```

**Complexity:** WAL logging must maintain deterministic order for recovery correctness.

### 3. Add Partitioned Table Scans

Create a new scan type that only scans a subset of blocks:

```rust
struct PartitionedTableScan {
    txn: Arc<Transaction>,
    layout: Layout,
    file_name: String,
    record_page: Option<RecordPage>,
    current_slot: Option<usize>,
    // NEW: Partition boundaries
    start_block: usize,
    end_block: usize,
}

impl PartitionedTableScan {
    fn new(
        txn: Arc<Transaction>,
        layout: Layout,
        table_name: &str,
        start_block: usize,
        end_block: usize,
    ) -> Self {
        // Initialize to start_block
        // Stop iteration at end_block
    }
}

impl Iterator for PartitionedTableScan {
    fn next(&mut self) -> Option<Self::Item> {
        // Existing logic, but check:
        if current_block >= self.end_block {
            return None;  // Stop at partition boundary
        }
        // ... continue scanning ...
    }
}
```

### 4. Create Parallel Plan Operators

```rust
struct ParallelTableScanPlan {
    table_name: String,
    txn: Arc<Transaction>,
    layout: Layout,
    stat_info: StatInfo,
    num_workers: usize,  // Number of parallel workers
}

impl Plan for ParallelTableScanPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        // 1. Calculate partitions
        let total_blocks = self.txn.size(&format!("{}.tbl", self.table_name));
        let blocks_per_worker = total_blocks / self.num_workers;

        // 2. Spawn workers with PartitionedTableScan
        let workers: Vec<_> = (0..self.num_workers).map(|i| {
            let start = i * blocks_per_worker;
            let end = if i == self.num_workers - 1 {
                total_blocks  // Last worker takes remainder
            } else {
                (i + 1) * blocks_per_worker
            };

            spawn_worker(
                Arc::clone(&self.txn),
                self.layout.clone(),
                &self.table_name,
                start,
                end,
            )
        }).collect();

        // 3. Return coordinator that merges results
        Box::new(ParallelTableScan::new(workers))
    }
}
```

### 5. Result Coordination

**Option A: Push-Based with Channels**

```rust
struct ParallelTableScan {
    receiver: Receiver<Row>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl ParallelTableScan {
    fn new(workers: Vec<WorkerConfig>) -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded();

        let handles = workers.into_iter().map(|config| {
            let sender = sender.clone();
            thread::spawn(move || {
                let mut scan = PartitionedTableScan::new(config);
                while let Some(Ok(())) = scan.next() {
                    let row = scan.current_row();
                    sender.send(row).unwrap();
                }
            })
        }).collect();

        Self { receiver, worker_handles: handles }
    }
}

impl Iterator for ParallelTableScan {
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()  // Pull from any worker
    }
}
```

**Option B: Pull-Based with Round-Robin**

```rust
struct ParallelTableScan {
    workers: Vec<Box<dyn UpdateScan>>,
    current_worker: usize,
}

impl Iterator for ParallelTableScan {
    fn next(&mut self) -> Option<Self::Item> {
        // Round-robin through workers
        for _ in 0..self.workers.len() {
            if let Some(result) = self.workers[self.current_worker].next() {
                self.current_worker = (self.current_worker + 1) % self.workers.len();
                return Some(result);
            }
            self.current_worker = (self.current_worker + 1) % self.workers.len();
        }
        None  // All workers exhausted
    }
}
```

---

## Synchronization Challenges

### Challenge 1: BufferList Contention

**Problem:** Multiple workers pinning buffers concurrently

```
Worker 1                    Worker 2                    Worker 3
   │                           │                           │
   │ pin(block 5)              │                           │
   ├──────────────────────────►│                           │
   │ BufferList.buffers        │ pin(block 105)            │
   │ .insert(5, ...)           ├──────────────────────────►│
   │                           │ BufferList.buffers        │
   │                           │ .insert(105, ...)         │
```

**Solution:** Mutex<HashMap> or DashMap

### Challenge 2: Lock Table Coordination

All workers share the same Transaction ID for locking:

```
Worker 1: slock(block 5)   ──┐
Worker 2: slock(block 105) ──┼─→ All acquire locks under Txn(1)
Worker 3: slock(block 205) ──┘
```

**Status:** Already works! `LockTable` uses `Arc<LockTable>` with internal `Mutex`, so all workers can call `txn.slock()` concurrently.

### Challenge 3: Buffer Pool Contention

All workers compete for buffers from the shared pool:

```
         ┌────────────────────────────────┐
         │  BufferManager (Arc<Mutex<>>)  │
         │  buffer_pool: Vec<Arc<Mutex>>  │
         └────────────────────────────────┘
                       ▲
                       │
           ┌───────────┼───────────┐
           │           │           │
      Worker 1     Worker 2    Worker 3
      pin()        pin()       pin()
```

**Contention:** All workers serialize through `BufferManager` mutex.

**Mitigations:**
1. Keep `BufferManager` lock time minimal (already done)
2. Use `RwLock` instead of `Mutex` for buffer pool ([#27](https://github.com/redixhumayun/simpledb/issues/27))
3. Consider per-worker buffer pools (advanced)

### Challenge 4: RecoveryManager Log Sequence

For parallel **updates**, workers need to log modifications:

```
Worker 1: Write A=5  → Log record (Txn 1, block 5, A=5)
Worker 2: Write B=10 → Log record (Txn 1, block 105, B=10)
```

**Problem:** `RecoveryManager` methods take `&self`, not thread-safe.

**Solution:** Wrap in `Arc<Mutex<RecoveryManager>>` and ensure deterministic log order.

---

## Implementation Strategy

### Phase 1: Make Transaction Thread-Safe (Read-Only)

**Goal:** Enable `Arc<Transaction>` to be shared across threads for read-only queries.

**Tasks:**
- [ ] Replace `BufferList::buffers` from `RefCell` to `Mutex<HashMap>` (or `DashMap`)
- [ ] Replace `BufferList::txn_committed` from `Cell<bool>` to `AtomicBool`
- [ ] Add unit tests with `Arc<Transaction>` shared across threads
- [ ] Verify lock management works correctly with concurrent workers
- [ ] All tests pass

**Complexity:** 1-2 days

**Acceptance Criteria:**
- Multiple threads can share `Arc<Transaction>` without panics
- Concurrent `pin()`/`unpin()` operations work correctly
- Lock acquisition works across threads
- No data races (verified with `cargo test` and Miri)

---

### Phase 2: Implement Static Partitioned Scans

**Goal:** Create parallel table scan with static partitioning.

**Tasks:**
- [ ] Create `PartitionedTableScan` struct with `start_block`/`end_block`
- [ ] Implement `Iterator` for `PartitionedTableScan` that respects boundaries
- [ ] Create `ParallelTableScanPlan` that spawns N workers
- [ ] Implement result coordinator (start with crossbeam channels)
- [ ] Add unit tests for partitioned scans
- [ ] Add integration tests for parallel queries
- [ ] Benchmark performance vs serial scan

**Complexity:** 3-5 days

**Acceptance Criteria:**
- `PartitionedTableScan` correctly scans only its partition
- `ParallelTableScanPlan` spawns workers and merges results
- Results are correct (same as serial scan)
- Performance improves with multiple cores (benchmark)
- No deadlocks or race conditions

---

### Phase 3: Add Morsel-Driven Execution

**Goal:** Improve load balancing with dynamic work assignment.

**Tasks:**
- [ ] Create work queue structure with morsel management
- [ ] Implement worker pool that pulls morsels dynamically
- [ ] Add morsel size tuning (blocks per morsel)
- [ ] Benchmark morsel-driven vs static partitioning
- [ ] Compare load balancing on skewed data

**Complexity:** 3-5 days

**Acceptance Criteria:**
- Workers dynamically pull work from shared queue
- Better load balancing than static partitions (measured)
- Performance comparable or better on skewed data
- No performance regression on uniform data

---

### Phase 4: Support Parallel Updates (Advanced)

**Goal:** Enable parallel INSERT/UPDATE/DELETE operations.

**Tasks:**
- [ ] Wrap `RecoveryManager` in `Arc<Mutex<RecoveryManager>>`
- [ ] Ensure WAL logging order is deterministic
- [ ] Handle parallel modifications to the same table
- [ ] Add extensive correctness tests
- [ ] Add crash recovery tests
- [ ] Benchmark parallel insert performance

**Complexity:** 1-2 weeks (recovery is subtle!)

**Acceptance Criteria:**
- Parallel updates produce correct results
- WAL records are logged in deterministic order
- Crash recovery works correctly
- No lost updates or corruption
- Performance improves for bulk inserts

---

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_partitioned_scan_respects_boundaries() {
    // Create table with 100 blocks
    // Scan partition [25, 50)
    // Verify only blocks 25-49 are scanned
}

#[test]
fn test_parallel_scan_shared_transaction() {
    // Create Arc<Transaction>
    // Share across 3 threads
    // Each thread pins different blocks
    // Verify no panics or race conditions
}

#[test]
fn test_parallel_scan_correctness() {
    // Insert 10,000 rows
    // Scan with ParallelTableScanPlan (4 workers)
    // Verify all 10,000 rows are returned exactly once
}
```

### Integration Tests

```rust
#[test]
fn test_parallel_query_end_to_end() {
    let db = SimpleDB::new(...);
    let txn = Arc::new(db.new_tx());

    // INSERT 10,000 rows into emp
    for i in 0..10000 {
        db.planner.execute_update(
            format!("insert into emp(id, name) values ({i}, 'name{i}')"),
            Arc::clone(&txn)
        )?;
    }

    // SELECT with parallel scan
    let plan = db.planner.create_query_plan(
        "select * from emp".to_string(),
        Arc::clone(&txn)
    )?;

    // Count results
    let mut count = 0;
    let mut scan = plan.open();
    while let Some(Ok(())) = scan.next() {
        count += 1;
    }

    assert_eq!(count, 10000);
}
```

### Performance Benchmarks

```rust
#[bench]
fn bench_serial_scan(b: &mut Bencher) {
    // Scan 100,000 rows serially
}

#[bench]
fn bench_parallel_scan_2_workers(b: &mut Bencher) {
    // Scan 100,000 rows with 2 workers
}

#[bench]
fn bench_parallel_scan_4_workers(b: &mut Bencher) {
    // Scan 100,000 rows with 4 workers
}
```

**Expected Results:**
- 2 workers: ~1.8x speedup
- 4 workers: ~3.2x speedup
- Diminishing returns due to synchronization overhead

---

## Design Decisions

### Why Not Parallelize Everything?

**Short queries:** Parallelism overhead exceeds benefits for small tables.

**Threshold:** Only parallelize if `table_blocks > 100` (configurable).

### How Many Workers?

**Options:**
1. Fixed: Always use 4 workers
2. Adaptive: `min(num_cpus, table_blocks / 25)`
3. User-specified: `SET parallelism = 4`

**Recommendation:** Start with adaptive based on table size.

### Coordination Mechanism

**Crossbeam channels (recommended):**
- Simple push-based model
- Workers send rows to channel
- Main thread pulls from channel
- Good load balancing

**Work-stealing queues (advanced):**
- Lower overhead
- Better cache locality
- More complex implementation

---

## Related Issues

- [#31: Simplify Arc/Mutex usage and clarify multi-threading boundaries](https://github.com/redixhumayun/simpledb/issues/31) - Prerequisite for understanding threading model
- [#27: Replace Mutex<Buffer> with RwLock<Buffer>](https://github.com/redixhumayun/simpledb/issues/27) - Reduces buffer pool contention for parallel scans
- [#26: Remove redundant Mutex wrapper from BufferManager](https://github.com/redixhumayun/simpledb/issues/26) - Simplifies buffer management
- [#29: Implement ReadHandle and WriteHandle](https://github.com/redixhumayun/simpledb/issues/29) - Type-safe buffer access for parallel workers

---

## References

### Academic Papers
- **Morsel-Driven Parallelism:** Leis et al., "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age" (SIGMOD 2014)
- **Volcano Model:** Graefe, "Volcano—An Extensible and Parallel Query Evaluation System" (IEEE TKDE 1994)

### Industry Implementations
- **DuckDB:** Morsel-driven execution with work-stealing
- **PostgreSQL:** Parallel sequential scans with dynamic worker assignment
- **SQL Server:** Exchange operators with partition parallelism

---

## Conclusion

Intra-query parallelism is a significant architectural enhancement that requires careful consideration of thread safety, synchronization, and performance trade-offs. The phased approach allows incremental implementation with validation at each step, starting with read-only queries and eventually supporting parallel updates.

**Key Takeaway:** SimpleDB's current architecture is well-positioned for this enhancement - most shared resources are already properly synchronized, and the main work is making `Transaction` thread-safe and implementing parallel scan operators.
