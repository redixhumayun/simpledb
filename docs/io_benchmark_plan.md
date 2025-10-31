# I/O Performance Benchmarking Plan

## Summary

Create comprehensive benchmarks to measure raw I/O performance characteristics at the FileManager layer, isolated from buffer pool caching effects. These benchmarks will establish baselines for future storage optimizations (direct I/O, different block sizes, etc.).

## Motivation

Issue [#15](https://github.com/redixhumayun/simpledb/issues/15) originally covered both buffer pool and I/O benchmarks. PR [#36](https://github.com/redixhumayun/simpledb/pull/36) completed the buffer pool portion. This document focuses on the remaining I/O benchmarking requirements. Issue [#37](https://github.com/redixhumayun/simpledb/issues/37) tracks IO benchmarks.

**Why separate I/O benchmarks?**
- **Isolation**: Buffer pool benchmarks measure caching effectiveness; I/O benchmarks measure disk performance
- **Attribution**: Clean separation enables identifying whether performance issues are cache-related or I/O-related
- **Reproducibility**: I/O benchmarks bypass non-deterministic cache behavior
- **Comparison**: Provides clean baseline for measuring impact of direct I/O implementation (#12)

## Benchmark Categories

### 1. Sequential vs Random I/O Patterns

**Purpose**: Measure fundamental I/O performance difference between sequential and random access

**Implementation**:
```rust
// Sequential read: blocks 0→1→2→3...
benchmark("Sequential Read", || {
    for i in 0..num_blocks {
        file_manager.read(&BlockId::new("seq", i), &mut page);
    }
});

// Random read: blocks in random order
benchmark("Random Read", || {
    for &i in &random_indices {
        file_manager.read(&BlockId::new("rand", i), &mut page);
    }
});
```

**Expected Results**:
- **SSD**: Sequential 2-3x faster than random
- **HDD**: Sequential 10-100x faster than random (seek time dominates)

**Metrics**:
- Throughput (MB/sec)
- Latency per operation (µs)
- IOPS

### 2. Block Size Sensitivity

**Purpose**: Determine optimal block size for this workload and hardware

**Test Sizes**: 1KB, 4KB (OS page), 8KB, 16KB, 64KB

**Implementation**:
```rust
for block_size in [1024, 4096, 8192, 16384, 65536] {
    let fm = FileManager::new(&dir, block_size, true);
    benchmark(&format!("Block size {}", block_size), || {
        // Same workload, different block sizes
    });
}
```

**Trade-offs**:
- **Smaller blocks**: More overhead per operation, finer granularity
- **Larger blocks**: Amortized overhead, but waste if partial reads
- **OS alignment**: 4KB typically optimal (matches OS page size)

**Metrics**:
- Throughput vs block size curve
- Overhead percentage

### 3. Write-Ahead Log (WAL) Performance

**Purpose**: Measure WAL append throughput and fsync impact

**Test Cases**:
```rust
// 1. WAL append without fsync (best case)
benchmark("WAL append (no fsync)", || {
    let log = Arc::clone(&log_manager);
    let lsn = log.lock().unwrap().append(make_record()); // Synthetic WAL payload
    flush_policy.record(lsn, &log); // FlushPolicy::None => no-op
});

// 2. WAL append with immediate fsync (worst case)
benchmark("WAL append + fsync", || {
    let log = Arc::clone(&log_manager);
    let lsn = log.lock().unwrap().append(make_record());
    flush_policy.record(lsn, &log); // FlushPolicy::Immediate => flush_lsn(lsn)
});

// 3. Group commit (batched fsync)
benchmark("WAL group commit", || {
    let log = Arc::clone(&log_manager);
    for i in 0..batch_size {
        let lsn = log.lock().unwrap().append(make_record_for_batch(i));
        flush_policy.record(lsn, &log); // FlushPolicy::Group => flush every N
    }
    flush_policy.finish_batch(&log); // Flush left-over entries
});
```

**Metrics**:
- Commits/sec
- Latency distribution (p50, p90, p99)
- Throughput (records/sec)

### 4. Mixed Read/Write Workloads

**Purpose**: Simulate realistic OLTP patterns

**Workload Mix**:
- 70% reads / 30% writes (typical OLTP)
- 50% reads / 50% writes (balanced)
- 10% reads / 90% writes (write-heavy)

**Implementation**:
```rust
benchmark("70/30 read/write", || {
    let log = Arc::clone(&log_manager);
    for op in &operations {
        match op {
            Read(block_id) => file_manager.read(block_id, &mut page),
            Write(block_id) => {
                file_manager.write(block_id, &mut page);
                let lsn = log.lock().unwrap().append(record_for(block_id));
                flush_policy.record(lsn, &log); // none, per-op fsync, or group commit
            }
        }
    }
});
```

**Flush policy**: benchmark variants cover no fsync, immediate fsync, and batched group commit so mixed workloads align with WAL-only measurements.

**Metrics**:
- Combined throughput
- Read vs write latency breakdown

### 5. Concurrent I/O Stress Test

**Purpose**: Measure I/O subsystem under multi-threaded load

**Implementation**:
```rust
let handles: Vec<_> = (0..num_threads).map(|thread_id| {
    let log = Arc::clone(&log_manager);
    let policy = flush_policy.clone(); // Clone per thread
    thread::spawn(move || {
        let mut policy = policy;
        for _ in 0..ops_per_thread {
            // Each thread does mix of reads/writes
            let lsn = log.lock().unwrap().append(record_for_thread(thread_id));
            policy.record(lsn, &log);
        }
        policy.finish_batch(&log);
    })
}).collect();
```

**Metrics**:
- Aggregate throughput (all threads)
- Per-thread latency
- Contention overhead
- Shared-file case: all threads operate on one pre-sized file with overlapping block ranges (exercises intra-file locking)
- Sharded case: each thread uses a disjoint, pre-sized file or non-overlapping block range (captures scale-out behavior without coordination)
- All files must be pre-sized before the run; file-extension cost is out of scope for these benchmarks.

**Flush policy abstraction**: All benchmarks share a small helper that wraps `LogManager::flush_lsn` decisions so variants stay consistent:

```rust
#[derive(Clone)]
enum FlushPolicy {
    None,
    Immediate,
    Group { batch: usize, pending: usize, last_lsn: Option<Lsn> },
}

impl FlushPolicy {
    fn record(&mut self, lsn: Lsn, log: &Arc<Mutex<LogManager>>) {
        match self {
            FlushPolicy::None => {}
            FlushPolicy::Immediate => log.lock().unwrap().flush_lsn(lsn),
            FlushPolicy::Group { batch, pending, last_lsn } => {
                *pending += 1;
                *last_lsn = Some(lsn);
                if *pending == *batch {
                    log.lock().unwrap().flush_lsn(last_lsn.unwrap());
                    *pending = 0;
                    *last_lsn = None;
                }
            }
        }
    }

    fn finish_batch(&mut self, log: &Arc<Mutex<LogManager>>) {
        if let FlushPolicy::Group { pending, last_lsn, .. } = self {
            if *pending > 0 {
                log.lock().unwrap().flush_lsn(last_lsn.unwrap());
                *pending = 0;
                *last_lsn = None;
            }
        }
    }
}
```

Each benchmark initializes the policy variant it wants (none, immediate, group) and calls `record`/`finish_batch` around every WAL append.

## Benchmark Structure

```
benches/
├── buffer_pool.rs     # Existing: buffer pool/caching benchmarks
└── io_patterns.rs     # New: raw I/O performance benchmarks
```

**Single file approach**: All I/O benchmarks in `io_patterns.rs` to keep related tests together

## Implementation Details

### Layer to Benchmark

**Target**: `FileManager` directly (bypass buffer pool)

```rust
// Direct FileManager access
let file_manager = FileManager::new(&dir, block_size, true)?;
let mut page = Page::new(block_size);

// Bypass buffer pool
file_manager.read(&block_id, &mut page);
file_manager.write(&block_id, &mut page);
```

**Why this layer?**
- Measures pure I/O performance
- No cache interference
- Reproducible results
- Clean comparison point for direct I/O implementation

### Configurable Parameters

The codebase is already amenable to different block sizes:
- `SimpleDB::new_for_test(block_size, num_buffers)` accepts any block size
- No hardcoded assumptions (verified in code review)
- Can benchmark 1KB, 4KB, 8KB, 16KB, 64KB without code changes

### Future Compatibility

**Direct I/O testing** (once #12 is implemented):
```rust
// Standard I/O (current)
let fm_standard = FileManager::new(&dir, block_size, true)?;
benchmark("Standard I/O", || { /* ... */ });

// Direct I/O (future)
let fm_direct = DirectIOFileManager::new(&dir, block_size, true)?;
benchmark("Direct I/O", || { /* ... */ });  // Same benchmark, different impl
```

Benchmarks remain unchanged; only the FileManager implementation swaps.

## Success Criteria

- [ ] Sequential vs random I/O benchmarks showing expected ratios (2-3x SSD, 10-100x HDD)
- [ ] Block size sensitivity tests for 1KB, 4KB, 8KB, 16KB, 64KB
- [ ] WAL append benchmarks with different fsync strategies
- [ ] Mixed read/write workload benchmarks (70/30, 50/50, 10/90)
- [ ] Concurrent I/O stress test (4, 8, 16 threads)
- [ ] Baseline measurements documented for future direct I/O comparison
- [ ] Results exportable and reproducible

## Metrics to Collect

### Primary Metrics
- **Throughput**: Operations/sec, MB/sec
- **Latency**: Mean, median, p90, p99 (microseconds)
- **IOPS**: I/O operations per second

### Secondary Metrics
- **CPU utilization**: Time in I/O vs computation
- **Overhead**: Protocol overhead vs useful data transfer
- **Scalability**: Performance vs thread count

## Out of Scope

**File fragmentation testing**: Excluded because:
- Hard to control on modern filesystems (auto-defrag)
- Filesystem-dependent behavior
- Limited pedagogical value
- Can revisit if needed

## Related Issues

- [Issue #15](https://github.com/redixhumayun/simpledb/issues/15) - Original buffer pool + I/O benchmarks issue
- [PR #36](https://github.com/redixhumayun/simpledb/pull/36) - Completed buffer pool benchmarks
- [Issue #12](https://github.com/redixhumayun/simpledb/issues/12) - Direct I/O implementation (prerequisite for comparison benchmarks)
- [Issue #17](https://github.com/redixhumayun/simpledb/issues/17) - LRU buffer replacement (related to buffer pool performance)
- [Issue #37](https://github.com/redixhumayun/simpledb/issues/37) - New issue set up to track IO benchmarks

## References

- Buffer pool benchmarks: `benches/buffer_pool.rs`
- FileManager implementation: `src/main.rs` (~line 10800)
- Existing benchmark framework: `src/benchmark_framework.rs`
