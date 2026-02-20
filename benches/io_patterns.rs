#![allow(clippy::arc_with_non_send_sync)]

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use simpledb::FileSystemInterface;
use simpledb::{
    benchmark_framework::{
        benchmark, benchmark_with_teardown, parse_bench_args, print_header,
        render_throughput_section, should_run, BenchResult, ThroughputRow,
    },
    direct_io_fallback_count,
    test_utils::generate_random_number,
    BlockId, LogManager, Page, SimpleDB, TestDir,
};

type BenchFS = Arc<Mutex<Box<dyn FileSystemInterface + Send + 'static>>>;

type Lsn = usize;

// ============================================================================
// Core Infrastructure: WALFlushPolicy Abstraction
// ============================================================================

#[derive(Clone, Debug)]
enum WALFlushPolicy {
    None,
    Immediate,
    Group {
        batch: usize,
        pending: usize,
        last_lsn: Option<Lsn>,
    },
}

impl WALFlushPolicy {
    fn record(&mut self, lsn: Lsn, log: &Arc<Mutex<LogManager>>) {
        match self {
            WALFlushPolicy::None => {}
            WALFlushPolicy::Immediate => {
                log.lock().unwrap().flush_lsn(lsn);
            }
            WALFlushPolicy::Group {
                batch,
                pending,
                last_lsn,
            } => {
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
        if let WALFlushPolicy::Group {
            pending, last_lsn, ..
        } = self
        {
            if *pending > 0 {
                log.lock().unwrap().flush_lsn(last_lsn.unwrap());
                *pending = 0;
                *last_lsn = None;
            }
        }
    }

    fn label(&self) -> String {
        match self {
            WALFlushPolicy::None => "no-fsync".to_string(),
            WALFlushPolicy::Immediate => "immediate-fsync".to_string(),
            WALFlushPolicy::Group { batch, .. } => format!("group-{}", batch),
        }
    }
}

// ============================================================================
// Core Infrastructure: DataSyncPolicy Abstraction
// ============================================================================
#[derive(Clone, Debug)]
enum DataSyncPolicy {
    None,
    Immediate,
}

impl DataSyncPolicy {
    fn label(&self) -> String {
        match self {
            DataSyncPolicy::None => "data-nosync".to_string(),
            DataSyncPolicy::Immediate => "data-fsync".to_string(),
        }
    }

    fn record(&mut self, file: &str, fm: &BenchFS) {
        match self {
            DataSyncPolicy::None => (),
            DataSyncPolicy::Immediate => {
                let mut fm = fm.lock().unwrap();
                fm.sync(file);
                fm.sync_directory();
            }
        }
    }
}

// ============================================================================
// Setup Helpers
// ============================================================================

fn setup_io_test() -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(12, 5000) // 12 buffers (enough for tests)
}

fn precreate_blocks_direct(db: &SimpleDB, file: &str, count: usize) {
    let mut file_manager = db.file_manager.lock().unwrap();

    for block_num in 0..count {
        let mut page = Page::new();
        write_i32_at(page.bytes_mut(), 60, block_num as i32);
        file_manager.write(&BlockId::new(file.to_string(), block_num), &page);
    }
}

fn write_i32_at(bytes: &mut [u8], offset: usize, value: i32) {
    let le = value.to_le_bytes();
    bytes[offset..offset + 4].copy_from_slice(&le);
}

fn make_wal_record(size: usize) -> Vec<u8> {
    vec![0u8; size]
}

// ============================================================================
// Fast RNG (xorshift64) — used for per-iteration sequence generation.
// Seeded once from /dev/urandom so each iteration sees different access patterns.
// ============================================================================

struct FastRng(u64);

impl FastRng {
    fn new() -> Self {
        Self(generate_random_number() as u64 | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_range(&mut self, n: usize) -> usize {
        (self.next_u64() as usize) % n
    }
}

// ============================================================================
// Cache eviction helper (Linux only)
// ============================================================================

/// Drop a file's pages from the OS page cache via posix_fadvise(POSIX_FADV_DONTNEED).
/// Used in cache-evict benchmark variants to ensure a cold cache before each iteration.
#[cfg(target_os = "linux")]
fn posix_fadvise_dontneed(path: &std::path::Path) {
    use std::os::unix::io::AsRawFd;
    if let Ok(f) = std::fs::OpenOptions::new().read(true).open(path) {
        let ret = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        debug_assert_eq!(
            ret,
            0,
            "posix_fadvise(POSIX_FADV_DONTNEED) failed for {} with errno {}",
            path.display(),
            ret
        );
    }
}

// ============================================================================
// Phase 1: Sequential vs Random I/O Patterns
// ============================================================================
// Filter tokens: seq_read, seq_write, rand_read, rand_write

fn sequential_read(working_set: usize, total_ops: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("seqread_{}", working_set);

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    benchmark(
        &format!("Sequential Read (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for i in 0..total_ops {
                let block_id = BlockId::new(file.clone(), i % working_set);
                fm.read(&block_id, &mut page);
            }
        },
    )
}

fn sequential_write(working_set: usize, total_ops: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("seqwrite_{}", working_set);

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    benchmark(
        &format!("Sequential Write (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for i in 0..total_ops {
                write_i32_at(page.bytes_mut(), 60, i as i32);
                let block_id = BlockId::new(file.clone(), i % working_set);
                fm.write(&block_id, &page);
            }
        },
    )
}

fn random_read(working_set: usize, total_ops: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("randread_{}", working_set);

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    // Seed RNG once; indices are re-generated each iteration for fresh access patterns.
    let mut rng = FastRng::new();

    benchmark(
        &format!("Random Read (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let random_indices: Vec<usize> = (0..total_ops)
                .map(|_| rng.next_range(working_set))
                .collect();
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for &block_idx in &random_indices {
                let block_id = BlockId::new(file.clone(), block_idx);
                fm.read(&block_id, &mut page);
            }
        },
    )
}

fn random_write(working_set: usize, total_ops: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("randwrite_{}", working_set);

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    // Seed RNG once; indices are re-generated each iteration for fresh access patterns.
    let mut rng = FastRng::new();

    benchmark(
        &format!("Random Write (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let random_indices: Vec<usize> = (0..total_ops)
                .map(|_| rng.next_range(working_set))
                .collect();
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for (i, &block_idx) in random_indices.iter().enumerate() {
                write_i32_at(page.bytes_mut(), 60, i as i32);
                let block_id = BlockId::new(file.clone(), block_idx);
                fm.write(&block_id, &page);
            }
        },
    )
}

// ============================================================================
// Phase 2: WAL Performance
// ============================================================================

fn wal_append_no_fsync(iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let log = db.log_manager();
    let total_ops = 1000;
    let mut policy = WALFlushPolicy::None;

    benchmark("WAL append (no fsync)", iterations, 2, || {
        for _ in 0..total_ops {
            let record = make_wal_record(100);
            let lsn = log.lock().unwrap().append(record).unwrap();
            policy.record(lsn, &log);
        }
        policy.finish_batch(&log);
    })
}

fn wal_append_immediate_fsync(iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let log = db.log_manager();
    // Note: Uses 100 ops vs 1000 for other WAL benchmarks due to fsync cost (~2-4ms per op).
    // This keeps benchmark runtime reasonable (~1s vs ~10s) without affecting commits/sec
    // calculations since the ratio remains constant: 100ops/0.5s = 1000ops/5s = 200 commits/sec
    let total_ops = 100;
    let mut policy = WALFlushPolicy::Immediate;

    benchmark("WAL append + immediate fsync", iterations, 2, || {
        for _ in 0..total_ops {
            let record = make_wal_record(100);
            let lsn = log.lock().unwrap().append(record).unwrap();
            policy.record(lsn, &log);
        }
        policy.finish_batch(&log);
    })
}

fn wal_group_commit(batch_size: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let log = db.log_manager();
    let total_ops = 1000;
    let mut policy = WALFlushPolicy::Group {
        batch: batch_size,
        pending: 0,
        last_lsn: None,
    };

    benchmark(
        &format!("WAL group commit (batch={})", batch_size),
        iterations,
        2,
        || {
            for _ in 0..total_ops {
                let record = make_wal_record(100);
                let lsn = log.lock().unwrap().append(record).unwrap();
                policy.record(lsn, &log);
            }
            policy.finish_batch(&log);
        },
    )
}

struct WalResult {
    label: String,
    commits_per_sec: f64,
    mean_duration: Duration,
}

// ============================================================================
// Phase 3: Mixed Read/Write Workloads
// ============================================================================

fn mixed_workload(
    read_pct: usize,
    working_set: usize,
    total_ops: usize,
    flush_policy: WALFlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("mixedfile_{}_{}", working_set, total_ops);

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    let log = db.log_manager();
    let policy_label = flush_policy.label();

    // Seed RNG once; ops and indices re-generated each iteration for fresh access patterns.
    let mut rng = FastRng::new();

    benchmark(
        &format!("Mixed {}/{}R/W {}", read_pct, 100 - read_pct, policy_label),
        iterations,
        2,
        || {
            let ops: Vec<bool> = (0..total_ops)
                .map(|_| rng.next_range(100) < read_pct)
                .collect();
            let block_indices: Vec<usize> = (0..total_ops)
                .map(|_| rng.next_range(working_set))
                .collect();

            let mut page = Page::new();
            let mut policy = flush_policy.clone();

            for (i, &is_read) in ops.iter().enumerate() {
                let block_id = BlockId::new(file.clone(), block_indices[i]);

                if is_read {
                    db.file_manager.lock().unwrap().read(&block_id, &mut page);
                } else {
                    write_i32_at(page.bytes_mut(), 60, i as i32);
                    db.file_manager.lock().unwrap().write(&block_id, &page);
                    let record = make_wal_record(100);
                    let lsn = log.lock().unwrap().append(record).unwrap();
                    policy.record(lsn, &log);
                }
            }
            policy.finish_batch(&log);
        },
    )
}

// ============================================================================
// Phase 4: Concurrent I/O Stress Test
// ============================================================================

fn concurrent_io_shared(
    num_threads: usize,
    ops_per_thread: usize,
    working_set_blocks: usize,
    flush_policy: WALFlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = "concurrent_shared".to_string();
    // Bound file precreation to blocks that can actually be touched in one run.
    // This avoids multi-GiB untimed setup in large regimes when ops/thread is small.
    let max_touchable = num_threads.saturating_mul(ops_per_thread).max(1);
    let total_blocks = working_set_blocks.min(max_touchable).max(1);

    // Pre-size file (critical!)
    precreate_blocks_direct(&db, &file, total_blocks);

    let log = db.log_manager();
    let policy_label = flush_policy.label();

    benchmark(
        &format!("Concurrent shared {}T {}", num_threads, policy_label),
        iterations,
        2,
        || {
            let handles: Vec<_> = (0..num_threads)
                .map(|_tid| {
                    let file = file.clone();
                    let log = Arc::clone(&log);
                    let mut policy = flush_policy.clone();
                    let fm = Arc::clone(&db.file_manager);

                    thread::spawn(move || {
                        let mut page = Page::new();

                        for i in 0..ops_per_thread {
                            let block_num = generate_random_number() % total_blocks;
                            let block_id = BlockId::new(file.clone(), block_num);

                            // 70% read / 30% write
                            if (i % 10) < 7 {
                                fm.lock().unwrap().read(&block_id, &mut page);
                            } else {
                                write_i32_at(page.bytes_mut(), 60, i as i32);
                                fm.lock().unwrap().write(&block_id, &page);
                                let record = make_wal_record(100);
                                let lsn = log.lock().unwrap().append(record).unwrap();
                                policy.record(lsn, &log);
                            }
                        }
                        policy.finish_batch(&log);
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn concurrent_io_sharded(
    num_threads: usize,
    ops_per_thread: usize,
    working_set_blocks: usize,
    flush_policy: WALFlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    // Per shard, cap precreation to the max blocks addressable by this workload.
    // Each thread uses i % blocks_per_file for exactly ops_per_thread operations.
    let target_per_file = (working_set_blocks / num_threads).max(1);
    let blocks_per_file = target_per_file.min(ops_per_thread).max(1);

    // Pre-create separate file for each thread
    for tid in 0..num_threads {
        let file = format!("concurrent_shard_{}", tid);
        precreate_blocks_direct(&db, &file, blocks_per_file);
    }

    let log = db.log_manager();
    let policy_label = flush_policy.label();

    benchmark(
        &format!("Concurrent sharded {}T {}", num_threads, policy_label),
        iterations,
        2,
        || {
            let handles: Vec<_> = (0..num_threads)
                .map(|tid| {
                    let file = format!("concurrent_shard_{}", tid);
                    let log = Arc::clone(&log);
                    let mut policy = flush_policy.clone();
                    let fm = Arc::clone(&db.file_manager);

                    thread::spawn(move || {
                        let mut page = Page::new();

                        for i in 0..ops_per_thread {
                            let block_num = i % blocks_per_file;
                            let block_id = BlockId::new(file.clone(), block_num);

                            // 70% read / 30% write
                            if (i % 10) < 7 {
                                fm.lock().unwrap().read(&block_id, &mut page);
                            } else {
                                write_i32_at(page.bytes_mut(), 60, i as i32);
                                fm.lock().unwrap().write(&block_id, &page);
                                let record = make_wal_record(100);
                                let lsn = log.lock().unwrap().append(record).unwrap();
                                policy.record(lsn, &log);
                            }
                        }
                        policy.finish_batch(&log);
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

// ============================================================================
// Phase 5: Random Write Durability (Data File Sync)
// ============================================================================

fn random_write_durability(
    working_set: usize,
    total_ops: usize,
    wal_policy: WALFlushPolicy,
    data_policy: DataSyncPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("randwrite_durable_{}_{}", working_set, total_ops);

    // Pre-create blocks to avoid extension cost during timed section
    precreate_blocks_direct(&db, &file, working_set);

    let log = db.log_manager();
    let policy_label = wal_policy.label();
    let data_label = data_policy.label();

    // Seed RNG once; indices re-generated each iteration for fresh access patterns.
    let mut rng = FastRng::new();

    benchmark(
        &format!("Random Write durability {} {}", policy_label, data_label),
        iterations,
        2,
        || {
            let block_indices: Vec<usize> = (0..total_ops)
                .map(|_| rng.next_range(working_set))
                .collect();

            let mut page = Page::new();
            let mut wal_policy = wal_policy.clone();
            let mut data_policy = data_policy.clone();
            let fm = Arc::clone(&db.file_manager);

            for (i, &block_num) in block_indices.iter().enumerate() {
                let block_id = BlockId::new(file.clone(), block_num);

                write_i32_at(page.bytes_mut(), 60, i as i32);
                {
                    fm.lock().unwrap().write(&block_id, &page);
                }

                let record = make_wal_record(100);
                let lsn = log.lock().unwrap().append(record).unwrap();
                wal_policy.record(lsn, &log);

                data_policy.record(&file, &fm);
            }

            wal_policy.finish_batch(&log);
        },
    )
}

// ============================================================================
// Phase 7: Cache-Adverse I/O Variants
// ============================================================================
// Filter tokens: onepass_seq, lo_loc_rand, multi_stream

/// One-pass sequential scan: read all `working_set` blocks once per iteration.
/// Every iteration starts fresh from block 0; no temporal reuse.
fn onepass_seq_scan(working_set: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("onepass_seq_{}", working_set);
    precreate_blocks_direct(&db, &file, working_set);

    benchmark(
        &format!("One-pass Seq Scan (K={})", working_set),
        iterations,
        1,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for i in 0..working_set {
                fm.read(&BlockId::new(file.clone(), i), &mut page);
            }
        },
    )
}

/// Low-locality random read: Fisher-Yates shuffle of all blocks, re-randomized each iteration.
/// Each block is touched exactly once per iteration; access order changes every run.
fn low_locality_rand_read(working_set: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let file = format!("lo_loc_rand_{}", working_set);
    precreate_blocks_direct(&db, &file, working_set);

    let mut rng = FastRng::new();

    benchmark(
        &format!("Low-locality Rand Read (K={})", working_set),
        iterations,
        1,
        || {
            // Fisher-Yates shuffle: produces a full permutation of [0, working_set).
            let mut indices: Vec<usize> = (0..working_set).collect();
            for i in (1..working_set).rev() {
                let j = rng.next_range(i + 1);
                indices.swap(i, j);
            }
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for &idx in &indices {
                fm.read(&BlockId::new(file.clone(), idx), &mut page);
            }
        },
    )
}

/// Multi-stream sequential scan: `num_streams` threads each do a full sequential pass over
/// their own file. Aggregate footprint = `working_set` blocks, split across streams.
fn multi_stream_scan(num_streams: usize, working_set: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test();
    let blocks_per_stream = (working_set / num_streams).max(1);

    for s in 0..num_streams {
        let file = format!("multi_stream_{}_{}", num_streams, s);
        precreate_blocks_direct(&db, &file, blocks_per_stream);
    }

    benchmark(
        &format!("Multi-stream Scan {}x{}blk", num_streams, blocks_per_stream),
        iterations,
        1,
        || {
            let handles: Vec<_> = (0..num_streams)
                .map(|s| {
                    let file = format!("multi_stream_{}_{}", num_streams, s);
                    let fm = Arc::clone(&db.file_manager);
                    thread::spawn(move || {
                        let mut page = Page::new();
                        for i in 0..blocks_per_stream {
                            fm.lock()
                                .unwrap()
                                .read(&BlockId::new(file.clone(), i), &mut page);
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        },
    )
}

// ============================================================================
// Phase 8: Cache-Evict Variants (Linux only; posix_fadvise DONTNEED between iters)
// ============================================================================
// Filter tokens: onepass_seq_evict, lo_loc_rand_evict

/// One-pass sequential scan with cache eviction between iterations.
#[cfg(target_os = "linux")]
fn onepass_seq_scan_evict(working_set: usize, iterations: usize) -> BenchResult {
    let (db, test_dir) = setup_io_test();
    let file_name = format!("onepass_seq_evict_{}", working_set);
    let file_path: PathBuf = test_dir.path.join(&file_name);
    precreate_blocks_direct(&db, &file_name, working_set);

    benchmark_with_teardown(
        &format!("One-pass Seq Scan+Evict (K={})", working_set),
        iterations,
        1,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for i in 0..working_set {
                fm.read(&BlockId::new(file_name.clone(), i), &mut page);
            }
        },
        || {
            #[cfg(target_os = "linux")]
            posix_fadvise_dontneed(&file_path);
        },
    )
}

/// Low-locality random read with cache eviction between iterations.
#[cfg(target_os = "linux")]
fn low_locality_rand_read_evict(working_set: usize, iterations: usize) -> BenchResult {
    let (db, test_dir) = setup_io_test();
    let file_name = format!("lo_loc_rand_evict_{}", working_set);
    let file_path: PathBuf = test_dir.path.join(&file_name);
    precreate_blocks_direct(&db, &file_name, working_set);

    let mut rng = FastRng::new();

    benchmark_with_teardown(
        &format!("Low-locality Rand Read+Evict (K={})", working_set),
        iterations,
        1,
        || {
            let mut indices: Vec<usize> = (0..working_set).collect();
            for i in (1..working_set).rev() {
                let j = rng.next_range(i + 1);
                indices.swap(i, j);
            }
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new();
            for &idx in &indices {
                fm.read(&BlockId::new(file_name.clone(), idx), &mut page);
            }
        },
        || {
            #[cfg(target_os = "linux")]
            posix_fadvise_dontneed(&file_path);
        },
    )
}

// ============================================================================
// Rendering Functions
// ============================================================================

fn render_latency_section(title: &str, results: &[BenchResult]) {
    if results.is_empty() {
        return;
    }

    println!("{}", title);
    print_header();
    for result in results {
        println!("{}", result);
    }
    println!();
}

fn render_wal_comparison(results: &[WalResult]) {
    if results.is_empty() {
        return;
    }

    println!("Phase 3: WAL Performance (100-byte records)");
    println!(
        "{:<40} | {:>20} | {:>15}",
        "Flush Strategy", "Commits/sec", "Mean Duration"
    );
    println!("{}", "-".repeat(120));

    for result in results {
        println!(
            "{:<40} | {:>20.2} | {:>15?}",
            result.label, result.commits_per_sec, result.mean_duration
        );
    }
    println!();
}

// ============================================================================
// RAM detection and regime resolution
// ============================================================================

#[cfg(target_os = "linux")]
fn total_ram_bytes() -> u64 {
    let content = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kb * 1024;
        }
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn total_ram_bytes() -> u64 {
    0
}

fn resolve_working_set_blocks(regime: &str, page_bytes: u64) -> usize {
    let target_bytes: u64 = match regime {
        // Fixed capped defaults for practical runtime.
        "hot" => 64 * 1024 * 1024,          // 64 MiB
        "pressure" => 512 * 1024 * 1024,    // 512 MiB
        "thrash" => 2 * 1024 * 1024 * 1024, // 2 GiB
        other => panic!("unknown regime: {}", other),
    };
    let blocks = target_bytes / page_bytes.max(1);
    blocks.max(1) as usize
}

struct IoBenchConfig {
    working_set_blocks: usize,
    regime_label: String,
    phase1_ops: usize,
    mixed_ops: usize,
    durability_ops: usize,
}

/// Parse io_patterns-specific flags from argv (second pass after parse_bench_args).
/// Precedence: --working-set-blocks wins over --regime for working-set sizing.
fn parse_io_args() -> IoBenchConfig {
    let args: Vec<String> = env::args().collect();
    let page_bytes = simpledb::PAGE_SIZE_BYTES as u64;

    let mut explicit_blocks: Option<usize> = None;
    let mut regime: Option<String> = None;
    let mut phase1_ops = 1000usize;
    let mut mixed_ops = 500usize;
    let mut durability_ops = 1000usize;
    let mut phase1_ops_explicit = false;
    let mut mixed_ops_explicit = false;
    let mut durability_ops_explicit = false;

    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        if arg == "--working-set-blocks" {
            if let Some(val) = iter.next() {
                explicit_blocks = val.parse().ok();
            }
        } else if arg == "--regime" {
            if let Some(val) = iter.next() {
                regime = Some(val.clone());
            }
        } else if arg == "--phase1-ops" {
            if let Some(val) = iter.next() {
                phase1_ops = val.parse().unwrap_or(phase1_ops);
                phase1_ops_explicit = true;
            }
        } else if arg == "--mixed-ops" {
            if let Some(val) = iter.next() {
                mixed_ops = val.parse().unwrap_or(mixed_ops);
                mixed_ops_explicit = true;
            }
        } else if arg == "--durability-ops" {
            if let Some(val) = iter.next() {
                durability_ops = val.parse().unwrap_or(durability_ops);
                durability_ops_explicit = true;
            }
        } else if arg == "--json" {
            // Handled by parse_bench_args (first pass).
        } else if arg == "--filter" {
            // Handled by parse_bench_args (first pass).
            let _ = iter.next();
        } else if arg.starts_with("--") {
            eprintln!("warning: unknown flag: {}", arg);
            if iter
                .clone()
                .next()
                .is_some_and(|next| !next.starts_with("--"))
            {
                let _ = iter.next();
            }
        }
    }

    // Regime-derived sizing: when --regime is set (not --working-set-blocks) and ops are
    // not explicitly overridden, scale ops to working_set_blocks so hot/pressure/thrash
    // materially changes reuse distance rather than just the modulo range.
    let regime_was_set = regime.is_some() && explicit_blocks.is_none();

    let (working_set_blocks, regime_label) = if let Some(n) = explicit_blocks {
        (n, format!("custom({})", n))
    } else if let Some(r) = regime {
        let blocks = resolve_working_set_blocks(&r, page_bytes);
        (blocks, r)
    } else {
        (1000, "default".to_string())
    };

    if regime_was_set && !phase1_ops_explicit {
        phase1_ops = working_set_blocks;
    }
    if regime_was_set && !mixed_ops_explicit {
        mixed_ops = (working_set_blocks / 2).max(1);
    }
    if regime_was_set && !durability_ops_explicit {
        durability_ops = working_set_blocks;
    }

    IoBenchConfig {
        working_set_blocks,
        regime_label,
        phase1_ops,
        mixed_ops,
        durability_ops,
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let (iterations, _num_buffers, json_output, filter) = parse_bench_args();
    let io_cfg = parse_io_args();
    let filter_ref = filter.as_deref();
    let block_size = simpledb::PAGE_SIZE_BYTES as usize;

    // Cap iterations for fsync-heavy phases (3-5) to avoid excessive runtime.
    // Phases 3-5 involve real fsync operations which have constant ~2-4ms cost.
    // At 100 iterations, these phases would perform 50,000+ fsyncs (~10 minutes).
    // 5 iterations provides statistical validity without excessive runtime.
    let fsync_iterations = iterations.min(5);

    if json_output {
        // Phase 1 - no filters in JSON mode
        let mut results = vec![
            sequential_read(io_cfg.working_set_blocks, io_cfg.phase1_ops, iterations),
            sequential_write(io_cfg.working_set_blocks, io_cfg.phase1_ops, iterations),
            random_read(io_cfg.working_set_blocks, io_cfg.phase1_ops, iterations),
            random_write(io_cfg.working_set_blocks, io_cfg.phase1_ops, iterations),
        ];

        // Phase 3 - no filters in JSON mode (use fsync_iterations)
        results.push(wal_append_no_fsync(fsync_iterations));
        results.push(wal_append_immediate_fsync(fsync_iterations));
        results.push(wal_group_commit(10, fsync_iterations));
        results.push(wal_group_commit(50, fsync_iterations));
        results.push(wal_group_commit(100, fsync_iterations));

        // Phase 4 - no filters in JSON mode (use fsync_iterations)
        for read_pct in [70, 50, 10] {
            for policy in [
                WALFlushPolicy::None,
                WALFlushPolicy::Immediate,
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
            ] {
                results.push(mixed_workload(
                    read_pct,
                    io_cfg.working_set_blocks,
                    io_cfg.mixed_ops,
                    policy,
                    fsync_iterations,
                ));
            }
        }

        // Phase 5 - no filters in JSON mode (use fsync_iterations)
        for threads in [2, 4, 8, 16] {
            for policy in [
                WALFlushPolicy::None,
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
            ] {
                results.push(concurrent_io_shared(
                    threads,
                    100,
                    io_cfg.working_set_blocks,
                    policy.clone(),
                    fsync_iterations,
                ));
                results.push(concurrent_io_sharded(
                    threads,
                    100,
                    io_cfg.working_set_blocks,
                    policy,
                    fsync_iterations,
                ));
            }
        }

        // Phase 7: Cache-adverse variants
        results.push(onepass_seq_scan(io_cfg.working_set_blocks, iterations));
        results.push(low_locality_rand_read(
            io_cfg.working_set_blocks,
            iterations,
        ));
        results.push(multi_stream_scan(4, io_cfg.working_set_blocks, iterations));

        // Phase 8: Cache-evict variants (Linux only — posix_fadvise DONTNEED unavailable elsewhere)
        #[cfg(target_os = "linux")]
        results.push(onepass_seq_scan_evict(
            io_cfg.working_set_blocks,
            iterations,
        ));
        #[cfg(target_os = "linux")]
        results.push(low_locality_rand_read_evict(
            io_cfg.working_set_blocks,
            iterations,
        ));

        // Phase 6 - random write durability (use fsync_iterations, working_set_blocks ops)
        results.push(random_write_durability(
            io_cfg.working_set_blocks,
            io_cfg.durability_ops,
            WALFlushPolicy::Immediate,
            DataSyncPolicy::None,
            fsync_iterations,
        ));
        results.push(random_write_durability(
            io_cfg.working_set_blocks,
            io_cfg.durability_ops,
            WALFlushPolicy::Immediate,
            DataSyncPolicy::Immediate,
            fsync_iterations,
        ));

        let json_results: Vec<String> = results.iter().map(|r| r.to_json()).collect();
        println!("[{}]", json_results.join(","));
        return;
    }

    // Human-readable mode
    let ram_bytes = total_ram_bytes();
    let working_set_bytes = io_cfg.working_set_blocks * block_size;
    println!("SimpleDB I/O Performance Benchmark Suite");
    println!("=========================================");
    println!(
        "Running benchmarks with {} iterations per operation",
        iterations
    );
    if fsync_iterations < iterations {
        println!(
            "Note: Phases 3-5 (fsync-heavy) capped at {} iterations to avoid excessive runtime",
            fsync_iterations
        );
    }
    println!("Block size:          {} bytes", block_size);
    println!(
        "Total RAM:           {} bytes ({:.1} GiB)",
        ram_bytes,
        ram_bytes as f64 / (1u64 << 30) as f64
    );
    println!("Regime:              {}", io_cfg.regime_label);
    println!(
        "Working set:         {} blocks = {} bytes ({:.1} MiB)",
        io_cfg.working_set_blocks,
        working_set_bytes,
        working_set_bytes as f64 / (1u64 << 20) as f64
    );
    println!("Phase 1 ops:         {}", io_cfg.phase1_ops);
    println!("Mixed ops:           {}", io_cfg.mixed_ops);
    println!("Durability ops:      {}", io_cfg.durability_ops);
    println!(
        "Direct I/O:          {}",
        if cfg!(feature = "direct-io") {
            "enabled"
        } else {
            "disabled (buffered)"
        }
    );
    println!(
        "Environment:         {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!();

    // Phase 1: Sequential vs Random I/O
    let mut phase1_results = Vec::new();

    if should_run("seq_read", filter_ref) {
        phase1_results.push(sequential_read(
            io_cfg.working_set_blocks,
            io_cfg.phase1_ops,
            iterations,
        ));
    }
    if should_run("seq_write", filter_ref) {
        phase1_results.push(sequential_write(
            io_cfg.working_set_blocks,
            io_cfg.phase1_ops,
            iterations,
        ));
    }
    if should_run("rand_read", filter_ref) {
        phase1_results.push(random_read(
            io_cfg.working_set_blocks,
            io_cfg.phase1_ops,
            iterations,
        ));
    }
    if should_run("rand_write", filter_ref) {
        phase1_results.push(random_write(
            io_cfg.working_set_blocks,
            io_cfg.phase1_ops,
            iterations,
        ));
    }

    render_latency_section(
        "Phase 1: Sequential vs Random I/O Patterns",
        &phase1_results,
    );

    // Calculate and display throughput for Phase 1
    if !phase1_results.is_empty() {
        let mut throughput_rows = Vec::new();
        for result in &phase1_results {
            let ops = io_cfg.phase1_ops;
            let throughput_mb = (ops * block_size) as f64 / result.mean.as_secs_f64() / 1_000_000.0;
            let iops = ops as f64 / result.mean.as_secs_f64();
            throughput_rows.push(ThroughputRow {
                label: format!("{} (MB/s)", result.operation),
                throughput: throughput_mb,
                unit: "MB/s".to_string(),
                mean_duration: result.mean,
            });
            throughput_rows.push(ThroughputRow {
                label: format!("{} (IOPS)", result.operation),
                throughput: iops,
                unit: "ops/s".to_string(),
                mean_duration: result.mean,
            });
        }
        render_throughput_section("Phase 1: Throughput Metrics", &throughput_rows);
    }

    // Phase 3: WAL Performance
    // Filter tokens: wal_no_fsync, wal_immediate, wal_group_10, wal_group_50, wal_group_100
    let mut wal_results = Vec::new();

    if should_run("wal_no_fsync", filter_ref) {
        let no_fsync = wal_append_no_fsync(fsync_iterations);
        wal_results.push(WalResult {
            label: "No fsync (1000 ops)".to_string(),
            commits_per_sec: 1000.0 / no_fsync.mean.as_secs_f64(),
            mean_duration: no_fsync.mean,
        });
    }

    if should_run("wal_immediate", filter_ref) {
        let immediate = wal_append_immediate_fsync(fsync_iterations);
        wal_results.push(WalResult {
            label: "Immediate fsync (100 ops)".to_string(),
            commits_per_sec: 100.0 / immediate.mean.as_secs_f64(),
            mean_duration: immediate.mean,
        });
    }

    for batch_size in [10, 50, 100] {
        let token = format!("wal_group_{}", batch_size);
        if should_run(&token, filter_ref) {
            let group = wal_group_commit(batch_size, fsync_iterations);
            wal_results.push(WalResult {
                label: format!("Group commit batch={} (1000 ops)", batch_size),
                commits_per_sec: 1000.0 / group.mean.as_secs_f64(),
                mean_duration: group.mean,
            });
        }
    }

    if !wal_results.is_empty() {
        render_wal_comparison(&wal_results);
    }

    // Phase 4: Mixed Read/Write Workloads
    // Filter tokens: mixed_70r_no_fsync, mixed_70r_immediate, mixed_70r_group_10,
    //                mixed_50r_no_fsync, mixed_50r_immediate, mixed_50r_group_10,
    //                mixed_10r_no_fsync, mixed_10r_immediate, mixed_10r_group_10
    let mut mixed_results = Vec::new();

    for read_pct in [70, 50, 10] {
        for (policy, policy_name) in [
            (WALFlushPolicy::None, "no_fsync"),
            (WALFlushPolicy::Immediate, "immediate"),
            (
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
                "group_10",
            ),
        ] {
            let token = format!("mixed_{}r_{}", read_pct, policy_name);
            if should_run(&token, filter_ref) {
                mixed_results.push(mixed_workload(
                    read_pct,
                    io_cfg.working_set_blocks,
                    io_cfg.mixed_ops,
                    policy,
                    fsync_iterations,
                ));
            }
        }
    }

    if !mixed_results.is_empty() {
        render_latency_section(
            &format!(
                "Phase 4: Mixed Read/Write Workloads ({} ops)",
                io_cfg.mixed_ops
            ),
            &mixed_results,
        );

        // Throughput
        let mut throughput_rows = Vec::new();
        for result in &mixed_results {
            let ops_per_sec = io_cfg.mixed_ops as f64 / result.mean.as_secs_f64();
            throughput_rows.push(ThroughputRow {
                label: result.operation.clone(),
                throughput: ops_per_sec,
                unit: "ops/s".to_string(),
                mean_duration: result.mean,
            });
        }
        render_throughput_section("Phase 4: Throughput", &throughput_rows);
    }

    // Phase 5: Concurrent I/O
    // Filter tokens: concurrent_2t_shared_no_fsync, concurrent_2t_shared_group_10,
    //                concurrent_2t_sharded_no_fsync, concurrent_2t_sharded_group_10,
    //                (and similarly for 4t, 8t, 16t)
    let mut concurrent_results = Vec::new();

    for threads in [2, 4, 8, 16] {
        for (policy, policy_name) in [
            (WALFlushPolicy::None, "no_fsync"),
            (
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
                "group_10",
            ),
        ] {
            let shared_token = format!("concurrent_{}t_shared_{}", threads, policy_name);
            if should_run(&shared_token, filter_ref) {
                concurrent_results.push(concurrent_io_shared(
                    threads,
                    100,
                    io_cfg.working_set_blocks,
                    policy.clone(),
                    fsync_iterations,
                ));
            }

            let sharded_token = format!("concurrent_{}t_sharded_{}", threads, policy_name);
            if should_run(&sharded_token, filter_ref) {
                concurrent_results.push(concurrent_io_sharded(
                    threads,
                    100,
                    io_cfg.working_set_blocks,
                    policy.clone(),
                    fsync_iterations,
                ));
            }
        }
    }

    if !concurrent_results.is_empty() {
        render_latency_section(
            "Phase 5: Concurrent I/O Stress Test (100 ops/thread)",
            &concurrent_results,
        );

        // Aggregate throughput
        let mut throughput_rows = Vec::new();
        for result in &concurrent_results {
            // Extract thread count from operation name
            let threads: usize = if result.operation.contains("2T") {
                2
            } else if result.operation.contains("4T") {
                4
            } else if result.operation.contains("8T") {
                8
            } else if result.operation.contains("16T") {
                16
            } else {
                1
            };

            let total_ops = threads * 100;
            let ops_per_sec = total_ops as f64 / result.mean.as_secs_f64();
            throughput_rows.push(ThroughputRow {
                label: result.operation.clone(),
                throughput: ops_per_sec,
                unit: "ops/s".to_string(),
                mean_duration: result.mean,
            });
        }
        render_throughput_section("Phase 5: Aggregate Throughput", &throughput_rows);
    }

    // Phase 6: Random Write Durability (WAL + data sync combinations)
    // Filter tokens: durability_wal_immediate_data_nosync, durability_wal_immediate_data_fsync
    let mut durability_results = Vec::new();
    let durability_ops = io_cfg.durability_ops;

    for (wal_policy, wal_name) in [(WALFlushPolicy::Immediate, "wal_immediate")] {
        for (data_policy, data_name) in [
            (DataSyncPolicy::None, "data_nosync"),
            (DataSyncPolicy::Immediate, "data_fsync"),
        ] {
            let token = format!("durability_{}_{}", wal_name, data_name);
            if should_run(&token, filter_ref) {
                durability_results.push(random_write_durability(
                    io_cfg.working_set_blocks,
                    durability_ops,
                    wal_policy.clone(),
                    data_policy.clone(),
                    fsync_iterations,
                ));
            }
        }
    }

    if !durability_results.is_empty() {
        render_latency_section(
            &format!("Phase 6: Random Write Durability ({} ops)", durability_ops),
            &durability_results,
        );

        let mut throughput_rows = Vec::new();
        for result in &durability_results {
            let ops_per_sec = durability_ops as f64 / result.mean.as_secs_f64();
            throughput_rows.push(ThroughputRow {
                label: result.operation.clone(),
                throughput: ops_per_sec,
                unit: "ops/s".to_string(),
                mean_duration: result.mean,
            });
        }
        render_throughput_section(
            "Phase 6: Random Write Durability Throughput",
            &throughput_rows,
        );
    }

    // Phase 7: Cache-Adverse I/O
    // Filter tokens: onepass_seq, lo_loc_rand, multi_stream
    let mut cache_adverse_results = Vec::new();

    if should_run("onepass_seq", filter_ref) {
        cache_adverse_results.push(onepass_seq_scan(io_cfg.working_set_blocks, iterations));
    }
    if should_run("lo_loc_rand", filter_ref) {
        cache_adverse_results.push(low_locality_rand_read(
            io_cfg.working_set_blocks,
            iterations,
        ));
    }
    if should_run("multi_stream", filter_ref) {
        cache_adverse_results.push(multi_stream_scan(4, io_cfg.working_set_blocks, iterations));
    }

    if !cache_adverse_results.is_empty() {
        render_latency_section(
            "Phase 7: Cache-Adverse I/O (one-pass, low-locality, multi-stream)",
            &cache_adverse_results,
        );
        let mut throughput_rows = Vec::new();
        for result in &cache_adverse_results {
            let throughput_mb = (io_cfg.working_set_blocks * block_size) as f64
                / result.mean.as_secs_f64()
                / 1_000_000.0;
            throughput_rows.push(ThroughputRow {
                label: result.operation.clone(),
                throughput: throughput_mb,
                unit: "MB/s".to_string(),
                mean_duration: result.mean,
            });
        }
        render_throughput_section("Phase 7: Cache-Adverse Throughput", &throughput_rows);
    }

    // Phase 8: Cache-Evict Variants (Linux only — posix_fadvise DONTNEED unavailable elsewhere)
    // Filter tokens: onepass_seq_evict, lo_loc_rand_evict
    #[cfg(target_os = "linux")]
    {
        let mut evict_results = Vec::new();

        if should_run("onepass_seq_evict", filter_ref) {
            evict_results.push(onepass_seq_scan_evict(
                io_cfg.working_set_blocks,
                iterations,
            ));
        }
        if should_run("lo_loc_rand_evict", filter_ref) {
            evict_results.push(low_locality_rand_read_evict(
                io_cfg.working_set_blocks,
                iterations,
            ));
        }

        if !evict_results.is_empty() {
            render_latency_section(
                "Phase 8: Cache-Evict Variants (posix_fadvise DONTNEED between iterations)",
                &evict_results,
            );
            let mut throughput_rows = Vec::new();
            for result in &evict_results {
                let throughput_mb = (io_cfg.working_set_blocks * block_size) as f64
                    / result.mean.as_secs_f64()
                    / 1_000_000.0;
                throughput_rows.push(ThroughputRow {
                    label: result.operation.clone(),
                    throughput: throughput_mb,
                    unit: "MB/s".to_string(),
                    mean_duration: result.mean,
                });
            }
            render_throughput_section("Phase 8: Cache-Evict Throughput", &throughput_rows);
        }
    }

    let fallbacks = direct_io_fallback_count();
    if cfg!(feature = "direct-io") {
        println!(
            "Direct I/O fallbacks: {} ({})",
            fallbacks,
            if fallbacks == 0 {
                "O_DIRECT engaged on all data files"
            } else {
                "some files fell back to buffered mode — check stderr for details"
            }
        );
    }
    println!("All benchmarks completed!");
}
