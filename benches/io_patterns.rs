#![allow(clippy::arc_with_non_send_sync)]

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use simpledb::{
    benchmark_framework::{
        benchmark, parse_bench_args, print_header, render_throughput_section, should_run,
        BenchResult, ThroughputRow,
    },
    test_utils::generate_random_number,
    BlockId, LogManager, Page, SimpleDB, TestDir,
};

type Lsn = usize;

// ============================================================================
// Core Infrastructure: FlushPolicy Abstraction
// ============================================================================

#[derive(Clone, Debug)]
enum FlushPolicy {
    None,
    Immediate,
    Group {
        batch: usize,
        pending: usize,
        last_lsn: Option<Lsn>,
    },
}

impl FlushPolicy {
    fn record(&mut self, lsn: Lsn, log: &Arc<Mutex<LogManager>>) {
        match self {
            FlushPolicy::None => {}
            FlushPolicy::Immediate => {
                log.lock().unwrap().flush_lsn(lsn);
            }
            FlushPolicy::Group {
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
        if let FlushPolicy::Group {
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
            FlushPolicy::None => "no-fsync".to_string(),
            FlushPolicy::Immediate => "immediate-fsync".to_string(),
            FlushPolicy::Group { batch, .. } => format!("group-{}", batch),
        }
    }
}

// ============================================================================
// Setup Helpers
// ============================================================================

fn setup_io_test(block_size: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(block_size, 12) // 12 buffers (enough for tests)
}

fn precreate_blocks_direct(db: &SimpleDB, file: &str, count: usize) {
    let block_size = db.file_manager.lock().unwrap().block_size();
    let mut file_manager = db.file_manager.lock().unwrap();

    for block_num in 0..count {
        let mut page = Page::new(block_size);
        page.set_int(0, block_num as i32);
        file_manager.write(&BlockId::new(file.to_string(), block_num), &mut page);
    }
}

fn make_wal_record(size: usize) -> Vec<u8> {
    vec![0u8; size]
}

// ============================================================================
// Phase 1: Sequential vs Random I/O Patterns
// ============================================================================
// Filter tokens: seq_read, seq_write, rand_read, rand_write

fn sequential_read(block_size: usize, num_blocks: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "seqread".to_string();

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, num_blocks);

    benchmark(
        &format!("Sequential Read ({} blocks)", num_blocks),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new(block_size);
            for i in 0..num_blocks {
                let block_id = BlockId::new(file.clone(), i);
                fm.read(&block_id, &mut page);
            }
        },
    )
}

fn sequential_write(block_size: usize, num_blocks: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "seqwrite".to_string();

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, num_blocks);

    benchmark(
        &format!("Sequential Write ({} blocks)", num_blocks),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new(block_size);
            for i in 0..num_blocks {
                page.set_int(0, i as i32);
                let block_id = BlockId::new(file.clone(), i);
                fm.write(&block_id, &mut page);
            }
        },
    )
}

fn random_read(
    block_size: usize,
    working_set: usize,
    total_ops: usize,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "randread".to_string();

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    // Pre-generate random sequence
    let random_indices: Vec<usize> = (0..total_ops)
        .map(|_| generate_random_number() % working_set)
        .collect();

    benchmark(
        &format!("Random Read (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new(block_size);
            for &block_idx in &random_indices {
                let block_id = BlockId::new(file.clone(), block_idx);
                fm.read(&block_id, &mut page);
            }
        },
    )
}

fn random_write(
    block_size: usize,
    working_set: usize,
    total_ops: usize,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "randwrite".to_string();

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    // Pre-generate random sequence
    let random_indices: Vec<usize> = (0..total_ops)
        .map(|_| generate_random_number() % working_set)
        .collect();

    benchmark(
        &format!("Random Write (K={}, {} ops)", working_set, total_ops),
        iterations,
        2,
        || {
            let mut fm = db.file_manager.lock().unwrap();
            let mut page = Page::new(block_size);
            for (i, &block_idx) in random_indices.iter().enumerate() {
                page.set_int(0, i as i32);
                let block_id = BlockId::new(file.clone(), block_idx);
                fm.write(&block_id, &mut page);
            }
        },
    )
}

// ============================================================================
// Phase 2: Block Size Sensitivity
// ============================================================================

struct BlockSizeResult {
    block_size: usize,
    throughput_mb: f64,
    mean_duration: Duration,
}

fn block_size_scaling(iterations: usize) -> Vec<BlockSizeResult> {
    let block_sizes = vec![1024, 4096, 8192, 16384, 65536];
    let num_blocks = 1000;
    let mut results = Vec::new();

    for &block_size in &block_sizes {
        let result = sequential_read(block_size, num_blocks, iterations);
        let throughput_mb =
            (num_blocks * block_size) as f64 / result.mean.as_secs_f64() / 1_000_000.0;
        results.push(BlockSizeResult {
            block_size,
            throughput_mb,
            mean_duration: result.mean,
        });
    }

    results
}

// ============================================================================
// Phase 3: WAL Performance
// ============================================================================

fn wal_append_no_fsync(block_size: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let log = db.log_manager();
    let total_ops = 1000;
    let mut policy = FlushPolicy::None;

    benchmark("WAL append (no fsync)", iterations, 2, || {
        for _ in 0..total_ops {
            let record = make_wal_record(100);
            let lsn = log.lock().unwrap().append(record);
            policy.record(lsn, &log);
        }
        policy.finish_batch(&log);
    })
}

fn wal_append_immediate_fsync(block_size: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let log = db.log_manager();
    // Note: Uses 100 ops vs 1000 for other WAL benchmarks due to fsync cost (~2-4ms per op).
    // This keeps benchmark runtime reasonable (~1s vs ~10s) without affecting commits/sec
    // calculations since the ratio remains constant: 100ops/0.5s = 1000ops/5s = 200 commits/sec
    let total_ops = 100;
    let mut policy = FlushPolicy::Immediate;

    benchmark("WAL append + immediate fsync", iterations, 2, || {
        for _ in 0..total_ops {
            let record = make_wal_record(100);
            let lsn = log.lock().unwrap().append(record);
            policy.record(lsn, &log);
        }
        policy.finish_batch(&log);
    })
}

fn wal_group_commit(block_size: usize, batch_size: usize, iterations: usize) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let log = db.log_manager();
    let total_ops = 1000;
    let mut policy = FlushPolicy::Group {
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
                let lsn = log.lock().unwrap().append(record);
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
// Phase 4: Mixed Read/Write Workloads
// ============================================================================

fn mixed_workload(
    block_size: usize,
    read_pct: usize,
    total_ops: usize,
    flush_policy: FlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "mixedfile".to_string();
    let working_set = 100;

    // Pre-create blocks
    precreate_blocks_direct(&db, &file, working_set);

    // Pre-generate operation sequence
    let ops: Vec<bool> = (0..total_ops)
        .map(|_| (generate_random_number() % 100) < read_pct)
        .collect();

    let block_indices: Vec<usize> = (0..total_ops)
        .map(|_| generate_random_number() % working_set)
        .collect();

    let log = db.log_manager();
    let policy_label = flush_policy.label();

    benchmark(
        &format!("Mixed {}/{}R/W {}", read_pct, 100 - read_pct, policy_label),
        iterations,
        2,
        || {
            let mut page = Page::new(block_size);
            let mut policy = flush_policy.clone();

            for (i, &is_read) in ops.iter().enumerate() {
                let block_id = BlockId::new(file.clone(), block_indices[i]);

                if is_read {
                    db.file_manager.lock().unwrap().read(&block_id, &mut page);
                } else {
                    page.set_int(0, i as i32);
                    db.file_manager.lock().unwrap().write(&block_id, &mut page);
                    let record = make_wal_record(100);
                    let lsn = log.lock().unwrap().append(record);
                    policy.record(lsn, &log);
                }
            }
            policy.finish_batch(&log);
        },
    )
}

// ============================================================================
// Phase 5: Concurrent I/O Stress Test
// ============================================================================

fn concurrent_io_shared(
    block_size: usize,
    num_threads: usize,
    ops_per_thread: usize,
    flush_policy: FlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let file = "concurrent_shared".to_string();
    let total_blocks = num_threads * 100;

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
                        let mut page = Page::new(block_size);

                        for i in 0..ops_per_thread {
                            let block_num = generate_random_number() % total_blocks;
                            let block_id = BlockId::new(file.clone(), block_num);

                            // 70% read / 30% write
                            if (i % 10) < 7 {
                                fm.lock().unwrap().read(&block_id, &mut page);
                            } else {
                                page.set_int(0, i as i32);
                                fm.lock().unwrap().write(&block_id, &mut page);
                                let record = make_wal_record(100);
                                let lsn = log.lock().unwrap().append(record);
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
    block_size: usize,
    num_threads: usize,
    ops_per_thread: usize,
    flush_policy: FlushPolicy,
    iterations: usize,
) -> BenchResult {
    let (db, _test_dir) = setup_io_test(block_size);
    let blocks_per_file = 100;

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
                        let mut page = Page::new(block_size);

                        for i in 0..ops_per_thread {
                            let block_num = i % blocks_per_file;
                            let block_id = BlockId::new(file.clone(), block_num);

                            // 70% read / 30% write
                            if (i % 10) < 7 {
                                fm.lock().unwrap().read(&block_id, &mut page);
                            } else {
                                page.set_int(0, i as i32);
                                fm.lock().unwrap().write(&block_id, &mut page);
                                let record = make_wal_record(100);
                                let lsn = log.lock().unwrap().append(record);
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

fn render_block_size_table(results: &[BlockSizeResult]) {
    if results.is_empty() {
        return;
    }

    println!("Phase 2: Block Size Sensitivity");
    println!("Fixed workload: 1000 sequential reads");
    println!(
        "{:<15} | {:>20} | {:>15}",
        "Block Size", "Throughput (MB/s)", "Mean Duration"
    );
    println!("{}", "-".repeat(100));

    for result in results {
        println!(
            "{:<15} | {:>20.2} | {:>15.2?}",
            format!("{} bytes", result.block_size),
            result.throughput_mb,
            result.mean_duration
        );
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
    println!("{}", "-".repeat(100));

    for result in results {
        println!(
            "{:<40} | {:>20.2} | {:>15?}",
            result.label, result.commits_per_sec, result.mean_duration
        );
    }
    println!();
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let (iterations, _num_buffers, json_output, filter) = parse_bench_args();
    let filter_ref = filter.as_deref();
    let block_size = 4096;

    // Cap iterations for fsync-heavy phases (3-5) to avoid excessive runtime.
    // Phases 3-5 involve real fsync operations which have constant ~2-4ms cost.
    // At 100 iterations, these phases would perform 50,000+ fsyncs (~10 minutes).
    // 5 iterations provides statistical validity without excessive runtime.
    let fsync_iterations = iterations.min(5);

    if json_output {
        // Phase 1 - no filters in JSON mode
        let mut results = vec![
            sequential_read(block_size, 1000, iterations),
            sequential_write(block_size, 1000, iterations),
            random_read(block_size, 1000, 1000, iterations),
            random_write(block_size, 1000, 1000, iterations),
        ];

        // Phase 3 - no filters in JSON mode (use fsync_iterations)
        results.push(wal_append_no_fsync(block_size, fsync_iterations));
        results.push(wal_append_immediate_fsync(block_size, fsync_iterations));
        results.push(wal_group_commit(block_size, 10, fsync_iterations));
        results.push(wal_group_commit(block_size, 50, fsync_iterations));
        results.push(wal_group_commit(block_size, 100, fsync_iterations));

        // Phase 4 - no filters in JSON mode (use fsync_iterations)
        for read_pct in [70, 50, 10] {
            for policy in [
                FlushPolicy::None,
                FlushPolicy::Immediate,
                FlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
            ] {
                results.push(mixed_workload(
                    block_size,
                    read_pct,
                    500,
                    policy,
                    fsync_iterations,
                ));
            }
        }

        // Phase 5 - no filters in JSON mode (use fsync_iterations)
        for threads in [2, 4, 8, 16] {
            for policy in [
                FlushPolicy::None,
                FlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
            ] {
                results.push(concurrent_io_shared(
                    block_size,
                    threads,
                    100,
                    policy.clone(),
                    fsync_iterations,
                ));
                results.push(concurrent_io_sharded(
                    block_size,
                    threads,
                    100,
                    policy,
                    fsync_iterations,
                ));
            }
        }

        let json_results: Vec<String> = results.iter().map(|r| r.to_json()).collect();
        println!("[{}]", json_results.join(","));
        return;
    }

    // Cap iterations for fsync-heavy phases (3-5) to avoid excessive runtime.
    // Phases 3-5 involve real fsync operations which have constant ~2-4ms cost.
    // At 100 iterations, these phases would perform 50,000+ fsyncs (~10 minutes).
    // 5 iterations provides statistical validity without excessive runtime.
    let fsync_iterations = iterations.min(5);

    // Human-readable mode
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
    println!("Block size: {} bytes", block_size);
    println!(
        "Environment: {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!();

    // Phase 1: Sequential vs Random I/O
    let mut phase1_results = Vec::new();

    if should_run("seq_read", filter_ref) {
        phase1_results.push(sequential_read(block_size, 1000, iterations));
    }
    if should_run("seq_write", filter_ref) {
        phase1_results.push(sequential_write(block_size, 1000, iterations));
    }
    if should_run("rand_read", filter_ref) {
        phase1_results.push(random_read(block_size, 1000, 1000, iterations));
    }
    if should_run("rand_write", filter_ref) {
        phase1_results.push(random_write(block_size, 1000, 1000, iterations));
    }

    render_latency_section(
        "Phase 1: Sequential vs Random I/O Patterns",
        &phase1_results,
    );

    // Calculate and display throughput for Phase 1
    if !phase1_results.is_empty() {
        let mut throughput_rows = Vec::new();
        for result in &phase1_results {
            let ops = 1000; // All Phase 1 benchmarks use 1000 operations
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

    // Phase 2: Block Size Sensitivity
    // Filter token: block_size
    if should_run("block_size", filter_ref) {
        let block_size_results = block_size_scaling(iterations);
        render_block_size_table(&block_size_results);
    }

    // Phase 3: WAL Performance
    // Filter tokens: wal_no_fsync, wal_immediate, wal_group_10, wal_group_50, wal_group_100
    let mut wal_results = Vec::new();

    if should_run("wal_no_fsync", filter_ref) {
        let no_fsync = wal_append_no_fsync(block_size, fsync_iterations);
        wal_results.push(WalResult {
            label: "No fsync (1000 ops)".to_string(),
            commits_per_sec: 1000.0 / no_fsync.mean.as_secs_f64(),
            mean_duration: no_fsync.mean,
        });
    }

    if should_run("wal_immediate", filter_ref) {
        let immediate = wal_append_immediate_fsync(block_size, fsync_iterations);
        wal_results.push(WalResult {
            label: "Immediate fsync (100 ops)".to_string(),
            commits_per_sec: 100.0 / immediate.mean.as_secs_f64(),
            mean_duration: immediate.mean,
        });
    }

    for batch_size in [10, 50, 100] {
        let token = format!("wal_group_{}", batch_size);
        if should_run(&token, filter_ref) {
            let group = wal_group_commit(block_size, batch_size, fsync_iterations);
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
            (FlushPolicy::None, "no_fsync"),
            (FlushPolicy::Immediate, "immediate"),
            (
                FlushPolicy::Group {
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
                    block_size,
                    read_pct,
                    500,
                    policy,
                    fsync_iterations,
                ));
            }
        }
    }

    if !mixed_results.is_empty() {
        render_latency_section(
            "Phase 4: Mixed Read/Write Workloads (500 ops)",
            &mixed_results,
        );

        // Throughput
        let mut throughput_rows = Vec::new();
        for result in &mixed_results {
            let ops_per_sec = 500.0 / result.mean.as_secs_f64();
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
            (FlushPolicy::None, "no_fsync"),
            (
                FlushPolicy::Group {
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
                    block_size,
                    threads,
                    100,
                    policy.clone(),
                    fsync_iterations,
                ));
            }

            let sharded_token = format!("concurrent_{}t_sharded_{}", threads, policy_name);
            if should_run(&sharded_token, filter_ref) {
                concurrent_results.push(concurrent_io_sharded(
                    block_size,
                    threads,
                    100,
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

    println!("All benchmarks completed!");
}
