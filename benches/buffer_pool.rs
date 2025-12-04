#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use simpledb::{
    benchmark_framework::{
        benchmark, parse_bench_args, print_header, render_throughput_section, should_run,
        BenchResult, ThroughputRow,
    },
    test_utils::generate_random_number,
    BlockId, Lsn, Page, SimpleDB, TestDir,
};

fn setup_buffer_pool(num_buffers: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(num_buffers, 5000)
}

fn setup_buffer_pool_with_stats(num_buffers: usize) -> (SimpleDB, TestDir) {
    let (db, test_dir) = SimpleDB::new_for_test(num_buffers, 5000);
    db.buffer_manager().enable_stats();
    (db, test_dir)
}

fn precreate_blocks(db: &SimpleDB, file: &str, count: usize) {
    let mut file_manager = db.file_manager.lock().unwrap();
    let file = file.to_string();

    for block_num in 0..count {
        let mut page = Page::new();
        page.set_int(0, block_num as i32);
        file_manager.write(&BlockId::new(file.clone(), block_num), &mut page);
    }
}

fn partition_work(total: usize, workers: usize) -> Vec<usize> {
    let base = total / workers;
    let remainder = total % workers;
    (0..workers)
        .map(|tid| base + usize::from(tid < remainder))
        .collect()
}

fn throughput_row_from_benchmark(
    result: BenchResult,
    total_ops: usize,
    unit: &str,
) -> ThroughputRow {
    let mean_duration = result.mean;
    let throughput = total_ops as f64 / mean_duration.as_secs_f64();
    ThroughputRow {
        label: result.operation,
        throughput,
        unit: unit.to_string(),
        mean_duration,
    }
}

enum AccessPattern {
    Sequential,
    SequentialMt {
        threads: usize,
    },
    Repeated {
        total_ops: usize,
    },
    RepeatedMt {
        threads: usize,
        total_ops: usize,
    },
    Random {
        working_set: usize,
        total_ops: usize,
    },
    RandomMt {
        threads: usize,
        working_set: usize,
        total_ops: usize,
    },
    Zipfian {
        total_ops: usize,
    },
    ZipfianMt {
        threads: usize,
        total_ops: usize,
    },
}

struct AccessCase {
    filter_token: &'static str,
    pattern: AccessPattern,
}

impl AccessCase {
    fn run(&self, num_buffers: usize, iterations: usize) -> BenchResult {
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        match self.pattern {
            AccessPattern::Sequential => sequential_scan(&db, num_buffers, iterations),
            AccessPattern::SequentialMt { threads } => sequential_scan_multithreaded(
                &db,
                num_buffers,
                iterations,
                threads,
                num_buffers * 10,
            ),
            AccessPattern::Repeated { .. } => repeated_access(&db, num_buffers, iterations),
            AccessPattern::RepeatedMt { threads, total_ops } => {
                repeated_access_multithreaded(&db, num_buffers, iterations, threads, total_ops)
            }
            AccessPattern::Random {
                working_set,
                total_ops: _,
            } => random_access(&db, working_set, iterations),
            AccessPattern::RandomMt {
                threads,
                working_set,
                total_ops,
            } => random_access_multithreaded(&db, working_set, iterations, threads, total_ops),
            AccessPattern::Zipfian { .. } => zipfian_access(&db, num_buffers, iterations),
            AccessPattern::ZipfianMt { threads, total_ops } => {
                zipfian_access_multithreaded(&db, num_buffers, iterations, threads, total_ops)
            }
        }
    }

    fn total_ops(&self, num_buffers: usize) -> usize {
        match self.pattern {
            AccessPattern::Sequential | AccessPattern::SequentialMt { .. } => num_buffers * 10,
            AccessPattern::Repeated { total_ops }
            | AccessPattern::RepeatedMt { total_ops, .. }
            | AccessPattern::Random { total_ops, .. }
            | AccessPattern::RandomMt { total_ops, .. }
            | AccessPattern::Zipfian { total_ops }
            | AccessPattern::ZipfianMt { total_ops, .. } => total_ops,
        }
    }
}

fn render_latency_section(title: &str, results: &[BenchResult]) {
    if results.is_empty() {
        return;
    }

    println!("{title}");
    print_header();
    for result in results {
        println!("{result}");
    }
    println!();
}

struct PinCase {
    filter_token: &'static str,
    threads: usize,
    ops_per_thread: usize,
}

impl PinCase {
    const fn total_ops(&self) -> usize {
        self.threads * self.ops_per_thread
    }

    fn label(&self) -> String {
        format!(
            "{} threads, {} ops/thread",
            self.threads, self.ops_per_thread
        )
    }
}

struct HotsetCase {
    filter_token: &'static str,
    threads: usize,
    ops_per_thread: usize,
    hot_set_size: usize,
}

impl HotsetCase {
    const fn total_ops(&self) -> usize {
        self.threads * self.ops_per_thread
    }

    fn label(&self) -> String {
        format!(
            "{} threads, K={}, {} ops/thread",
            self.threads, self.hot_set_size, self.ops_per_thread
        )
    }
}

const PIN_CASES: &[PinCase] = &[
    PinCase {
        filter_token: "[pin:t2]",
        threads: 2,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t4]",
        threads: 4,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t8]",
        threads: 8,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t16]",
        threads: 16,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t64]",
        threads: 64,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t128]",
        threads: 128,
        ops_per_thread: 1000,
    },
    PinCase {
        filter_token: "[pin:t256]",
        threads: 256,
        ops_per_thread: 1000,
    },
];

const HOTSET_CASES: &[HotsetCase] = &[
    HotsetCase {
        filter_token: "[hotset:t4_k4]",
        threads: 4,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
    HotsetCase {
        filter_token: "[hotset:t8_k4]",
        threads: 8,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
    HotsetCase {
        filter_token: "[hotset:t16_k4]",
        threads: 16,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
    HotsetCase {
        filter_token: "[hotset:t64_k4]",
        threads: 64,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
    HotsetCase {
        filter_token: "[hotset:t128_k4]",
        threads: 128,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
    HotsetCase {
        filter_token: "[hotset:t256_k4]",
        threads: 256,
        ops_per_thread: 1000,
        hot_set_size: 4,
    },
];

const ACCESS_CASES: &[AccessCase] = &[
    AccessCase {
        filter_token: "Sequential Scan",
        pattern: AccessPattern::Sequential,
    },
    AccessCase {
        filter_token: "Seq Scan MT",
        pattern: AccessPattern::SequentialMt { threads: 4 },
    },
    AccessCase {
        filter_token: "Seq Scan MT x16",
        pattern: AccessPattern::SequentialMt { threads: 16 },
    },
    AccessCase {
        filter_token: "Seq Scan MT x64",
        pattern: AccessPattern::SequentialMt { threads: 64 },
    },
    AccessCase {
        filter_token: "Seq Scan MT x128",
        pattern: AccessPattern::SequentialMt { threads: 128 },
    },
    AccessCase {
        filter_token: "Seq Scan MT x256",
        pattern: AccessPattern::SequentialMt { threads: 256 },
    },
    AccessCase {
        filter_token: "Repeated Access (1000 ops)",
        pattern: AccessPattern::Repeated { total_ops: 1000 },
    },
    AccessCase {
        filter_token: "Repeated Access MT",
        pattern: AccessPattern::RepeatedMt {
            threads: 4,
            total_ops: 1000,
        },
    },
    AccessCase {
        filter_token: "Repeated Access MT x16",
        pattern: AccessPattern::RepeatedMt {
            threads: 16,
            total_ops: 1000,
        },
    },
    AccessCase {
        filter_token: "Repeated Access MT x64",
        pattern: AccessPattern::RepeatedMt {
            threads: 64,
            total_ops: 1000,
        },
    },
    AccessCase {
        filter_token: "Repeated Access MT x128",
        pattern: AccessPattern::RepeatedMt {
            threads: 128,
            total_ops: 1000,
        },
    },
    AccessCase {
        filter_token: "Repeated Access MT x256",
        pattern: AccessPattern::RepeatedMt {
            threads: 256,
            total_ops: 1000,
        },
    },
    AccessCase {
        filter_token: "Random (K=10,",
        pattern: AccessPattern::Random {
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random (K=50,",
        pattern: AccessPattern::Random {
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random (K=100,",
        pattern: AccessPattern::Random {
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x4 (K=10,",
        pattern: AccessPattern::RandomMt {
            threads: 4,
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x16 (K=10,",
        pattern: AccessPattern::RandomMt {
            threads: 16,
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x64 (K=10,",
        pattern: AccessPattern::RandomMt {
            threads: 64,
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x128 (K=10,",
        pattern: AccessPattern::RandomMt {
            threads: 128,
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x256 (K=10,",
        pattern: AccessPattern::RandomMt {
            threads: 256,
            working_set: 10,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x4 (K=50,",
        pattern: AccessPattern::RandomMt {
            threads: 4,
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x16 (K=50,",
        pattern: AccessPattern::RandomMt {
            threads: 16,
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x64 (K=50,",
        pattern: AccessPattern::RandomMt {
            threads: 64,
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x128 (K=50,",
        pattern: AccessPattern::RandomMt {
            threads: 128,
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x256 (K=50,",
        pattern: AccessPattern::RandomMt {
            threads: 256,
            working_set: 50,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x4 (K=100,",
        pattern: AccessPattern::RandomMt {
            threads: 4,
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x16 (K=100,",
        pattern: AccessPattern::RandomMt {
            threads: 16,
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x64 (K=100,",
        pattern: AccessPattern::RandomMt {
            threads: 64,
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x128 (K=100,",
        pattern: AccessPattern::RandomMt {
            threads: 128,
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Random MT x256 (K=100,",
        pattern: AccessPattern::RandomMt {
            threads: 256,
            working_set: 100,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Zipfian (80/20,",
        pattern: AccessPattern::Zipfian { total_ops: 500 },
    },
    AccessCase {
        filter_token: "Zipfian MT",
        pattern: AccessPattern::ZipfianMt {
            threads: 4,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Zipfian MT x16",
        pattern: AccessPattern::ZipfianMt {
            threads: 16,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Zipfian MT x64",
        pattern: AccessPattern::ZipfianMt {
            threads: 64,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Zipfian MT x128",
        pattern: AccessPattern::ZipfianMt {
            threads: 128,
            total_ops: 500,
        },
    },
    AccessCase {
        filter_token: "Zipfian MT x256",
        pattern: AccessPattern::ZipfianMt {
            threads: 256,
            total_ops: 500,
        },
    },
];

fn pin_unpin_overhead(db: &SimpleDB, iterations: usize) -> BenchResult {
    let test_file = "testfile".to_string();
    let buffer_manager = db.buffer_manager();

    precreate_blocks(db, &test_file, 2);

    benchmark("Pin/Unpin (hit)", iterations, 5, || {
        let block_id = BlockId::new(test_file.clone(), 1);
        let buffer = buffer_manager.pin(&block_id).unwrap();
        buffer_manager.unpin(buffer);
    })
}

fn cold_pin(db: &SimpleDB, iterations: usize) -> BenchResult {
    let test_file = "coldfile".to_string();
    let buffer_manager = db.buffer_manager();

    // Pre-create blocks on disk
    precreate_blocks(db, &test_file, iterations);

    let mut block_idx = 0;
    benchmark("Cold Pin (miss)", iterations, 0, || {
        let block_id = BlockId::new(test_file.clone(), block_idx);
        let buffer = buffer_manager.pin(&block_id).unwrap();
        buffer_manager.unpin(buffer);
        block_idx += 1;
    })
}

fn dirty_eviction(db: &SimpleDB, iterations: usize, num_buffers: usize) -> BenchResult {
    let test_file = "dirtyfile".to_string();
    let buffer_manager = db.buffer_manager();

    // Benchmark closure pins a fresh block each time (starting at `num_buffers`).
    // Account for both warmup runs and measured iterations so we never read
    // beyond the blocks we laid out on disk.
    const WARMUP_ITERS: usize = 2;
    let total_unique_blocks = num_buffers + WARMUP_ITERS + iterations;
    precreate_blocks(db, &test_file, total_unique_blocks);

    // Fill buffer pool with dirty buffers using transactions
    let txn = db.new_tx();
    for i in 0..num_buffers {
        let block_id = BlockId::new(test_file.clone(), i);
        // Pin the block first, then modify it
        let mut guard = txn.pin_write_guard(&block_id);
        guard.set_int(0, 999);
        guard.mark_modified(txn.id(), Lsn::MAX);
    }
    // Don't commit - keeps buffers dirty and pinned by this transaction

    // Now benchmark: pinning new blocks forces dirty buffer eviction + flush
    let mut block_idx = num_buffers;
    let result = benchmark("Dirty Eviction", iterations, WARMUP_ITERS, || {
        let block_id = BlockId::new(test_file.clone(), block_idx);
        let buffer = buffer_manager.pin(&block_id).unwrap(); // Forces eviction + flush
        buffer_manager.unpin(buffer);
        block_idx += 1;
    });

    // Clean up: commit transaction to release locks
    txn.commit().unwrap();

    result
}

// Phase 2: Access Pattern Benchmarks
fn sequential_scan(db: &SimpleDB, num_buffers: usize, iterations: usize) -> BenchResult {
    let test_file = "seqfile".to_string();
    let buffer_manager = db.buffer_manager();
    let total_blocks = num_buffers * 10; // Working set > pool

    // Pre-create blocks
    precreate_blocks(db, &test_file, total_blocks);

    // Benchmark: complete scan as one workload
    benchmark(
        &format!("Sequential Scan ({} blocks)", total_blocks),
        iterations,
        2,
        || {
            for i in 0..total_blocks {
                let block_id = BlockId::new(test_file.clone(), i);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            }
        },
    )
}

fn repeated_access(db: &SimpleDB, num_buffers: usize, iterations: usize) -> BenchResult {
    let test_file = "repeatfile".to_string();
    let buffer_manager = db.buffer_manager();
    let working_set = 10.min(num_buffers - 2); // Small working set < pool
    let total_accesses = 1000;

    // Pre-create blocks
    precreate_blocks(db, &test_file, working_set);

    // Benchmark: repeated access pattern as one workload
    benchmark(
        &format!("Repeated Access ({} ops)", total_accesses),
        iterations,
        2,
        || {
            for i in 0..total_accesses {
                let block_idx = i % working_set;
                let block_id = BlockId::new(test_file.clone(), block_idx);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            }
        },
    )
}

fn random_access(db: &SimpleDB, working_set_size: usize, iterations: usize) -> BenchResult {
    let test_file = format!("randomfile_{working_set_size}");
    let buffer_manager = db.buffer_manager();
    let total_accesses = 500;

    // Pre-create blocks
    precreate_blocks(db, &test_file, working_set_size);

    // Pre-generate random sequence (exclude RNG overhead from benchmark)
    let random_indices: Vec<usize> = (0..total_accesses)
        .map(|_| generate_random_number() % working_set_size)
        .collect();

    // Benchmark: random access pattern as one workload
    benchmark(
        &format!("Random (K={}, {} ops)", working_set_size, total_accesses),
        iterations,
        2,
        || {
            for &block_idx in &random_indices {
                let block_id = BlockId::new(test_file.clone(), block_idx);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            }
        },
    )
}

fn zipfian_access(db: &SimpleDB, num_buffers: usize, iterations: usize) -> BenchResult {
    let test_file = "zipffile".to_string();
    let buffer_manager = db.buffer_manager();
    let total_blocks = num_buffers * 3;
    let hot_set_size = ((total_blocks as f64 * 0.2) as usize).max(1); // 20% hot
    let total_accesses = 500;

    // Pre-create blocks
    precreate_blocks(db, &test_file, total_blocks);

    // Pre-generate zipfian sequence (exclude RNG overhead from benchmark)
    let zipfian_indices: Vec<usize> = (0..total_accesses)
        .map(|_| {
            let rand_val = generate_random_number();
            let is_hot = (rand_val % 100) < 80; // 80% chance

            if is_hot {
                generate_random_number() % hot_set_size
            } else {
                hot_set_size + (generate_random_number() % (total_blocks - hot_set_size))
            }
        })
        .collect();

    // Benchmark: zipfian access pattern as one workload
    benchmark(
        &format!("Zipfian (80/20, {} ops)", total_accesses),
        iterations,
        2,
        || {
            for &block_idx in &zipfian_indices {
                let block_id = BlockId::new(test_file.clone(), block_idx);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            }
        },
    )
}

fn sequential_scan_multithreaded(
    db: &SimpleDB,
    _num_buffers: usize,
    iterations: usize,
    num_threads: usize,
    total_blocks: usize,
) -> BenchResult {
    let test_file = format!("seqfile_mt_{num_threads}");

    precreate_blocks(db, &test_file, total_blocks);

    let base = total_blocks / num_threads;
    let remainder = total_blocks % num_threads;
    let ranges = Arc::new(
        (0..num_threads)
            .map(|tid| {
                let start = tid * base + remainder.min(tid);
                let extra = usize::from(tid < remainder);
                let end = start + base + extra;
                (start, end)
            })
            .collect::<Vec<_>>(),
    );
    let test_file = Arc::new(test_file);

    benchmark(
        &format!("Seq Scan MT x{} ({} blocks)", num_threads, total_blocks),
        iterations,
        2,
        || {
            let handles: Vec<_> = ranges
                .iter()
                .map(|&(start, end)| {
                    let test_file = Arc::clone(&test_file);
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for i in start..end {
                            let block_id = BlockId::new(test_file.as_ref().clone(), i);
                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn repeated_access_multithreaded(
    db: &SimpleDB,
    num_buffers: usize,
    iterations: usize,
    num_threads: usize,
    total_accesses: usize,
) -> BenchResult {
    let test_file = format!("repeatfile_mt_{num_threads}");
    let working_set = 10.min(num_buffers.saturating_sub(2)).max(1);

    precreate_blocks(db, &test_file, working_set);

    let per_thread_ops = partition_work(total_accesses, num_threads);
    let per_thread_ops = Arc::new(per_thread_ops);
    let test_file = Arc::new(test_file);

    benchmark(
        &format!(
            "Repeated Access MT x{} ({} ops)",
            num_threads, total_accesses
        ),
        iterations,
        2,
        || {
            let handles: Vec<_> = per_thread_ops
                .iter()
                .enumerate()
                .map(|(thread_id, &ops)| {
                    let test_file = Arc::clone(&test_file);
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for i in 0..ops {
                            let block_idx = (i + thread_id) % working_set;
                            let block_id = BlockId::new(test_file.as_ref().clone(), block_idx);
                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn random_access_multithreaded(
    db: &SimpleDB,
    working_set_size: usize,
    iterations: usize,
    num_threads: usize,
    total_accesses: usize,
) -> BenchResult {
    let test_file = format!("randomfile_mt_{working_set_size}_{num_threads}");

    precreate_blocks(db, &test_file, working_set_size);

    let per_thread_ops = partition_work(total_accesses, num_threads);

    let sequences: Vec<Vec<usize>> = per_thread_ops
        .iter()
        .map(|&ops| {
            (0..ops)
                .map(|_| generate_random_number() % working_set_size)
                .collect()
        })
        .collect();

    let sequences = Arc::new(sequences);
    let test_file = Arc::new(test_file);

    benchmark(
        &format!(
            "Random MT x{} (K={}, {} ops)",
            num_threads, working_set_size, total_accesses
        ),
        iterations,
        2,
        || {
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let sequences = sequences.clone();
                    let test_file = Arc::clone(&test_file);
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for &block_idx in &sequences[thread_id] {
                            let block_id = BlockId::new(test_file.as_ref().clone(), block_idx);
                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn zipfian_access_multithreaded(
    db: &SimpleDB,
    num_buffers: usize,
    iterations: usize,
    num_threads: usize,
    total_accesses: usize,
) -> BenchResult {
    let test_file = format!("zipffile_mt_{num_threads}");
    let total_blocks = num_buffers * 3;
    let hot_set_size = ((total_blocks as f64 * 0.2) as usize).max(1);

    precreate_blocks(db, &test_file, total_blocks);

    let per_thread_ops = partition_work(total_accesses, num_threads);

    let cold_span = total_blocks.saturating_sub(hot_set_size);

    let sequences: Vec<Vec<usize>> = per_thread_ops
        .iter()
        .map(|&ops| {
            (0..ops)
                .map(|_| {
                    let rand_val = generate_random_number();
                    let is_hot = (rand_val % 100) < 80;
                    if is_hot || cold_span == 0 {
                        generate_random_number() % hot_set_size
                    } else {
                        hot_set_size + (generate_random_number() % cold_span)
                    }
                })
                .collect()
        })
        .collect();

    let sequences = Arc::new(sequences);
    let test_file = Arc::new(test_file);

    benchmark(
        &format!(
            "Zipfian MT x{} (80/20, {} ops)",
            num_threads, total_accesses
        ),
        iterations,
        2,
        || {
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let sequences = sequences.clone();
                    let test_file = Arc::clone(&test_file);
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for &block_idx in &sequences[thread_id] {
                            let block_id = BlockId::new(test_file.as_ref().clone(), block_idx);
                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

// Phase 3: Pool Size Sensitivity
fn run_fixed_workload_with_pool_size(
    num_buffers: usize,
    working_set_size: usize,
    iterations: usize,
) -> f64 {
    let (db, _test_dir) = setup_buffer_pool(num_buffers);
    let test_file = "scaling_test".to_string();
    let buffer_manager = db.buffer_manager();
    let total_accesses = 500;

    // Pre-create blocks
    for i in 0..working_set_size {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new();
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    // Pre-generate random sequence
    let random_indices: Vec<usize> = (0..total_accesses)
        .map(|_| generate_random_number() % working_set_size)
        .collect();

    // Run workload
    let result = benchmark("Pool Size Test", iterations, 2, || {
        for &block_idx in &random_indices {
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer = buffer_manager.pin(&block_id).unwrap();
            buffer_manager.unpin(buffer);
        }
    });

    total_accesses as f64 / result.mean.as_secs_f64()
}

fn pool_size_scaling(iterations: usize) {
    let pool_sizes = vec![8, 16, 32, 64, 128, 256];
    let working_set_size = 100; // Fixed workload: 100 blocks

    println!("Phase 3A: Pool Size Sensitivity");
    println!();
    println!("Fixed workload: Random access to {working_set_size} blocks");
    println!("Pool Size (buffers) | Throughput (blocks/sec)");
    println!("{}", "-".repeat(50));

    for pool_size in pool_sizes {
        let throughput = run_fixed_workload_with_pool_size(pool_size, working_set_size, iterations);
        println!("{pool_size:19} | {throughput:>10.0}");
    }
}

fn memory_pressure_test(iterations: usize) {
    println!();
    println!("Phase 3B: Memory Pressure Test");
    println!("Memory Pressure Test: Working set = pool_size + K");
    println!("Pool Size | Working Set | Throughput (blocks/sec)");
    println!("{}", "-".repeat(60));

    let base_pool_size = 32;
    let pressure_offsets = vec![0, 1, 5, 10, 20];

    for offset in pressure_offsets {
        let working_set = base_pool_size + offset;
        let throughput = run_fixed_workload_with_pool_size(base_pool_size, working_set, iterations);
        println!("{base_pool_size:9} | {working_set:11} | {throughput:>10.0}");
    }
}

// Phase 4: Hit Rate Measurement
fn run_pattern_with_stats(
    name: &str,
    pool_buffers: usize,
    pattern_arg: usize,
    iterations: usize,
    pattern_fn: impl Fn(&SimpleDB, usize, usize) -> BenchResult,
) {
    let (db, _test_dir) = setup_buffer_pool_with_stats(pool_buffers);

    // Reset stats before run
    db.buffer_manager().reset_stats();

    // Run the pattern (ignore timing result, we only care about hit rate here)
    let _ = pattern_fn(&db, pattern_arg, iterations);

    // Get stats
    if let Some(stats) = db.buffer_manager().stats() {
        let hit_rate = stats.hit_rate();
        let (hits, misses) = stats.get();
        println!("{name:20} | Hit rate: {hit_rate:>5.1}% (hits: {hits}, misses: {misses})");
    }
}

// Phase 5: Concurrent Access
fn multithreaded_pin(
    db: &SimpleDB,
    num_threads: usize,
    ops_per_thread: usize,
    iterations: usize,
) -> BenchResult {
    let test_file = "concurrent_test".to_string();

    // Pre-create blocks (each thread gets its own range)
    precreate_blocks(db, &test_file, num_threads * 10);

    benchmark(
        &format!(
            "Concurrent ({} threads, {} ops)",
            num_threads, ops_per_thread
        ),
        iterations,
        2,
        || {
            // Spawn threads
            let handles: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let test_file = test_file.clone();
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for i in 0..ops_per_thread {
                            // Each thread accesses blocks in its own range to reduce contention
                            let block_num = (thread_id * 10) + (i % 10);
                            let block_id = BlockId::new(test_file.clone(), block_num);

                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            // Wait for all threads
            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn multithreaded_hotset_contention(
    db: &SimpleDB,
    num_threads: usize,
    ops_per_thread: usize,
    hot_set_size: usize,
    iterations: usize,
) -> BenchResult {
    assert!(hot_set_size > 0, "hot set size must be greater than zero");

    let test_file = "concurrent_hotset".to_string();

    // Pre-create a small hot set shared across all threads
    precreate_blocks(db, &test_file, hot_set_size);

    benchmark(
        &format!(
            "Concurrent Hotset ({} threads, K={}, {} ops)",
            num_threads, hot_set_size, ops_per_thread
        ),
        iterations,
        2,
        || {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let test_file = test_file.clone();
                    let buffer_manager = db.buffer_manager();

                    thread::spawn(move || {
                        for i in 0..ops_per_thread {
                            // All threads reuse the same hot set to maximize latch contention
                            let block_num = i % hot_set_size;
                            let block_id = BlockId::new(test_file.clone(), block_num);

                            let buffer = buffer_manager.pin(&block_id).unwrap();
                            buffer_manager.unpin(buffer);
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        },
    )
}

fn buffer_starvation(db: &SimpleDB, num_buffers: usize) {
    let test_file = "starvation_test".to_string();

    // Pre-create blocks
    precreate_blocks(db, &test_file, num_buffers + 10);

    // Pin entire buffer pool
    let buffer_manager = db.buffer_manager();
    let mut pinned_buffers = Vec::new();

    for i in 0..num_buffers {
        let block_id = BlockId::new(test_file.clone(), i);
        let buffer = buffer_manager.pin(&block_id).unwrap();
        pinned_buffers.push(buffer);
    }

    // Now spawn threads that will need to wait
    let num_waiting_threads = 4;
    let start = Instant::now();

    let handles: Vec<_> = (0..num_waiting_threads)
        .map(|thread_id| {
            let test_file = test_file.clone();
            let buffer_manager = buffer_manager.clone();

            thread::spawn(move || {
                let block_id = BlockId::new(test_file.clone(), num_buffers + thread_id);

                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            })
        })
        .collect();

    // NOTE: Timing assumption - we sleep 50ms hoping all threads reach pin() and block.
    // On loaded systems, threads might not be scheduled in time, causing them to start
    // their pin() calls after we begin unpinning, which would measure thread startup
    // overhead rather than starvation recovery. If you observe high variance in results
    // (e.g., 50ms one run, 200ms another), this race condition is likely occurring.
    // Consider instrumenting BufferManager with a waiting_threads counter for deterministic
    // measurement (see docs/buffer_pool_thrashing_analysis.md for implementation).
    thread::sleep(std::time::Duration::from_millis(50));

    // Unpin one buffer at a time with small delay to observe gradual recovery
    for buffer in pinned_buffers {
        buffer_manager.unpin(buffer);
        thread::sleep(std::time::Duration::from_millis(10));
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    println!("Starved {num_waiting_threads} threads | Pool recovery time: {elapsed:>10.2?}");
}

fn run_multithreaded_pin_benchmarks(db: &SimpleDB, iterations: usize, cases: &[&PinCase]) {
    let mut rows = Vec::new();

    if cases.is_empty() {
        return;
    }

    for case in cases {
        let result = multithreaded_pin(db, case.threads, case.ops_per_thread, iterations);
        let mut row = throughput_row_from_benchmark(result, case.total_ops(), "ops/sec");
        row.label = case.label();
        rows.push(row);
    }

    render_throughput_section("Multi-threaded Pin/Unpin (lock contention)", &rows);
}

fn run_hotset_contention_benchmarks(db: &SimpleDB, iterations: usize, cases: &[&HotsetCase]) {
    let mut rows = Vec::new();

    if cases.is_empty() {
        return;
    }

    for case in cases {
        let result = multithreaded_hotset_contention(
            db,
            case.threads,
            case.ops_per_thread,
            case.hot_set_size,
            iterations,
        );
        let mut row = throughput_row_from_benchmark(result, case.total_ops(), "ops/sec");
        row.label = case.label();
        rows.push(row);
    }

    render_throughput_section("Hot-set Contention (shared buffers)", &rows);
}

fn run_buffer_starvation_benchmark(db: &SimpleDB, num_buffers: usize) {
    println!("Buffer Starvation (cond.wait() latency):");
    println!("{}", "-".repeat(70));
    buffer_starvation(db, num_buffers);
    println!();
}

fn main() {
    let (iterations, num_buffers, json_output, filter) = parse_bench_args();
    let filter_ref = filter.as_deref();
    let block_size = 4096;

    if json_output {
        let mut results = Vec::new();

        // Phase 1
        {
            let (db, _test_dir) = setup_buffer_pool(num_buffers);
            results.push(pin_unpin_overhead(&db, iterations));
        }
        {
            let (db, _test_dir) = setup_buffer_pool(num_buffers);
            results.push(cold_pin(&db, iterations));
        }
        {
            let (db, _test_dir) = setup_buffer_pool(num_buffers);
            results.push(dirty_eviction(&db, iterations, num_buffers));
        }

        for case in ACCESS_CASES {
            results.push(case.run(num_buffers, iterations));
        }

        results.extend(PIN_CASES.iter().map(|case| {
            let (db, _test_dir) = setup_buffer_pool(num_buffers);
            multithreaded_pin(&db, case.threads, case.ops_per_thread, iterations)
        }));

        results.extend(HOTSET_CASES.iter().map(|case| {
            let (db, _test_dir) = setup_buffer_pool(num_buffers);
            multithreaded_hotset_contention(
                &db,
                case.threads,
                case.ops_per_thread,
                case.hot_set_size,
                iterations,
            )
        }));

        let json_results: Vec<String> = results.iter().map(|r| r.to_json()).collect();
        println!("[{}]", json_results.join(","));
        return;
    }

    // Normal mode: run all phases with human-readable output
    println!("SimpleDB Buffer Pool Benchmark Suite");
    println!("====================================");
    println!("Running benchmarks with {iterations} iterations per operation");
    println!("Pool size: {num_buffers} buffers, Block size: {block_size} bytes");
    println!(
        "Environment: {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!();

    // Phase 1
    let mut phase1_results = Vec::new();

    if should_run("Pin/Unpin (hit)", filter_ref) {
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        phase1_results.push(pin_unpin_overhead(&db, iterations));
    }
    if should_run("Cold Pin (miss)", filter_ref) {
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        phase1_results.push(cold_pin(&db, iterations));
    }
    if should_run("Dirty Eviction", filter_ref) {
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        phase1_results.push(dirty_eviction(&db, iterations, num_buffers));
    }

    render_latency_section("Phase 1: Core Latency Benchmarks", &phase1_results);

    // Phase 2
    let mut phase2_rows = Vec::new();

    for case in ACCESS_CASES {
        if should_run(case.filter_token, filter_ref) {
            let result = case.run(num_buffers, iterations);
            let total_ops = case.total_ops(num_buffers);
            phase2_rows.push(throughput_row_from_benchmark(
                result,
                total_ops,
                "blocks/sec",
            ));
        }
    }

    if !phase2_rows.is_empty() {
        render_throughput_section("Phase 2: Access Pattern Benchmarks", &phase2_rows);
    }

    // Phase 3
    if should_run("Pool Size", filter_ref) {
        pool_size_scaling(iterations);
        println!();
    }

    if should_run("Memory Pressure", filter_ref) {
        memory_pressure_test(iterations);
        println!();
    }

    // Phase 4
    let mut phase4_header_printed = false;
    let mut print_phase4_header = || {
        if !phase4_header_printed {
            println!("Phase 4: Hit Rate Measurement");
            println!("Operation            | Hit Rate & Statistics");
            println!("{}", "-".repeat(70));
            phase4_header_printed = true;
        }
    };

    if should_run("Sequential Scan", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats(
            "Sequential Scan",
            num_buffers,
            num_buffers,
            iterations,
            sequential_scan,
        );
    }

    if should_run("Repeated Access", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats(
            "Repeated Access",
            num_buffers,
            num_buffers,
            iterations,
            repeated_access,
        );
    }

    if should_run("Zipfian", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats(
            "Zipfian (80/20)",
            num_buffers,
            num_buffers,
            iterations,
            zipfian_access,
        );
    }

    if should_run("Random (K=10)", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats("Random (K=10)", num_buffers, 10, iterations, random_access);
    }

    if should_run("Random (K=50)", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats("Random (K=50)", num_buffers, 50, iterations, random_access);
    }

    if should_run("Random (K=100)", filter_ref) {
        print_phase4_header();
        run_pattern_with_stats(
            "Random (K=100)",
            num_buffers,
            100,
            iterations,
            random_access,
        );
    }

    if phase4_header_printed {
        println!();
    }

    // Phase 5
    let mut phase5_has_output = false;

    let pin_cases: Vec<&PinCase> = match filter_ref {
        None => PIN_CASES.iter().collect(),
        Some(_) => PIN_CASES
            .iter()
            .filter(|case| should_run(case.filter_token, filter_ref))
            .collect(),
    };
    if !pin_cases.is_empty() {
        if !phase5_has_output {
            println!("Phase 5: Concurrent Access");
            println!();
            phase5_has_output = true;
        }
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        run_multithreaded_pin_benchmarks(&db, iterations, &pin_cases);
    }

    let hotset_cases: Vec<&HotsetCase> = match filter_ref {
        None => HOTSET_CASES.iter().collect(),
        Some(_) => HOTSET_CASES
            .iter()
            .filter(|case| should_run(case.filter_token, filter_ref))
            .collect(),
    };
    if !hotset_cases.is_empty() {
        if !phase5_has_output {
            println!("Phase 5: Concurrent Access");
            println!();
            phase5_has_output = true;
        }
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        run_hotset_contention_benchmarks(&db, iterations, &hotset_cases);
    }

    if should_run("Starvation", filter_ref) {
        if !phase5_has_output {
            println!("Phase 5: Concurrent Access");
            println!();
        }
        let (db, _test_dir) = setup_buffer_pool(num_buffers);
        run_buffer_starvation_benchmark(&db, num_buffers);
    }

    println!("All benchmarks completed!");
}
