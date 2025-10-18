#![allow(clippy::arc_with_non_send_sync)]

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use simpledb::{
    benchmark_framework::{benchmark, parse_bench_args, print_header, BenchResult},
    test_utils::generate_random_number,
    BlockId, Page, SimpleDB, TestDir,
};

fn setup_buffer_pool(block_size: usize, num_buffers: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(block_size, num_buffers)
}

fn setup_buffer_pool_with_stats(block_size: usize, num_buffers: usize) -> (SimpleDB, TestDir) {
    let (db, test_dir) = SimpleDB::new_for_test(block_size, num_buffers);
    db.buffer_manager().enable_stats();
    (db, test_dir)
}

fn pin_unpin_overhead(db: &SimpleDB, block_size: usize, iterations: usize) -> BenchResult {
    let test_file = "testfile".to_string();
    let buffer_manager = db.buffer_manager();

    let block_id = BlockId::new(test_file, 1);
    let mut page = Page::new(block_size);
    page.set_int(80, 1);
    db.file_manager.lock().unwrap().write(&block_id, &mut page);

    benchmark("Pin/Unpin (hit)", iterations, 5, || {
        let buffer = buffer_manager.pin(&block_id).unwrap();
        buffer_manager.unpin(buffer);
    })
}

fn cold_pin(db: &SimpleDB, block_size: usize, iterations: usize) -> BenchResult {
    let test_file = "coldfile".to_string();
    let buffer_manager = db.buffer_manager();

    // Pre-create blocks on disk
    for i in 0..iterations {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    let mut block_idx = 0;
    benchmark("Cold Pin (miss)", iterations, 0, || {
        let block_id = BlockId::new(test_file.clone(), block_idx);
        let buffer = buffer_manager.pin(&block_id).unwrap();
        buffer_manager.unpin(buffer);
        block_idx += 1;
    })
}

fn dirty_eviction(db: &SimpleDB, block_size: usize, iterations: usize, num_buffers: usize) -> BenchResult {
    let test_file = "dirtyfile".to_string();
    let buffer_manager = db.buffer_manager();

    // Pre-create blocks on disk (twice the buffer pool size)
    for i in 0..(num_buffers * 2) {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    // Fill buffer pool with dirty buffers using transactions
    let txn = Arc::new(db.new_tx());
    for i in 0..num_buffers {
        let block_id = BlockId::new(test_file.clone(), i);
        // Pin the block first, then modify it
        let _handle = txn.pin(&block_id);
        txn.set_int(&block_id, 0, 999, false).unwrap();
    }
    // Don't commit - keeps buffers dirty and pinned by this transaction

    // Now benchmark: pinning new blocks forces dirty buffer eviction + flush
    let mut block_idx = num_buffers;
    let result = benchmark("Dirty Eviction", iterations, 2, || {
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

fn sequential_scan(db: &SimpleDB, block_size: usize, num_buffers: usize, iterations: usize) {
    let test_file = "seqfile".to_string();
    let buffer_manager = db.buffer_manager();
    let total_blocks = num_buffers * 10; // Working set > pool

    // Pre-create blocks
    for i in 0..total_blocks {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    // Benchmark: complete scan as one workload
    let result = benchmark("Sequential Scan", iterations, 2, || {
        for i in 0..total_blocks {
            let block_id = BlockId::new(test_file.clone(), i);
            let buffer = buffer_manager.pin(&block_id).unwrap();
            buffer_manager.unpin(buffer);
        }
    });

    // Convert timing to throughput
    let mean_throughput = total_blocks as f64 / result.mean.as_secs_f64();
    let median_throughput = total_blocks as f64 / result.median.as_secs_f64();

    println!(
        "{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
        "Sequential Scan", mean_throughput, median_throughput
    );
}

fn repeated_access(db: &SimpleDB, block_size: usize, num_buffers: usize, iterations: usize) {
    let test_file = "repeatfile".to_string();
    let buffer_manager = db.buffer_manager();
    let working_set = 10.min(num_buffers - 2); // Small working set < pool
    let total_accesses = 1000;

    // Pre-create blocks
    for i in 0..working_set {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    // Benchmark: repeated access pattern as one workload
    let result = benchmark("Repeated Access", iterations, 2, || {
        for i in 0..total_accesses {
            let block_idx = i % working_set;
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer = buffer_manager.pin(&block_id).unwrap();
            buffer_manager.unpin(buffer);
        }
    });

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!(
        "{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
        "Repeated Access", mean_throughput, median_throughput
    );
}

fn random_access(db: &SimpleDB, block_size: usize, working_set_size: usize, iterations: usize) {
    let test_file = format!("randomfile_{working_set_size}");
    let buffer_manager = db.buffer_manager();
    let total_accesses = 500;

    // Pre-create blocks
    for i in 0..working_set_size {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    // Pre-generate random sequence (exclude RNG overhead from benchmark)
    let random_indices: Vec<usize> = (0..total_accesses)
        .map(|_| generate_random_number() % working_set_size)
        .collect();

    // Benchmark: random access pattern as one workload
    let result = benchmark(
        &format!("Random (K={working_set_size})"),
        iterations,
        2,
        || {
            for &block_idx in &random_indices {
                let block_id = BlockId::new(test_file.clone(), block_idx);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            }
        },
    );

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!(
        "{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
        format!("Random (K={:3})", working_set_size),
        mean_throughput,
        median_throughput
    );
}

fn zipfian_access(db: &SimpleDB, block_size: usize, num_buffers: usize, iterations: usize) {
    let test_file = "zipffile".to_string();
    let buffer_manager = db.buffer_manager();
    let total_blocks = num_buffers * 3;
    let hot_set_size = (total_blocks as f64 * 0.2) as usize; // 20% hot
    let total_accesses = 500;

    // Pre-create blocks
    for i in 0..total_blocks {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

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
    let result = benchmark("Zipfian (80/20)", iterations, 2, || {
        for &block_idx in &zipfian_indices {
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer = buffer_manager.pin(&block_id).unwrap();
            buffer_manager.unpin(buffer);
        }
    });

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!(
        "{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
        "Zipfian (80/20)", mean_throughput, median_throughput
    );
}

// Phase 3: Pool Size Sensitivity

fn run_fixed_workload_with_pool_size(
    block_size: usize,
    num_buffers: usize,
    working_set_size: usize,
    iterations: usize,
) -> f64 {
    let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
    let test_file = "scaling_test".to_string();
    let buffer_manager = db.buffer_manager();
    let total_accesses = 500;

    // Pre-create blocks
    for i in 0..working_set_size {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
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

fn pool_size_scaling(block_size: usize, iterations: usize) {
    let pool_sizes = vec![8, 16, 32, 64, 128, 256];
    let working_set_size = 100; // Fixed workload: 100 blocks

    println!("Phase 3: Pool Size Sensitivity");
    println!("Fixed workload: Random access to {working_set_size} blocks");
    println!();
    println!("Pool Size (buffers) | Throughput (blocks/sec)");
    println!("{}", "-".repeat(50));

    for pool_size in pool_sizes {
        let throughput =
            run_fixed_workload_with_pool_size(block_size, pool_size, working_set_size, iterations);
        println!("{pool_size:19} | {throughput:>10.0}");
    }
}

fn memory_pressure_test(block_size: usize, iterations: usize) {
    println!();
    println!("Memory Pressure Test: Working set = pool_size + K");
    println!("Pool Size | Working Set | Throughput (blocks/sec)");
    println!("{}", "-".repeat(60));

    let base_pool_size = 32;
    let pressure_offsets = vec![0, 1, 5, 10, 20];

    for offset in pressure_offsets {
        let working_set = base_pool_size + offset;
        let throughput =
            run_fixed_workload_with_pool_size(block_size, base_pool_size, working_set, iterations);
        println!("{base_pool_size:9} | {working_set:11} | {throughput:>10.0}");
    }
}

// Phase 4: Hit Rate Measurement

fn run_pattern_with_stats(
    name: &str,
    db: &SimpleDB,
    block_size: usize,
    num_buffers: usize,
    iterations: usize,
    pattern_fn: impl Fn(&SimpleDB, usize, usize, usize),
) {
    // Reset stats before run
    db.buffer_manager().reset_stats();

    // Run the pattern
    pattern_fn(db, block_size, num_buffers, iterations);

    // Get stats
    if let Some(stats) = db.buffer_manager().stats() {
        let hit_rate = stats.hit_rate();
        let (hits, misses) = stats.get();
        println!("{name:20} | Hit rate: {hit_rate:>5.1}% (hits: {hits}, misses: {misses})");
    }
}

fn hit_rate_benchmarks(block_size: usize, num_buffers: usize, iterations: usize) {
    let (db, _test_dir) = setup_buffer_pool_with_stats(block_size, num_buffers);

    println!("Phase 4: Hit Rate Measurement");
    println!("Operation            | Hit Rate & Statistics");
    println!("{}", "-".repeat(70));

    run_pattern_with_stats(
        "Sequential Scan",
        &db,
        block_size,
        num_buffers,
        iterations,
        sequential_scan,
    );
    run_pattern_with_stats(
        "Repeated Access",
        &db,
        block_size,
        num_buffers,
        iterations,
        repeated_access,
    );
    run_pattern_with_stats(
        "Zipfian (80/20)",
        &db,
        block_size,
        num_buffers,
        iterations,
        zipfian_access,
    );
    run_pattern_with_stats(
        "Random (K=10)",
        &db,
        block_size,
        10,
        iterations,
        random_access,
    );
    run_pattern_with_stats(
        "Random (K=50)",
        &db,
        block_size,
        50,
        iterations,
        random_access,
    );
    run_pattern_with_stats(
        "Random (K=100)",
        &db,
        block_size,
        100,
        iterations,
        random_access,
    );
}

// Phase 5: Concurrent Access

fn multithreaded_pin(db: &SimpleDB, block_size: usize, num_threads: usize, ops_per_thread: usize) {
    let test_file = "concurrent_test".to_string();

    // Pre-create blocks (each thread gets its own range)
    for i in 0..(num_threads * 10) {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

    let start = Instant::now();

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

    let elapsed = start.elapsed();
    let total_ops = num_threads * ops_per_thread;
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{num_threads} threads, {ops_per_thread} ops/thread | {throughput:>10.0} ops/sec | {elapsed:>10.2?} total"
    );
}

fn buffer_starvation(db: &SimpleDB, block_size: usize, num_buffers: usize) {
    let test_file = "starvation_test".to_string();

    // Pre-create blocks
    for i in 0..(num_buffers + 10) {
        let block_id = BlockId::new(test_file.clone(), i);
        let mut page = Page::new(block_size);
        page.set_int(0, i as i32);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);
    }

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

fn concurrent_benchmarks(block_size: usize, num_buffers: usize) {
    let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);

    println!("Phase 5: Concurrent Access");
    println!();

    println!("5.1 Multi-threaded Pin/Unpin (lock contention):");
    println!("{}", "-".repeat(70));
    multithreaded_pin(&db, block_size, 2, 1000);
    multithreaded_pin(&db, block_size, 4, 1000);
    multithreaded_pin(&db, block_size, 8, 1000);

    println!();
    println!("5.2 Buffer Starvation (cond.wait() latency):");
    println!("{}", "-".repeat(70));
    buffer_starvation(&db, block_size, num_buffers);
}

fn main() {
    let (iterations, num_buffers, json_output) = parse_bench_args();
    let block_size = 4096;

    if json_output {
        // In JSON mode, only run Phase 1 benchmarks and output JSON
        let mut results = Vec::new();
        {
            let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
            results.push(pin_unpin_overhead(&db, block_size, iterations));
        }
        {
            let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
            results.push(cold_pin(&db, block_size, iterations));
        }
        {
            let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
            results.push(dirty_eviction(&db, block_size, iterations, num_buffers));
        }

        // Output as JSON array
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
    println!("Phase 1: Core Latency Benchmarks");
    print_header();
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        println!("{}", pin_unpin_overhead(&db, block_size, iterations));
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        println!("{}", cold_pin(&db, block_size, iterations));
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        println!("{}", dirty_eviction(&db, block_size, iterations, num_buffers));
    }
    println!();

    // Phase 2
    println!("Phase 2: Access Pattern Benchmarks");
    println!("Operation            | Throughput (mean)          | Throughput (median)");
    println!("{}", "-".repeat(75));
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        sequential_scan(&db, block_size, num_buffers, iterations);
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        repeated_access(&db, block_size, num_buffers, iterations);
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        random_access(&db, block_size, 10, iterations);
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        random_access(&db, block_size, 50, iterations);
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        random_access(&db, block_size, 100, iterations);
    }
    {
        let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);
        zipfian_access(&db, block_size, num_buffers, iterations);
    }
    println!();

    // Phase 3
    pool_size_scaling(block_size, iterations);
    memory_pressure_test(block_size, iterations);
    println!();

    // Phase 4
    hit_rate_benchmarks(block_size, num_buffers, iterations);
    println!();

    // Phase 5
    concurrent_benchmarks(block_size, num_buffers);

    println!();
    println!("All benchmarks completed!");
}
