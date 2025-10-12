use std::sync::Arc;

use simpledb::{
    benchmark_framework::{benchmark, parse_iterations, print_header},
    test_utils::generate_random_number,
    BlockId, Page, SimpleDB, TestDir,
};

fn setup_buffer_pool(block_size: usize, num_buffers: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(block_size, num_buffers)
}

fn setup_buffer_pool_with_stats(block_size: usize, num_buffers: usize) -> (SimpleDB, TestDir) {
    let (db, test_dir) = SimpleDB::new_for_test(block_size, num_buffers);
    db.buffer_manager()
        .lock()
        .unwrap()
        .enable_stats();
    (db, test_dir)
}

fn pin_unpin_overhead(db: &SimpleDB, block_size: usize, iterations: usize) {
    let test_file = "testfile".to_string();
    let buffer_manager = db.buffer_manager();

    let block_id = BlockId::new(test_file, 1);
    let mut page = Page::new(block_size);
    page.set_int(80, 1);
    db.file_manager.lock().unwrap().write(&block_id, &mut page);

    let result = benchmark("Pin/Unpin (hit)", iterations, || {
        let buffer_manager_guard = buffer_manager.lock().unwrap();
        let buffer = buffer_manager_guard.pin(&block_id).unwrap();
        buffer_manager_guard.unpin(buffer);
    });
    println!("{result}");
}

fn cold_pin(db: &SimpleDB, block_size: usize, iterations: usize, _num_buffers: usize) {
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
    let result = benchmark("Cold Pin (miss)", iterations, || {
        let block_id = BlockId::new(test_file.clone(), block_idx);
        let buffer_manager_guard = buffer_manager.lock().unwrap();
        let buffer = buffer_manager_guard.pin(&block_id).unwrap();
        buffer_manager_guard.unpin(buffer);
        block_idx += 1;
    });
    println!("{result}");
}

fn dirty_eviction(db: &SimpleDB, _block_size: usize, iterations: usize, num_buffers: usize) {
    // Create a table to generate dirty buffers
    let txn = Arc::new(db.new_tx());
    db.planner
        .execute_update(
            "CREATE TABLE dirty_test(id int, value int)".to_string(),
            Arc::clone(&txn),
        )
        .unwrap();
    txn.commit().unwrap();

    // Fill buffer pool with dirty buffers by inserting records
    let txn = Arc::new(db.new_tx());
    for i in 0..num_buffers * 2 {
        db.planner
            .execute_update(
                format!(
                    "INSERT INTO dirty_test(id, value) VALUES ({}, {})",
                    i,
                    i * 10
                ),
                Arc::clone(&txn),
            )
            .unwrap();
    }
    // Don't commit yet - keeps buffers dirty

    // Now benchmark: pinning new blocks will evict dirty buffers
    let mut counter = num_buffers * 2;
    let result = benchmark("Dirty Eviction", iterations, || {
        db.planner
            .execute_update(
                format!(
                    "INSERT INTO dirty_test(id, value) VALUES ({}, {})",
                    counter,
                    counter * 10
                ),
                Arc::clone(&txn),
            )
            .unwrap();
        counter += 1;
    });
    println!("{result}");

    txn.commit().unwrap();
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
    let result = benchmark("Sequential Scan", iterations, || {
        for i in 0..total_blocks {
            let block_id = BlockId::new(test_file.clone(), i);
            let buffer_manager_guard = buffer_manager.lock().unwrap();
            let buffer = buffer_manager_guard.pin(&block_id).unwrap();
            buffer_manager_guard.unpin(buffer);
        }
    });

    // Convert timing to throughput
    let mean_throughput = total_blocks as f64 / result.mean.as_secs_f64();
    let median_throughput = total_blocks as f64 / result.median.as_secs_f64();

    println!("{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
             "Sequential Scan", mean_throughput, median_throughput);
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
    let result = benchmark("Repeated Access", iterations, || {
        for i in 0..total_accesses {
            let block_idx = i % working_set;
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer_manager_guard = buffer_manager.lock().unwrap();
            let buffer = buffer_manager_guard.pin(&block_id).unwrap();
            buffer_manager_guard.unpin(buffer);
        }
    });

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!("{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
             "Repeated Access", mean_throughput, median_throughput);
}

fn random_access(db: &SimpleDB, block_size: usize, working_set_size: usize, iterations: usize) {
    let test_file = format!("randomfile_{}", working_set_size);
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
    let result = benchmark(&format!("Random (K={})", working_set_size), iterations, || {
        for &block_idx in &random_indices {
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer_manager_guard = buffer_manager.lock().unwrap();
            let buffer = buffer_manager_guard.pin(&block_id).unwrap();
            buffer_manager_guard.unpin(buffer);
        }
    });

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!("{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
             format!("Random (K={:3})", working_set_size), mean_throughput, median_throughput);
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
    let result = benchmark("Zipfian (80/20)", iterations, || {
        for &block_idx in &zipfian_indices {
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer_manager_guard = buffer_manager.lock().unwrap();
            let buffer = buffer_manager_guard.pin(&block_id).unwrap();
            buffer_manager_guard.unpin(buffer);
        }
    });

    let mean_throughput = total_accesses as f64 / result.mean.as_secs_f64();
    let median_throughput = total_accesses as f64 / result.median.as_secs_f64();

    println!("{:20} | {:>10.0} blocks/sec (mean) | {:>10.0} blocks/sec (median)",
             "Zipfian (80/20)", mean_throughput, median_throughput);
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
    let result = benchmark("Pool Size Test", iterations, || {
        for &block_idx in &random_indices {
            let block_id = BlockId::new(test_file.clone(), block_idx);
            let buffer_manager_guard = buffer_manager.lock().unwrap();
            let buffer = buffer_manager_guard.pin(&block_id).unwrap();
            buffer_manager_guard.unpin(buffer);
        }
    });

    total_accesses as f64 / result.mean.as_secs_f64()
}

fn pool_size_scaling(block_size: usize, iterations: usize) {
    let pool_sizes = vec![8, 16, 32, 64, 128, 256];
    let working_set_size = 100; // Fixed workload: 100 blocks

    println!("Phase 3: Pool Size Sensitivity");
    println!("Fixed workload: Random access to {} blocks", working_set_size);
    println!();
    println!("Pool Size (buffers) | Throughput (blocks/sec)");
    println!("{}", "-".repeat(50));

    for pool_size in pool_sizes {
        let throughput = run_fixed_workload_with_pool_size(
            block_size,
            pool_size,
            working_set_size,
            iterations,
        );
        println!("{:19} | {:>10.0}", pool_size, throughput);
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
        let throughput = run_fixed_workload_with_pool_size(
            block_size,
            base_pool_size,
            working_set,
            iterations,
        );
        println!(
            "{:9} | {:11} | {:>10.0}",
            base_pool_size, working_set, throughput
        );
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
    db.buffer_manager().lock().unwrap().reset_stats();

    // Run the pattern
    pattern_fn(db, block_size, num_buffers, iterations);

    // Get stats
    if let Some(stats) = db.buffer_manager().lock().unwrap().stats() {
        let hit_rate = stats.hit_rate();
        let (hits, misses) = stats.get();
        println!("{:20} | Hit rate: {:>5.1}% (hits: {}, misses: {})",
                 name, hit_rate, hits, misses);
    }
}

fn run_random_pattern_with_stats(
    name: &str,
    db: &SimpleDB,
    block_size: usize,
    working_set: usize,
    iterations: usize,
) {
    db.buffer_manager().lock().unwrap().reset_stats();
    random_access(db, block_size, working_set, iterations);

    if let Some(stats) = db.buffer_manager().lock().unwrap().stats() {
        let hit_rate = stats.hit_rate();
        let (hits, misses) = stats.get();
        println!("{:20} | Hit rate: {:>5.1}% (hits: {}, misses: {})",
                 name, hit_rate, hits, misses);
    }
}

fn hit_rate_benchmarks(block_size: usize, num_buffers: usize, iterations: usize) {
    let (db, _test_dir) = setup_buffer_pool_with_stats(block_size, num_buffers);

    println!("Phase 4: Hit Rate Measurement");
    println!("Operation            | Hit Rate & Statistics");
    println!("{}", "-".repeat(70));

    run_pattern_with_stats("Sequential Scan", &db, block_size, num_buffers, iterations, sequential_scan);
    run_pattern_with_stats("Repeated Access", &db, block_size, num_buffers, iterations, repeated_access);
    run_random_pattern_with_stats("Random (K=10)", &db, block_size, 10, iterations);
    run_random_pattern_with_stats("Random (K=50)", &db, block_size, 50, iterations);
    run_random_pattern_with_stats("Random (K=100)", &db, block_size, 100, iterations);
    run_pattern_with_stats("Zipfian (80/20)", &db, block_size, num_buffers, iterations, zipfian_access);
}

fn main() {
    let iterations = parse_iterations();
    let block_size = 4096;
    let num_buffers = 12;

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

    let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);

    // Phase 1
    println!("Phase 1: Core Latency Benchmarks");
    print_header();
    pin_unpin_overhead(&db, block_size, iterations);
    cold_pin(&db, block_size, iterations, num_buffers);
    dirty_eviction(&db, block_size, iterations, num_buffers);
    println!();

    // Phase 2
    println!("Phase 2: Access Pattern Benchmarks");
    println!("Operation            | Throughput (mean)          | Throughput (median)");
    println!("{}", "-".repeat(75));
    sequential_scan(&db, block_size, num_buffers, iterations);
    repeated_access(&db, block_size, num_buffers, iterations);
    random_access(&db, block_size, 10, iterations);
    random_access(&db, block_size, 50, iterations);
    random_access(&db, block_size, 100, iterations);
    zipfian_access(&db, block_size, num_buffers, iterations);
    println!();

    // Phase 3
    pool_size_scaling(block_size, iterations);
    memory_pressure_test(block_size, iterations);
    println!();

    // Phase 4
    hit_rate_benchmarks(block_size, num_buffers, iterations);

    println!();
    println!("All benchmarks completed!");
}
