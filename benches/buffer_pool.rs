use std::sync::Arc;

use simpledb::{
    benchmark_framework::{benchmark, parse_iterations, print_header},
    BlockId, Page, SimpleDB, TestDir,
};

fn setup_buffer_pool(block_size: usize, num_buffers: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(block_size, num_buffers)
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

fn main() {
    let iterations = parse_iterations();
    let block_size = 4096;
    let num_buffers = 12;

    println!("SimpleDB Buffer Pool Benchmark Suite");
    println!("====================================");
    println!("Phase 1: Core Latency Benchmarks");
    println!("Running benchmarks with {iterations} iterations per operation");
    println!("Pool size: {num_buffers} buffers, Block size: {block_size} bytes");
    println!(
        "Environment: {} ({})",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!();

    let (db, _test_dir) = setup_buffer_pool(block_size, num_buffers);

    print_header();
    pin_unpin_overhead(&db, block_size, iterations);
    cold_pin(&db, block_size, iterations, num_buffers);
    dirty_eviction(&db, block_size, iterations, num_buffers);

    println!();
    println!("Phase 1 benchmarks completed!");
}
