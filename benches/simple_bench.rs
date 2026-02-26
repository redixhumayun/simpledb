#![allow(clippy::arc_with_non_send_sync)]

use std::env;
use std::error::Error;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use simpledb::{SimpleDB, TableScan, WalMode};

use simpledb::benchmark_framework;

use benchmark_framework::{
    benchmark, benchmark_with_teardown, parse_bench_args, print_header, should_run,
};

fn cleanup_bench_data() {
    let bench_path = Path::new("./bench-data");
    if bench_path.exists() {
        std::fs::remove_dir_all(bench_path).ok();
    }
}

fn log_phase_start(label: &str, enabled: bool) -> Option<Instant> {
    if enabled {
        println!("[phase] start: {}", label);
        Some(Instant::now())
    } else {
        None
    }
}

fn log_phase_end(label: &str, start: Option<Instant>, enabled: bool) {
    if enabled {
        if let Some(t0) = start {
            println!("[phase] end: {} (elapsed: {:?})", label, t0.elapsed());
        } else {
            println!("[phase] end: {}", label);
        }
    }
}

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

fn setup_test_table(db: &SimpleDB) -> Result<(), Box<dyn Error>> {
    let txn = db.new_tx();

    // Create the table using SQL
    let create_sql = "CREATE TABLE bench_table(id int, name varchar(20), age int)";
    db.planner
        .execute_update(create_sql.to_string(), Arc::clone(&txn))?;

    txn.commit()?;
    Ok(())
}

fn populate_table(db: &SimpleDB, num_records: usize) -> Result<(), Box<dyn Error>> {
    let txn = db.new_tx();

    for i in 0..num_records {
        let insert_sql = format!(
            "INSERT INTO bench_table(id, name, age) VALUES ({}, 'user{}', {})",
            i,
            i,
            20 + (i % 50)
        );
        db.planner.execute_update(insert_sql, Arc::clone(&txn))?;
    }

    txn.commit()?;
    Ok(())
}

fn run_insert_benchmarks(db: &SimpleDB, iterations: usize) -> benchmark_framework::BenchResult {
    // Benchmark single INSERT operations
    benchmark("INSERT (single record)", iterations, 2, || {
        let txn = db.new_tx();
        let insert_sql = "INSERT INTO bench_table(id, name, age) VALUES (99999, 'test_user', 25)";
        db.planner
            .execute_update(insert_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Clean up the inserted record
        let txn = db.new_tx();
        let delete_sql = "DELETE FROM bench_table WHERE id = 99999";
        db.planner
            .execute_update(delete_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    })
}

fn run_select_benchmarks(
    db: &SimpleDB,
    iterations: usize,
) -> Vec<benchmark_framework::BenchResult> {
    // Benchmark SELECT operations
    let result1 = benchmark("SELECT (table scan)", iterations, 2, || {
        let txn = db.new_tx();
        let select_sql = "SELECT id, name FROM bench_table WHERE age > 30";
        let _plan = db
            .planner
            .create_query_plan(select_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    });

    let result2 = benchmark("SELECT COUNT(*)", iterations, 2, || {
        let txn = db.new_tx();
        let select_sql = "SELECT * FROM bench_table";
        let plan = db
            .planner
            .create_query_plan(select_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        {
            let mut scan = plan.open();
            let _count = scan.by_ref().count();
        } // scan is dropped here, before transaction commit
        txn.commit().unwrap();
    });

    vec![result1, result2]
}

fn run_update_benchmarks(db: &SimpleDB, iterations: usize) -> benchmark_framework::BenchResult {
    // Benchmark UPDATE operations
    benchmark("UPDATE (single record)", iterations, 2, || {
        let txn = db.new_tx();
        let update_sql = "UPDATE bench_table SET age = 99 WHERE id = 0";
        db.planner
            .execute_update(update_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Reset the record
        let txn = db.new_tx();
        let reset_sql = "UPDATE bench_table SET age = 20 WHERE id = 0";
        db.planner
            .execute_update(reset_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    })
}

fn run_delete_benchmarks(db: &SimpleDB, iterations: usize) -> benchmark_framework::BenchResult {
    // Benchmark DELETE operations
    benchmark("DELETE (single record)", iterations, 2, || {
        // Insert a record to delete
        let txn = db.new_tx();
        let insert_sql = "INSERT INTO bench_table(id, name, age) VALUES (88888, 'delete_me', 25)";
        db.planner
            .execute_update(insert_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Delete the record
        let txn = db.new_tx();
        let delete_sql = "DELETE FROM bench_table WHERE id = 88888";
        db.planner
            .execute_update(delete_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    })
}

fn parse_macro_args(buffer_pool_blocks: usize) -> (usize, usize) {
    let args: Vec<String> = env::args().collect();
    let mut working_set_blocks = buffer_pool_blocks.saturating_mul(2).max(1);
    let mut prefetch_window_blocks = 16usize;
    let mut i = 1usize;

    while i < args.len() {
        match args[i].as_str() {
            "--macro-working-set-blocks" => {
                if i + 1 < args.len() {
                    working_set_blocks = args[i + 1].parse().unwrap_or(working_set_blocks);
                    i += 1;
                }
            }
            "--prefetch-window" => {
                if i + 1 < args.len() {
                    prefetch_window_blocks = args[i + 1].parse().unwrap_or(prefetch_window_blocks);
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    (working_set_blocks, prefetch_window_blocks)
}

fn setup_macro_scan_table(
    db: &SimpleDB,
    working_set_blocks: usize,
) -> Result<String, Box<dyn Error>> {
    let previous_wal_mode = db.wal_mode();
    db.set_wal_mode(WalMode::UnsafeNoWal);
    let result = (|| -> Result<String, Box<dyn Error>> {
        let table_name = format!("macro_scan_{}", working_set_blocks);
        let create_sql = format!("CREATE TABLE {}(id int, v int)", table_name);
        let txn = db.new_tx();
        db.planner.execute_update(create_sql, Arc::clone(&txn))?;
        txn.commit()?;

        let table_file = format!("{}.tbl", table_name);
        let mut next_id = 0usize;
        let txn = db.new_tx();
        let mut last_blocks = db.file_manager.length(table_file.clone());
        let mut last_reported_block_group = 0usize;
        while db.file_manager.length(table_file.clone()) < working_set_blocks {
            let insert_sql = format!(
                "INSERT INTO {}(id, v) VALUES ({}, {})",
                table_name, next_id, next_id
            );
            db.planner.execute_update(insert_sql, Arc::clone(&txn))?;
            next_id += 1;

            let blocks = db.file_manager.length(table_file.clone());
            if blocks != last_blocks {
                last_blocks = blocks;
                let block_group = blocks / 64;
                if block_group > last_reported_block_group {
                    last_reported_block_group = block_group;
                    println!(
                        "[setup_macro_scan_table] blocks={} inserts_so_far={}",
                        blocks, next_id
                    );
                }
            }
        }
        println!(
            "[setup_macro_scan_table] pre-commit blocks={} total_inserts={}",
            db.file_manager.length(table_file.clone()),
            next_id
        );
        let commit_start = Instant::now();
        txn.commit()?;
        println!(
            "[setup_macro_scan_table] commit_done elapsed={:?}",
            commit_start.elapsed()
        );
        Ok(table_name)
    })();
    db.set_wal_mode(previous_wal_mode);
    result
}

fn run_full_table_scan_macro(
    db: &SimpleDB,
    table_name: &str,
    iterations: usize,
    buffer_pool_blocks: usize,
    working_set_blocks: usize,
    prefetch_window: usize,
    emit_metrics: bool,
) -> benchmark_framework::BenchResult {
    let query = format!("SELECT * FROM {}", table_name);
    let table_file_path = db.db_directory().join(format!("{}.tbl", table_name));
    db.file_manager.enable_io_stats();
    db.file_manager.reset_io_batch_counters();
    db.buffer_manager().enable_stats();
    db.buffer_manager().reset_stats();

    let op_label = format!(
        "MACRO SELECT * (pool={}, ws={}, prefetch={})",
        buffer_pool_blocks, working_set_blocks, prefetch_window
    );
    let phase_start = log_phase_start(&op_label, emit_metrics);
    let result = benchmark_with_teardown(
        &op_label,
        iterations,
        1,
        || {
            TableScan::set_default_prefetch_window_blocks(prefetch_window);
            let txn = db.new_tx();
            let plan = db
                .planner
                .create_query_plan(query.clone(), Arc::clone(&txn))
                .unwrap();
            let mut scan = plan.open();
            while let Some(row) = scan.next() {
                row.unwrap();
            }
            drop(scan);
            txn.commit().unwrap();
        },
        || {
            #[cfg(target_os = "linux")]
            posix_fadvise_dontneed(&table_file_path);
        },
    );
    log_phase_end(&op_label, phase_start, emit_metrics);
    if emit_metrics {
        let (submitted, completed) = db.file_manager.io_batch_counters();
        println!(
            "I/O batch counters [{}]: submitted={} completed={}",
            result.operation, submitted, completed
        );
        if let Some(stats) = db.buffer_manager().stats() {
            let attempted = stats.prefetch_attempted.load(Ordering::Relaxed);
            let installed = stats.prefetch_installed.load(Ordering::Relaxed);
            let discarded = stats.prefetch_discarded.load(Ordering::Relaxed);
            println!(
                "Prefetch counters [{}]: attempted={} installed={} discarded={}",
                result.operation, attempted, installed, discarded
            );
        }
    }
    result
}

fn main() -> Result<(), Box<dyn Error>> {
    let (iterations, num_buffers, json_output, filter) = parse_bench_args();
    let (macro_working_set_blocks, prefetch_window_blocks) = parse_macro_args(num_buffers);
    let filter_ref = filter.as_deref();

    if !json_output {
        println!("SimpleDB Stdlib-Only Benchmark Suite");
        println!("====================================");
        println!("Running benchmarks with {iterations} iterations per operation");
        println!(
            "Environment: {} ({})",
            std::env::consts::OS,
            std::env::consts::ARCH
        );
        println!();
    }

    // Clean up any existing benchmark data
    cleanup_bench_data();

    // Initialize database with clean=true for fresh benchmark runs
    let db = SimpleDB::new("./bench-data", num_buffers, true, 100);

    // Setup test table
    let phase = log_phase_start("setup_test_table", !json_output);
    setup_test_table(&db)?;
    log_phase_end("setup_test_table", phase, !json_output);
    if !json_output {
        println!("Created benchmark table with schema: id (int), name (varchar(20)), age (int)");
    }

    // Populate with initial data
    let phase = log_phase_start("populate_table", !json_output);
    populate_table(&db, 100)?;
    log_phase_end("populate_table", phase, !json_output);
    let phase = log_phase_start("setup_macro_scan_table", !json_output);
    let macro_table_name = setup_macro_scan_table(&db, macro_working_set_blocks)?;
    log_phase_end("setup_macro_scan_table", phase, !json_output);
    if !json_output {
        println!("Populated table with 100 records");
        println!("Buffer pool blocks: {}", num_buffers);
        println!(
            "Prepared macro scan table with {} blocks",
            macro_working_set_blocks
        );
        println!("Macro prefetch window: {}", prefetch_window_blocks);
        println!();
        print_header();
    }

    // Run benchmarks and collect results
    // In JSON mode (CI), ignore filter and run all benchmarks
    let effective_filter = if json_output { None } else { filter_ref };
    let mut results = Vec::new();

    if should_run("INSERT", effective_filter) {
        results.push(run_insert_benchmarks(&db, iterations));
    }

    if should_run("SELECT", effective_filter) {
        results.extend(run_select_benchmarks(&db, iterations));
    }

    if should_run("UPDATE", effective_filter) {
        results.push(run_update_benchmarks(&db, iterations));
    }

    if should_run("DELETE", effective_filter) {
        results.push(run_delete_benchmarks(&db, iterations));
    }
    if should_run("macro_full_scan_none", effective_filter) {
        results.push(run_full_table_scan_macro(
            &db,
            &macro_table_name,
            iterations,
            num_buffers,
            macro_working_set_blocks,
            0,
            !json_output,
        ));
    }
    if should_run("macro_full_scan_prefetch", effective_filter) {
        results.push(run_full_table_scan_macro(
            &db,
            &macro_table_name,
            iterations,
            num_buffers,
            macro_working_set_blocks,
            prefetch_window_blocks,
            !json_output,
        ));
    }

    let filtered_results = results;

    // Output results
    if json_output {
        // Output as JSON array for github-action-benchmark
        let json_results: Vec<String> = filtered_results.iter().map(|r| r.to_json()).collect();
        println!("[{}]", json_results.join(","));
    } else {
        // Output as human-readable table
        for result in &filtered_results {
            println!("{result}");
        }
        println!();
        println!("All benchmarks completed successfully!");
        println!("Note: These results are for educational purposes and system comparison");
    }

    // Cleanup
    cleanup_bench_data();

    Ok(())
}
