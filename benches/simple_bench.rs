#![allow(clippy::arc_with_non_send_sync)]

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use simpledb::SimpleDB;

use simpledb::benchmark_framework;

use benchmark_framework::{benchmark, parse_bench_args, print_header, should_run};

fn cleanup_bench_data() {
    let bench_path = Path::new("./bench-data");
    if bench_path.exists() {
        std::fs::remove_dir_all(bench_path).ok();
    }
}

fn setup_test_table(db: &SimpleDB) -> Result<(), Box<dyn Error>> {
    let txn = Arc::new(db.new_tx());

    // Create the table using SQL
    let create_sql = "CREATE TABLE bench_table(id int, name varchar(20), age int)";
    db.planner
        .execute_update(create_sql.to_string(), Arc::clone(&txn))?;

    txn.commit()?;
    Ok(())
}

fn populate_table(db: &SimpleDB, num_records: usize) -> Result<(), Box<dyn Error>> {
    let txn = Arc::new(db.new_tx());

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
        let txn = Arc::new(db.new_tx());
        let insert_sql = "INSERT INTO bench_table(id, name, age) VALUES (99999, 'test_user', 25)";
        db.planner
            .execute_update(insert_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Clean up the inserted record
        let txn = Arc::new(db.new_tx());
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
        let txn = Arc::new(db.new_tx());
        let select_sql = "SELECT id, name FROM bench_table WHERE age > 30";
        let _plan = db
            .planner
            .create_query_plan(select_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    });

    let result2 = benchmark("SELECT COUNT(*)", iterations, 2, || {
        let txn = Arc::new(db.new_tx());
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
        let txn = Arc::new(db.new_tx());
        let update_sql = "UPDATE bench_table SET age = 99 WHERE id = 0";
        db.planner
            .execute_update(update_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Reset the record
        let txn = Arc::new(db.new_tx());
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
        let txn = Arc::new(db.new_tx());
        let insert_sql = "INSERT INTO bench_table(id, name, age) VALUES (88888, 'delete_me', 25)";
        db.planner
            .execute_update(insert_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();

        // Delete the record
        let txn = Arc::new(db.new_tx());
        let delete_sql = "DELETE FROM bench_table WHERE id = 88888";
        db.planner
            .execute_update(delete_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let (iterations, _num_buffers, json_output, filter) = parse_bench_args();
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
    let db = SimpleDB::new("./bench-data", 1024, 64, true, 100);

    // Setup test table
    setup_test_table(&db)?;
    if !json_output {
        println!("Created benchmark table with schema: id (int), name (varchar(20)), age (int)");
    }

    // Populate with initial data
    populate_table(&db, 100)?;
    if !json_output {
        println!("Populated table with 100 records");
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
