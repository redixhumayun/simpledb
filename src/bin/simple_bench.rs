use std::env;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use simpledb::SimpleDB;

// Include our benchmark framework
mod benchmark_framework {
    include!("../../benches/benchmark_framework.rs");
}

use benchmark_framework::{benchmark, print_header};

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

fn run_insert_benchmarks(db: &SimpleDB, iterations: usize) {
    // Benchmark single INSERT operations
    let result = benchmark("INSERT (single record)", iterations, || {
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
    });
    println!("{}", result);
}

fn run_select_benchmarks(db: &SimpleDB, iterations: usize) {
    // Benchmark SELECT operations
    let result = benchmark("SELECT (table scan)", iterations, || {
        let txn = Arc::new(db.new_tx());
        let select_sql = "SELECT id, name FROM bench_table WHERE age > 30";
        let _plan = db
            .planner
            .create_query_plan(select_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        txn.commit().unwrap();
    });
    println!("{}", result);

    let result = benchmark("SELECT COUNT(*)", iterations, || {
        let txn = Arc::new(db.new_tx());
        let select_sql = "SELECT * FROM bench_table";
        let plan = db
            .planner
            .create_query_plan(select_sql.to_string(), Arc::clone(&txn))
            .unwrap();
        {
            let mut scan = plan.open();
            let mut _count = 0;
            while let Some(_) = scan.next() {
                _count += 1;
            }
            scan.close();
        } // scan is dropped here, before transaction commit
        txn.commit().unwrap();
    });
    println!("{}", result);
}

fn run_update_benchmarks(db: &SimpleDB, iterations: usize) {
    // Benchmark UPDATE operations
    let result = benchmark("UPDATE (single record)", iterations, || {
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
    });
    println!("{}", result);
}

fn run_delete_benchmarks(db: &SimpleDB, iterations: usize) {
    // Benchmark DELETE operations
    let result = benchmark("DELETE (single record)", iterations, || {
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
    });
    println!("{}", result);
}

fn parse_iterations() -> usize {
    let args: Vec<String> = env::args().collect();

    match args.len() {
        1 => 10, // No args, use default
        2 => args[1].parse().unwrap_or_else(|_| {
            eprintln!(
                "Warning: Invalid number '{}', using default 10 iterations",
                args[1]
            );
            10
        }),
        _ => {
            eprintln!("Usage: {} [iterations]", args[0]);
            eprintln!("Using default 10 iterations");
            10
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let iterations = parse_iterations();

    println!("SimpleDB Stdlib-Only Benchmark Suite");
    println!("====================================");
    println!(
        "Running benchmarks with {} iterations per operation",
        iterations
    );
    println!("Environment: {} ({})", std::env::consts::OS, std::env::consts::ARCH);
    println!();

    // Clean up any existing benchmark data
    cleanup_bench_data();

    // Initialize database with clean=true for fresh benchmark runs
    let db = SimpleDB::new("./bench-data", 1024, 64, true);

    // Setup test table
    setup_test_table(&db)?;
    println!("Created benchmark table with schema: id (int), name (varchar(20)), age (int)");

    // Populate with initial data
    populate_table(&db, 100)?;
    println!("Populated table with 100 records");
    println!();

    print_header();

    // Run benchmarks
    run_insert_benchmarks(&db, iterations);
    run_select_benchmarks(&db, iterations);
    run_update_benchmarks(&db, iterations);
    run_delete_benchmarks(&db, iterations);

    println!();
    println!("All benchmarks completed successfully!");
    println!("Note: These results are for educational purposes and system comparison");

    // Cleanup
    cleanup_bench_data();

    Ok(())
}
