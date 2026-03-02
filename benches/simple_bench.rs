#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use simpledb::{Layout, Scan, SimpleDB, TableScan, Transaction, UpdateScan};
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// CI config for fast in-memory groups: 1s warmup, 5s measurement, 100 samples.
/// Returns None outside CI, leaving Criterion defaults untouched.
fn ci_fast() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(1), Duration::from_secs(5), 100))
}

fn setup_db() -> (SimpleDB, simpledb::TestDir) {
    let (db, dir) = SimpleDB::new_for_test(64, 100);
    let txn = db.new_tx();
    db.planner
        .execute_update(
            "CREATE TABLE bench_table(id int, name varchar(20), age int)".to_string(),
            Arc::clone(&txn),
        )
        .unwrap();
    txn.commit().unwrap();

    let txn = db.new_tx();
    for i in 0..100 {
        let sql = format!(
            "INSERT INTO bench_table(id, name, age) VALUES ({}, 'user{}', {})",
            i,
            i,
            20 + (i % 50)
        );
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
    }
    txn.commit().unwrap();

    (db, dir)
}

const CONC_TABLE: &str = "bench_lock_table";
const CONC_WORKERS: usize = 4;
const CONC_IDS_PER_WORKER: usize = 8;
const CONC_OPS_PER_WORKER: usize = 24;
const CONC_MAX_RETRIES: usize = 3;

#[derive(Clone)]
struct ConcurrencyRuntime {
    file_manager: Arc<dyn simpledb::FileSystemInterface + Send + Sync + 'static>,
    log_manager: Arc<std::sync::Mutex<simpledb::LogManager>>,
    buffer_manager: Arc<simpledb::BufferManager>,
    lock_table: Arc<simpledb::LockTable>,
    layout: Layout,
    table_id: u32,
}

#[derive(Default, Clone, Copy)]
struct RunStats {
    retries: u64,
    timeouts: u64,
    errors: u64,
}

fn setup_concurrency_runtime() -> (Arc<ConcurrencyRuntime>, simpledb::TestDir) {
    let (db, dir) = SimpleDB::new_for_test(64, 1000);
    let txn = db.new_tx();
    db.planner
        .execute_update(
            format!("CREATE TABLE {CONC_TABLE}(id int, age int)"),
            Arc::clone(&txn),
        )
        .unwrap();
    txn.commit().unwrap();

    let txn = db.new_tx();
    for id in 0..(CONC_WORKERS * CONC_IDS_PER_WORKER) {
        db.planner
            .execute_update(
                format!(
                    "INSERT INTO {CONC_TABLE}(id, age) VALUES ({}, {})",
                    id,
                    20 + (id % 10)
                ),
                Arc::clone(&txn),
            )
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.new_tx();
    let layout = db
        .metadata_manager()
        .get_layout(CONC_TABLE, Arc::clone(&txn));
    let table_id = db
        .metadata_manager()
        .get_table_id(CONC_TABLE, Arc::clone(&txn))
        .unwrap();
    txn.commit().unwrap();

    let runtime = ConcurrencyRuntime {
        file_manager: Arc::clone(&db.file_manager),
        log_manager: db.log_manager(),
        buffer_manager: db.buffer_manager(),
        lock_table: db.lock_table(),
        layout,
        table_id,
    };

    (Arc::new(runtime), dir)
}

fn new_tx(rt: &ConcurrencyRuntime) -> Arc<Transaction> {
    Arc::new(Transaction::new(
        Arc::clone(&rt.file_manager),
        Arc::clone(&rt.log_manager),
        Arc::clone(&rt.buffer_manager),
        Arc::clone(&rt.lock_table),
    ))
}

fn run_select_once(rt: &ConcurrencyRuntime, id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let txn = new_tx(rt);
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut scan = TableScan::new(Arc::clone(&txn), rt.layout.clone(), CONC_TABLE, rt.table_id)?;
        scan.move_to_start();
        while scan.next().is_some() {
            if scan.get_int("id")? == id as i32 {
                let _ = scan.get_int("age")?;
                break;
            }
        }
        txn.commit()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = txn.rollback();
    }
    result
}

fn run_update_once(rt: &ConcurrencyRuntime, id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let txn = new_tx(rt);
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut scan = TableScan::new(Arc::clone(&txn), rt.layout.clone(), CONC_TABLE, rt.table_id)?;
        scan.move_to_start();
        while scan.next().is_some() {
            if scan.get_int("id")? == id as i32 {
                let age = scan.get_int("age")?;
                scan.set_int("age", age + 1)?;
                break;
            }
        }
        txn.commit()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = txn.rollback();
    }
    result
}

fn run_with_retry(
    rt: &ConcurrencyRuntime,
    op: impl Fn(&ConcurrencyRuntime) -> Result<(), Box<dyn std::error::Error>>,
) -> RunStats {
    let mut stats = RunStats::default();
    let mut attempts = 0usize;
    loop {
        attempts += 1;
        match op(rt) {
            Ok(()) => {
                stats.retries = (attempts.saturating_sub(1)) as u64;
                return stats;
            }
            Err(err) => {
                let is_timeout = err.to_string().contains("Timeout");
                if is_timeout {
                    stats.timeouts += 1;
                } else {
                    stats.errors += 1;
                }
                if is_timeout && attempts < CONC_MAX_RETRIES {
                    continue;
                }
                stats.retries = (attempts.saturating_sub(1)) as u64;
                return stats;
            }
        }
    }
}

fn run_concurrent_disjoint_ids(
    rt: &Arc<ConcurrencyRuntime>,
    op: Arc<dyn Fn(&ConcurrencyRuntime, usize, usize) -> RunStats + Send + Sync>,
) -> RunStats {
    let start_barrier = Arc::new(Barrier::new(CONC_WORKERS));
    let mut handles = Vec::with_capacity(CONC_WORKERS);

    for worker in 0..CONC_WORKERS {
        let rt = Arc::clone(rt);
        let barrier = Arc::clone(&start_barrier);
        let op = Arc::clone(&op);
        handles.push(thread::spawn(move || {
            let mut stats = RunStats::default();
            barrier.wait();
            for i in 0..CONC_OPS_PER_WORKER {
                let id = worker * CONC_IDS_PER_WORKER + (i % CONC_IDS_PER_WORKER);
                let s = op(&rt, id, i);
                stats.retries += s.retries;
                stats.timeouts += s.timeouts;
                stats.errors += s.errors;
            }
            stats
        }));
    }

    let mut stats = RunStats::default();
    for handle in handles {
        let s = handle.join().unwrap();
        stats.retries += s.retries;
        stats.timeouts += s.timeouts;
        stats.errors += s.errors;
    }
    stats
}

fn bench_insert(c: &mut Criterion) {
    let (db, _dir) = setup_db();

    let mut group = c.benchmark_group("DML/Insert");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    group.bench_function("INSERT single record", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "INSERT INTO bench_table(id, name, age) VALUES (99999, 'test_user', 25)"
                        .to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();

            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "DELETE FROM bench_table WHERE id = 99999".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();
        })
    });

    group.finish();
}

fn bench_select(c: &mut Criterion) {
    let (db, _dir) = setup_db();

    let mut group = c.benchmark_group("SELECT");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    group.bench_function("table scan", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            let plan = db
                .planner
                .create_query_plan(
                    "SELECT id, name FROM bench_table WHERE age > 30".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            {
                let mut scan = plan.open();
                let _ = scan.by_ref().count();
            }
            txn.commit().unwrap();
        })
    });

    group.bench_function("full scan count", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            let plan = db
                .planner
                .create_query_plan("SELECT * FROM bench_table".to_string(), Arc::clone(&txn))
                .unwrap();
            {
                let mut scan = plan.open();
                let _count = scan.by_ref().count();
            }
            txn.commit().unwrap();
        })
    });

    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let (db, _dir) = setup_db();

    let mut group = c.benchmark_group("DML/Update");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    group.bench_function("UPDATE single record", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "UPDATE bench_table SET age = 99 WHERE id = 0".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();

            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "UPDATE bench_table SET age = 20 WHERE id = 0".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();
        })
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let (db, _dir) = setup_db();

    let mut group = c.benchmark_group("DML/Delete");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    group.bench_function("DELETE single record", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "INSERT INTO bench_table(id, name, age) VALUES (88888, 'delete_me', 25)"
                        .to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();

            let txn = db.new_tx();
            db.planner
                .execute_update(
                    "DELETE FROM bench_table WHERE id = 88888".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
            txn.commit().unwrap();
        })
    });

    group.finish();
}

fn bench_sql_concurrency(c: &mut Criterion) {
    let (rt, _dir) = setup_concurrency_runtime();
    let mut group = c.benchmark_group("SQL Concurrency");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }
    group.throughput(Throughput::Elements(
        (CONC_WORKERS * CONC_OPS_PER_WORKER) as u64,
    ));

    group.bench_function("Concurrent SELECT same-page disjoint-id", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut total = RunStats::default();
            let op: Arc<dyn Fn(&ConcurrencyRuntime, usize, usize) -> RunStats + Send + Sync> =
                Arc::new(|rt, id, _op_idx| run_with_retry(rt, |rt| run_select_once(rt, id)));
            for _ in 0..iters {
                let s = run_concurrent_disjoint_ids(&rt, Arc::clone(&op));
                total.retries += s.retries;
                total.timeouts += s.timeouts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=select_same_page_disjoint_id iters={iters} retries={} timeouts={} errors={}",
                total.retries, total.timeouts, total.errors
            );
            start.elapsed()
        });
    });

    group.bench_function("Concurrent UPDATE same-page disjoint-id", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut total = RunStats::default();
            let op: Arc<dyn Fn(&ConcurrencyRuntime, usize, usize) -> RunStats + Send + Sync> =
                Arc::new(|rt, id, _op_idx| run_with_retry(rt, |rt| run_update_once(rt, id)));
            for _ in 0..iters {
                let s = run_concurrent_disjoint_ids(&rt, Arc::clone(&op));
                total.retries += s.retries;
                total.timeouts += s.timeouts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=update_same_page_disjoint_id iters={iters} retries={} timeouts={} errors={}",
                total.retries, total.timeouts, total.errors
            );
            start.elapsed()
        });
    });

    group.bench_function("Concurrent mixed 80/20 RW same-page disjoint-id", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut total = RunStats::default();
            let op: Arc<dyn Fn(&ConcurrencyRuntime, usize, usize) -> RunStats + Send + Sync> =
                Arc::new(|rt, id, op_idx| {
                    run_with_retry(rt, |rt| {
                        let mixed_probe = op_idx % 5;
                        if mixed_probe == 0 {
                            run_update_once(rt, id)
                        } else {
                            run_select_once(rt, id)
                        }
                    })
                });
            for _ in 0..iters {
                let s = run_concurrent_disjoint_ids(&rt, Arc::clone(&op));
                total.retries += s.retries;
                total.timeouts += s.timeouts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=mixed_80_20_same_page_disjoint_id iters={iters} retries={} timeouts={} errors={}",
                total.retries, total.timeouts, total.errors
            );
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_select,
    bench_update,
    bench_delete,
    bench_sql_concurrency
);
criterion_main!(benches);
