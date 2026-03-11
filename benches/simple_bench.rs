#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use simpledb::{
    BTreeIndex, Constant, Index, Layout, LockError, Scan, SimpleDB, SplitGate, TableScan,
    Transaction, UpdateScan, RID,
};
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

/// CI config for heavy index concurrency macros: keep the same timing window,
/// but use fewer samples so the suite finishes in a practical amount of time.
fn ci_index() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(1), Duration::from_secs(5), 25))
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
    aborts: u64,
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
        let mut scan =
            TableScan::new(Arc::clone(&txn), rt.layout.clone(), CONC_TABLE, rt.table_id)?;
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
        let mut scan =
            TableScan::new(Arc::clone(&txn), rt.layout.clone(), CONC_TABLE, rt.table_id)?;
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
                match err.downcast_ref::<LockError>() {
                    Some(LockError::Timeout) => {
                        stats.timeouts += 1;
                        if attempts < CONC_MAX_RETRIES {
                            continue;
                        }
                    }
                    Some(LockError::WaitDieAbort) => {
                        stats.aborts += 1;
                        if attempts < CONC_MAX_RETRIES {
                            continue;
                        }
                    }
                    None => stats.errors += 1,
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
                stats.aborts += s.aborts;
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
        stats.aborts += s.aborts;
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
                total.aborts += s.aborts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=select_same_page_disjoint_id iters={iters} retries={} timeouts={} aborts={} errors={}",
                total.retries, total.timeouts, total.aborts, total.errors
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
                total.aborts += s.aborts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=update_same_page_disjoint_id iters={iters} retries={} timeouts={} aborts={} errors={}",
                total.retries, total.timeouts, total.aborts, total.errors
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
                total.aborts += s.aborts;
                total.errors += s.errors;
            }
            eprintln!(
                "bench=mixed_80_20_same_page_disjoint_id iters={iters} retries={} timeouts={} aborts={} errors={}",
                total.retries, total.timeouts, total.aborts, total.errors
            );
            start.elapsed()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Index concurrency benchmark
// ---------------------------------------------------------------------------

const IDX_BENCH_TABLE: &str = "idx_bench";
const IDX_BENCH_INDEX: &str = "idx_bench_id";

#[derive(Clone)]
struct IndexConcurrencyRuntime {
    file_manager: Arc<dyn simpledb::FileSystemInterface + Send + Sync + 'static>,
    log_manager: Arc<std::sync::Mutex<simpledb::LogManager>>,
    buffer_manager: Arc<simpledb::BufferManager>,
    lock_table: Arc<simpledb::LockTable>,
    index_name: String,
    leaf_layout: Layout,
    indexed_table_id: u32,
    split_gate: Arc<SplitGate>,
}

fn new_idx_txn(rt: &IndexConcurrencyRuntime) -> Arc<Transaction> {
    Arc::new(Transaction::new(
        Arc::clone(&rt.file_manager),
        Arc::clone(&rt.log_manager),
        Arc::clone(&rt.buffer_manager),
        Arc::clone(&rt.lock_table),
    ))
}

fn setup_index_concurrency_runtime() -> (Arc<IndexConcurrencyRuntime>, simpledb::TestDir) {
    let (db, dir) = SimpleDB::new_for_test(64, 5000);

    let txn = db.new_tx();
    db.planner
        .execute_update(
            format!("CREATE TABLE {IDX_BENCH_TABLE}(id int, val int)"),
            Arc::clone(&txn),
        )
        .unwrap();
    txn.commit().unwrap();

    let txn = db.new_tx();
    db.planner
        .execute_update(
            format!("CREATE INDEX {IDX_BENCH_INDEX} ON {IDX_BENCH_TABLE}(id)"),
            Arc::clone(&txn),
        )
        .unwrap();
    txn.commit().unwrap();

    // Pre-populate 200 rows so lookups always find data.
    let txn = db.new_tx();
    for id in 0..200_i32 {
        db.planner
            .execute_update(
                format!(
                    "INSERT INTO {IDX_BENCH_TABLE}(id, val) VALUES ({id}, {})",
                    id * 10
                ),
                Arc::clone(&txn),
            )
            .unwrap();
    }
    txn.commit().unwrap();

    // Extract index metadata for direct BTreeIndex construction in worker threads.
    let txn = db.new_tx();
    let index_info = db
        .metadata_manager()
        .get_index_info(IDX_BENCH_TABLE, Arc::clone(&txn))
        .remove("id")
        .expect("index on 'id' should exist");
    let index_name = index_info.index_name().to_string();
    let leaf_layout = index_info.index_layout().clone();
    let indexed_table_id = index_info.indexed_table_id();
    txn.commit().unwrap();

    let runtime = IndexConcurrencyRuntime {
        file_manager: Arc::clone(&db.file_manager),
        log_manager: db.log_manager(),
        buffer_manager: db.buffer_manager(),
        lock_table: db.lock_table(),
        index_name,
        leaf_layout,
        indexed_table_id,
        split_gate: Arc::new(SplitGate::new()),
    };

    (Arc::new(runtime), dir)
}

/// Drive CONC_WORKERS threads, each calling `op(rt, worker_id, op_idx)` for
/// CONC_OPS_PER_WORKER iterations, all starting behind a Barrier.
fn run_idx_concurrent(
    rt: &Arc<IndexConcurrencyRuntime>,
    op: Arc<dyn Fn(&IndexConcurrencyRuntime, usize, usize) + Send + Sync>,
) {
    let start_barrier = Arc::new(Barrier::new(CONC_WORKERS));
    let mut handles = Vec::with_capacity(CONC_WORKERS);

    for worker in 0..CONC_WORKERS {
        let rt = Arc::clone(rt);
        let barrier = Arc::clone(&start_barrier);
        let op = Arc::clone(&op);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..CONC_OPS_PER_WORKER {
                op(&rt, worker, i);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

fn bench_index_concurrency(c: &mut Criterion) {
    let mut group = c.benchmark_group("Index Concurrency");
    if let Some((wu, mt, ss)) = ci_index() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }
    group.throughput(Throughput::Elements(
        (CONC_WORKERS * CONC_OPS_PER_WORKER) as u64,
    ));

    group.bench_function("Concurrent INSERT disjoint-key", |b| {
        let op: Arc<dyn Fn(&IndexConcurrencyRuntime, usize, usize) + Send + Sync> =
            Arc::new(|rt, worker, op_idx| {
                let key = (1000 + worker * CONC_OPS_PER_WORKER + op_idx) as i32;
                let txn = new_idx_txn(rt);
                let mut idx = BTreeIndex::new(
                    Arc::clone(&txn),
                    &rt.index_name,
                    rt.leaf_layout.clone(),
                    rt.indexed_table_id,
                    Arc::clone(&rt.split_gate),
                )
                .unwrap();
                idx.insert(&Constant::Int(key), &RID::new(0, key as usize));
                txn.commit().unwrap();
            });
        b.iter_batched(
            setup_index_concurrency_runtime,
            |(rt, _dir)| {
                run_idx_concurrent(&rt, Arc::clone(&op));
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("Concurrent LOOKUP pre-populated", |b| {
        let op: Arc<dyn Fn(&IndexConcurrencyRuntime, usize, usize) + Send + Sync> =
            Arc::new(|rt, worker, op_idx| {
                let key = ((worker * CONC_OPS_PER_WORKER + op_idx) % 200) as i32;
                let txn = new_idx_txn(rt);
                let mut idx = BTreeIndex::new(
                    Arc::clone(&txn),
                    &rt.index_name,
                    rt.leaf_layout.clone(),
                    rt.indexed_table_id,
                    Arc::clone(&rt.split_gate),
                )
                .unwrap();
                idx.before_first(&Constant::Int(key));
                let _ = idx.next();
                txn.commit().unwrap();
            });
        b.iter_batched(
            setup_index_concurrency_runtime,
            |(rt, _dir)| {
                run_idx_concurrent(&rt, Arc::clone(&op));
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("Concurrent mixed 80/20 RW", |b| {
        let op: Arc<dyn Fn(&IndexConcurrencyRuntime, usize, usize) + Send + Sync> =
            Arc::new(|rt, worker, op_idx| {
                let txn = new_idx_txn(rt);
                let mut idx = BTreeIndex::new(
                    Arc::clone(&txn),
                    &rt.index_name,
                    rt.leaf_layout.clone(),
                    rt.indexed_table_id,
                    Arc::clone(&rt.split_gate),
                )
                .unwrap();
                if op_idx % 5 == 0 {
                    // 20% writes
                    let key = (1000 + worker * CONC_OPS_PER_WORKER + op_idx) as i32;
                    idx.insert(&Constant::Int(key), &RID::new(0, key as usize));
                } else {
                    // 80% reads
                    let key = ((worker * CONC_OPS_PER_WORKER + op_idx) % 200) as i32;
                    idx.before_first(&Constant::Int(key));
                    let _ = idx.next();
                }
                txn.commit().unwrap();
            });
        b.iter_batched(
            setup_index_concurrency_runtime,
            |(rt, _dir)| {
                run_idx_concurrent(&rt, Arc::clone(&op));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_select,
    bench_update,
    bench_delete,
    bench_sql_concurrency,
    bench_index_concurrency
);
criterion_main!(benches);
