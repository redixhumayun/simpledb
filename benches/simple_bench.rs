#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, Criterion};
use simpledb::SimpleDB;
use std::sync::Arc;
use std::time::Duration;

/// Returns (warm_up, measurement) durations. Tighter in CI to keep total run time reasonable.
fn ci_bench_times() -> (Duration, Duration) {
    if std::env::var("CI").is_ok() {
        (Duration::from_secs(1), Duration::from_secs(2))
    } else {
        (Duration::from_secs(3), Duration::from_secs(5))
    }
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

fn bench_insert(c: &mut Criterion) {
    let (db, _dir) = setup_db();
    let (warm_up, measurement) = ci_bench_times();

    let mut group = c.benchmark_group("DML/Insert");
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

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
    let (warm_up, measurement) = ci_bench_times();

    let mut group = c.benchmark_group("SELECT");
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

    group.bench_function("table scan", |b| {
        b.iter(|| {
            let txn = db.new_tx();
            let _plan = db
                .planner
                .create_query_plan(
                    "SELECT id, name FROM bench_table WHERE age > 30".to_string(),
                    Arc::clone(&txn),
                )
                .unwrap();
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
    let (warm_up, measurement) = ci_bench_times();

    let mut group = c.benchmark_group("DML/Update");
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

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
    let (warm_up, measurement) = ci_bench_times();

    let mut group = c.benchmark_group("DML/Delete");
    group.warm_up_time(warm_up);
    group.measurement_time(measurement);

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

criterion_group!(
    benches,
    bench_insert,
    bench_select,
    bench_update,
    bench_delete
);
criterion_main!(benches);
