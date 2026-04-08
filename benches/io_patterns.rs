#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use simpledb::FileSystemInterface;
use simpledb::{
    direct_io_fallback_count, test_utils::generate_random_number, BlockId, Page, SimpleDB, TestDir,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type BenchFS = Arc<dyn FileSystemInterface + Send + Sync + 'static>;
type Lsn = usize;

#[derive(Clone, Debug)]
enum WALFlushPolicy {
    None,
    Immediate,
    Group {
        batch: usize,
        pending: usize,
        last_lsn: Option<Lsn>,
    },
}

impl WALFlushPolicy {
    fn record(&mut self, lsn: Lsn, log: &Arc<Mutex<simpledb::LogManager>>) {
        match self {
            WALFlushPolicy::None => {}
            WALFlushPolicy::Immediate => {
                log.lock().unwrap().flush_lsn(lsn);
            }
            WALFlushPolicy::Group {
                batch,
                pending,
                last_lsn,
            } => {
                *pending += 1;
                *last_lsn = Some(lsn);
                if *pending == *batch {
                    log.lock().unwrap().flush_lsn(last_lsn.unwrap());
                    *pending = 0;
                }
            }
        }
    }

    fn finish_batch(&mut self, log: &Arc<Mutex<simpledb::LogManager>>) {
        if let WALFlushPolicy::Group {
            last_lsn, pending, ..
        } = self
        {
            if *pending > 0 {
                if let Some(lsn) = *last_lsn {
                    log.lock().unwrap().flush_lsn(lsn);
                }
                *pending = 0;
            }
        }
    }
}

#[derive(Clone, Debug)]
enum DataSyncPolicy {
    None,
    Immediate,
}

impl DataSyncPolicy {
    fn record(&mut self, file: &str, fm: &BenchFS) {
        if let DataSyncPolicy::Immediate = self {
            fm.sync(file);
            fm.sync_directory();
        }
    }
}

fn num_buffers() -> usize {
    std::env::var("SIMPLEDB_BENCH_BUFFERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|n: usize| n.max(1))
        .unwrap_or(12)
}

fn setup_io_test() -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(num_buffers(), 5000)
}

fn precreate_blocks(db: &SimpleDB, file: &str, count: usize) {
    for block_num in 0..count {
        let mut page = Page::new();
        write_i32_at(page.bytes_mut(), 60, block_num as i32);
        db.file_manager
            .write(&BlockId::new(file.to_string(), block_num), &page);
    }
}

fn write_i32_at(bytes: &mut [u8], offset: usize, value: i32) {
    let le = value.to_le_bytes();
    bytes[offset..offset + 4].copy_from_slice(&le);
}

fn make_wal_record(size: usize) -> Vec<u8> {
    vec![0u8; size]
}

fn working_set_blocks() -> usize {
    std::env::var("SIMPLEDB_BENCH_WORKING_SET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

/// CI config for fsync/durability groups: 3s warmup, 15s measurement, 10 samples.
fn ci_fsync() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(3), Duration::from_secs(15), 10))
}

fn touch_block(
    txn: &Arc<simpledb::Transaction>,
    file: &str,
    block_num: usize,
    value: i32,
    log: &Arc<Mutex<simpledb::LogManager>>,
) {
    let block_id = BlockId::new(file.to_string(), block_num);
    let ws = txn.write_session();
    let mut guard = ws.pin_write_guard(&block_id).unwrap();
    write_i32_at(guard.bytes_mut(), 60, value);
    let lsn = log.lock().unwrap().append(make_wal_record(100)).unwrap();
    guard.mark_modified(txn.id(), lsn);
}

fn bench_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("Phase2/WAL");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    {
        let (db, _dir) = setup_io_test();
        let log = db.log_manager();
        let total_ops = 1000usize;
        let mut policy = WALFlushPolicy::None;

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_function("append no-fsync", |b| {
            b.iter(|| {
                for _ in 0..total_ops {
                    let record = make_wal_record(100);
                    let lsn = log.lock().unwrap().append(record).unwrap();
                    policy.record(lsn, &log);
                }
                policy.finish_batch(&log);
            })
        });
    }

    {
        let (db, _dir) = setup_io_test();
        let log = db.log_manager();
        let total_ops = 100usize;
        let mut policy = WALFlushPolicy::Immediate;

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_function("append immediate-fsync", |b| {
            b.iter(|| {
                for _ in 0..total_ops {
                    let record = make_wal_record(100);
                    let lsn = log.lock().unwrap().append(record).unwrap();
                    policy.record(lsn, &log);
                }
                policy.finish_batch(&log);
            })
        });
    }

    {
        let batch_size = 10usize;
        let (db, _dir) = setup_io_test();
        let log = db.log_manager();
        let total_ops = 1000usize;
        let mut policy = WALFlushPolicy::Group {
            batch: batch_size,
            pending: 0,
            last_lsn: None,
        };

        group.throughput(Throughput::Elements(total_ops as u64));
        group.bench_with_input(
            BenchmarkId::new("group commit", batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    for _ in 0..total_ops {
                        let record = make_wal_record(100);
                        let lsn = log.lock().unwrap().append(record).unwrap();
                        policy.record(lsn, &log);
                    }
                    policy.finish_batch(&log);
                })
            },
        );
    }

    group.finish();
}

// Focus on writeback-heavy engine behavior rather than raw filesystem throughput.
fn bench_writeback(c: &mut Criterion) {
    let nb = num_buffers();
    let ws = working_set_blocks().max(nb * 8);
    let mut group = c.benchmark_group("Phase3/Writeback");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    {
        let (db, _dir) = setup_io_test();
        let file = format!("writeback_stream_{ws}_{nb}");
        precreate_blocks(&db, &file, ws);
        let log = db.log_manager();
        let mut next_block = 0usize;
        let pages_per_txn = nb.max(1);

        group.throughput(Throughput::Elements(pages_per_txn as u64));
        group.bench_function("stream fresh pages", |b| {
            b.iter(|| {
                let txn = db.new_tx();
                for i in 0..pages_per_txn {
                    let block_num = (next_block + i) % ws;
                    touch_block(&txn, &file, block_num, (next_block + i) as i32, &log);
                }
                txn.write_session().commit().unwrap();
                next_block = (next_block + pages_per_txn) % ws;
            })
        });
    }

    {
        let (db, _dir) = setup_io_test();
        let file = format!("writeback_overfull_{ws}_{nb}");
        precreate_blocks(&db, &file, ws * 2);
        let log = db.log_manager();
        let mut next_block = 0usize;
        let pages_per_txn = (nb * 2).max(2);

        group.throughput(Throughput::Elements(pages_per_txn as u64));
        group.bench_function("overfull transaction", |b| {
            b.iter(|| {
                let txn = db.new_tx();
                for i in 0..pages_per_txn {
                    let block_num = (next_block + i) % (ws * 2);
                    touch_block(&txn, &file, block_num, (next_block + i) as i32, &log);
                }
                txn.write_session().commit().unwrap();
                next_block = (next_block + pages_per_txn) % (ws * 2);
            })
        });
    }

    {
        let (db, _dir) = setup_io_test();
        let file = format!("writeback_redirty_{ws}_{nb}");
        precreate_blocks(&db, &file, ws * 2);
        let log = db.log_manager();
        let hot_pages = (nb / 4).max(1);
        let cold_pages = nb.max(1);
        let cold_span = (ws * 2).saturating_sub(hot_pages).max(1);
        let mut next_cold = 0usize;

        group.throughput(Throughput::Elements((hot_pages + cold_pages) as u64));
        group.bench_function("hot re-dirty plus stream", |b| {
            b.iter(|| {
                let txn = db.new_tx();
                for i in 0..hot_pages {
                    touch_block(&txn, &file, i, generate_random_number() as i32, &log);
                }
                for i in 0..cold_pages {
                    let block_num = hot_pages + ((next_cold + i) % cold_span);
                    touch_block(&txn, &file, block_num, block_num as i32, &log);
                }
                txn.write_session().commit().unwrap();
                next_cold = (next_cold + cold_pages) % cold_span;
            })
        });
    }

    group.finish();
}

fn bench_durability(c: &mut Criterion) {
    let ws = working_set_blocks();
    let durability_ops = ws.min(100);

    let mut group = c.benchmark_group("Phase5/Durability");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    for (wal_template, wal_name) in [(
        WALFlushPolicy::Immediate,
        "Random Write durability immediate-fsync",
    )] {
        for (data_template, data_name) in [
            (DataSyncPolicy::None, "data-nosync"),
            (DataSyncPolicy::Immediate, "data-fsync"),
        ] {
            let (db, _dir) = setup_io_test();
            let file = format!("randwrite_durable_{ws}_{durability_ops}");
            precreate_blocks(&db, &file, ws);
            let log = db.log_manager();

            group.bench_function(format!("{wal_name} {data_name}"), |b| {
                b.iter(|| {
                    let mut page = Page::new();
                    let mut wp = wal_template.clone();
                    let mut dp = data_template.clone();
                    let fm = Arc::clone(&db.file_manager);

                    for i in 0..durability_ops {
                        let block_num = generate_random_number() % ws;
                        let block_id = BlockId::new(file.clone(), block_num);
                        write_i32_at(page.bytes_mut(), 60, i as i32);
                        fm.write(&block_id, &page);
                        let record = make_wal_record(100);
                        let lsn = log.lock().unwrap().append(record).unwrap();
                        wp.record(lsn, &log);
                        dp.record(&file, &fm);
                    }
                    wp.finish_batch(&log);
                })
            });
        }
    }

    group.finish();
}

fn report_direct_io(_c: &mut Criterion) {
    let fallbacks = direct_io_fallback_count();
    if cfg!(feature = "direct-io") && fallbacks > 0 {
        eprintln!(
            "Direct I/O fallbacks: {} (some files fell back to buffered mode)",
            fallbacks
        );
    }
}

criterion_group!(
    benches,
    bench_wal,
    bench_writeback,
    bench_durability,
    report_direct_io,
);
criterion_main!(benches);
