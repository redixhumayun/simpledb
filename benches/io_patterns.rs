#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use simpledb::FileSystemInterface;
use simpledb::{
    direct_io_fallback_count, test_utils::generate_random_number, BatchReadReq, BlockId, Page,
    SimpleDB, TestDir,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

type BenchFS = Arc<dyn FileSystemInterface + Send + Sync + 'static>;
type Lsn = usize;

// ============================================================================
// WALFlushPolicy / DataSyncPolicy
// ============================================================================

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

// ============================================================================
// Setup helpers
// ============================================================================

fn setup_io_test() -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(12, 5000)
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

struct FastRng(u64);

impl FastRng {
    fn new() -> Self {
        Self(generate_random_number() as u64 | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_range(&mut self, n: usize) -> usize {
        (self.next_u64() as usize) % n
    }
}

fn working_set_blocks() -> usize {
    std::env::var("SIMPLEDB_BENCH_WORKING_SET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

/// CI config for fast in-memory / buffered-IO groups: 1s warmup, 5s measurement, 100 samples.
/// Returns None outside CI, leaving Criterion defaults untouched.
fn ci_fast() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(1), Duration::from_secs(5), 100))
}

/// CI config for thread-contention groups: 2s warmup, 8s measurement, 50 samples.
fn ci_contention() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(2), Duration::from_secs(8), 50))
}

/// CI config for fsync/durability groups: 3s warmup, 15s measurement, 20 samples.
fn ci_fsync() -> Option<(Duration, Duration, usize)> {
    std::env::var("CI")
        .ok()
        .map(|_| (Duration::from_secs(3), Duration::from_secs(15), 20))
}

#[cfg(target_os = "linux")]
fn posix_fadvise_dontneed(path: &std::path::Path) {
    use std::os::unix::io::AsRawFd;
    if let Ok(f) = std::fs::OpenOptions::new().read(true).open(path) {
        let _ = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    }
}

// ============================================================================
// Phase 1: Sequential vs Random I/O
// ============================================================================

fn bench_phase1_io(c: &mut Criterion) {
    let ws = working_set_blocks();
    let total_ops = ws.min(1000);

    let mut group = c.benchmark_group("Phase1/IO Throughput");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // Sequential Read
    {
        let (db, _dir) = setup_io_test();
        let file = format!("seqread_{ws}");
        precreate_blocks(&db, &file, ws);

        group.bench_function(&format!("Sequential Read ({total_ops} ops)"), |b| {
            b.iter(|| {
                let mut page = Page::new();
                for i in 0..total_ops {
                    let block_id = BlockId::new(file.clone(), i % ws);
                    db.file_manager.read(&block_id, &mut page);
                }
            })
        });
    }

    // Sequential Write
    {
        let (db, _dir) = setup_io_test();
        let file = format!("seqwrite_{ws}");
        precreate_blocks(&db, &file, ws);

        group.bench_function(&format!("Sequential Write ({total_ops} ops)"), |b| {
            b.iter(|| {
                let mut page = Page::new();
                for i in 0..total_ops {
                    write_i32_at(page.bytes_mut(), 60, i as i32);
                    let block_id = BlockId::new(file.clone(), i % ws);
                    db.file_manager.write(&block_id, &page);
                }
            })
        });
    }

    // Random Read
    {
        let (db, _dir) = setup_io_test();
        let file = format!("randread_{ws}");
        precreate_blocks(&db, &file, ws);
        let mut rng = FastRng::new();

        group.bench_function(&format!("Random Read ({total_ops} ops)"), |b| {
            b.iter(|| {
                let indices: Vec<usize> = (0..total_ops).map(|_| rng.next_range(ws)).collect();
                let mut page = Page::new();
                for &idx in &indices {
                    let block_id = BlockId::new(file.clone(), idx);
                    db.file_manager.read(&block_id, &mut page);
                }
            })
        });
    }

    // Random Write
    {
        let (db, _dir) = setup_io_test();
        let file = format!("randwrite_{ws}");
        precreate_blocks(&db, &file, ws);
        let mut rng = FastRng::new();

        group.bench_function(&format!("Random Write ({total_ops} ops)"), |b| {
            b.iter(|| {
                let indices: Vec<usize> = (0..total_ops).map(|_| rng.next_range(ws)).collect();
                let mut page = Page::new();
                for (i, &idx) in indices.iter().enumerate() {
                    write_i32_at(page.bytes_mut(), 60, i as i32);
                    let block_id = BlockId::new(file.clone(), idx);
                    db.file_manager.write(&block_id, &page);
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Phase 1: Queue Depth variants
// ============================================================================

fn bench_phase1_qd(c: &mut Criterion) {
    let ws = working_set_blocks();
    let total_ops = ws.min(1000);

    let mut group = c.benchmark_group("Phase1/Queue Depth");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }
    group.throughput(Throughput::Elements(total_ops as u64));

    for qd in [1usize, 4, 16, 32] {
        // Sequential Read QD
        {
            let (db, _dir) = setup_io_test();
            let file = format!("seqread_qd{qd}_{ws}");
            precreate_blocks(&db, &file, ws);

            group.bench_with_input(BenchmarkId::new("Sequential Read QD", qd), &qd, |b, &qd| {
                b.iter(|| {
                    let mut done = 0usize;
                    while done < total_ops {
                        let n = (total_ops - done).min(qd.max(1));
                        let mut reqs = Vec::with_capacity(n);
                        let mut pages = Vec::with_capacity(n);
                        for j in 0..n {
                            reqs.push(BatchReadReq {
                                block_id: BlockId::new(file.clone(), (done + j) % ws),
                            });
                            pages.push(Page::new());
                        }
                        db.file_manager.read_batch(&reqs, &mut pages);
                        done += n;
                    }
                })
            });
        }

        // Random Read QD
        {
            let (db, _dir) = setup_io_test();
            let file = format!("randread_qd{qd}_{ws}");
            precreate_blocks(&db, &file, ws);
            let mut rng = FastRng::new();

            group.bench_with_input(BenchmarkId::new("Random Read QD", qd), &qd, |b, &qd| {
                b.iter(|| {
                    let indices: Vec<usize> = (0..total_ops).map(|_| rng.next_range(ws)).collect();
                    let mut done = 0usize;
                    while done < total_ops {
                        let n = (total_ops - done).min(qd.max(1));
                        let mut reqs = Vec::with_capacity(n);
                        let mut pages = Vec::with_capacity(n);
                        for j in 0..n {
                            reqs.push(BatchReadReq {
                                block_id: BlockId::new(file.clone(), indices[done + j]),
                            });
                            pages.push(Page::new());
                        }
                        db.file_manager.read_batch(&reqs, &mut pages);
                        done += n;
                    }
                })
            });
        }

        // Multi-stream Scan QD
        {
            let num_streams = 4usize;
            let (db, _dir) = setup_io_test();
            let blocks_per_stream = (ws / num_streams).max(1);
            let file_names: Vec<String> = (0..num_streams)
                .map(|s| format!("multi_stream_qd{qd}_{num_streams}_{s}"))
                .collect();
            for name in &file_names {
                precreate_blocks(&db, name, blocks_per_stream);
            }

            group.bench_with_input(
                BenchmarkId::new("Multi-stream Scan QD", qd),
                &qd,
                |b, &qd| {
                    b.iter(|| {
                        for file in &file_names {
                            let mut done = 0usize;
                            while done < blocks_per_stream {
                                let n = (blocks_per_stream - done).min(qd.max(1));
                                let mut reqs = Vec::with_capacity(n);
                                let mut pages = Vec::with_capacity(n);
                                for j in 0..n {
                                    reqs.push(BatchReadReq {
                                        block_id: BlockId::new(file.clone(), done + j),
                                    });
                                    pages.push(Page::new());
                                }
                                db.file_manager.read_batch(&reqs, &mut pages);
                                done += n;
                            }
                        }
                    })
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Phase 2: WAL Performance
// ============================================================================

fn bench_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("Phase2/WAL");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // WAL append no fsync (1000 ops)
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

    // WAL append immediate fsync (100 ops — fsync is slow)
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

    // WAL group commit (1000 ops, batch sizes 10/50/100)
    for batch_size in [10usize, 50, 100] {
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

// ============================================================================
// Phase 3: Mixed Read/Write Workloads
// ============================================================================

fn bench_mixed(c: &mut Criterion) {
    let ws = working_set_blocks();
    let mixed_ops = (ws / 2).clamp(1, 500);

    let mut group = c.benchmark_group("Phase3/Mixed R/W");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }
    group.throughput(Throughput::Elements(mixed_ops as u64));

    for read_pct in [70usize, 50, 10] {
        for (policy_template, policy_name) in [
            (WALFlushPolicy::None, "no-fsync"),
            (WALFlushPolicy::Immediate, "immediate-fsync"),
            (
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
                "group-10",
            ),
        ] {
            let (db, _dir) = setup_io_test();
            let file = format!("mixedfile_{ws}_{mixed_ops}");
            precreate_blocks(&db, &file, ws);
            let log = db.log_manager();
            let mut rng = FastRng::new();
            let policy = policy_template.clone();

            group.bench_function(
                format!("Mixed {read_pct}/{}/{policy_name}", 100 - read_pct),
                |b| {
                    b.iter(|| {
                        let ops: Vec<bool> = (0..mixed_ops)
                            .map(|_| rng.next_range(100) < read_pct)
                            .collect();
                        let block_indices: Vec<usize> =
                            (0..mixed_ops).map(|_| rng.next_range(ws)).collect();
                        let mut page = Page::new();
                        let mut p = policy.clone();

                        for (i, &is_read) in ops.iter().enumerate() {
                            let block_id = BlockId::new(file.clone(), block_indices[i]);
                            if is_read {
                                db.file_manager.read(&block_id, &mut page);
                            } else {
                                write_i32_at(page.bytes_mut(), 60, i as i32);
                                db.file_manager.write(&block_id, &page);
                                let record = make_wal_record(100);
                                let lsn = log.lock().unwrap().append(record).unwrap();
                                p.record(lsn, &log);
                            }
                        }
                        p.finish_batch(&log);
                    })
                },
            );
            let _policy = policy;
        }
    }

    group.finish();
}

// ============================================================================
// Phase 4: Concurrent I/O
// ============================================================================

fn bench_concurrent_io(c: &mut Criterion) {
    let ws = working_set_blocks();
    let concurrent_ops = ws.min(100);

    let mut group = c.benchmark_group("Phase4/Concurrent IO");
    if let Some((wu, mt, ss)) = ci_contention() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    for num_threads in [2usize, 4, 8, 16] {
        for (policy_template, policy_name) in [
            (WALFlushPolicy::None, "no-fsync"),
            (
                WALFlushPolicy::Group {
                    batch: 10,
                    pending: 0,
                    last_lsn: None,
                },
                "group-10",
            ),
        ] {
            // Shared file
            {
                let (db, _dir) = setup_io_test();
                let file = "concurrent_shared".to_string();
                let max_touchable = num_threads.saturating_mul(concurrent_ops).max(1);
                let total_blocks = ws.min(max_touchable).max(1);
                precreate_blocks(&db, &file, total_blocks);
                let log = db.log_manager();
                let policy_t = policy_template.clone();

                group.throughput(Throughput::Elements((num_threads * concurrent_ops) as u64));
                group.bench_function(format!("Shared {num_threads}T {policy_name}"), |b| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|_| {
                                let file = file.clone();
                                let log = Arc::clone(&log);
                                let mut policy = policy_t.clone();
                                let fm = Arc::clone(&db.file_manager);

                                thread::spawn(move || {
                                    let mut page = Page::new();
                                    for i in 0..concurrent_ops {
                                        let block_num = generate_random_number() % total_blocks;
                                        let block_id = BlockId::new(file.clone(), block_num);
                                        if (i % 10) < 7 {
                                            fm.read(&block_id, &mut page);
                                        } else {
                                            write_i32_at(page.bytes_mut(), 60, i as i32);
                                            fm.write(&block_id, &page);
                                            let record = make_wal_record(100);
                                            let lsn = log.lock().unwrap().append(record).unwrap();
                                            policy.record(lsn, &log);
                                        }
                                    }
                                    policy.finish_batch(&log);
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                    })
                });
            }

            // Sharded files
            {
                let (db, _dir) = setup_io_test();
                let target_per_file = (ws / num_threads).max(1);
                let blocks_per_file = target_per_file.min(concurrent_ops).max(1);
                for tid in 0..num_threads {
                    let f = format!("concurrent_shard_{tid}");
                    precreate_blocks(&db, &f, blocks_per_file);
                }
                let log = db.log_manager();
                let policy_t = policy_template.clone();

                group.throughput(Throughput::Elements((num_threads * concurrent_ops) as u64));
                group.bench_function(format!("Sharded {num_threads}T {policy_name}"), |b| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|tid| {
                                let file = format!("concurrent_shard_{tid}");
                                let log = Arc::clone(&log);
                                let mut policy = policy_t.clone();
                                let fm = Arc::clone(&db.file_manager);

                                thread::spawn(move || {
                                    let mut page = Page::new();
                                    for i in 0..concurrent_ops {
                                        let block_num = i % blocks_per_file;
                                        let block_id = BlockId::new(file.clone(), block_num);
                                        if (i % 10) < 7 {
                                            fm.read(&block_id, &mut page);
                                        } else {
                                            write_i32_at(page.bytes_mut(), 60, i as i32);
                                            fm.write(&block_id, &page);
                                            let record = make_wal_record(100);
                                            let lsn = log.lock().unwrap().append(record).unwrap();
                                            policy.record(lsn, &log);
                                        }
                                    }
                                    policy.finish_batch(&log);
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                    })
                });
            }
        }
    }

    group.finish();
}

// ============================================================================
// Phase 5: Random Write Durability
// ============================================================================

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
            let mut rng = FastRng::new();

            group.bench_function(&format!("{wal_name} {data_name}"), |b| {
                b.iter(|| {
                    let indices: Vec<usize> =
                        (0..durability_ops).map(|_| rng.next_range(ws)).collect();
                    let mut page = Page::new();
                    let mut wp = wal_template.clone();
                    let mut dp = data_template.clone();
                    let fm = Arc::clone(&db.file_manager);

                    for (i, &block_num) in indices.iter().enumerate() {
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

// ============================================================================
// Phase 7: Cache-Adverse I/O Variants
// ============================================================================

fn bench_cache_adverse(c: &mut Criterion) {
    let ws = working_set_blocks();

    let mut group = c.benchmark_group("Phase7/Cache Adverse");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // One-pass sequential scan
    {
        let (db, _dir) = setup_io_test();
        let file = format!("onepass_seq_{ws}");
        precreate_blocks(&db, &file, ws);

        group.bench_function(&format!("One-pass Seq Scan ({ws} blocks)"), |b| {
            b.iter(|| {
                let mut page = Page::new();
                for i in 0..ws {
                    db.file_manager
                        .read(&BlockId::new(file.clone(), i), &mut page);
                }
            })
        });
    }

    // Low-locality random read
    {
        let (db, _dir) = setup_io_test();
        let file = format!("lo_loc_rand_{ws}");
        precreate_blocks(&db, &file, ws);
        let mut rng = FastRng::new();

        group.bench_function(&format!("Low-locality Rand Read ({ws} blocks)"), |b| {
            b.iter(|| {
                let mut indices: Vec<usize> = (0..ws).collect();
                for i in (1..ws).rev() {
                    let j = rng.next_range(i + 1);
                    indices.swap(i, j);
                }
                let mut page = Page::new();
                for &idx in &indices {
                    db.file_manager
                        .read(&BlockId::new(file.clone(), idx), &mut page);
                }
            })
        });
    }

    // Multi-stream scan
    {
        let num_streams = 4usize;
        let (db, _dir) = setup_io_test();
        let blocks_per_stream = (ws / num_streams).max(1);
        for s in 0..num_streams {
            let f = format!("multi_stream_{num_streams}_{s}");
            precreate_blocks(&db, &f, blocks_per_stream);
        }

        group.bench_function(&format!("Multi-stream Scan ({ws} blocks)"), |b| {
            b.iter(|| {
                let handles: Vec<_> = (0..num_streams)
                    .map(|s| {
                        let file = format!("multi_stream_{num_streams}_{s}");
                        let fm = Arc::clone(&db.file_manager);
                        thread::spawn(move || {
                            let mut page = Page::new();
                            for i in 0..blocks_per_stream {
                                fm.read(&BlockId::new(file.clone(), i), &mut page);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Phase 8: Cache-Evict Variants (Linux only)
// ============================================================================

#[cfg(target_os = "linux")]
fn bench_cache_evict(c: &mut Criterion) {
    let ws = working_set_blocks();

    let mut group = c.benchmark_group("Phase8/Cache Evict");
    if let Some((wu, mt, ss)) = ci_fsync() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // One-pass sequential scan + evict
    {
        let (db, test_dir) = setup_io_test();
        let file_name = format!("onepass_seq_evict_{ws}");
        let file_path: PathBuf = test_dir.path.join(&file_name);
        precreate_blocks(&db, &file_name, ws);

        group.bench_function(&format!("One-pass Seq Scan+Evict ({ws} blocks)"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let t = Instant::now();
                    let mut page = Page::new();
                    for i in 0..ws {
                        db.file_manager
                            .read(&BlockId::new(file_name.clone(), i), &mut page);
                    }
                    total += t.elapsed();
                    posix_fadvise_dontneed(&file_path);
                }
                total
            })
        });
    }

    // Low-locality random read + evict
    {
        let (db, test_dir) = setup_io_test();
        let file_name = format!("lo_loc_rand_evict_{ws}");
        let file_path: PathBuf = test_dir.path.join(&file_name);
        precreate_blocks(&db, &file_name, ws);
        let mut rng = FastRng::new();

        group.bench_function(
            &format!("Low-locality Rand Read+Evict ({ws} blocks)"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut indices: Vec<usize> = (0..ws).collect();
                        for i in (1..ws).rev() {
                            let j = rng.next_range(i + 1);
                            indices.swap(i, j);
                        }
                        let t = Instant::now();
                        let mut page = Page::new();
                        for &idx in &indices {
                            db.file_manager
                                .read(&BlockId::new(file_name.clone(), idx), &mut page);
                        }
                        total += t.elapsed();
                        posix_fadvise_dontneed(&file_path);
                    }
                    total
                })
            },
        );
    }

    // Multi-stream scan + evict
    {
        let num_streams = 4usize;
        let (db, test_dir) = setup_io_test();
        let blocks_per_stream = (ws / num_streams).max(1);
        let file_names: Vec<String> = (0..num_streams)
            .map(|s| format!("multi_stream_evict_{num_streams}_{s}"))
            .collect();
        let file_paths: Vec<PathBuf> = file_names.iter().map(|f| test_dir.path.join(f)).collect();
        for name in &file_names {
            precreate_blocks(&db, name, blocks_per_stream);
        }

        group.bench_function(&format!("Multi-stream Scan+Evict ({ws} blocks)"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let t = Instant::now();
                    let handles: Vec<_> = file_names
                        .iter()
                        .map(|name| {
                            let name = name.clone();
                            let fm = Arc::clone(&db.file_manager);
                            thread::spawn(move || {
                                let mut page = Page::new();
                                for i in 0..blocks_per_stream {
                                    fm.read(&BlockId::new(name.clone(), i), &mut page);
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                    total += t.elapsed();
                    for path in &file_paths {
                        posix_fadvise_dontneed(path);
                    }
                }
                total
            })
        });
    }

    group.finish();
}

#[cfg(not(target_os = "linux"))]
fn bench_cache_evict(_c: &mut Criterion) {}

// ============================================================================
// Direct I/O fallback reporting (attached to io_patterns for visibility)
// ============================================================================

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
    bench_phase1_io,
    bench_phase1_qd,
    bench_wal,
    bench_mixed,
    bench_concurrent_io,
    bench_durability,
    bench_cache_adverse,
    bench_cache_evict,
    report_direct_io,
);
criterion_main!(benches);
