#![allow(clippy::arc_with_non_send_sync)]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use simpledb::{test_utils::generate_random_number, BlockId, Lsn, Page, SimpleDB, TestDir};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

// Keep in sync with scripts/bench/config.py
const PIN_HOTSET_POOL_SIZE: usize = 4096;
const PIN_TOTAL_OPS: usize = 10_000;
const HOTSET_TOTAL_OPS: usize = 10_000;
const HOTSET_K: usize = 4;

fn num_buffers() -> usize {
    std::env::var("SIMPLEDB_BENCH_BUFFERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12)
}

fn setup_buffer_pool(num_buffers: usize) -> (SimpleDB, TestDir) {
    SimpleDB::new_for_test(num_buffers, 5000)
}

fn precreate_blocks(db: &SimpleDB, file: &str, count: usize) {
    let file = file.to_string();
    for block_num in 0..count {
        let mut page = Page::new();
        write_i32_at(page.bytes_mut(), 60, block_num as i32);
        db.file_manager
            .write(&BlockId::new(file.clone(), block_num), &page);
    }
}

fn write_i32_at(bytes: &mut [u8], offset: usize, value: i32) {
    let le = value.to_le_bytes();
    bytes[offset..offset + 4].copy_from_slice(&le);
}

/// CI config for fast in-memory groups: 1s warmup, 5s measurement, 100 samples.
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

// ============================================================================
// Phase 1: Core Latency Benchmarks
// ============================================================================

fn bench_phase1(c: &mut Criterion) {
    let nb = num_buffers();

    let mut group = c.benchmark_group("Phase1/Core Latency");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // Pin/Unpin (buffer pool hit)
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "testfile".to_string();
        precreate_blocks(&db, &test_file, 2);
        let buffer_manager = db.buffer_manager();

        group.bench_function("Pin/Unpin (hit)", |b| {
            b.iter(|| {
                let block_id = BlockId::new(test_file.clone(), 1);
                let buffer = buffer_manager.pin(&block_id).unwrap();
                buffer_manager.unpin(buffer);
            })
        });
    }

    // Cold Pin (cache miss — advance block each iter)
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "coldfile".to_string();
        // Pre-create enough blocks; Criterion may run many iterations
        precreate_blocks(&db, &test_file, 1_000_000);
        let buffer_manager = db.buffer_manager();

        let mut block_idx: usize = 0;
        group.bench_function("Cold Pin (miss)", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let block_id = BlockId::new(test_file.clone(), block_idx);
                    let t = Instant::now();
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    total += t.elapsed();
                    buffer_manager.unpin(buffer);
                    block_idx += 1;
                }
                total
            })
        });
    }

    // Dirty Eviction
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "dirtyfile".to_string();
        // Pre-create a generous budget; Criterion controls actual iteration count
        precreate_blocks(&db, &test_file, 100_000 + nb);
        let buffer_manager = db.buffer_manager();

        // Fill pool with dirty buffers
        let txn = db.new_tx();
        for i in 0..nb {
            let block_id = BlockId::new(test_file.clone(), i);
            let mut guard = txn.pin_write_guard(&block_id);
            write_i32_at(guard.bytes_mut(), 60, 999);
            guard.mark_modified(txn.id(), Lsn::MAX);
        }

        let mut block_idx: usize = nb;
        group.bench_function("Dirty Eviction", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let block_id = BlockId::new(test_file.clone(), block_idx);
                    let t = Instant::now();
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    total += t.elapsed();
                    buffer_manager.unpin(buffer);
                    block_idx += 1;
                }
                total
            })
        });

        txn.commit().unwrap();
    }

    group.finish();
}

// ============================================================================
// Phase 2: Access Pattern Benchmarks (single-threaded)
// ============================================================================

fn bench_access_patterns_st(c: &mut Criterion) {
    let nb = num_buffers();
    let total_blocks = nb * 10;

    let mut group = c.benchmark_group("Phase2/Access Patterns ST");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    // Sequential Scan
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "seqfile".to_string();
        precreate_blocks(&db, &test_file, total_blocks);
        let buffer_manager = db.buffer_manager();

        group.bench_function(&format!("Sequential Scan ({total_blocks} blocks)"), |b| {
            b.iter(|| {
                for i in 0..total_blocks {
                    let block_id = BlockId::new(test_file.clone(), i);
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    buffer_manager.unpin(buffer);
                }
            })
        });
    }

    // Repeated Access
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "repeatfile".to_string();
        let working_set = 10.min(nb.saturating_sub(2)).max(1);
        let total_accesses = 1000usize;
        precreate_blocks(&db, &test_file, working_set);
        let buffer_manager = db.buffer_manager();

        group.bench_function(&format!("Repeated Access ({total_accesses} ops)"), |b| {
            b.iter(|| {
                for i in 0..total_accesses {
                    let block_idx = i % working_set;
                    let block_id = BlockId::new(test_file.clone(), block_idx);
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    buffer_manager.unpin(buffer);
                }
            })
        });
    }

    // Random Access (K=10, K=50, K=100)
    for working_set_size in [10usize, 50, 100] {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = format!("randomfile_{working_set_size}");
        let total_accesses = 500usize;
        precreate_blocks(&db, &test_file, working_set_size);
        let random_indices: Vec<usize> = (0..total_accesses)
            .map(|_| generate_random_number() % working_set_size)
            .collect();
        let buffer_manager = db.buffer_manager();

        group.bench_function(
            &format!("Random (K={working_set_size}, {total_accesses} ops)"),
            |b| {
                b.iter(|| {
                    for &block_idx in &random_indices {
                        let block_id = BlockId::new(test_file.clone(), block_idx);
                        let buffer = buffer_manager.pin(&block_id).unwrap();
                        buffer_manager.unpin(buffer);
                    }
                })
            },
        );
    }

    // Zipfian Access
    {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = "zipffile".to_string();
        let total_blocks_zip = nb * 3;
        let hot_set_size = ((total_blocks_zip as f64 * 0.2) as usize).max(1);
        let total_accesses = 500usize;
        precreate_blocks(&db, &test_file, total_blocks_zip);
        let zipfian_indices: Vec<usize> = (0..total_accesses)
            .map(|_| {
                let rand_val = generate_random_number();
                if (rand_val % 100) < 80 {
                    generate_random_number() % hot_set_size
                } else {
                    hot_set_size + (generate_random_number() % (total_blocks_zip - hot_set_size))
                }
            })
            .collect();
        let buffer_manager = db.buffer_manager();

        group.bench_function(&format!("Zipfian (80/20, {total_accesses} ops)"), |b| {
            b.iter(|| {
                for &block_idx in &zipfian_indices {
                    let block_id = BlockId::new(test_file.clone(), block_idx);
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    buffer_manager.unpin(buffer);
                }
            })
        });
    }

    group.finish();
}

// ============================================================================
// Phase 2: Access Pattern Benchmarks (multi-threaded)
// ============================================================================

fn spawn_mt_seq_scan(
    db: &SimpleDB,
    num_threads: usize,
    total_blocks: usize,
    test_file: Arc<String>,
    start_barrier: Arc<Barrier>,
    end_barrier: Arc<Barrier>,
    stop: Arc<AtomicBool>,
) -> Vec<thread::JoinHandle<()>> {
    let base = total_blocks / num_threads;
    let remainder = total_blocks % num_threads;
    let ranges: Arc<Vec<(usize, usize)>> = Arc::new(
        (0..num_threads)
            .map(|tid| {
                let start = tid * base + remainder.min(tid);
                let extra = usize::from(tid < remainder);
                let end = start + base + extra;
                (start, end)
            })
            .collect(),
    );

    (0..num_threads)
        .map(|tid| {
            let test_file = Arc::clone(&test_file);
            let buffer_manager = db.buffer_manager();
            let start_barrier = Arc::clone(&start_barrier);
            let end_barrier = Arc::clone(&end_barrier);
            let stop = Arc::clone(&stop);
            let ranges = Arc::clone(&ranges);

            thread::spawn(move || loop {
                start_barrier.wait();
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let (start, end) = ranges[tid];
                for i in start..end {
                    let block_id = BlockId::new(test_file.as_ref().clone(), i);
                    let buffer = buffer_manager.pin(&block_id).unwrap();
                    buffer_manager.unpin(buffer);
                }
                end_barrier.wait();
            })
        })
        .collect()
}

fn bench_access_patterns_mt(c: &mut Criterion) {
    let nb = num_buffers();
    let total_blocks = nb * 10;

    let mut group = c.benchmark_group("Phase2/Access Patterns MT");
    if let Some((wu, mt, ss)) = ci_contention() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    for num_threads in [2usize, 4, 8, 16, 32, 64, 128, 256] {
        let (db, _dir) = setup_buffer_pool(nb);
        let test_file = Arc::new(format!("seqfile_mt_{num_threads}"));
        precreate_blocks(&db, &test_file, total_blocks);

        let start_barrier = Arc::new(Barrier::new(num_threads + 1));
        let end_barrier = Arc::new(Barrier::new(num_threads + 1));
        let stop = Arc::new(AtomicBool::new(false));

        let handles = spawn_mt_seq_scan(
            &db,
            num_threads,
            total_blocks,
            Arc::clone(&test_file),
            Arc::clone(&start_barrier),
            Arc::clone(&end_barrier),
            Arc::clone(&stop),
        );

        group.bench_function(
            &format!("Seq Scan MT x{num_threads} ({total_blocks} blocks)"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let t = Instant::now();
                        start_barrier.wait();
                        end_barrier.wait();
                        total += t.elapsed();
                    }
                    total
                })
            },
        );

        stop.store(true, Ordering::Release);
        start_barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    group.finish();
}

// ============================================================================
// Phase 3: Pool Size Scaling
// ============================================================================

fn bench_pool_scaling(c: &mut Criterion) {
    let working_set_size = 100usize;
    let total_accesses = 500usize;
    let pool_sizes = [8usize, 16, 32, 64, 128, 256];

    let mut group = c.benchmark_group("Phase3/Pool Scaling");
    if let Some((wu, mt, ss)) = ci_fast() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }
    group.throughput(Throughput::Elements(total_accesses as u64));

    for &pool_size in &pool_sizes {
        let (db, _dir) = setup_buffer_pool(pool_size);
        let test_file = "scaling_test".to_string();
        precreate_blocks(&db, &test_file, working_set_size);
        let random_indices: Vec<usize> = (0..total_accesses)
            .map(|_| generate_random_number() % working_set_size)
            .collect();
        let buffer_manager = db.buffer_manager();

        group.bench_with_input(
            BenchmarkId::new("Random Access", pool_size),
            &pool_size,
            |b, _| {
                b.iter(|| {
                    for &block_idx in &random_indices {
                        let block_id = BlockId::new(test_file.clone(), block_idx);
                        let buffer = buffer_manager.pin(&block_id).unwrap();
                        buffer_manager.unpin(buffer);
                    }
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Phase 5: Concurrent Pin/Unpin
// ============================================================================

fn bench_concurrent_pin(c: &mut Criterion) {
    let pin_hotset_pool = PIN_HOTSET_POOL_SIZE;

    let mut group = c.benchmark_group("Phase5/Concurrent Pin");
    if let Some((wu, mt, ss)) = ci_contention() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    for num_threads in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        let ops_per_thread = PIN_TOTAL_OPS / num_threads;
        let (db, _dir) = setup_buffer_pool(pin_hotset_pool);
        let test_file = "concurrent_test".to_string();
        precreate_blocks(&db, &test_file, num_threads * 10);

        let start_barrier = Arc::new(Barrier::new(num_threads + 1));
        let end_barrier = Arc::new(Barrier::new(num_threads + 1));
        let stop = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let test_file = test_file.clone();
                let buffer_manager = db.buffer_manager();
                let start_barrier = Arc::clone(&start_barrier);
                let end_barrier = Arc::clone(&end_barrier);
                let stop = Arc::clone(&stop);

                thread::spawn(move || loop {
                    start_barrier.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for i in 0..ops_per_thread {
                        let block_num = (thread_id * 10) + (i % 10);
                        let block_id = BlockId::new(test_file.clone(), block_num);
                        let buffer = buffer_manager.pin(&block_id).unwrap();
                        buffer_manager.unpin(buffer);
                    }
                    end_barrier.wait();
                })
            })
            .collect();

        group.bench_function(
            &format!("Concurrent ({num_threads} threads, {ops_per_thread} ops)"),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let t = Instant::now();
                        start_barrier.wait();
                        end_barrier.wait();
                        total += t.elapsed();
                    }
                    total
                })
            },
        );

        stop.store(true, Ordering::Release);
        start_barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    group.finish();
}

// ============================================================================
// Phase 5: Hotset Contention
// ============================================================================

fn bench_hotset_contention(c: &mut Criterion) {
    let pin_hotset_pool = PIN_HOTSET_POOL_SIZE;

    let mut group = c.benchmark_group("Phase5/Hotset Contention");
    if let Some((wu, mt, ss)) = ci_contention() {
        group.warm_up_time(wu);
        group.measurement_time(mt);
        group.sample_size(ss);
    }

    for num_threads in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
        let ops_per_thread = HOTSET_TOTAL_OPS / num_threads;
        let (db, _dir) = setup_buffer_pool(pin_hotset_pool);
        let test_file = "concurrent_hotset".to_string();
        precreate_blocks(&db, &test_file, HOTSET_K);

        let start_barrier = Arc::new(Barrier::new(num_threads + 1));
        let end_barrier = Arc::new(Barrier::new(num_threads + 1));
        let stop = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let test_file = test_file.clone();
                let buffer_manager = db.buffer_manager();
                let start_barrier = Arc::clone(&start_barrier);
                let end_barrier = Arc::clone(&end_barrier);
                let stop = Arc::clone(&stop);

                thread::spawn(move || loop {
                    start_barrier.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    for i in 0..ops_per_thread {
                        let block_num = i % HOTSET_K;
                        let block_id = BlockId::new(test_file.clone(), block_num);
                        let buffer = buffer_manager.pin(&block_id).unwrap();
                        buffer_manager.unpin(buffer);
                    }
                    end_barrier.wait();
                })
            })
            .collect();

        group.bench_function(
            &format!(
                "Concurrent Hotset ({num_threads} threads, K={HOTSET_K}, {ops_per_thread} ops)"
            ),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let t = Instant::now();
                        start_barrier.wait();
                        end_barrier.wait();
                        total += t.elapsed();
                    }
                    total
                })
            },
        );

        stop.store(true, Ordering::Release);
        start_barrier.wait();
        for h in handles {
            h.join().unwrap();
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_phase1,
    bench_access_patterns_st,
    bench_access_patterns_mt,
    bench_pool_scaling,
    bench_concurrent_pin,
    bench_hotset_contention
);
criterion_main!(benches);
