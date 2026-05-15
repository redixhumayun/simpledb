use simpledb::{BlockId, Page, SimpleDB};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

fn write_i32_at(bytes: &mut [u8], offset: usize, value: i32) {
    let le = value.to_le_bytes();
    bytes[offset..offset + 4].copy_from_slice(&le);
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

fn prime_resident_set(db: &SimpleDB, file: &str, num_threads: usize, blocks_per_thread: usize) {
    let buffer_manager = db.buffer_manager();
    for thread_id in 0..num_threads {
        for i in 0..blocks_per_thread {
            let block_num = (thread_id * blocks_per_thread) + i;
            let block_id = BlockId::new(file.to_string(), block_num);
            let buffer = buffer_manager.pin(&block_id).unwrap();
            buffer_manager.unpin(buffer);
        }
    }
}

fn parse_arg<T>(args: &[String], flag: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
{
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .and_then(|pair| pair[1].parse().ok())
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_threads = parse_arg(&args, "--threads", 4usize);
    let duration_secs = parse_arg(&args, "--duration-secs", 20u64);
    let num_buffers = parse_arg(&args, "--buffers", 4096usize);
    let blocks_per_thread = parse_arg(&args, "--blocks-per-thread", 10usize);
    let test_file = "profile_concurrent_pin".to_string();

    let (db, _dir) = SimpleDB::new_for_test(num_buffers, 5000);
    precreate_blocks(&db, &test_file, num_threads * blocks_per_thread);
    prime_resident_set(&db, &test_file, num_threads, blocks_per_thread);

    let start_barrier = Arc::new(Barrier::new(num_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let test_file = test_file.clone();
            let buffer_manager = db.buffer_manager();
            let start_barrier = Arc::clone(&start_barrier);
            let stop = Arc::clone(&stop);

            thread::spawn(move || -> u64 {
                let mut ops = 0u64;
                start_barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..blocks_per_thread {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let block_num = (thread_id * blocks_per_thread) + i;
                        let block_id = BlockId::new(test_file.clone(), block_num);
                        let buffer = buffer_manager.pin(&block_id).unwrap();
                        buffer_manager.unpin(buffer);
                        ops += 1;
                    }
                }
                ops
            })
        })
        .collect();

    start_barrier.wait();
    let start = Instant::now();
    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    let total_ops: u64 = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .sum();
    let elapsed = start.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "threads={} duration_secs={} buffers={} blocks_per_thread={} total_ops={} ops_per_sec={:.0}",
        num_threads, duration_secs, num_buffers, blocks_per_thread, total_ops, ops_per_sec
    );
}
