#![allow(dead_code)]
//! No-Drop BufferManager implementation.
//!
//! Variant of baseline with:
//! - Single global `latch_table: Mutex<HashMap<...>>` (same as baseline)
//! - NO Drop-based latch cleanup (latches persist)
//!
//! This isolates the effect of removing Drop cleanup without sharding.

use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak},
    time::{Duration, Instant},
};

use crate::{replacement::PolicyState, BlockId, LogManager, SharedFS};

use super::{BufferFrame, BufferStats, FrameMeta};

// ============================================================================
// LatchTableGuard (NO Drop - latches persist)
// ============================================================================

struct LatchTableGuard<'a> {
    latch_table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    block_id: BlockId,
    latch: Arc<Mutex<()>>,
}

impl<'a> LatchTableGuard<'a> {
    pub fn new(table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>, block_id: &BlockId) -> Self {
        let latch = {
            let mut guard = table.lock().unwrap();
            let block_latch_ptr = guard
                .entry(block_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(block_latch_ptr)
        };
        Self {
            latch_table: table,
            block_id: block_id.clone(),
            latch,
        }
    }

    fn lock(&'a self) -> MutexGuard<'a, ()> {
        self.latch.lock().unwrap()
    }
}

// No Drop impl - latches are NOT pruned on the hot path

// ============================================================================
// BufferManager (no_drop)
// ============================================================================

#[derive(Debug)]
pub struct BufferManager {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<BufferFrame>>,
    num_available: Mutex<usize>,
    cond: Condvar,
    stats: OnceLock<Arc<BufferStats>>,
    latch_table: Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    resident_table: Mutex<HashMap<BlockId, Weak<BufferFrame>>>,
    policy: PolicyState,
}

impl BufferManager {
    const MAX_TIME: u64 = 10;

    pub fn new(
        file_manager: SharedFS,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        let buffer_pool: Vec<Arc<BufferFrame>> = (0..num_buffers)
            .map(|index| {
                Arc::new(BufferFrame::new(
                    Arc::clone(&file_manager),
                    Arc::clone(&log_manager),
                    index,
                ))
            })
            .collect();
        let policy = PolicyState::new(&buffer_pool);

        Self {
            file_manager,
            log_manager,
            buffer_pool,
            num_available: Mutex::new(num_buffers),
            cond: Condvar::new(),
            stats: OnceLock::new(),
            latch_table: Mutex::new(HashMap::new()),
            resident_table: Mutex::new(HashMap::new()),
            policy,
        }
    }

    pub fn enable_stats(&self) {
        let _ = self.stats.set(Arc::new(BufferStats::new()));
    }

    pub fn get_stats(&self) -> Option<(usize, usize)> {
        self.stats.get().map(|s| s.get())
    }

    pub fn stats(&self) -> Option<&Arc<BufferStats>> {
        self.stats.get()
    }

    pub fn reset_stats(&self) {
        if let Some(stats) = self.stats.get() {
            stats.reset();
        }
    }

    pub fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    pub fn file_manager(&self) -> SharedFS {
        Arc::clone(&self.file_manager)
    }

    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        Arc::clone(&self.log_manager)
    }

    pub(crate) fn flush_all(&self, txn_num: usize) {
        for buffer in &self.buffer_pool {
            let mut meta = buffer.lock_meta();
            if matches!(meta.txn, Some(t) if t == txn_num) {
                buffer.flush_locked(&mut meta);
            }
        }
    }

    pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
        let start = Instant::now();
        loop {
            if let Some(buffer) = self.try_to_pin(block_id) {
                return Ok(buffer);
            }

            let mut avail = self.num_available.lock().unwrap();
            while *avail == 0 {
                let elapsed = start.elapsed();
                if elapsed >= Duration::from_secs(Self::MAX_TIME) {
                    return Err("Timed out waiting for buffer".into());
                }
                let timeout = Duration::from_secs(Self::MAX_TIME) - elapsed;
                let (guard, wait_res) = self.cond.wait_timeout(avail, timeout).unwrap();
                avail = guard;
                if wait_res.timed_out() {
                    return Err("Timed out waiting for buffer".into());
                }
            }
            drop(avail);
        }
    }

    fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
        let latch_table_guard = LatchTableGuard::new(&self.latch_table, block_id);
        #[allow(unused_variables)]
        let block_latch = latch_table_guard.lock();

        let frame_ptr = {
            let mut resident_guard = self.resident_table.lock().unwrap();
            match resident_guard.get(block_id) {
                Some(weak_frame_ptr) => match weak_frame_ptr.upgrade() {
                    Some(frame_ptr) => Some(frame_ptr),
                    None => {
                        resident_guard.remove(block_id);
                        return None;
                    }
                },
                None => None,
            }
        };

        if let Some(frame_ptr) = frame_ptr {
            {
                let mut meta_guard = self.record_hit(&frame_ptr, block_id)?;
                let was_unpinned = meta_guard.pin();
                if was_unpinned {
                    *self.num_available.lock().unwrap() -= 1;
                }
                if let Some(stats) = self.stats.get() {
                    stats
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            return Some(frame_ptr);
        }

        if let Some(stats) = self.stats.get() {
            stats
                .misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let (tail_idx, mut meta_guard) = match self.evict_frame() {
            Some((idx, guard)) => (idx, guard),
            None => return None,
        };

        if let Some(old) = meta_guard.block_id.clone() {
            self.resident_table.lock().unwrap().remove(&old);
        }
        let frame = Arc::clone(&self.buffer_pool[tail_idx]);
        frame.assign_to_block_locked(&mut meta_guard, block_id);
        let became_pinned = meta_guard.pin();
        debug_assert!(became_pinned, "newly assigned frame must have zero pins");
        drop(meta_guard);

        self.policy.on_frame_assigned(&self.buffer_pool, tail_idx);

        self.resident_table
            .lock()
            .unwrap()
            .insert(block_id.clone(), Arc::downgrade(&frame));
        *self.num_available.lock().unwrap() -= 1;
        Some(frame)
    }

    pub fn unpin(&self, frame: Arc<BufferFrame>) {
        let mut meta = frame.lock_meta();
        let became_unpinned = meta.unpin();
        if became_unpinned {
            *self.num_available.lock().unwrap() += 1;
            self.cond.notify_all();
        }
    }

    fn evict_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
        self.policy.evict_frame(&self.buffer_pool)
    }

    fn record_hit<'a>(
        &'a self,
        frame_ptr: &'a Arc<BufferFrame>,
        block_id: &BlockId,
    ) -> Option<MutexGuard<'a, FrameMeta>> {
        self.policy
            .record_hit(&self.buffer_pool, frame_ptr, block_id, &self.resident_table)
    }

    #[cfg(test)]
    pub fn assert_buffer_count_invariant(&self) {
        let available = *self.num_available.lock().unwrap();
        let num_pinned_buffers: usize = self
            .buffer_pool
            .iter()
            .filter(|buf| buf.is_pinned())
            .count();

        assert_eq!(
            available + num_pinned_buffers,
            self.buffer_pool.len(),
            "Buffer count invariant violated: available={}, pinned_buffers={}, total={}",
            available,
            num_pinned_buffers,
            self.buffer_pool.len()
        );
    }
}
