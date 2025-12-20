//! Buffer Manager implementation.
//!
//! Sharded latch/resident tables with no Drop-based latch cleanup.
//!
//! # Shared Types
//!
//! - `FrameMeta`: Per-frame metadata (pins, block_id, replacement policy state)
//! - `BufferFrame`: A buffer pool frame containing page data and metadata
//! - `BufferStats`: Hit/miss statistics for the buffer pool
//!
//! # Implementation
//!
//! Single sharded implementation with 16-shard latch/resident tables and no
//! Drop-based latch cleanup.

use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
    },
    time::{Duration, Instant},
};

use crate::{
    page::PageType,
    page::{BTreeInternalPageMut, BTreeLeafPageMut, BTreeMetaPageMut, HeapPageMut},
    replacement::PolicyState,
    BlockId, LogManager, Lsn, Page, SharedFS,
};

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
use crate::intrusive_dll::IntrusiveNode;

#[derive(Debug)]
pub struct FrameMeta {
    pub(crate) block_id: Option<BlockId>,
    pub(crate) pins: usize,
    pub(crate) txn: Option<usize>,
    pub(crate) lsn: Option<Lsn>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) prev_idx: Option<usize>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) next_idx: Option<usize>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) index: usize,
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub(crate) ref_bit: bool,
}

impl FrameMeta {
    pub(crate) fn new(index: usize) -> Self {
        #[cfg(not(any(feature = "replacement_lru", feature = "replacement_sieve")))]
        let _ = index;
        Self {
            block_id: None,
            pins: 0,
            txn: None,
            lsn: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            prev_idx: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            next_idx: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            index,
            #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
            ref_bit: false,
        }
    }

    pub(crate) fn pin(&mut self) -> bool {
        let was_zero = self.pins == 0;
        self.pins += 1;
        was_zero
    }

    pub(crate) fn unpin(&mut self) -> bool {
        assert!(self.pins > 0, "FrameMeta::unpin on zero pins");
        self.pins -= 1;
        self.pins == 0
    }

    pub(crate) fn reset_pins(&mut self) {
        self.pins = 0;
    }
}

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
impl IntrusiveNode for FrameMeta {
    fn prev(&self) -> Option<usize> {
        self.prev_idx
    }

    fn set_prev(&mut self, prev: Option<usize>) {
        self.prev_idx = prev
    }

    fn next(&self) -> Option<usize> {
        self.next_idx
    }

    fn set_next(&mut self, next: Option<usize>) {
        self.next_idx = next
    }
}

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
impl IntrusiveNode for MutexGuard<'_, FrameMeta> {
    fn prev(&self) -> Option<usize> {
        self.prev_idx
    }

    fn set_prev(&mut self, prev: Option<usize>) {
        self.prev_idx = prev;
    }

    fn next(&self) -> Option<usize> {
        self.next_idx
    }

    fn set_next(&mut self, next: Option<usize>) {
        self.next_idx = next;
    }
}

// ============================================================================
// BufferFrame
// ============================================================================

#[derive(Debug)]
pub struct BufferFrame {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    page: RwLock<Page>,
    meta: Mutex<FrameMeta>,
}

impl BufferFrame {
    pub fn new(file_manager: SharedFS, log_manager: Arc<Mutex<LogManager>>, index: usize) -> Self {
        #[cfg(feature = "replacement_clock")]
        let _ = index;
        Self {
            file_manager,
            log_manager,
            page: RwLock::new(Page::new()),
            meta: Mutex::new(FrameMeta::new(index)),
        }
    }

    pub(crate) fn lock_meta(&self) -> MutexGuard<'_, FrameMeta> {
        self.meta.lock().unwrap()
    }

    pub fn block_id_owned(&self) -> Option<BlockId> {
        self.lock_meta().block_id.clone()
    }

    pub fn pin_count(&self) -> usize {
        self.lock_meta().pins
    }

    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub fn replacement_index(&self) -> usize {
        self.lock_meta().index
    }

    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn ref_bit(&self) -> bool {
        self.lock_meta().ref_bit
    }

    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn set_ref_bit(&self, value: bool) {
        self.lock_meta().ref_bit = value;
    }

    pub fn read_page(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read().unwrap()
    }

    pub fn write_page(&self) -> RwLockWriteGuard<'_, Page> {
        self.page.write().unwrap()
    }

    pub(crate) fn set_modified(&self, txn_num: usize, lsn: usize) {
        let mut meta = self.lock_meta();
        meta.txn = Some(txn_num);
        meta.lsn = Some(lsn);
    }

    #[cfg(test)]
    pub(crate) fn is_pinned(&self) -> bool {
        self.lock_meta().pins > 0
    }

    pub(crate) fn flush_locked(&self, meta: &mut FrameMeta) {
        if let (Some(block_id), Some(lsn)) = (meta.block_id.clone(), meta.lsn) {
            self.log_manager.lock().unwrap().flush_lsn(lsn);
            let mut page_guard = self.page.write().unwrap();
            match page_guard.peek_page_type().unwrap() {
                PageType::Heap => {
                    let mut page = HeapPageMut::new(page_guard.bytes_mut()).unwrap();
                    page.update_crc32();
                }
                PageType::IndexLeaf => {
                    let mut page = BTreeLeafPageMut::new(page_guard.bytes_mut()).unwrap();
                    page.update_crc32();
                }
                PageType::IndexInternal => {
                    let mut page = BTreeInternalPageMut::new(page_guard.bytes_mut()).unwrap();
                    page.update_crc32();
                }
                PageType::Overflow => {}
                PageType::Meta => {
                    let mut page = BTreeMetaPageMut::new(page_guard.bytes_mut()).unwrap();
                    page.update_crc32();
                }
                PageType::Free => {}
            }
            self.file_manager
                .lock()
                .unwrap()
                .write(&block_id, &page_guard);
            meta.txn = None;
            meta.lsn = None;
        }
    }

    pub(crate) fn assign_to_block_locked(&self, meta: &mut FrameMeta, block_id: &BlockId) {
        self.flush_locked(meta);
        meta.block_id = Some(block_id.clone());
        let mut page_guard = self.page.write().unwrap();
        self.file_manager
            .lock()
            .unwrap()
            .read(block_id, &mut page_guard);
        match page_guard.peek_page_type().unwrap() {
            PageType::Heap => {
                let mut page = HeapPageMut::new(page_guard.bytes_mut()).unwrap();
                if !page.verify_crc32() {
                    panic!(
                        "crc mismatch for {:?} on page type {:?}",
                        block_id,
                        PageType::Heap
                    );
                }
            }
            PageType::IndexLeaf => {
                let mut page = BTreeLeafPageMut::new(page_guard.bytes_mut()).unwrap();
                if !page.verify_crc32() {
                    panic!(
                        "crc mismatch for {:?} on page type {:?}",
                        block_id,
                        PageType::IndexLeaf
                    );
                }
            }
            PageType::IndexInternal => {
                let mut page = BTreeInternalPageMut::new(page_guard.bytes_mut()).unwrap();
                if !page.verify_crc32() {
                    panic!(
                        "crc mismatch for {:?} on page type {:?}",
                        block_id,
                        PageType::IndexInternal
                    );
                }
            }
            PageType::Overflow => {}
            PageType::Meta => {
                let mut page = BTreeMetaPageMut::new(page_guard.bytes_mut()).unwrap();
                if !page.verify_crc32() {
                    panic!(
                        "crc mismatch for {:?} on page type {:?}",
                        block_id,
                        PageType::Meta
                    );
                }
            }
            PageType::Free => {}
        }
        meta.reset_pins();
        meta.txn = None;
        meta.lsn = None;
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
}

impl Default for BufferStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferStats {
    pub fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    pub fn get(&self) -> (usize, usize) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub fn reset(&self) {
        self.hits.store(0, std::sync::atomic::Ordering::Relaxed);
        self.misses.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let (hits, misses) = self.get();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }
}

// ============================================================================
// LatchTableGuard (NO Drop - latches persist)
// ============================================================================

struct LatchTableGuard<'a> {
    #[allow(dead_code)]
    latch_shards: &'a [Mutex<HashMap<BlockId, Arc<Mutex<()>>>>],
    #[allow(dead_code)]
    block_id: BlockId,
    latch: Arc<Mutex<()>>,
}

impl<'a> LatchTableGuard<'a> {
    pub fn new(
        latch_shards: &'a [Mutex<HashMap<BlockId, Arc<Mutex<()>>>>],
        block_id: &BlockId,
        shard_index: usize,
    ) -> Self {
        let latch = {
            let mut guard = latch_shards[shard_index].lock().unwrap();
            let block_latch_ptr = guard
                .entry(block_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(block_latch_ptr)
        };
        Self {
            latch_shards,
            block_id: block_id.clone(),
            latch,
        }
    }

    fn lock(&'a self) -> MutexGuard<'a, ()> {
        self.latch.lock().unwrap()
    }
}

#[derive(Debug)]
pub struct BufferManager {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<BufferFrame>>,
    num_available: AtomicUsize,
    wait_mutex: Mutex<()>,
    cond: Condvar,
    stats: OnceLock<Arc<BufferStats>>,
    latch_shards: [Mutex<HashMap<BlockId, Arc<Mutex<()>>>>; Self::SHARDS],
    resident_shards: [Mutex<HashMap<BlockId, Weak<BufferFrame>>>; Self::SHARDS],
    policy: PolicyState,
}

impl BufferManager {
    const MAX_TIME: u64 = 10;
    const SHARDS: usize = 16;

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
            num_available: AtomicUsize::new(num_buffers),
            wait_mutex: Mutex::new(()),
            cond: Condvar::new(),
            stats: OnceLock::new(),
            latch_shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            resident_shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            policy,
        }
    }

    /// FNV-1a hash to select shard
    fn shard_index(&self, block_id: &BlockId) -> usize {
        let mut h = 0xcbf29ce484222325u64;
        for &byte in block_id.filename.as_bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= block_id.block_num as u64;
        h = h.wrapping_mul(0x100000001b3);
        (h as usize) & (Self::SHARDS - 1)
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
        self.num_available.load(Ordering::Acquire)
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

            let mut guard = self.wait_mutex.lock().unwrap();
            while self.num_available.load(Ordering::Acquire) == 0 {
                let elapsed = start.elapsed();
                if elapsed >= Duration::from_secs(Self::MAX_TIME) {
                    return Err("Timed out waiting for buffer".into());
                }
                let timeout = Duration::from_secs(Self::MAX_TIME) - elapsed;
                let (wait_guard, wait_res) = self.cond.wait_timeout(guard, timeout).unwrap();
                guard = wait_guard;
                if wait_res.timed_out() {
                    return Err("Timed out waiting for buffer".into());
                }
            }
            drop(guard);
        }
    }

    fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
        let shard_index = self.shard_index(block_id);
        let latch_table_guard = LatchTableGuard::new(&self.latch_shards, block_id, shard_index);
        let _block_latch = latch_table_guard.lock();

        let frame_ptr = {
            let mut resident_guard = self.resident_shards[shard_index].lock().unwrap();
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
                    self.num_available.fetch_sub(1, Ordering::AcqRel);
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
            let old_shard = self.shard_index(&old);
            self.resident_shards[old_shard].lock().unwrap().remove(&old);
        }
        let frame = Arc::clone(&self.buffer_pool[tail_idx]);
        frame.assign_to_block_locked(&mut meta_guard, block_id);
        let became_pinned = meta_guard.pin();
        debug_assert!(became_pinned, "newly assigned frame must have zero pins");
        drop(meta_guard);

        self.policy.on_frame_assigned(&self.buffer_pool, tail_idx);

        self.resident_shards[shard_index]
            .lock()
            .unwrap()
            .insert(block_id.clone(), Arc::downgrade(&frame));
        self.num_available.fetch_sub(1, Ordering::AcqRel);
        Some(frame)
    }

    pub fn unpin(&self, frame: Arc<BufferFrame>) {
        let mut meta = frame.lock_meta();
        let became_unpinned = meta.unpin();
        if became_unpinned {
            self.num_available.fetch_add(1, Ordering::AcqRel);
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
        let shard_index = self.shard_index(block_id);
        self.policy.record_hit(
            &self.buffer_pool,
            frame_ptr,
            block_id,
            &self.resident_shards[shard_index],
        )
    }

    #[cfg(test)]
    pub fn assert_buffer_count_invariant(&self) {
        let available = self.num_available.load(Ordering::Acquire);
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
