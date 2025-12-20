//! Buffer Manager implementations for benchmarking.
//!
//! This module provides multiple BufferManager implementations that can be
//! switched at compile time for A/B testing different optimization strategies.
//!
//! # Shared Types
//!
//! - `FrameMeta`: Per-frame metadata (pins, block_id, replacement policy state)
//! - `BufferFrame`: A buffer pool frame containing page data and metadata
//! - `BufferStats`: Hit/miss statistics for the buffer pool
//!
//! # Implementations
//!
//! - `baseline`: Original implementation with global latch_table and Drop-based cleanup
//! - `sharded`: Optimized with 16-shard latch table and no Drop cleanup

mod baseline;
mod no_drop;
mod sharded;

use std::sync::{
    atomic::AtomicUsize, Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use crate::{
    page::PageType,
    page::{BTreeInternalPageMut, BTreeLeafPageMut, BTreeMetaPageMut, HeapPageMut},
    BlockId, LogManager, Lsn, Page, SharedFS,
};

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
use crate::intrusive_dll::IntrusiveNode;

// === SWITCH IMPLEMENTATION HERE ===
// Uncomment ONE of the following lines:

pub use baseline::BufferManager;  // Global latch_table + Drop cleanup
// pub use no_drop::BufferManager;   // Global latch_table + NO Drop cleanup
// pub use sharded::BufferManager;   // Sharded latch_table + NO Drop cleanup

// ============================================================================
// FrameMeta
// ============================================================================

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

// ============================================================================
// BufferStats
// ============================================================================

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
