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
    collections::{HashMap, VecDeque},
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    page::PageType,
    page::{set_page_lsn, BTreeInternalPageMut, BTreeLeafPageMut, BTreeMetaPageMut, HeapPageMut},
    replacement::PolicyState,
    BatchReadReq, BatchWriteReq, BlockId, LogManager, Lsn, Page, SharedFS,
};

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
use crate::intrusive_dll::IntrusiveNode;

/// Result of a resident-only fast pin attempt.
///
/// Fast pin is used by restart-oriented B-tree traversal code that must distinguish
/// between a page being absent from the buffer pool and the fast path encountering
/// lock contention.
pub enum FastPinOutcome<T> {
    /// Fast pin succeeded and produced the requested value.
    Ready(T),
    /// The page is not currently resident and must be pinned through the slow path.
    NotResident,
    /// The page may be resident, but the fast path would have to wait on an internal lock.
    Contended,
}

/// Result of moving a frame from clean/unpinned into an actively pinned state.
///
/// The buffer manager uses this to keep the clean-frame accounting in one place
/// instead of re-deriving it around every pin path.
#[derive(Debug)]
struct PinTransition {
    /// Whether this call consumed the transition from zero pins to one pin.
    became_pinned: bool,
    /// Whether pinning removed one frame from the clean unpinned slack pool.
    left_clean_unpinned: bool,
}

/// Result of dropping a pin on a frame.
///
/// This is where the transaction side hands work to the flush side: once the
/// last pin is gone, a dirty frame may become eligible to enqueue for flush.
#[derive(Debug)]
struct UnpinTransition {
    /// Whether this call released the final pin on the frame.
    became_unpinned: bool,
    /// Whether the frame became clean and fully available after the unpin.
    became_clean_unpinned: bool,
    /// Frame index to enqueue if the frame became flushable.
    enqueue_dirty: Option<usize>,
}

/// Result of marking a frame dirty after a page mutation.
///
/// Dirty transitions advance the generation and may enqueue the frame if the
/// transaction path is no longer actively using it.
#[derive(Debug)]
struct DirtyTransition {
    /// Whether dirtying the frame removed one clean frame from slack.
    left_clean_unpinned: bool,
    /// Frame index to enqueue if the dirty image is already flushable.
    enqueue_dirty: Option<usize>,
}

/// Result of reconciling a completed snapshot write back into frame state.
///
/// Completion is generation-based: a write only clears dirtiness if no newer
/// mutation has advanced the frame since the snapshot was claimed.
#[derive(Debug)]
struct WritebackCompletion {
    /// Whether completion turned the frame back into a clean available frame.
    became_clean_unpinned: bool,
    /// Frame index to requeue if a newer dirty generation still remains.
    enqueue_dirty: Option<usize>,
}

/// Dirty-page/writeback substate of a frame.
///
/// This keeps the flush protocol explicit without forcing pin count or
/// replacement metadata into the same enum.
#[derive(Debug, Clone, Copy)]
enum FlushState {
    Clean,
    Dirty {
        /// Transaction that last dirtied the current resident page image.
        txn: usize,
        /// WAL LSN that must be durable before this page image reaches disk.
        lsn: Lsn,
        /// Monotonic generation of the current dirty page image.
        generation: u64,
        /// Whether this generation is already present in the dirty queue.
        queued: bool,
    },
    Writeback {
        /// Transaction that most recently dirtied the resident page image.
        txn: usize,
        /// WAL LSN that still governs WAL-before-data ordering.
        lsn: Lsn,
        /// Generation of the current resident dirty page image.
        dirty_generation: u64,
        /// Generation of the snapshot currently being written out.
        writeback_generation: u64,
    },
}

/// Residency substate of a frame.
///
/// Kept separate from flush state so callers can see whether a frame is free or
/// bound to a block without coupling that question to dirty/writeback details.
#[derive(Debug, Clone)]
enum ResidencyState {
    /// Frame currently does not have any block loaded
    Free,
    /// Home block currently loaded into this frame.
    Resident(BlockId),
}

/// Composed runtime state of a frame.
///
/// The frame protocol is now explicit at this level: residency answers where a
/// frame is bound, pin count answers who is actively using it, and flush state
/// answers how the transaction and writeback subsystems coordinate durability.
#[derive(Debug, Clone)]
struct FrameState {
    /// Whether the frame is free or bound to a specific block.
    residency: ResidencyState,
    /// Active pin count held by readers/writers.
    pins: usize,
    /// Dirty/writeback protocol state shared with the flusher.
    flush: FlushState,
    /// Next dirty generation to assign when the page is modified again.
    next_flush_generation: u64,
}

/// Per-frame state shared by transaction and flush paths.
///
/// Why this exists: transaction code and the background flusher must coordinate
/// through explicit frame metadata rather than direct callbacks into each other.
/// The key invariants are:
/// - `dirty_generation` advances on every new dirty image
/// - at most one writeback generation is in flight at a time
/// - writeback completion only clears dirty state when the completed generation
///   still matches the current frame generation
#[derive(Debug)]
pub struct FrameMeta {
    /// Composed runtime state visible to transaction, replacement, and flush paths.
    state: FrameState,
    /// LRU/SIEVE intrusive predecessor link.
    pub(crate) prev_idx: Option<usize>,
    /// LRU/SIEVE intrusive successor link.
    pub(crate) next_idx: Option<usize>,
    /// Stable frame index used by replacement and dirty-queue bookkeeping.
    pub(crate) index: usize,
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    /// CLOCK/SIEVE reference bit updated on observed hits.
    pub(crate) ref_bit: bool,
}

impl FrameMeta {
    pub(crate) fn new(index: usize) -> Self {
        Self {
            state: FrameState {
                residency: ResidencyState::Free,
                pins: 0,
                flush: FlushState::Clean,
                next_flush_generation: 0,
            },
            prev_idx: None,
            next_idx: None,
            index,
            #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
            ref_bit: false,
        }
    }

    pub(crate) fn pin(&mut self) -> bool {
        let was_zero = self.state.pins == 0;
        self.state.pins += 1;
        was_zero
    }

    pub(crate) fn unpin(&mut self) -> bool {
        assert!(self.state.pins > 0, "FrameMeta::unpin on zero pins");
        self.state.pins -= 1;
        self.state.pins == 0
    }

    pub(crate) fn reset_pins(&mut self) {
        self.state.pins = 0;
    }

    pub(crate) fn pin_count(&self) -> usize {
        self.state.pins
    }

    pub(crate) fn block_id(&self) -> Option<&BlockId> {
        match &self.state.residency {
            ResidencyState::Free => None,
            ResidencyState::Resident(block_id) => Some(block_id),
        }
    }

    pub(crate) fn block_id_owned(&self) -> Option<BlockId> {
        self.block_id().cloned()
    }

    pub(crate) fn assign_resident(&mut self, block_id: BlockId) {
        self.state.residency = ResidencyState::Resident(block_id);
    }

    pub(crate) fn clear_residency(&mut self) {
        self.state.residency = ResidencyState::Free;
    }

    /// Returns whether the frame currently owns a dirty page image.
    fn is_dirty(&self) -> bool {
        !matches!(self.state.flush, FlushState::Clean)
    }

    pub(crate) fn is_writeback_in_progress(&self) -> bool {
        matches!(self.state.flush, FlushState::Writeback { .. })
    }

    fn txn(&self) -> Option<usize> {
        match self.state.flush {
            FlushState::Clean => None,
            FlushState::Dirty { txn, .. } | FlushState::Writeback { txn, .. } => Some(txn),
        }
    }

    fn mark_flush_clean(&mut self) {
        self.state.flush = FlushState::Clean;
    }

    /// Returns whether this frame counts toward the clean slack the flusher is
    /// trying to maintain.
    fn is_clean_unpinned(&self) -> bool {
        self.state.pins == 0 && matches!(self.state.flush, FlushState::Clean)
    }

    fn try_queue_dirty_if_flushable(&mut self) -> Option<usize> {
        match &mut self.state.flush {
            FlushState::Dirty { queued, .. } if self.state.pins == 0 && !*queued => {
                *queued = true;
                Some(self.index)
            }
            _ => None,
        }
    }

    fn mark_dequeued(&mut self) {
        if let FlushState::Dirty { queued, .. } = &mut self.state.flush {
            *queued = false;
        }
    }

    /// Applies the pin-side transition and reports whether that removed one
    /// clean frame from the available pool.
    fn pin_transition(&mut self) -> PinTransition {
        let left_clean_unpinned = self.is_clean_unpinned();
        let became_pinned = self.pin();
        PinTransition {
            became_pinned,
            left_clean_unpinned,
        }
    }

    /// Applies the unpin-side transition and reports whether the frame became
    /// flushable or newly clean-and-unpinned.
    fn unpin_transition(&mut self) -> UnpinTransition {
        let became_unpinned = self.unpin();
        let enqueue_dirty = if became_unpinned {
            self.try_queue_dirty_if_flushable()
        } else {
            None
        };
        let became_clean_unpinned = became_unpinned && self.is_clean_unpinned();
        UnpinTransition {
            became_unpinned,
            became_clean_unpinned,
            enqueue_dirty,
        }
    }

    /// Marks the frame dirty for a new page generation.
    ///
    /// The transition decides whether the dirty image should be queued for the
    /// background flusher immediately or only after the last pin is released.
    fn mark_dirty_transition(&mut self, txn_num: usize, lsn: Lsn) -> DirtyTransition {
        let left_clean_unpinned = self.is_clean_unpinned();
        let generation = self.state.next_flush_generation.wrapping_add(1);
        self.state.next_flush_generation = generation;
        self.state.flush = match self.state.flush {
            FlushState::Clean => FlushState::Dirty {
                txn: txn_num,
                lsn,
                generation,
                queued: false,
            },
            FlushState::Dirty { queued, .. } => FlushState::Dirty {
                txn: txn_num,
                lsn,
                generation,
                queued,
            },
            FlushState::Writeback {
                writeback_generation,
                ..
            } => FlushState::Writeback {
                txn: txn_num,
                lsn,
                dirty_generation: generation,
                writeback_generation,
            },
        };
        let enqueue_dirty = self.try_queue_dirty_if_flushable();
        DirtyTransition {
            left_clean_unpinned,
            enqueue_dirty,
        }
    }

    /// Claims the current dirty generation for writeback.
    ///
    /// Why this is separate: the flusher must establish one explicit in-flight
    /// generation before it snapshots bytes, otherwise completion cannot tell
    /// whether a newer mutation arrived while the write was outstanding.
    fn try_begin_writeback(&mut self, require_unpinned: bool) -> Option<(Lsn, u64)> {
        if require_unpinned && self.state.pins > 0 {
            return None;
        }
        match self.state.flush {
            FlushState::Dirty {
                txn,
                lsn,
                generation,
                ..
            } => {
                self.state.flush = FlushState::Writeback {
                    txn,
                    lsn,
                    dirty_generation: generation,
                    writeback_generation: generation,
                };
                Some((lsn, generation))
            }
            _ => None,
        }
    }

    /// Reconciles a completed snapshot write with the current frame state.
    ///
    /// Completion only clears dirty metadata when the completed generation still
    /// matches the frame. Newer mutations leave the frame dirty and requeue it.
    fn complete_writeback_transition(
        &mut self,
        block_still_matches: bool,
        generation: u64,
    ) -> Option<WritebackCompletion> {
        if !block_still_matches {
            return None;
        }

        let (txn, lsn, dirty_generation, writeback_generation) = match self.state.flush {
            FlushState::Writeback {
                txn,
                lsn,
                dirty_generation,
                writeback_generation,
            } => (txn, lsn, dirty_generation, writeback_generation),
            _ => return None,
        };
        if writeback_generation != generation {
            return None;
        }

        let was_clean_unpinned = self.is_clean_unpinned();
        self.state.flush = if dirty_generation == writeback_generation {
            FlushState::Clean
        } else {
            FlushState::Dirty {
                txn,
                lsn,
                generation: dirty_generation,
                queued: false,
            }
        };

        let enqueue_dirty = self.try_queue_dirty_if_flushable();

        Some(WritebackCompletion {
            became_clean_unpinned: !was_clean_unpinned && self.is_clean_unpinned(),
            enqueue_dirty,
        })
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

    pub(crate) fn try_lock_meta(&self) -> Option<MutexGuard<'_, FrameMeta>> {
        self.meta.try_lock().ok()
    }

    pub fn block_id_owned(&self) -> Option<BlockId> {
        self.lock_meta().block_id_owned()
    }

    pub fn pin_count(&self) -> usize {
        self.lock_meta().pin_count()
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

    #[cfg(test)]
    pub(crate) fn is_pinned(&self) -> bool {
        self.lock_meta().pin_count() > 0
    }

    /// Claims one dirty generation and snapshots stable page bytes for it.
    ///
    /// This is the snapshot-first protocol boundary: page bytes are copied while
    /// holding `meta -> page`, then the page lock is released before I/O begins.
    fn claim_snapshot_for_writeback_locked(
        &self,
        meta: &mut FrameMeta,
        require_unpinned: bool,
    ) -> Option<(BlockId, Lsn, u64, Page)> {
        // Snapshot writeback is the current protocol choice because it lets the
        // flusher release the page lock before waiting on I/O.
        let block_id = match meta.block_id() {
            Some(block_id) => block_id.clone(),
            _ => return None,
        };
        let (lsn, generation) = meta.try_begin_writeback(require_unpinned)?;

        let mut page_guard = self.page.write().unwrap();
        set_page_lsn(page_guard.bytes_mut(), lsn);
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

        let mut snapshot = Page::new();
        snapshot.bytes_mut().copy_from_slice(page_guard.bytes());
        Some((block_id, lsn, generation, snapshot))
    }

    /// Completes one claimed writeback generation if the frame still matches.
    ///
    /// Stale completions are ignored so an older snapshot cannot clear newer
    /// dirty state after the frame has advanced.
    fn complete_writeback_locked(meta: &mut FrameMeta, block_id: &BlockId, generation: u64) {
        let _ = meta.complete_writeback_transition(meta.block_id() == Some(block_id), generation);
    }

    pub(crate) fn flush_locked(&self, meta: &mut FrameMeta) {
        if let Some((block_id, lsn, generation, snapshot)) =
            self.claim_snapshot_for_writeback_locked(meta, true)
        {
            self.log_manager.lock().unwrap().flush_lsn(lsn);
            let req = [BatchWriteReq {
                block_id: block_id.clone(),
            }];
            let pages = [snapshot];
            self.file_manager.write_batch(&req, &pages);
            Self::complete_writeback_locked(meta, &block_id, generation);
        }
    }

    pub(crate) fn assign_to_block_locked(&self, meta: &mut FrameMeta, block_id: &BlockId) {
        self.flush_locked(meta);
        meta.assign_resident(block_id.clone());
        let mut page_guard = self.page.write().unwrap();
        self.file_manager.read(block_id, &mut page_guard);
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
        meta.mark_flush_clean();
    }
}

#[derive(Debug)]
pub struct BufferStats {
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
    pub prefetch_attempted: AtomicUsize,
    pub prefetch_installed: AtomicUsize,
    pub prefetch_discarded: AtomicUsize,
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
            prefetch_attempted: AtomicUsize::new(0),
            prefetch_installed: AtomicUsize::new(0),
            prefetch_discarded: AtomicUsize::new(0),
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
        self.prefetch_attempted
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.prefetch_installed
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.prefetch_discarded
            .store(0, std::sync::atomic::Ordering::Relaxed);
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
// Latch cleanup is intentionally avoided on the pin path to reduce contention.
// If latch growth becomes an issue, prefer periodic/thresholded cleanup off
// the hot path.
// ============================================================================

type LatchShards = [Mutex<HashMap<BlockId, Arc<Mutex<()>>>>];

struct LatchTableGuard {
    latch: Arc<Mutex<()>>,
}

impl LatchTableGuard {
    pub fn new(latch_shards: &LatchShards, block_id: &BlockId, shard_index: usize) -> Self {
        let latch = {
            let mut guard = latch_shards[shard_index].lock().unwrap();
            let block_latch_ptr = guard
                .entry(block_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(block_latch_ptr)
        };
        Self { latch }
    }

    fn try_new(latch_shards: &LatchShards, block_id: &BlockId, shard_index: usize) -> Option<Self> {
        let latch = {
            let mut guard = latch_shards[shard_index].try_lock().ok()?;
            let block_latch_ptr = guard
                .entry(block_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(block_latch_ptr)
        };
        Some(Self { latch })
    }

    fn lock<'a>(&'a self) -> MutexGuard<'a, ()> {
        self.latch.lock().unwrap()
    }

    fn try_lock<'a>(&'a self) -> Option<MutexGuard<'a, ()>> {
        self.latch.try_lock().ok()
    }
}

struct PrefetchReservation {
    block_id: BlockId,
    frame_idx: usize,
}

#[derive(Debug)]
struct FlushControl {
    /// Enqueue time of the oldest dirty frame currently waiting for flush.
    oldest_dirty_signal: Option<Instant>,
    /// Requests shutdown of the background flusher thread.
    shutdown: bool,
}

#[derive(Debug)]
struct FlushCoordinator {
    /// Shared timed-wakeup state for the background flusher.
    state: Mutex<FlushControl>,
    /// Wakes the flusher on new work, timeout expiry, or shutdown.
    cond: Condvar,
}

impl FlushCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(FlushControl {
                oldest_dirty_signal: None,
                shutdown: false,
            }),
            cond: Condvar::new(),
        }
    }
}

/// Buffer manager plus the background dirty-page flusher.
///
/// The important split is operational: normal transaction commit no longer
/// writes data pages directly. Transactions mark frames dirty, and the flusher
/// consumes queued dirty frames when clean-frame slack falls below a target.
#[derive(Debug)]
pub struct BufferManager {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<BufferFrame>>,
    num_available: AtomicUsize,
    /// Number of frames currently available as clean, unpinned eviction targets.
    clean_unpinned: Arc<AtomicUsize>,
    wait_mutex: Mutex<()>,
    cond: Condvar,
    stats: OnceLock<Arc<BufferStats>>,
    latch_shards: [Mutex<HashMap<BlockId, Arc<Mutex<()>>>>; Self::SHARDS],
    resident_shards: [Mutex<HashMap<BlockId, Weak<BufferFrame>>>; Self::SHARDS],
    policy: PolicyState,
    /// Frames that transitioned into a flushable dirty state.
    dirty_queue: Arc<Mutex<VecDeque<usize>>>,
    flush_coordinator: Arc<FlushCoordinator>,
    flusher_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BufferManager {
    const MAX_TIME: u64 = 10;
    const SHARDS: usize = 16;
    const FLUSH_BATCH_SIZE: usize = 32;
    const FLUSH_AGE_THRESHOLD: Duration = Duration::from_millis(2);
    const _SHARDS_POWER_OF_TWO: () = assert!(Self::SHARDS.is_power_of_two());

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
        let flush_coordinator = Arc::new(FlushCoordinator::new());
        let clean_unpinned = Arc::new(AtomicUsize::new(num_buffers));
        let dirty_queue = Arc::new(Mutex::new(VecDeque::new()));
        let flusher_handle = {
            let buffer_pool = buffer_pool.clone();
            let file_manager = Arc::clone(&file_manager);
            let log_manager = Arc::clone(&log_manager);
            let flush_coordinator = Arc::clone(&flush_coordinator);
            let clean_unpinned = Arc::clone(&clean_unpinned);
            let dirty_queue = Arc::clone(&dirty_queue);
            thread::spawn(move || {
                Self::background_flush_loop(
                    buffer_pool,
                    file_manager,
                    log_manager,
                    clean_unpinned,
                    dirty_queue,
                    flush_coordinator,
                )
            })
        };

        Self {
            file_manager,
            log_manager,
            buffer_pool,
            num_available: AtomicUsize::new(num_buffers),
            clean_unpinned,
            wait_mutex: Mutex::new(()),
            cond: Condvar::new(),
            stats: OnceLock::new(),
            latch_shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            resident_shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            policy,
            dirty_queue,
            flush_coordinator,
            flusher_handle: Mutex::new(Some(flusher_handle)),
        }
    }

    /// Wakes the flusher after a frame becomes flush-eligible.
    fn notify_flusher(&self) {
        let mut state = self.flush_coordinator.state.lock().unwrap();
        if state.oldest_dirty_signal.is_none() {
            state.oldest_dirty_signal = Some(Instant::now());
        }
        self.flush_coordinator.cond.notify_one();
    }

    /// Enqueues a dirty frame exactly once and wakes the flusher.
    fn enqueue_dirty_frame(&self, frame_idx: usize) {
        self.dirty_queue.lock().unwrap().push_back(frame_idx);
        self.notify_flusher();
    }

    /// Waits until either the oldest queued dirty frame has aged enough to
    /// flush or shutdown has been requested.
    fn wait_for_flush_signal(flush_coordinator: &FlushCoordinator) -> bool {
        let mut state = flush_coordinator.state.lock().unwrap();
        loop {
            if state.shutdown {
                return false;
            }
            match state.oldest_dirty_signal {
                Some(oldest) => {
                    let elapsed = oldest.elapsed();
                    if elapsed >= Self::FLUSH_AGE_THRESHOLD {
                        state.oldest_dirty_signal = None;
                        return true;
                    }
                    let timeout = Self::FLUSH_AGE_THRESHOLD - elapsed;
                    let (next_state, _) =
                        flush_coordinator.cond.wait_timeout(state, timeout).unwrap();
                    state = next_state;
                }
                None => {
                    state = flush_coordinator.cond.wait(state).unwrap();
                }
            }
        }
    }

    /// Drains up to one batch of flush-eligible frames from the dirty queue and
    /// snapshots stable page images for them.
    fn collect_dirty_snapshots(
        buffer_pool: &[Arc<BufferFrame>],
        dirty_queue: &Mutex<VecDeque<usize>>,
        batch_limit: usize,
        txn_filter: Option<usize>,
    ) -> Vec<(usize, Arc<BufferFrame>, BlockId, Lsn, u64, Page)> {
        // The queue keeps the flusher off the full buffer-pool scan path. We
        // only revisit frames that transitioned into a flush-eligible state.
        let mut pending = Vec::new();
        let mut deferred = Vec::new();
        while pending.len() < batch_limit {
            let Some(frame_idx) = dirty_queue.lock().unwrap().pop_front() else {
                break;
            };
            let buffer = &buffer_pool[frame_idx];
            let mut meta = buffer.lock_meta();
            meta.mark_dequeued();
            if !meta.is_dirty() {
                continue;
            }
            if pending.len() >= batch_limit {
                break;
            }
            if let Some(txn_num) = txn_filter {
                if meta.txn() != Some(txn_num) {
                    if meta.try_queue_dirty_if_flushable().is_some() {
                        deferred.push(frame_idx);
                    }
                    continue;
                }
            }
            let Some((block_id, lsn, generation, snapshot)) =
                buffer.claim_snapshot_for_writeback_locked(&mut meta, true)
            else {
                if meta.try_queue_dirty_if_flushable().is_some() {
                    deferred.push(frame_idx);
                }
                continue;
            };
            pending.push((
                frame_idx,
                Arc::clone(buffer),
                block_id,
                lsn,
                generation,
                snapshot,
            ));
        }
        if !deferred.is_empty() {
            let mut queue = dirty_queue.lock().unwrap();
            for frame_idx in deferred {
                queue.push_back(frame_idx);
            }
        }
        pending
    }

    /// Writes one batch of already-snapshotted pages after enforcing WAL-before-data.
    fn write_snapshot_batch(
        file_manager: &SharedFS,
        log_manager: &Arc<Mutex<LogManager>>,
        pending: &[(usize, Arc<BufferFrame>, BlockId, Lsn, u64, Page)],
    ) {
        // WAL-before-data is enforced once per batch so snapshot writeback does
        // not turn into one WAL flush per frame.
        if pending.is_empty() {
            return;
        }
        let max_lsn = pending
            .iter()
            .map(|(_, _, _, lsn, _, _)| *lsn)
            .max()
            .expect("pending batch has at least one lsn");
        log_manager.lock().unwrap().flush_lsn(max_lsn);

        let reqs: Vec<BatchWriteReq> = pending
            .iter()
            .map(|(_, _, block_id, _, _, _)| BatchWriteReq {
                block_id: block_id.clone(),
            })
            .collect();
        let pages: Vec<Page> = pending
            .iter()
            .map(|(_, _, _, _, _, snapshot)| snapshot.clone())
            .collect();
        file_manager.write_batch(&reqs, &pages);
    }

    /// Applies completion-side frame transitions for one finished snapshot batch.
    fn complete_snapshot_batch(
        pending: Vec<(usize, Arc<BufferFrame>, BlockId, Lsn, u64, Page)>,
        clean_unpinned: &AtomicUsize,
        dirty_queue: &Mutex<VecDeque<usize>>,
        flush_coordinator: &FlushCoordinator,
    ) {
        for (_frame_idx, frame, block_id, _, generation, _) in pending {
            let mut meta = frame.lock_meta();
            let block_still_matches = meta.block_id() == Some(&block_id);
            if let Some(transition) =
                meta.complete_writeback_transition(block_still_matches, generation)
            {
                if transition.became_clean_unpinned {
                    clean_unpinned.fetch_add(1, Ordering::AcqRel);
                }
                if let Some(frame_idx) = transition.enqueue_dirty {
                    dirty_queue.lock().unwrap().push_back(frame_idx);
                }
            }
        }
        flush_coordinator.cond.notify_all();
    }

    /// Returns whether this transaction still owns any dirty generations that
    /// must be forced before rollback/recovery returns.
    fn has_dirty_for_txn(&self, txn_num: usize) -> bool {
        self.buffer_pool.iter().any(|buffer| {
            let meta = buffer.lock_meta();
            meta.txn() == Some(txn_num) && meta.is_dirty()
        })
    }

    /// Background precleaning loop.
    ///
    /// The loop stays pressure-driven: it only consumes the dirty queue when the
    /// number of clean unpinned frames falls below a low watermark.
    fn background_flush_loop(
        buffer_pool: Vec<Arc<BufferFrame>>,
        file_manager: SharedFS,
        log_manager: Arc<Mutex<LogManager>>,
        clean_unpinned: Arc<AtomicUsize>,
        dirty_queue: Arc<Mutex<VecDeque<usize>>>,
        flush_coordinator: Arc<FlushCoordinator>,
    ) {
        // A low-watermark policy keeps this first cut focused on precleaning
        // under buffer-pool pressure instead of eagerly flushing every dirty page.
        let clean_target = (buffer_pool.len() / 4).max(1);
        while Self::wait_for_flush_signal(&flush_coordinator) {
            if clean_unpinned.load(Ordering::Acquire) >= clean_target {
                continue;
            }
            loop {
                let pending = Self::collect_dirty_snapshots(
                    &buffer_pool,
                    &dirty_queue,
                    Self::FLUSH_BATCH_SIZE,
                    None,
                );
                if pending.is_empty() {
                    break;
                }
                Self::write_snapshot_batch(&file_manager, &log_manager, &pending);
                let hit_batch_limit = pending.len() == Self::FLUSH_BATCH_SIZE;
                Self::complete_snapshot_batch(
                    pending,
                    clean_unpinned.as_ref(),
                    &dirty_queue,
                    &flush_coordinator,
                );
                if !hit_batch_limit {
                    break;
                }
            }
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

    fn resident_frame_if_present(
        &self,
        block_id: &BlockId,
        shard_index: usize,
    ) -> Option<Arc<BufferFrame>> {
        let mut resident_guard = self.resident_shards[shard_index].lock().unwrap();
        match resident_guard.get(block_id) {
            Some(weak_frame_ptr) => match weak_frame_ptr.upgrade() {
                Some(frame_ptr) => Some(frame_ptr),
                None => {
                    resident_guard.remove(block_id);
                    None
                }
            },
            None => None,
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
        self.num_available.load(Ordering::Acquire)
    }

    pub fn file_manager(&self) -> SharedFS {
        Arc::clone(&self.file_manager)
    }

    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        Arc::clone(&self.log_manager)
    }

    /// Best-effort prefetch for a sequential block range.
    ///
    /// Never blocks waiting for frames: reserves as many frames as are currently
    /// evictable, submits one batch read, then installs pages with a resident recheck.
    pub fn prefetch(&self, file: &str, start_block: usize, count: usize) -> usize {
        if count == 0 {
            return 0;
        }

        let end_block = start_block.saturating_add(count);

        let mut reservations: Vec<PrefetchReservation> = Vec::new();
        let mut reqs: Vec<BatchReadReq> = Vec::new();

        for block_num in start_block..start_block.saturating_add(count) {
            let block_id = BlockId::new(file.to_string(), block_num);
            let shard_index = self.shard_index(&block_id);
            let latch_table_guard =
                LatchTableGuard::new(&self.latch_shards, &block_id, shard_index);
            let _block_latch = latch_table_guard.lock();

            if self
                .resident_frame_if_present(&block_id, shard_index)
                .is_some()
            {
                continue;
            }

            let mut victim = None;
            for _ in 0..self.buffer_pool.len() {
                let Some((frame_idx, meta_guard)) = self.evict_frame() else {
                    break;
                };
                let protects_target_range = meta_guard.block_id().is_some_and(|old| {
                    old.filename == file
                        && old.block_num >= start_block
                        && old.block_num < end_block
                });
                if protects_target_range {
                    drop(meta_guard);
                    self.policy.on_frame_assigned(&self.buffer_pool, frame_idx);
                    continue;
                }
                victim = Some((frame_idx, meta_guard));
                break;
            }
            let (frame_idx, mut meta_guard) = match victim {
                Some(victim) => victim,
                None => break, // best-effort: do not block waiting for frames
            };
            let frame = Arc::clone(&self.buffer_pool[frame_idx]);

            if let Some(old) = meta_guard.block_id_owned() {
                let old_shard = self.shard_index(&old);
                self.resident_shards[old_shard].lock().unwrap().remove(&old);
            }
            frame.flush_locked(&mut meta_guard);
            meta_guard.clear_residency();
            meta_guard.mark_flush_clean();

            let transition = meta_guard.pin_transition();
            debug_assert!(
                transition.became_pinned,
                "reserved prefetch frame must have zero pins"
            );
            drop(meta_guard);
            self.num_available.fetch_sub(1, Ordering::AcqRel);
            if transition.left_clean_unpinned {
                self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
            }

            reservations.push(PrefetchReservation {
                block_id: block_id.clone(),
                frame_idx,
            });
            reqs.push(BatchReadReq { block_id });
        }

        if reqs.is_empty() {
            return 0;
        }
        if let Some(stats) = self.stats.get() {
            stats
                .prefetch_attempted
                .fetch_add(reqs.len(), Ordering::Relaxed);
        }

        let mut pages: Vec<Page> = (0..reqs.len()).map(|_| Page::new()).collect();
        self.file_manager.read_batch(&reqs, &mut pages);

        let mut installed = 0usize;

        let mut frames_to_release: Vec<Arc<BufferFrame>> = Vec::with_capacity(reservations.len());

        for (idx, reservation) in reservations.into_iter().enumerate() {
            let shard_index = self.shard_index(&reservation.block_id);
            let latch_table_guard =
                LatchTableGuard::new(&self.latch_shards, &reservation.block_id, shard_index);
            let _block_latch = latch_table_guard.lock();
            let frame = Arc::clone(&self.buffer_pool[reservation.frame_idx]);

            let already_resident = self
                .resident_frame_if_present(&reservation.block_id, shard_index)
                .is_some();

            if !already_resident {
                {
                    let mut meta_guard = frame.lock_meta();
                    let mut page_guard = frame.write_page();
                    *page_guard = std::mem::take(&mut pages[idx]);
                    meta_guard.assign_resident(reservation.block_id.clone());
                    meta_guard.mark_flush_clean();
                }
                self.policy
                    .on_frame_assigned(&self.buffer_pool, reservation.frame_idx);
                self.resident_shards[shard_index]
                    .lock()
                    .unwrap()
                    .insert(reservation.block_id.clone(), Arc::downgrade(&frame));
                installed += 1;
                if let Some(stats) = self.stats.get() {
                    stats.prefetch_installed.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // LRU/SIEVE remove victims from list during eviction. Reinsert free
                // frames into replacement state even if this prefetch becomes redundant.
                self.policy
                    .on_frame_assigned(&self.buffer_pool, reservation.frame_idx);
                if let Some(stats) = self.stats.get() {
                    stats.prefetch_discarded.fetch_add(1, Ordering::Relaxed);
                }
            }

            frames_to_release.push(frame);
        }

        for frame in frames_to_release {
            let transition = {
                let mut meta_guard = frame.lock_meta();
                meta_guard.unpin_transition()
            };
            if transition.became_unpinned {
                self.num_available.fetch_add(1, Ordering::AcqRel);
                self.cond.notify_all();
            }
            if transition.became_clean_unpinned {
                self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
            }
            if let Some(frame_idx) = transition.enqueue_dirty {
                self.enqueue_dirty_frame(frame_idx);
            }
        }

        installed
    }

    pub(crate) fn flush_all(&self, txn_num: usize) {
        while self.has_dirty_for_txn(txn_num) {
            let pending = Self::collect_dirty_snapshots(
                &self.buffer_pool,
                &self.dirty_queue,
                Self::FLUSH_BATCH_SIZE,
                Some(txn_num),
            );
            if pending.is_empty() {
                let state = self.flush_coordinator.state.lock().unwrap();
                let _ = self
                    .flush_coordinator
                    .cond
                    .wait_timeout(state, Self::FLUSH_AGE_THRESHOLD)
                    .unwrap();
                continue;
            }
            Self::write_snapshot_batch(&self.file_manager, &self.log_manager, &pending);
            Self::complete_snapshot_batch(
                pending,
                self.clean_unpinned.as_ref(),
                &self.dirty_queue,
                &self.flush_coordinator,
            );
        }
    }

    /// Fast path for latch-crabbing callers.
    ///
    /// This is resident-only and never performs replacement policy bookkeeping,
    /// eviction, or blocking waits.
    pub fn pin_fast(&self, block_id: &BlockId) -> FastPinOutcome<Arc<BufferFrame>> {
        let shard_index = self.shard_index(block_id);
        let Some(latch_table_guard) =
            LatchTableGuard::try_new(&self.latch_shards, block_id, shard_index)
        else {
            return FastPinOutcome::Contended;
        };
        let Some(_block_latch) = latch_table_guard.try_lock() else {
            return FastPinOutcome::Contended;
        };

        let frame_ptr = {
            let Some(mut resident_guard) = self.resident_shards[shard_index].try_lock().ok() else {
                return FastPinOutcome::Contended;
            };
            match resident_guard.get(block_id) {
                Some(weak_frame_ptr) => match weak_frame_ptr.upgrade() {
                    Some(frame_ptr) => Some(frame_ptr),
                    None => {
                        resident_guard.remove(block_id);
                        return FastPinOutcome::NotResident;
                    }
                },
                None => None,
            }
        };

        let Some(frame_ptr) = frame_ptr else {
            return FastPinOutcome::NotResident;
        };

        {
            // Use try_lock to avoid blocking while page latches are held
            let Some(mut meta_guard) = frame_ptr.try_lock_meta() else {
                return FastPinOutcome::Contended;
            };
            if !meta_guard
                .block_id()
                .is_some_and(|current| current == block_id)
            {
                if let Ok(mut resident_guard) = self.resident_shards[shard_index].try_lock() {
                    resident_guard.remove(block_id);
                }
                return FastPinOutcome::NotResident;
            }
            let transition = meta_guard.pin_transition();
            if transition.became_pinned {
                self.num_available.fetch_sub(1, Ordering::AcqRel);
                if transition.left_clean_unpinned {
                    self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }

        FastPinOutcome::Ready(frame_ptr)
    }

    /// Full pin path with immediate replacement policy updates and eviction.
    pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
        let start = Instant::now();
        loop {
            if let Some(buffer) = self.try_to_pin(block_id) {
                return Ok(buffer);
            }

            // Slow path: only use wait_mutex when pool is empty. num_available is
            // atomic, so wakeups can be spurious (TOCTOU), but pin() retries.
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
                let transition = meta_guard.pin_transition();
                if transition.became_pinned {
                    self.num_available.fetch_sub(1, Ordering::AcqRel);
                    if transition.left_clean_unpinned {
                        self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
                    }
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

        if let Some(old) = meta_guard.block_id_owned() {
            let old_shard = self.shard_index(&old);
            self.resident_shards[old_shard].lock().unwrap().remove(&old);
        }
        let frame = Arc::clone(&self.buffer_pool[tail_idx]);
        frame.assign_to_block_locked(&mut meta_guard, block_id);
        let transition = meta_guard.pin_transition();
        debug_assert!(
            transition.became_pinned,
            "newly assigned frame must have zero pins"
        );
        drop(meta_guard);

        self.policy.on_frame_assigned(&self.buffer_pool, tail_idx);

        self.resident_shards[shard_index]
            .lock()
            .unwrap()
            .insert(block_id.clone(), Arc::downgrade(&frame));
        self.num_available.fetch_sub(1, Ordering::AcqRel);
        if transition.left_clean_unpinned {
            self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
        }
        Some(frame)
    }

    pub fn unpin(&self, frame: Arc<BufferFrame>) {
        let transition = {
            let mut meta = frame.lock_meta();
            meta.unpin_transition()
        };
        if transition.became_unpinned {
            self.num_available.fetch_add(1, Ordering::AcqRel);
            self.cond.notify_all();
        }
        if transition.became_clean_unpinned {
            self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
        }
        if let Some(frame_idx) = transition.enqueue_dirty {
            self.enqueue_dirty_frame(frame_idx);
        }
    }

    /// Applies dirty metadata updates through the buffer manager boundary so
    /// callers cannot mutate frame metadata directly.
    /// Marks a frame dirty through the buffer-manager protocol boundary.
    ///
    /// Callers hand off the new txn/LSN pair here so queueing and clean-slack
    /// bookkeeping stay centralized with the rest of the frame-state machine.
    pub(crate) fn mark_modified(&self, frame: &Arc<BufferFrame>, txn_num: usize, lsn: usize) {
        let transition = {
            let mut meta = frame.lock_meta();
            meta.mark_dirty_transition(txn_num, lsn)
        };
        if transition.left_clean_unpinned {
            self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
        }
        if let Some(frame_idx) = transition.enqueue_dirty {
            self.enqueue_dirty_frame(frame_idx);
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

impl Drop for BufferManager {
    fn drop(&mut self) {
        {
            let mut state = self.flush_coordinator.state.lock().unwrap();
            state.shutdown = true;
            self.flush_coordinator.cond.notify_all();
        }
        if let Some(handle) = self.flusher_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}
