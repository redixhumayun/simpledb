//! Buffer Manager implementation.
//!
//! # Shared Types
//!
//! - `FrameMeta`: Per-frame metadata (pins, block_id, replacement policy state)
//! - `BufferFrame`: A buffer pool frame containing page data and metadata
//! - `BufferStats`: Hit/miss statistics for the buffer pool
//!
//! # Implementation
//!
use std::{
    collections::{hash_map::RandomState, HashMap, VecDeque},
    error::Error,
    hash::BuildHasher,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard,
        TryLockError,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
use std::sync::atomic::AtomicBool;

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

/// Opt-in counters for profiling shared buffer-pool synchronization points.
///
/// Enable with `SIMPLEDB_BUFFER_POOL_PROFILE_LOCKS=1`. The counters are
/// intentionally process-global because the profiling binary owns one active
/// buffer manager at a time and needs cheap end-of-run reporting.
#[derive(Debug, Clone, Copy, Default)]
pub struct BufferPoolProfileCounters {
    pub directory_lock_calls: u64,
    pub directory_lock_contended: u64,
    pub directory_lock_elapsed_ns: u64,
    pub directory_try_lock_calls: u64,
    pub directory_try_lock_failed: u64,
    pub frame_meta_lock_calls: u64,
    pub frame_meta_lock_contended: u64,
    pub frame_meta_lock_elapsed_ns: u64,
    pub frame_meta_try_lock_calls: u64,
    pub frame_meta_try_lock_failed: u64,
    pub free_wait_notify_all_calls: u64,
    pub free_wait_notify_all_elapsed_ns: u64,
}

static PROFILE_COUNTERS_ENABLED: OnceLock<bool> = OnceLock::new();
static DIRECTORY_LOCK_CALLS: AtomicU64 = AtomicU64::new(0);
static DIRECTORY_LOCK_CONTENDED: AtomicU64 = AtomicU64::new(0);
static DIRECTORY_LOCK_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);
static DIRECTORY_TRY_LOCK_CALLS: AtomicU64 = AtomicU64::new(0);
static DIRECTORY_TRY_LOCK_FAILED: AtomicU64 = AtomicU64::new(0);
static FRAME_META_LOCK_CALLS: AtomicU64 = AtomicU64::new(0);
static FRAME_META_LOCK_CONTENDED: AtomicU64 = AtomicU64::new(0);
static FRAME_META_LOCK_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);
static FRAME_META_TRY_LOCK_CALLS: AtomicU64 = AtomicU64::new(0);
static FRAME_META_TRY_LOCK_FAILED: AtomicU64 = AtomicU64::new(0);
static FREE_WAIT_NOTIFY_ALL_CALLS: AtomicU64 = AtomicU64::new(0);
static FREE_WAIT_NOTIFY_ALL_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);

fn profile_counters_enabled() -> bool {
    *PROFILE_COUNTERS_ENABLED.get_or_init(|| {
        std::env::var("SIMPLEDB_BUFFER_POOL_PROFILE_LOCKS").is_ok_and(|value| value == "1")
    })
}

fn elapsed_ns_since(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Result of moving a frame from clean/unpinned into an actively pinned state.
///
/// The buffer manager uses this to keep the clean-frame accounting in one place
/// instead of re-deriving it around every pin path.
#[derive(Debug)]
enum PinTransition {
    /// Pin count was already non-zero, so no availability accounting changed.
    StillPinned,
    /// Pin count transitioned `0 -> 1` on a clean frame, consuming one unit of
    /// clean slack.
    BecamePinnedClean,
    /// Pin count transitioned `0 -> 1`, but the frame was not part of clean
    /// slack because it was dirty.
    BecamePinnedDirty,
}

/// Result of dropping a pin on a frame.
///
/// This is where the transaction side hands work to the flush side: once the
/// last pin is gone, a dirty frame may become eligible to enqueue for flush.
#[derive(Debug)]
enum UnpinTransition {
    /// Pin count remained non-zero, so no availability or flush eligibility changed.
    StillPinned,
    /// The last pin was released and the frame became part of clean slack.
    BecameUnpinnedClean,
    /// The last pin was released on a dirty frame. The frame is available for
    /// reuse, and may also need to be enqueued for background flush.
    BecameUnpinnedDirty { enqueue_dirty: Option<usize> },
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
/// frame is bound and flush state answers how the transaction and writeback
/// subsystems coordinate durability.
#[derive(Debug, Clone)]
struct FrameState {
    /// Whether the frame is free or bound to a specific block.
    residency: ResidencyState,
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
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    /// LRU/SIEVE intrusive predecessor link.
    pub(crate) prev_idx: Option<usize>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    /// LRU/SIEVE intrusive successor link.
    pub(crate) next_idx: Option<usize>,
    /// Stable frame index used by replacement and dirty-queue bookkeeping.
    pub(crate) index: usize,
}

impl FrameMeta {
    pub(crate) fn new(index: usize) -> Self {
        Self {
            state: FrameState {
                residency: ResidencyState::Free,
                flush: FlushState::Clean,
                next_flush_generation: 0,
            },
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            prev_idx: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            next_idx: None,
            index,
        }
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

    fn claim_for_eviction(&mut self) -> bool {
        if self.is_writeback_in_progress() {
            return false;
        }
        true
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
    fn is_clean_unpinned(&self, pin_count: usize) -> bool {
        pin_count == 0 && matches!(self.state.flush, FlushState::Clean)
    }

    fn try_queue_dirty_if_flushable(&mut self, pin_count: usize) -> Option<usize> {
        match &mut self.state.flush {
            FlushState::Dirty { queued, .. } if pin_count == 0 && !*queued => {
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
    fn pin_transition(&self, previous_pin_count: usize) -> PinTransition {
        if previous_pin_count > 0 {
            return PinTransition::StillPinned;
        }
        if self.is_clean_unpinned(0) {
            PinTransition::BecamePinnedClean
        } else {
            PinTransition::BecamePinnedDirty
        }
    }

    /// Applies the unpin-side transition and reports whether the frame became
    /// flushable or newly clean-and-unpinned.
    fn unpin_transition(&mut self, new_pin_count: usize) -> UnpinTransition {
        if new_pin_count > 0 {
            return UnpinTransition::StillPinned;
        }
        if self.is_clean_unpinned(0) {
            UnpinTransition::BecameUnpinnedClean
        } else {
            UnpinTransition::BecameUnpinnedDirty {
                enqueue_dirty: self.try_queue_dirty_if_flushable(0),
            }
        }
    }

    /// Marks the frame dirty for a new page generation.
    ///
    /// The transition decides whether the dirty image should be queued for the
    /// background flusher immediately or only after the last pin is released.
    fn mark_dirty_transition(
        &mut self,
        pin_count: usize,
        txn_num: usize,
        lsn: Lsn,
    ) -> DirtyTransition {
        let left_clean_unpinned = self.is_clean_unpinned(pin_count);
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
        let enqueue_dirty = self.try_queue_dirty_if_flushable(pin_count);
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
    fn try_begin_writeback(
        &mut self,
        pin_count: usize,
        require_unpinned: bool,
    ) -> Option<(Lsn, u64)> {
        if require_unpinned && pin_count > 0 {
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
        pin_count: usize,
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

        let was_clean_unpinned = self.is_clean_unpinned(pin_count);
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

        let enqueue_dirty = self.try_queue_dirty_if_flushable(pin_count);

        Some(WritebackCompletion {
            became_clean_unpinned: !was_clean_unpinned && self.is_clean_unpinned(pin_count),
            enqueue_dirty,
        })
    }
}

/// Atomic wrapper around the packed frame residency-control word.
///
/// Bit layout:
/// - bit 0: `loading`
/// - bit 1: `evicting`
/// - bits 2..: residency generation
///
/// Why this is one atomic word: resident pins must validate generation and
/// transient state from a single coherent snapshot, and install/evict paths
/// need to CAS that whole snapshot when moving between states.
#[derive(Debug)]
struct AtomicFrameControl {
    raw: AtomicU64,
}

/// Opaque raw residency-control snapshot used for validation and rollback.
type FrameControlSnapshot = u64;

impl AtomicFrameControl {
    const LOADING_BIT: u64 = 1;
    const EVICTING_BIT: u64 = 1 << 1;
    const FLAGS_MASK: u64 = Self::LOADING_BIT | Self::EVICTING_BIT;

    /// Creates one control word for generation zero with no transient flags set.
    fn new() -> Self {
        Self {
            raw: AtomicU64::new(0),
        }
    }

    /// Packs a generation and transient flags into the control word layout.
    fn encode(generation: u64, loading: bool, evicting: bool) -> u64 {
        (generation << 2)
            | if loading { Self::LOADING_BIT } else { 0 }
            | if evicting { Self::EVICTING_BIT } else { 0 }
    }

    /// Extracts the residency generation from a raw control snapshot.
    fn generation_from(snapshot: FrameControlSnapshot) -> u64 {
        snapshot >> 2
    }

    /// Returns whether a raw control snapshot has the loading flag set.
    fn is_loading_raw(snapshot: FrameControlSnapshot) -> bool {
        snapshot & Self::LOADING_BIT != 0
    }

    /// Returns whether a raw control snapshot has the evicting flag set.
    fn is_evicting_raw(snapshot: FrameControlSnapshot) -> bool {
        snapshot & Self::EVICTING_BIT != 0
    }

    /// Loads the current raw residency-control snapshot.
    ///
    /// The value should be interpreted only by helpers on this type or passed
    /// back to `store_raw()` for rollback.
    fn load_raw(&self) -> FrameControlSnapshot {
        self.raw.load(Ordering::Acquire)
    }

    /// Restores one previously observed raw residency-control snapshot.
    ///
    /// This is used only for rollback after a later cold-state check fails.
    fn store_raw(&self, snapshot: FrameControlSnapshot) {
        self.raw.store(snapshot, Ordering::Release);
    }

    /// Returns the current residency generation used by directory validation.
    fn generation(&self) -> u64 {
        Self::generation_from(self.load_raw())
    }

    /// Returns whether the frame is currently in its non-pinnable loading phase.
    fn is_loading(&self) -> bool {
        Self::is_loading_raw(self.load_raw())
    }

    /// Returns whether the frame has been claimed for reuse by an evict/install path.
    fn is_evicting(&self) -> bool {
        Self::is_evicting_raw(self.load_raw())
    }

    /// Returns whether a raw control snapshot may be pinned for the given
    /// directory generation.
    ///
    /// A resident pin is valid only when the frame still belongs to the same
    /// residency generation and is not in a transient non-pinnable phase.
    fn can_pin(snapshot: FrameControlSnapshot, residency_generation: u64) -> bool {
        Self::generation_from(snapshot) == residency_generation && snapshot & Self::FLAGS_MASK == 0
    }

    /// Starts a new residency generation and marks it non-pinnable.
    ///
    /// Why both bits are set here: while a frame is being refilled, hits must
    /// fail validation exactly as if the frame had already been claimed away.
    fn begin_loading(&self) -> u64 {
        loop {
            let current = self.load_raw();
            let generation = Self::generation_from(current).wrapping_add(1);
            let next = Self::encode(generation, true, true);
            if self
                .raw
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return generation;
            }
        }
    }

    /// Clears the transient loading/evicting bits for the current generation.
    ///
    /// After this transition, ordinary resident pins may validate and proceed again.
    fn finish_loading(&self) {
        loop {
            let current = self.load_raw();
            let next = Self::encode(Self::generation_from(current), false, false);
            if self
                .raw
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Tries to mark the current generation as claimed for eviction.
    ///
    /// Returns the pre-claim state so the caller can restore it if a later
    /// colder check under [`FrameMeta`] fails.
    fn try_claim_for_eviction(&self, pin_count: usize) -> Option<FrameControlSnapshot> {
        loop {
            let current = self.load_raw();
            if Self::is_loading_raw(current) || Self::is_evicting_raw(current) || pin_count > 0 {
                return None;
            }
            let claimed = current | Self::EVICTING_BIT;
            if self
                .raw
                .compare_exchange(current, claimed, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(current);
            }
        }
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

/// One stable slot in the buffer pool.
///
/// A frame owns three different kinds of state:
/// - page bytes behind `RwLock<Page>`
/// - cold metadata behind [`FrameMeta`]
/// - hot per-frame access state in atomics
///
/// Why this split exists: resident hits touch pin count, policy bits, and
/// residency-control flags far more often than they touch flush metadata or
/// page contents. Keeping those hot fields on `BufferFrame` lets the common
/// path avoid serializing on [`FrameMeta`].
#[derive(Debug)]
pub struct BufferFrame {
    /// Storage interface used to read and write the page currently assigned to this frame.
    file_manager: SharedFS,
    /// WAL manager used by writeback paths to preserve WAL-before-data ordering.
    log_manager: Arc<Mutex<LogManager>>,
    /// Page bytes currently cached in this frame.
    ///
    /// This latch protects page contents only. Buffer pinning and residency
    /// validation are handled separately.
    page: RwLock<Page>,
    /// Cold per-frame metadata: block identity, flush protocol state, and
    /// replacement-list links for list-based policies.
    meta: Mutex<FrameMeta>,
    /// Hot pin count updated on every pin/unpin.
    pin_count: AtomicUsize,
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    /// Hot policy reference bit used by Clock and SIEVE.
    ref_bit: AtomicBool,
    /// Packed residency-control word used by OCC validation.
    ///
    /// Holds residency generation plus transient `loading/evicting` flags.
    control: AtomicFrameControl,
}

impl BufferFrame {
    /// Constructs one buffer frame with empty page bytes and zeroed hot-path state.
    ///
    /// Why this split exists: page bytes, cold metadata, and hot residency/pin
    /// fields are initialized separately because later methods intentionally
    /// touch them with different synchronization mechanisms.
    pub fn new(file_manager: SharedFS, log_manager: Arc<Mutex<LogManager>>, index: usize) -> Self {
        Self {
            file_manager,
            log_manager,
            page: RwLock::new(Page::new()),
            meta: Mutex::new(FrameMeta::new(index)),
            pin_count: AtomicUsize::new(0),
            #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
            ref_bit: AtomicBool::new(false),
            control: AtomicFrameControl::new(),
        }
    }

    /// Locks cold per-frame metadata shared with flush and replacement code.
    ///
    /// This should stay off the uncontended resident-hit fast path as much as
    /// possible; atomics on `BufferFrame` exist to avoid taking this lock there.
    pub(crate) fn lock_meta(&self) -> MutexGuard<'_, FrameMeta> {
        if !profile_counters_enabled() {
            return self.meta.lock().unwrap();
        }

        FRAME_META_LOCK_CALLS.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        match self.meta.try_lock() {
            Ok(guard) => {
                FRAME_META_LOCK_ELAPSED_NS.fetch_add(elapsed_ns_since(start), Ordering::Relaxed);
                guard
            }
            Err(TryLockError::WouldBlock) => {
                FRAME_META_LOCK_CONTENDED.fetch_add(1, Ordering::Relaxed);
                let guard = self.meta.lock().unwrap();
                FRAME_META_LOCK_ELAPSED_NS.fetch_add(elapsed_ns_since(start), Ordering::Relaxed);
                guard
            }
            Err(TryLockError::Poisoned(_)) => self.meta.lock().unwrap(),
        }
    }

    /// Tries to lock cold metadata without blocking.
    ///
    /// Used by nonblocking paths such as `pin_fast()` where waiting behind page
    /// or flush work would violate the caller contract.
    pub(crate) fn try_lock_meta(&self) -> Option<MutexGuard<'_, FrameMeta>> {
        if profile_counters_enabled() {
            FRAME_META_TRY_LOCK_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        match self.meta.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::WouldBlock) => {
                if profile_counters_enabled() {
                    FRAME_META_TRY_LOCK_FAILED.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            Err(TryLockError::Poisoned(_)) => None,
        }
    }

    /// Returns the current resident block identity, if any.
    ///
    /// This consults cold metadata because block identity changes only on
    /// install/evict paths, not on ordinary hits.
    pub fn block_id_owned(&self) -> Option<BlockId> {
        self.lock_meta().block_id_owned()
    }

    /// Returns the live pin count from the hot atomic state.
    ///
    /// Pin count lives outside [`FrameMeta`] so resident hits can pin/unpin
    /// without serializing on the metadata mutex.
    pub fn pin_count(&self) -> usize {
        self.pin_count.load(Ordering::Acquire)
    }

    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub fn replacement_index(&self) -> usize {
        self.lock_meta().index
    }

    /// Returns the policy reference bit from hot state.
    ///
    /// Clock and SIEVE consult this frequently during hit/evict traffic, so it
    /// stays out of [`FrameMeta`].
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn ref_bit(&self) -> bool {
        self.ref_bit.load(Ordering::Acquire)
    }

    /// Updates the policy reference bit without touching cold metadata.
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn set_ref_bit(&self, value: bool) {
        self.ref_bit.store(value, Ordering::Release);
    }

    /// Acquires a shared latch on the page bytes.
    ///
    /// This is intentionally separate from buffer pinning; pin protects
    /// residency, while this lock protects page contents.
    pub fn read_page(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read().unwrap()
    }

    /// Acquires an exclusive latch on the page bytes.
    pub fn write_page(&self) -> RwLockWriteGuard<'_, Page> {
        self.page.write().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn is_pinned(&self) -> bool {
        self.pin_count() > 0
    }

    /// Returns the current residency generation used by directory validation.
    pub(crate) fn residency_generation(&self) -> u64 {
        self.control.generation()
    }

    /// Returns whether the frame is currently being filled with a new page.
    pub(crate) fn is_loading(&self) -> bool {
        self.control.is_loading()
    }

    /// Returns whether the frame has been claimed for reuse.
    pub(crate) fn is_evicting(&self) -> bool {
        self.control.is_evicting()
    }

    /// Starts installing a specific block into this frame.
    ///
    /// This bumps the residency generation before the new contents become
    /// pinnable so stale directory observations fail validation.
    fn begin_loading_residency_locked(&self, meta: &mut FrameMeta, block_id: BlockId) -> u64 {
        meta.assign_resident(block_id);
        self.control.begin_loading()
    }

    /// Marks the frame as reserved for an incoming block before bytes arrive.
    ///
    /// Prefetch uses this placeholder state so duplicate install attempts see a
    /// transient non-pinnable generation instead of a reusable frame.
    fn begin_loading_placeholder_locked(&self, meta: &mut FrameMeta) -> u64 {
        meta.clear_residency();
        self.control.begin_loading()
    }

    /// Makes the current residency visible to ordinary pins again.
    fn finish_loading_residency(&self) {
        self.control.finish_loading();
    }

    /// Tries to claim the frame for eviction/reuse.
    ///
    /// The claim is two-stage:
    /// - atomically set `evicting` so new pins fail OCC validation
    /// - then lock [`FrameMeta`] to verify colder writeback constraints
    fn try_claim_for_eviction(&self) -> Option<MutexGuard<'_, FrameMeta>> {
        let previous = self.control.try_claim_for_eviction(self.pin_count())?;

        let mut meta = self.lock_meta();
        if !meta.claim_for_eviction() {
            self.control.store_raw(previous);
            return None;
        }
        Some(meta)
    }

    /// Attempts one resident pin using directory-provided generation.
    ///
    /// This is the core OCC fast path:
    /// - validate control word
    /// - increment atomic pin count
    /// - revalidate and roll back on race
    ///
    /// Only the `0 -> 1` transition still consults [`FrameMeta`], because clean
    /// slack and dirty-queue accounting remain there.
    fn pin_from_directory_entry(&self, residency_generation: u64) -> Option<PinTransition> {
        let control = self.control.load_raw();
        if !AtomicFrameControl::can_pin(control, residency_generation) {
            return None;
        }

        let previous_pin_count = self.pin_count.fetch_add(1, Ordering::AcqRel);
        let validated = self.control.load_raw();
        if !AtomicFrameControl::can_pin(validated, residency_generation) {
            self.pin_count.fetch_sub(1, Ordering::AcqRel);
            return None;
        }

        let transition = if previous_pin_count == 0 {
            let meta = self.lock_meta();
            meta.pin_transition(previous_pin_count)
        } else {
            PinTransition::StillPinned
        };
        Some(transition)
    }

    /// Attempts the same OCC resident pin as [`BufferFrame::pin_from_directory_entry()`], but
    /// preserves the [`BufferManager::pin_fast()`] nonblocking contract.
    ///
    /// Why this helper exists: [`BufferManager::pin_fast()`] must distinguish
    /// three cases without ever waiting on [`FrameMeta`]:
    /// - the directory/generation was stale, so the caller should treat the
    ///   page as not resident
    /// - the frame was resident and the pin succeeded
    /// - the only remaining step was the `0 -> 1` accounting transition, but
    ///   that would have blocked on `FrameMeta`
    ///
    /// This method may speculatively increment `pin_count`; any stale-residency
    /// or would-block outcome rolls that increment back before returning.
    fn try_pin_from_directory_entry(
        &self,
        residency_generation: u64,
    ) -> Result<Option<PinTransition>, ()> {
        let control = self.control.load_raw();
        if !AtomicFrameControl::can_pin(control, residency_generation) {
            return Ok(None);
        }

        let previous_pin_count = self.pin_count.fetch_add(1, Ordering::AcqRel);
        let validated = self.control.load_raw();
        if !AtomicFrameControl::can_pin(validated, residency_generation) {
            self.pin_count.fetch_sub(1, Ordering::AcqRel);
            return Ok(None);
        }

        let transition = if previous_pin_count == 0 {
            let Some(meta) = self.try_lock_meta() else {
                self.pin_count.fetch_sub(1, Ordering::AcqRel);
                return Err(());
            };
            meta.pin_transition(previous_pin_count)
        } else {
            PinTransition::StillPinned
        };
        Ok(Some(transition))
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
        let block_id = match meta.block_id() {
            Some(block_id) => block_id.clone(),
            _ => return None,
        };
        let (lsn, generation) = meta.try_begin_writeback(self.pin_count(), require_unpinned)?;

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
    fn complete_writeback_locked(
        &self,
        meta: &mut FrameMeta,
        block_id: &BlockId,
        generation: u64,
    ) -> Option<WritebackCompletion> {
        meta.complete_writeback_transition(
            meta.block_id() == Some(block_id),
            generation,
            self.pin_count(),
        )
    }

    /// Flushes the current dirty image synchronously if one is present.
    ///
    /// This remains a cold path helper used by install/evict logic. The point
    /// is to keep the page snapshot and completion protocol in one place.
    fn flush_locked(&self, meta: &mut FrameMeta) -> Option<WritebackCompletion> {
        if let Some((block_id, lsn, generation, snapshot)) =
            self.claim_snapshot_for_writeback_locked(meta, true)
        {
            self.log_manager.lock().unwrap().flush_lsn(lsn);
            let req = [BatchWriteReq {
                block_id: block_id.clone(),
            }];
            let pages = [&snapshot];
            self.file_manager.write_batch(&req, &pages);
            return self.complete_writeback_locked(meta, &block_id, generation);
        }
        None
    }

    /// Reuses the frame for a new block after reconciling any prior dirty state.
    ///
    /// Callers enter with eviction/install ownership already established. This
    /// method performs the actual disk read and leaves the frame in
    /// `loading+evicting` until the caller publishes the directory entry.
    fn assign_to_block_locked(
        &self,
        meta: &mut FrameMeta,
        block_id: &BlockId,
    ) -> Option<WritebackCompletion> {
        let completion = self.flush_locked(meta);
        self.begin_loading_residency_locked(meta, block_id.clone());
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
        meta.mark_flush_clean();
        completion
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

#[derive(Debug, Clone)]
enum DirectoryEntry {
    /// A thread owns installation for this block, but no pinnable frame has
    /// been published yet.
    Installing,
    /// A block is resident in `frame_idx` for the recorded residency generation.
    ///
    /// The generation is part of the lookup result so callers can validate the
    /// frame after dropping the shard lock.
    Resident { frame_idx: usize, generation: u64 },
}

/// Fixed shard count for the resident directory.
///
/// Why this is constant: the directory is on the resident-hit path, so shard
/// layout should be predictable and dependency-free. A power of two lets shard
/// selection use a mask after hashing.
const DIRECTORY_SHARD_COUNT: usize = 64;

/// Sharded block-to-frame directory used before frame-local OCC validation.
///
/// Why this exists: resident hits need to find a candidate frame without the old
/// global directory mutex or per-block latch path. Each shard protects only the
/// `BlockId -> DirectoryEntry` map for that shard; correctness does not rely on
/// holding the shard lock after lookup. Callers must validate the returned frame
/// generation and transient loading/evicting bits before treating a lookup as a
/// real pin.
///
/// Invariants:
/// - at most one `DirectoryEntry` exists per `BlockId`
/// - `Installing` reserves installation ownership for one miss path
/// - `Resident` entries are advisory until frame generation validation succeeds
#[derive(Debug)]
struct ShardedDirectory {
    /// Independent maps so disjoint resident hits do not serialize on one mutex.
    shards: Vec<Mutex<HashMap<BlockId, DirectoryEntry>>>,
    /// Shared hash builder keeps shard choice consistent across operations.
    hash_builder: RandomState,
}

/// Nonblocking lookup result for `pin_fast()`.
///
/// The distinction between `Absent` and `Locked` is observable by B-tree
/// latch-crabbing code: a real miss can be slow-pinned after releasing latches,
/// while contention should restart without changing residency.
enum DirectoryTryGet {
    Present(DirectoryEntry),
    Absent,
    Locked,
}

impl ShardedDirectory {
    /// Creates an empty directory with fixed independent shards.
    fn new() -> Self {
        let shards = (0..DIRECTORY_SHARD_COUNT)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Self {
            shards,
            hash_builder: RandomState::new(),
        }
    }

    /// Chooses the shard that owns `block_id`.
    fn shard_idx(&self, block_id: &BlockId) -> usize {
        (self.hash_builder.hash_one(block_id) as usize) & (self.shards.len() - 1)
    }

    /// Locks one shard, recording opt-in profiling counters when enabled.
    fn lock_shard(&self, shard_idx: usize) -> MutexGuard<'_, HashMap<BlockId, DirectoryEntry>> {
        let shard = &self.shards[shard_idx];
        if !profile_counters_enabled() {
            return shard.lock().unwrap();
        }

        DIRECTORY_LOCK_CALLS.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        match shard.try_lock() {
            Ok(guard) => {
                DIRECTORY_LOCK_ELAPSED_NS.fetch_add(elapsed_ns_since(start), Ordering::Relaxed);
                guard
            }
            Err(TryLockError::WouldBlock) => {
                DIRECTORY_LOCK_CONTENDED.fetch_add(1, Ordering::Relaxed);
                let guard = shard.lock().unwrap();
                DIRECTORY_LOCK_ELAPSED_NS.fetch_add(elapsed_ns_since(start), Ordering::Relaxed);
                guard
            }
            Err(TryLockError::Poisoned(_)) => shard.lock().unwrap(),
        }
    }

    /// Tries to lock one shard without waiting.
    ///
    /// Used by `pin_fast()` so callers can report internal contention instead of
    /// blocking while holding higher-level page latches.
    fn try_lock_shard(
        &self,
        shard_idx: usize,
    ) -> Option<MutexGuard<'_, HashMap<BlockId, DirectoryEntry>>> {
        if profile_counters_enabled() {
            DIRECTORY_TRY_LOCK_CALLS.fetch_add(1, Ordering::Relaxed);
        }

        match self.shards[shard_idx].try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::WouldBlock) => {
                if profile_counters_enabled() {
                    DIRECTORY_TRY_LOCK_FAILED.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            Err(TryLockError::Poisoned(_)) => None,
        }
    }

    /// Looks up a block entry on the blocking pin path.
    fn get(&self, block_id: &BlockId) -> Option<DirectoryEntry> {
        let shard_idx = self.shard_idx(block_id);
        self.lock_shard(shard_idx).get(block_id).cloned()
    }

    /// Looks up a block entry without waiting on the shard mutex.
    fn try_get(&self, block_id: &BlockId) -> DirectoryTryGet {
        let shard_idx = self.shard_idx(block_id);
        let Some(directory) = self.try_lock_shard(shard_idx) else {
            return DirectoryTryGet::Locked;
        };
        match directory.get(block_id).cloned() {
            Some(entry) => DirectoryTryGet::Present(entry),
            None => DirectoryTryGet::Absent,
        }
    }

    /// Removes a resident entry only if it still names the same frame generation.
    ///
    /// This prevents an eviction/install race from clearing a newer mapping that
    /// reused the same `BlockId` after the caller observed an older generation.
    fn remove_if_matches(&self, block_id: &BlockId, frame_idx: usize, generation: u64) {
        let shard_idx = self.shard_idx(block_id);
        let mut directory = self.lock_shard(shard_idx);
        let remove = matches!(
            directory.get(block_id),
            Some(DirectoryEntry::Resident {
                frame_idx: existing_idx,
                generation: existing_generation
            }) if *existing_idx == frame_idx && *existing_generation == generation
        );
        if remove {
            directory.remove(block_id);
        }
    }

    /// Attempts to reserve installation ownership for a missing block.
    ///
    /// Returns the existing entry when another thread already owns installation
    /// or has published residency. Returning `None` means the caller inserted
    /// `Installing` and must either publish or clear it.
    fn begin_install_if_absent(&self, block_id: &BlockId) -> Option<DirectoryEntry> {
        let shard_idx = self.shard_idx(block_id);
        let mut directory = self.lock_shard(shard_idx);
        match directory.get(block_id).cloned() {
            Some(existing) => Some(existing),
            None => {
                directory.insert(block_id.clone(), DirectoryEntry::Installing);
                None
            }
        }
    }

    /// Publishes a frame generation after the page bytes and frame metadata are ready.
    fn publish_resident(&self, block_id: &BlockId, frame_idx: usize, generation: u64) {
        let shard_idx = self.shard_idx(block_id);
        self.lock_shard(shard_idx).insert(
            block_id.clone(),
            DirectoryEntry::Resident {
                frame_idx,
                generation,
            },
        );
    }

    /// Clears an abandoned installation reservation.
    ///
    /// The entry is removed only while it is still `Installing`, so a concurrent
    /// successful publisher cannot be erased by a stale cleanup path.
    fn clear_installing(&self, block_id: &BlockId) {
        let shard_idx = self.shard_idx(block_id);
        let mut directory = self.lock_shard(shard_idx);
        if matches!(directory.get(block_id), Some(DirectoryEntry::Installing)) {
            directory.remove(block_id);
        }
    }
}

enum PinAttempt {
    /// Pin succeeded and returned the resident frame.
    Ready(Arc<BufferFrame>),
    /// Pin could succeed soon, but the caller observed a transient race such as
    /// `Installing`, generation mismatch, or `loading/evicting`. Retry without
    /// entering the global no-free-buffer wait path.
    Retry,
    /// Pin could not make progress because no frame was currently claimable for
    /// installation. Caller should wait on the global free-buffer condition.
    NeedWait,
}

struct PrefetchReservation {
    block_id: BlockId,
    frame_idx: usize,
    generation: u64,
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

/// One claimed dirty generation plus its stable page snapshot.
struct PendingWriteback {
    frame: Arc<BufferFrame>,
    block_id: BlockId,
    lsn: Lsn,
    generation: u64,
    snapshot: Page,
}

/// Owns the background dirty-page flush policy and execution loop.
///
/// Why this exists: the flusher needs stable access to a narrow slice of the
/// buffer manager state, but its protocol should still read as one coherent
/// subsystem instead of a set of free helpers hanging off `BufferManager`.
struct BackgroundFlusher {
    buffer_pool: Vec<Arc<BufferFrame>>,
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    clean_unpinned: Arc<AtomicUsize>,
    dirty_queue: Arc<Mutex<VecDeque<usize>>>,
    flush_coordinator: Arc<FlushCoordinator>,
}

impl BackgroundFlusher {
    fn new(
        buffer_pool: Vec<Arc<BufferFrame>>,
        file_manager: SharedFS,
        log_manager: Arc<Mutex<LogManager>>,
        clean_unpinned: Arc<AtomicUsize>,
        dirty_queue: Arc<Mutex<VecDeque<usize>>>,
        flush_coordinator: Arc<FlushCoordinator>,
    ) -> Self {
        Self {
            buffer_pool,
            file_manager,
            log_manager,
            clean_unpinned,
            dirty_queue,
            flush_coordinator,
        }
    }

    /// Waits until either the oldest queued dirty frame has aged enough to
    /// flush or shutdown has been requested.
    fn wait_for_flush_signal(&self) -> bool {
        let mut state = self.flush_coordinator.state.lock().unwrap();
        loop {
            if state.shutdown {
                return false;
            }
            match state.oldest_dirty_signal {
                Some(oldest) => {
                    let elapsed = oldest.elapsed();
                    if elapsed >= BufferManager::FLUSH_AGE_THRESHOLD {
                        state.oldest_dirty_signal = None;
                        return true;
                    }
                    let timeout = BufferManager::FLUSH_AGE_THRESHOLD - elapsed;
                    let (next_state, _) = self
                        .flush_coordinator
                        .cond
                        .wait_timeout(state, timeout)
                        .unwrap();
                    state = next_state;
                }
                None => {
                    state = self.flush_coordinator.cond.wait(state).unwrap();
                }
            }
        }
    }

    /// Drains up to one batch of flush-eligible frames from the dirty queue and
    /// snapshots stable page images for them.
    fn collect_dirty_snapshots(&self, batch_limit: usize) -> Vec<PendingWriteback> {
        let mut pending = Vec::new();
        let mut deferred = Vec::new();
        while pending.len() < batch_limit {
            let Some(frame_idx) = self.dirty_queue.lock().unwrap().pop_front() else {
                break;
            };
            let buffer = &self.buffer_pool[frame_idx];
            let mut meta = buffer.lock_meta();
            meta.mark_dequeued();
            if !meta.is_dirty() {
                continue;
            }
            let Some((block_id, lsn, generation, snapshot)) =
                buffer.claim_snapshot_for_writeback_locked(&mut meta, true)
            else {
                if meta
                    .try_queue_dirty_if_flushable(buffer.pin_count())
                    .is_some()
                {
                    deferred.push(frame_idx);
                }
                continue;
            };
            pending.push(PendingWriteback {
                frame: Arc::clone(buffer),
                block_id,
                lsn,
                generation,
                snapshot,
            });
        }
        self.requeue_dirty_frames(deferred);
        pending
    }

    /// Writes one batch of already-snapshotted pages after enforcing WAL-before-data.
    fn write_snapshot_batch(&self, pending: &[PendingWriteback]) {
        if pending.is_empty() {
            return;
        }
        let max_lsn = pending
            .iter()
            .map(|pending| pending.lsn)
            .max()
            .expect("pending batch has at least one lsn");
        self.log_manager.lock().unwrap().flush_lsn(max_lsn);

        let reqs: Vec<BatchWriteReq> = pending
            .iter()
            .map(|pending| BatchWriteReq {
                block_id: pending.block_id.clone(),
            })
            .collect();
        let pages: Vec<&Page> = pending.iter().map(|pending| &pending.snapshot).collect();
        self.file_manager.write_batch(&reqs, &pages);
    }

    fn requeue_dirty_frames(&self, frame_indices: Vec<usize>) {
        if frame_indices.is_empty() {
            return;
        }
        {
            let mut queue = self.dirty_queue.lock().unwrap();
            for frame_idx in frame_indices {
                queue.push_back(frame_idx);
            }
        }
        let mut state = self.flush_coordinator.state.lock().unwrap();
        if state.oldest_dirty_signal.is_none() {
            state.oldest_dirty_signal = Some(Instant::now());
        }
        self.flush_coordinator.cond.notify_one();
    }

    /// Applies completion-side frame transitions for one finished snapshot batch.
    fn complete_snapshot_batch(&self, pending: Vec<PendingWriteback>) {
        let mut requeued = Vec::new();
        for pending in pending {
            let mut meta = pending.frame.lock_meta();
            let block_still_matches = meta.block_id() == Some(&pending.block_id);
            if let Some(transition) = meta.complete_writeback_transition(
                block_still_matches,
                pending.generation,
                pending.frame.pin_count(),
            ) {
                if transition.became_clean_unpinned {
                    self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
                }
                if let Some(frame_idx) = transition.enqueue_dirty {
                    requeued.push(frame_idx);
                }
            }
        }
        self.requeue_dirty_frames(requeued);
        self.flush_coordinator.cond.notify_all();
    }

    /// Background precleaning loop.
    ///
    /// The loop stays pressure-driven: it only consumes the dirty queue when the
    /// number of clean unpinned frames falls below a low watermark.
    fn run(&self) {
        let clean_target = (self.buffer_pool.len() / 4).max(1);
        while self.wait_for_flush_signal() {
            while self.clean_unpinned.load(Ordering::Acquire) < clean_target {
                let pending = self.collect_dirty_snapshots(BufferManager::FLUSH_BATCH_SIZE);
                if pending.is_empty() {
                    break;
                }
                self.write_snapshot_batch(&pending);
                self.complete_snapshot_batch(pending);
            }
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
    directory: ShardedDirectory,
    policy: PolicyState,
    /// Frames that transitioned into a flushable dirty state.
    dirty_queue: Arc<Mutex<VecDeque<usize>>>,
    flush_coordinator: Arc<FlushCoordinator>,
    flusher_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BufferManager {
    const MAX_TIME: u64 = 10;
    const FLUSH_BATCH_SIZE: usize = 32;
    const FLUSH_AGE_THRESHOLD: Duration = Duration::from_millis(2);

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
            let flusher = BackgroundFlusher::new(
                buffer_pool.clone(),
                Arc::clone(&file_manager),
                Arc::clone(&log_manager),
                Arc::clone(&clean_unpinned),
                Arc::clone(&dirty_queue),
                Arc::clone(&flush_coordinator),
            );
            thread::spawn(move || flusher.run())
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
            directory: ShardedDirectory::new(),
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

    /// Wakes one thread waiting for a reusable buffer frame.
    ///
    /// Why this stays narrow: one returned frame can satisfy at most one waiter,
    /// and waking every waiter turns ordinary last-unpin traffic into a global
    /// futex storm when the pool is not actually exhausted.
    fn notify_free_buffer_waiters_one(&self) {
        if !profile_counters_enabled() {
            self.cond.notify_one();
            return;
        }

        FREE_WAIT_NOTIFY_ALL_CALLS.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();
        self.cond.notify_one();
        FREE_WAIT_NOTIFY_ALL_ELAPSED_NS.fetch_add(elapsed_ns_since(start), Ordering::Relaxed);
    }

    /// Returns one frame to the available-frame count and wakes a waiter only
    /// when the pool transitions from exhausted to non-exhausted.
    ///
    /// The `0 -> 1` gate is the key invariant: if `num_available` was already
    /// positive, no pin caller should be blocked on the free-buffer condvar, so
    /// notifying would add synchronization cost without changing progress.
    fn release_available_frame(&self) {
        let previous = self.num_available.fetch_add(1, Ordering::AcqRel);
        if previous == 0 {
            self.notify_free_buffer_waiters_one();
        }
    }

    /// Claims snapshot writeback work for one transaction during synchronous force-flush paths.
    fn collect_dirty_snapshots_for_txn(
        &self,
        batch_limit: usize,
        txn_num: usize,
    ) -> Vec<PendingWriteback> {
        let mut pending = Vec::new();
        while pending.len() < batch_limit {
            let Some(frame_idx) = self.dirty_queue.lock().unwrap().pop_front() else {
                break;
            };
            let buffer = &self.buffer_pool[frame_idx];
            let mut meta = buffer.lock_meta();
            meta.mark_dequeued();
            if !meta.is_dirty() {
                continue;
            }
            if meta.txn() != Some(txn_num) {
                if let Some(frame_idx) = meta.try_queue_dirty_if_flushable(buffer.pin_count()) {
                    self.enqueue_dirty_frame(frame_idx);
                }
                continue;
            }
            let Some((block_id, lsn, generation, snapshot)) =
                buffer.claim_snapshot_for_writeback_locked(&mut meta, true)
            else {
                if let Some(frame_idx) = meta.try_queue_dirty_if_flushable(buffer.pin_count()) {
                    self.enqueue_dirty_frame(frame_idx);
                }
                continue;
            };
            pending.push(PendingWriteback {
                frame: Arc::clone(buffer),
                block_id,
                lsn,
                generation,
                snapshot,
            });
        }
        pending
    }

    /// Returns whether this transaction still owns any dirty generations that
    /// must be forced before rollback/recovery returns.
    fn has_dirty_for_txn(&self, txn_num: usize) -> bool {
        self.buffer_pool.iter().any(|buffer| {
            let meta = buffer.lock_meta();
            meta.txn() == Some(txn_num) && meta.is_dirty()
        })
    }

    fn directory_entry(&self, block_id: &BlockId) -> Option<DirectoryEntry> {
        self.directory.get(block_id)
    }

    fn remove_directory_entry_if_matches(
        &self,
        block_id: &BlockId,
        frame_idx: usize,
        generation: u64,
    ) {
        self.directory
            .remove_if_matches(block_id, frame_idx, generation);
    }

    fn begin_install_if_absent(&self, block_id: &BlockId) -> Option<DirectoryEntry> {
        self.directory.begin_install_if_absent(block_id)
    }

    fn publish_resident_entry(&self, block_id: &BlockId, frame_idx: usize, generation: u64) {
        self.directory
            .publish_resident(block_id, frame_idx, generation);
    }

    fn clear_installing_entry(&self, block_id: &BlockId) {
        self.directory.clear_installing(block_id);
    }

    fn claim_victim_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
        for _ in 0..self.buffer_pool.len() {
            let frame_idx = self.policy.evict_frame(&self.buffer_pool)?;
            let frame = &self.buffer_pool[frame_idx];
            if let Some(meta_guard) = frame.try_claim_for_eviction() {
                return Some((frame_idx, meta_guard));
            }
        }
        None
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

    pub fn reset_profile_counters() {
        DIRECTORY_LOCK_CALLS.store(0, Ordering::Relaxed);
        DIRECTORY_LOCK_CONTENDED.store(0, Ordering::Relaxed);
        DIRECTORY_LOCK_ELAPSED_NS.store(0, Ordering::Relaxed);
        DIRECTORY_TRY_LOCK_CALLS.store(0, Ordering::Relaxed);
        DIRECTORY_TRY_LOCK_FAILED.store(0, Ordering::Relaxed);
        FRAME_META_LOCK_CALLS.store(0, Ordering::Relaxed);
        FRAME_META_LOCK_CONTENDED.store(0, Ordering::Relaxed);
        FRAME_META_LOCK_ELAPSED_NS.store(0, Ordering::Relaxed);
        FRAME_META_TRY_LOCK_CALLS.store(0, Ordering::Relaxed);
        FRAME_META_TRY_LOCK_FAILED.store(0, Ordering::Relaxed);
        FREE_WAIT_NOTIFY_ALL_CALLS.store(0, Ordering::Relaxed);
        FREE_WAIT_NOTIFY_ALL_ELAPSED_NS.store(0, Ordering::Relaxed);
    }

    pub fn profile_counters() -> BufferPoolProfileCounters {
        BufferPoolProfileCounters {
            directory_lock_calls: DIRECTORY_LOCK_CALLS.load(Ordering::Relaxed),
            directory_lock_contended: DIRECTORY_LOCK_CONTENDED.load(Ordering::Relaxed),
            directory_lock_elapsed_ns: DIRECTORY_LOCK_ELAPSED_NS.load(Ordering::Relaxed),
            directory_try_lock_calls: DIRECTORY_TRY_LOCK_CALLS.load(Ordering::Relaxed),
            directory_try_lock_failed: DIRECTORY_TRY_LOCK_FAILED.load(Ordering::Relaxed),
            frame_meta_lock_calls: FRAME_META_LOCK_CALLS.load(Ordering::Relaxed),
            frame_meta_lock_contended: FRAME_META_LOCK_CONTENDED.load(Ordering::Relaxed),
            frame_meta_lock_elapsed_ns: FRAME_META_LOCK_ELAPSED_NS.load(Ordering::Relaxed),
            frame_meta_try_lock_calls: FRAME_META_TRY_LOCK_CALLS.load(Ordering::Relaxed),
            frame_meta_try_lock_failed: FRAME_META_TRY_LOCK_FAILED.load(Ordering::Relaxed),
            free_wait_notify_all_calls: FREE_WAIT_NOTIFY_ALL_CALLS.load(Ordering::Relaxed),
            free_wait_notify_all_elapsed_ns: FREE_WAIT_NOTIFY_ALL_ELAPSED_NS
                .load(Ordering::Relaxed),
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

        let mut reservations: Vec<PrefetchReservation> = Vec::new();
        let mut reqs: Vec<BatchReadReq> = Vec::new();

        for block_num in start_block..start_block.saturating_add(count) {
            let block_id = BlockId::new(file.to_string(), block_num);
            if self.begin_install_if_absent(&block_id).is_some() {
                continue;
            }

            let (frame_idx, mut meta_guard) = match self.claim_victim_frame() {
                Some(victim) => victim,
                None => {
                    self.clear_installing_entry(&block_id);
                    break;
                }
            };
            let frame = Arc::clone(&self.buffer_pool[frame_idx]);

            if let Some(old) = meta_guard.block_id_owned() {
                self.remove_directory_entry_if_matches(
                    &old,
                    frame_idx,
                    frame.residency_generation(),
                );
            }
            let flush_completion = frame.flush_locked(&mut meta_guard);
            if flush_completion
                .as_ref()
                .is_some_and(|transition| transition.became_clean_unpinned)
            {
                self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
            }
            let generation = frame.begin_loading_placeholder_locked(&mut meta_guard);
            meta_guard.mark_flush_clean();

            let previous_pin_count = frame.pin_count.fetch_add(1, Ordering::AcqRel);
            let transition = meta_guard.pin_transition(previous_pin_count);
            debug_assert!(
                matches!(
                    transition,
                    PinTransition::BecamePinnedClean | PinTransition::BecamePinnedDirty
                ),
                "reserved prefetch frame must have zero pins"
            );
            drop(meta_guard);
            self.num_available.fetch_sub(1, Ordering::AcqRel);
            if matches!(transition, PinTransition::BecamePinnedClean) {
                self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
            }

            reservations.push(PrefetchReservation {
                block_id: block_id.clone(),
                frame_idx,
                generation,
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
            let frame = Arc::clone(&self.buffer_pool[reservation.frame_idx]);
            {
                let mut meta_guard = frame.lock_meta();
                if frame.residency_generation() != reservation.generation || !frame.is_loading() {
                    self.clear_installing_entry(&reservation.block_id);
                    continue;
                }
                let mut page_guard = frame.write_page();
                *page_guard = std::mem::take(&mut pages[idx]);
                meta_guard.assign_resident(reservation.block_id.clone());
                meta_guard.mark_flush_clean();
                self.publish_resident_entry(
                    &reservation.block_id,
                    reservation.frame_idx,
                    reservation.generation,
                );
            }
            frame.finish_loading_residency();
            self.policy
                .on_frame_assigned(&self.buffer_pool, reservation.frame_idx);
            installed += 1;
            if let Some(stats) = self.stats.get() {
                stats.prefetch_installed.fetch_add(1, Ordering::Relaxed);
            }

            frames_to_release.push(frame);
        }

        for frame in frames_to_release {
            let transition = {
                let previous_pin_count = frame.pin_count.fetch_sub(1, Ordering::AcqRel);
                debug_assert!(previous_pin_count > 0, "prefetch release must hold a pin");
                let mut meta_guard = frame.lock_meta();
                meta_guard.unpin_transition(previous_pin_count - 1)
            };
            match transition {
                UnpinTransition::StillPinned => {}
                UnpinTransition::BecameUnpinnedClean => {
                    self.release_available_frame();
                    self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
                }
                UnpinTransition::BecameUnpinnedDirty { enqueue_dirty } => {
                    self.release_available_frame();
                    if let Some(frame_idx) = enqueue_dirty {
                        self.enqueue_dirty_frame(frame_idx);
                    }
                }
            }
        }

        installed
    }

    pub(crate) fn flush_all(&self, txn_num: usize) {
        assert!(
            !self.buffer_pool.iter().any(|buffer| {
                let meta = buffer.lock_meta();
                meta.txn() == Some(txn_num) && meta.is_dirty() && buffer.pin_count() > 0
            }),
            "flush_all assumes target transaction has released all page pins before forcing writeback"
        );
        let flusher = BackgroundFlusher::new(
            self.buffer_pool.clone(),
            Arc::clone(&self.file_manager),
            Arc::clone(&self.log_manager),
            Arc::clone(&self.clean_unpinned),
            Arc::clone(&self.dirty_queue),
            Arc::clone(&self.flush_coordinator),
        );
        while self.has_dirty_for_txn(txn_num) {
            let pending = self.collect_dirty_snapshots_for_txn(Self::FLUSH_BATCH_SIZE, txn_num);
            if pending.is_empty() {
                let state = self.flush_coordinator.state.lock().unwrap();
                let _ = self
                    .flush_coordinator
                    .cond
                    .wait_timeout(state, Self::FLUSH_AGE_THRESHOLD)
                    .unwrap();
                continue;
            }
            flusher.write_snapshot_batch(&pending);
            flusher.complete_snapshot_batch(pending);
        }
    }

    /// Applies global availability bookkeeping for one successful pin transition.
    ///
    /// [`PinTransition`] tells us whether this pin consumed a reusable frame from
    /// the buffer pool's slack accounting:
    /// - [`PinTransition::StillPinned`]: no global counters change
    /// - [`PinTransition::BecamePinnedClean`]: one available frame and one clean-slack frame were consumed
    /// - [`PinTransition::BecamePinnedDirty`]: one available frame was consumed, but not from clean slack
    fn apply_pin_transition_accounting(&self, transition: PinTransition) {
        if !matches!(transition, PinTransition::StillPinned) {
            self.num_available.fetch_sub(1, Ordering::AcqRel);
            if matches!(transition, PinTransition::BecamePinnedClean) {
                self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Full pin path with immediate replacement policy updates and eviction.
    pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
        let start = Instant::now();
        loop {
            match self.pin_once_without_free_wait(block_id) {
                PinAttempt::Ready(buffer) => return Ok(buffer),
                PinAttempt::Retry => {
                    thread::yield_now();
                    continue;
                }
                PinAttempt::NeedWait => {}
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

    /// Attempts one full pin without sleeping on the global free-buffer condvar.
    ///
    /// The method has three phases:
    /// - resident attempt: consult the directory and try the OCC resident-hit pin
    /// - install attempt: if absent, try to become the sole installer for this block
    /// - victim claim: if install ownership was acquired, try to claim and reuse one frame
    ///
    /// The return value tells `pin()` whether this call:
    /// - succeeded immediately
    /// - lost a transient race and should be retried soon
    /// - or hit a real no-frame-available condition and should enter the global wait path
    fn pin_once_without_free_wait(&self, block_id: &BlockId) -> PinAttempt {
        match self.directory_entry(block_id) {
            Some(DirectoryEntry::Resident {
                frame_idx,
                generation,
            }) => {
                let frame_ptr = Arc::clone(&self.buffer_pool[frame_idx]);
                let transition = match frame_ptr.pin_from_directory_entry(generation) {
                    Some(transition) => transition,
                    None => return PinAttempt::Retry,
                };
                self.apply_pin_transition_accounting(transition);
                self.policy.on_hit(&self.buffer_pool, frame_idx);
                if let Some(stats) = self.stats.get() {
                    stats.hits.fetch_add(1, Ordering::Relaxed);
                }
                return PinAttempt::Ready(frame_ptr);
            }
            Some(DirectoryEntry::Installing) => return PinAttempt::Retry,
            None => {}
        }

        if let Some(stats) = self.stats.get() {
            stats.misses.fetch_add(1, Ordering::Relaxed);
        }

        if self.begin_install_if_absent(block_id).is_some() {
            return PinAttempt::Retry;
        }

        let (frame_idx, mut meta_guard) = match self.claim_victim_frame() {
            Some(victim) => victim,
            None => {
                self.clear_installing_entry(block_id);
                return PinAttempt::NeedWait;
            }
        };
        let frame = Arc::clone(&self.buffer_pool[frame_idx]);

        if let Some(old) = meta_guard.block_id_owned() {
            self.remove_directory_entry_if_matches(&old, frame_idx, frame.residency_generation());
        }
        let flush_completion = frame.assign_to_block_locked(&mut meta_guard, block_id);
        if flush_completion
            .as_ref()
            .is_some_and(|transition| transition.became_clean_unpinned)
        {
            self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
        }
        let previous_pin_count = frame.pin_count.fetch_add(1, Ordering::AcqRel);
        let transition = meta_guard.pin_transition(previous_pin_count);
        let generation = frame.residency_generation();
        self.publish_resident_entry(block_id, frame_idx, generation);
        debug_assert!(
            matches!(
                transition,
                PinTransition::BecamePinnedClean | PinTransition::BecamePinnedDirty
            ),
            "newly assigned frame must have zero pins"
        );
        drop(meta_guard);

        frame.finish_loading_residency();
        self.policy.on_frame_assigned(&self.buffer_pool, frame_idx);

        self.apply_pin_transition_accounting(transition);
        PinAttempt::Ready(frame)
    }

    /// Fast path for latch-crabbing callers.
    ///
    /// This is resident-only and never performs eviction or blocking waits.
    ///
    /// A directory miss is a real residency miss, not contention: callers use
    /// [`FastPinOutcome::NotResident`] to slow-pin outside latch scope and then
    /// retry traversal. Only failure to acquire an internal latch reports
    /// [`FastPinOutcome::Contended`].
    ///
    /// The fast path still has to complete policy hit bookkeeping. If that
    /// bookkeeping would block, the speculative pin is rolled back and the
    /// caller sees [`FastPinOutcome::Contended`].
    pub fn pin_fast(&self, block_id: &BlockId) -> FastPinOutcome<Arc<BufferFrame>> {
        let entry = match self.directory.try_get(block_id) {
            DirectoryTryGet::Present(entry) => entry,
            DirectoryTryGet::Absent => return FastPinOutcome::NotResident,
            DirectoryTryGet::Locked => return FastPinOutcome::Contended,
        };
        let (frame_idx, generation) = match entry {
            DirectoryEntry::Resident {
                frame_idx,
                generation,
            } => (frame_idx, generation),
            DirectoryEntry::Installing => return FastPinOutcome::NotResident,
        };
        let frame_ptr = Arc::clone(&self.buffer_pool[frame_idx]);

        let transition = match frame_ptr.try_pin_from_directory_entry(generation) {
            Ok(Some(transition)) => transition,
            Ok(None) => return FastPinOutcome::NotResident,
            Err(()) => return FastPinOutcome::Contended,
        };
        if !self.policy.try_on_hit(&self.buffer_pool, frame_idx) {
            let previous_pin_count = frame_ptr.pin_count.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(
                previous_pin_count > 0,
                "fast-path rollback must undo an existing speculative pin"
            );
            return FastPinOutcome::Contended;
        }
        self.apply_pin_transition_accounting(transition);

        FastPinOutcome::Ready(frame_ptr)
    }

    /// Releases one buffer pin and applies any last-pin bookkeeping.
    ///
    /// The decrement itself is atomic, but the `1 -> 0` transition still has
    /// to consult [`FrameMeta`] to answer colder protocol questions:
    /// - did the frame become part of clean slack?
    /// - should a dirty frame now enter the flush queue?
    /// - should waiters be notified that one reusable frame is available?
    ///
    /// So the hot pin count lives on [`BufferFrame`], while the final transition
    /// still reconciles availability and flush state under [`FrameMeta`].
    pub fn unpin(&self, frame: Arc<BufferFrame>) {
        let transition = {
            let previous_pin_count = frame.pin_count.fetch_sub(1, Ordering::AcqRel);
            assert!(previous_pin_count > 0, "BufferManager::unpin on zero pins");
            let mut meta = frame.lock_meta();
            meta.unpin_transition(previous_pin_count - 1)
        };
        match transition {
            UnpinTransition::StillPinned => {}
            UnpinTransition::BecameUnpinnedClean => {
                self.release_available_frame();
                self.clean_unpinned.fetch_add(1, Ordering::AcqRel);
            }
            UnpinTransition::BecameUnpinnedDirty { enqueue_dirty } => {
                self.release_available_frame();
                if let Some(frame_idx) = enqueue_dirty {
                    self.enqueue_dirty_frame(frame_idx);
                }
            }
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
            meta.mark_dirty_transition(frame.pin_count(), txn_num, lsn)
        };
        if transition.left_clean_unpinned {
            self.clean_unpinned.fetch_sub(1, Ordering::AcqRel);
        }
        if let Some(frame_idx) = transition.enqueue_dirty {
            self.enqueue_dirty_frame(frame_idx);
        }
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
