//! Clock (Second-Chance) replacement policy.
//!
//! Implements the clock/second-chance algorithm using a circular buffer with
//! reference bits. Approximates LRU with lower overhead than maintaining a list.
//!
//! # Algorithm
//!
//! - On hit: Set reference bit
//! - On allocation: Set reference bit
//! - On eviction: Sweep clock hand circularly
//!   - If frame pinned: skip
//!   - If ref bit set: clear bit and continue
//!   - Otherwise: evict frame
//!
//! # Complexity
//!
//! - Hit: O(1)
//! - Eviction: O(n) worst case (full sweep), typically much better

use std::sync::{Arc, Mutex};

use crate::buffer_manager::{BufferFrame, FrameMeta};

/// Clock policy state with circular hand pointer.
#[derive(Debug)]
pub struct PolicyState {
    /// Clock hand position (next frame to examine)
    hand: Mutex<usize>,
    /// Buffer pool size for wraparound
    pool_len: usize,
}

impl PolicyState {
    /// Initializes clock state with hand at position 0.
    pub fn new(buffer_pool: &[Arc<BufferFrame>]) -> Self {
        assert!(
            !buffer_pool.is_empty(),
            "Clock policy requires at least one buffer frame"
        );
        Self {
            hand: Mutex::new(0),
            pool_len: buffer_pool.len(),
        }
    }

    /// Records a resident hit.
    ///
    /// Clock only needs to set the reference bit; no shared list mutation is required.
    pub fn on_hit(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        buffer_pool[frame_idx].set_ref_bit(true);
    }

    /// Tries to record a resident hit without blocking.
    ///
    /// Clock hit bookkeeping is frame-local, so this always succeeds.
    pub fn try_on_hit(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) -> bool {
        self.on_hit(buffer_pool, frame_idx);
        true
    }

    /// Notifies the policy that a frame has been assigned.
    ///
    /// Sets the reference bit to give the new frame a "second chance".
    pub fn on_frame_assigned(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        buffer_pool[frame_idx].set_ref_bit(true);
    }

    /// Selects a candidate victim frame using the clock algorithm.
    ///
    /// Sweeps the clock hand circularly, giving "second chances" by clearing reference
    /// bits. Returns the first unpinned frame with ref_bit = false. Returns None if
    /// all frames are pinned or have their reference bits set after a full sweep.
    ///
    /// The buffer manager owns the actual eviction claim; this method only suggests
    /// which frame should be tried next.
    pub fn select_victim(&self, buffer_pool: &[Arc<BufferFrame>]) -> Option<usize> {
        let mut hand = self.hand.lock().unwrap();
        for _ in 0..self.pool_len {
            let idx = *hand;
            let frame = &buffer_pool[idx];
            let frame_guard = frame.lock_meta();
            if frame.pin_count() > 0
                || frame_guard.is_writeback_in_progress()
                || frame.is_loading()
                || frame.is_evicting()
            {
                *hand = (idx + 1) % self.pool_len;
                continue;
            }
            drop(frame_guard);
            if frame.ref_bit() {
                frame.set_ref_bit(false);
                *hand = (idx + 1) % self.pool_len;
                continue;
            }
            *hand = (idx + 1) % self.pool_len;
            return Some(idx);
        }
        None
    }

    /// Clock keeps no intrusive membership to commit after a successful claim.
    pub fn try_on_frame_claimed_for_reuse(
        &self,
        _buffer_pool: &[Arc<BufferFrame>],
        _frame_idx: usize,
        _frame_guard: &mut FrameMeta,
    ) -> bool {
        true
    }

    /// Gives a skipped candidate another chance before it is selected again.
    pub fn on_victim_rejected(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        buffer_pool[frame_idx].set_ref_bit(true);
    }
}
