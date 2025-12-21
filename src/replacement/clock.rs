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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{
    buffer_manager::{BufferFrame, FrameMeta},
    BlockId,
};

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

    /// Records a cache hit by setting the frame's reference bit.
    ///
    /// Returns None if the frame no longer contains the requested block.
    pub fn record_hit<'a>(
        &self,
        _buffer_pool: &'a [Arc<BufferFrame>],
        frame_ptr: &'a Arc<BufferFrame>,
        block_id: &BlockId,
        resident_table: &Mutex<HashMap<BlockId, Weak<BufferFrame>>>,
    ) -> Option<MutexGuard<'a, FrameMeta>> {
        let mut frame_guard = frame_ptr.lock_meta();
        if !frame_guard
            .block_id
            .as_ref()
            .is_some_and(|current| current == block_id)
        {
            resident_table.lock().unwrap().remove(block_id);
            return None;
        }
        frame_guard.ref_bit = true;
        Some(frame_guard)
    }

    /// Notifies the policy that a frame has been assigned.
    ///
    /// Sets the reference bit to give the new frame a "second chance".
    pub fn on_frame_assigned(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        let mut guard = buffer_pool[frame_idx].lock_meta();
        guard.ref_bit = true;
    }

    /// Selects a victim frame using the clock algorithm.
    ///
    /// Sweeps the clock hand circularly, giving "second chances" by clearing reference
    /// bits. Evicts the first unpinned frame with ref_bit = false. Returns None if all
    /// frames are pinned or have their reference bits set after a full sweep.
    pub fn evict_frame<'a>(
        &self,
        buffer_pool: &'a [Arc<BufferFrame>],
    ) -> Option<(usize, MutexGuard<'a, FrameMeta>)> {
        let mut hand = self.hand.lock().unwrap();
        for _ in 0..self.pool_len {
            let idx = *hand;
            let mut frame_guard = buffer_pool[idx].lock_meta();
            if frame_guard.pins > 0 {
                *hand = (idx + 1) % self.pool_len;
                continue;
            }
            if frame_guard.ref_bit {
                frame_guard.ref_bit = false;
                *hand = (idx + 1) % self.pool_len;
                continue;
            }
            *hand = (idx + 1) % self.pool_len;
            return Some((idx, frame_guard));
        }
        None
    }
}
