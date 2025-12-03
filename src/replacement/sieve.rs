//! SIEVE replacement policy.
//!
//! Implements the SIEVE eviction algorithm which combines aspects of both LRU-style
//! lists and clock-style reference bits. Designed to be scan-resistant while maintaining
//! simplicity and efficiency.
//!
//! # Algorithm
//!
//! - Maintains an intrusive doubly-linked list ordered by insertion time
//! - Uses a "hand" pointer similar to clock, but traverses the list
//! - On hit: Sets reference bit (doesn't move in list)
//! - On allocation: Inserts at head with ref bit set
//! - On eviction: Sweeps hand backward from tail
//!   - If pinned: skip
//!   - If ref bit set: clear bit and continue
//!   - Otherwise: evict
//!
//! # Advantages
//!
//! - Scan-resistant: Sequential scans don't pollute the entire cache
//! - Simple: No complex promotion logic like LRU
//! - Efficient: Reference bits avoid unnecessary list manipulation
//!
//! See the SIEVE paper: https://cachemon.github.io/SIEVE-website/

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{
    intrusive_dll::{IntrusiveList, IntrusiveNode},
    BlockId, BufferFrame, FrameMeta,
};

/// Internal state combining list structure and hand pointer.
#[derive(Debug)]
struct ListState {
    intrusive_list: IntrusiveList,
    /// Hand pointer for eviction sweeps (None if list empty)
    hand: Option<usize>,
}

/// SIEVE policy state with list and hand.
#[derive(Debug)]
pub struct PolicyState {
    list_state: Mutex<ListState>,
    pool_len: usize,
}

impl PolicyState {
    /// Initializes SIEVE state with a list and hand pointing at tail.
    pub fn new(buffer_pool: &[Arc<BufferFrame>]) -> Self {
        let mut guards = buffer_pool
            .iter()
            .map(|frame| frame.lock_meta())
            .collect::<Vec<MutexGuard<'_, FrameMeta>>>();
        let intrusive_list = IntrusiveList::from_nodes(&mut guards);
        Self {
            list_state: Mutex::new(ListState {
                hand: intrusive_list.peek_tail(),
                intrusive_list,
            }),
            pool_len: buffer_pool.len(),
        }
    }

    /// Records a cache hit by setting the frame's reference bit.
    ///
    /// Unlike LRU, SIEVE does not move the frame in the list on hit.
    /// Returns None if the frame no longer contains the requested block.
    pub fn record_hit<'a>(
        &self,
        _buffer_pool: &[Arc<BufferFrame>],
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
    /// Inserts the frame at the head of the list with reference bit set,
    /// giving it protection from immediate eviction.
    pub fn on_frame_assigned(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        let mut list_guard = self.list_state.lock().unwrap();
        let current_head = list_guard.intrusive_list.peek_head();
        match current_head {
            Some(head) => {
                if frame_idx == head {
                    return;
                }
                let mut frame_guard = buffer_pool[frame_idx].lock_meta();
                frame_guard.ref_bit = true;
                let mut current_head_guard = buffer_pool[head].lock_meta();
                list_guard.intrusive_list.insert_at_head(
                    frame_idx,
                    &mut frame_guard,
                    Some(&mut current_head_guard),
                );
            }
            None => {
                let mut frame_guard = buffer_pool[frame_idx].lock_meta();
                frame_guard.ref_bit = true;
                list_guard
                    .intrusive_list
                    .insert_at_head(frame_idx, &mut frame_guard, None);
            }
        }
    }

    /// Selects a victim frame using the SIEVE algorithm.
    ///
    /// Sweeps the hand backward from tail through the list, clearing reference bits
    /// and skipping pinned frames. Evicts the first unpinned frame with ref_bit = false.
    /// Resets hand to tail if it reaches the head. Returns None if all frames are
    /// pinned or recently accessed after a full sweep.
    pub fn evict_frame<'a>(
        &self,
        buffer_pool: &'a [Arc<BufferFrame>],
    ) -> Option<(usize, MutexGuard<'a, FrameMeta>)> {
        let mut list_guard = self.list_state.lock().unwrap();

        for _ in 0..self.pool_len {
            match list_guard.hand {
                Some(hand) => {
                    let mut current_guard = buffer_pool[hand].lock_meta();
                    if current_guard.pins > 0 {
                        if let Some(head) = list_guard.intrusive_list.peek_head() {
                            if current_guard.index == head {
                                list_guard.hand = list_guard.intrusive_list.peek_tail();
                                continue;
                            }
                        } else {
                            assert!(
                                current_guard.prev().is_some(),
                                "Every node apart from head should have a previous pointer"
                            );
                        }
                        list_guard.hand = current_guard.prev();
                        continue;
                    }
                    if current_guard.ref_bit {
                        current_guard.ref_bit = false;
                        list_guard.hand = current_guard.prev();
                        continue;
                    }
                    let mut prev_node = current_guard
                        .prev()
                        .map(|prev| buffer_pool[prev].lock_meta());
                    let mut next_node = current_guard
                        .next()
                        .map(|next| buffer_pool[next].lock_meta());
                    list_guard.intrusive_list.remove_node(
                        hand,
                        &mut current_guard,
                        prev_node.as_mut(),
                        next_node.as_mut(),
                    );
                    list_guard.hand = current_guard
                        .prev()
                        .or_else(|| list_guard.intrusive_list.peek_tail());
                    return Some((hand, current_guard));
                }
                None => {
                    list_guard.hand = list_guard.intrusive_list.peek_tail();
                    continue;
                }
            }
        }
        None
    }
}
