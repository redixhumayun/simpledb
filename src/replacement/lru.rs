//! LRU (Least Recently Used) replacement policy.
//!
//! Implements classic LRU using an intrusive doubly-linked list where the head
//! represents the most recently used frame and the tail is the eviction candidate.
//!
//! # Algorithm
//!
//! - On hit: Move accessed frame to head
//! - On allocation: Insert new frame at head
//! - On eviction: Scan from tail to head, evicting first unpinned frame
//!
//! # Complexity
//!
//! - Hit: O(1) with optimized promotion
//! - Eviction: O(n) worst case if all frames pinned

use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    buffer_manager::{BufferFrame, FrameMeta},
    intrusive_dll::{IntrusiveList, IntrusiveNode},
};

/// LRU policy state maintaining an intrusive doubly-linked list.
///
/// The list is ordered by recency: head = most recent, tail = least recent.
#[derive(Debug)]
pub struct PolicyState {
    intrusive_list: Mutex<IntrusiveList>,
}

impl PolicyState {
    /// Initializes LRU state by constructing an intrusive list from buffer pool frames.
    pub fn new(buffer_pool: &[Arc<BufferFrame>]) -> Self {
        let mut guards = buffer_pool
            .iter()
            .map(|frame| frame.lock_meta())
            .collect::<Vec<MutexGuard<'_, FrameMeta>>>();
        let intrusive_list = IntrusiveList::from_nodes(&mut guards);
        Self {
            intrusive_list: Mutex::new(intrusive_list),
        }
    }

    /// Notifies the policy that a frame has been assigned a new block.
    ///
    /// Inserts the frame at the head of the LRU list as the most recently used.
    pub fn on_frame_assigned(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        let mut intrusive_list_guard = self.intrusive_list.lock().unwrap();
        let current_head = intrusive_list_guard.peek_head();
        match current_head {
            Some(head) => {
                if frame_idx == head {
                    return;
                }
                let mut frame_guard = buffer_pool[frame_idx].lock_meta();
                let mut current_head_guard = buffer_pool[head].lock_meta();
                intrusive_list_guard.insert_at_head(
                    frame_idx,
                    &mut frame_guard,
                    Some(&mut current_head_guard),
                );
            }
            None => {
                let mut frame_guard = buffer_pool[frame_idx].lock_meta();
                intrusive_list_guard.insert_at_head(frame_idx, &mut frame_guard, None);
            }
        }
    }

    /// Selects a victim frame for eviction.
    ///
    /// Scans from tail (LRU) towards head, skipping pinned frames, and returns the
    /// first unpinned frame. Returns None if all frames are pinned.
    pub fn evict_frame(&self, buffer_pool: &[Arc<BufferFrame>]) -> Option<usize> {
        assert!(
            buffer_pool.len() > 1,
            "Buffer pools must have more than one frame for LRU replacement"
        );
        let mut intrusive_list_guard = self.intrusive_list.lock().unwrap();
        let tail = intrusive_list_guard.peek_tail()?;
        let mut current = tail;
        loop {
            let mut current_guard = buffer_pool[current].lock_meta();
            if buffer_pool[current].pin_count() > 0
                || current_guard.is_writeback_in_progress()
                || buffer_pool[current].is_loading()
                || buffer_pool[current].is_evicting()
            {
                if let Some(head) = intrusive_list_guard.peek_head() {
                    if current_guard.index == head {
                        return None;
                    } else {
                        current = current_guard
                            .prev()
                            .expect("Every node apart from head should have a prev pointer");
                    }
                }
                continue;
            }
            let mut prev_node = current_guard
                .prev()
                .map(|prev| buffer_pool[prev].lock_meta());
            let mut next_node = current_guard
                .next()
                .map(|next| buffer_pool[next].lock_meta());
            intrusive_list_guard.remove_node(
                current,
                &mut current_guard,
                prev_node.as_mut(),
                next_node.as_mut(),
            );
            return Some(current);
        }
    }
}
