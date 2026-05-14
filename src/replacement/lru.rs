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

    /// Records a resident hit by promoting the frame to the head of the LRU list.
    pub fn on_hit(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) {
        let mut intrusive_list_guard = self.intrusive_list.lock().unwrap();
        self.promote_to_head_blocking(&mut intrusive_list_guard, buffer_pool, frame_idx);
    }

    /// Tries to record a resident hit without blocking.
    ///
    /// Returns `false` if the LRU list or any required frame metadata lock is
    /// contended. Callers may then roll back the speculative fast pin.
    pub fn try_on_hit(&self, buffer_pool: &[Arc<BufferFrame>], frame_idx: usize) -> bool {
        let Some(mut intrusive_list_guard) = self.intrusive_list.try_lock().ok() else {
            return false;
        };
        self.try_promote_to_head(&mut intrusive_list_guard, buffer_pool, frame_idx)
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

    /// Promotes one resident frame to the head of the LRU list, blocking as needed.
    ///
    /// This is the normal hit-path bookkeeping for `pin()`: once the frame is
    /// pinned, LRU recency must be updated before the hit is considered complete.
    fn promote_to_head_blocking(
        &self,
        intrusive_list_guard: &mut IntrusiveList,
        buffer_pool: &[Arc<BufferFrame>],
        frame_idx: usize,
    ) {
        let current_head = intrusive_list_guard.peek_head();
        if current_head == Some(frame_idx) {
            return;
        }

        let mut frame_guard = buffer_pool[frame_idx].lock_meta();
        let predecessor_index = frame_guard.prev();
        let adjacent_to_head =
            matches!((predecessor_index, current_head), (Some(prev), Some(head)) if prev == head);

        if adjacent_to_head {
            let mut current_head_guard =
                current_head.map(|current_head| buffer_pool[current_head].lock_meta());
            let mut next_guard = frame_guard.next().map(|idx| buffer_pool[idx].lock_meta());
            let head_guard = current_head_guard
                .as_mut()
                .expect("Head guard must exist when list is non-empty");
            intrusive_list_guard.promote_successor_to_head(
                head_guard,
                &mut frame_guard,
                next_guard.as_mut(),
            );
            return;
        }

        let mut current_head_guard =
            current_head.map(|current_head| buffer_pool[current_head].lock_meta());
        let mut prev_guard = predecessor_index.map(|prev| buffer_pool[prev].lock_meta());
        let mut next_guard = frame_guard.next().map(|idx| buffer_pool[idx].lock_meta());
        intrusive_list_guard.move_to_head(
            frame_idx,
            &mut frame_guard,
            current_head_guard.as_mut(),
            prev_guard.as_mut(),
            next_guard.as_mut(),
        );
    }

    /// Tries to promote one resident frame to the head of the LRU list without blocking.
    ///
    /// This is the [`crate::buffer_manager::BufferManager::pin_fast()`] companion to
    /// [`PolicyState::promote_to_head_blocking()`].
    /// Returning `false` means the fast path could not complete full LRU hit
    /// semantics without waiting, so the caller must roll back the speculative
    /// pin and report contention instead of half-succeeding.
    fn try_promote_to_head(
        &self,
        intrusive_list_guard: &mut IntrusiveList,
        buffer_pool: &[Arc<BufferFrame>],
        frame_idx: usize,
    ) -> bool {
        let current_head = intrusive_list_guard.peek_head();
        if current_head == Some(frame_idx) {
            return true;
        }

        let Some(mut frame_guard) = buffer_pool[frame_idx].try_lock_meta() else {
            return false;
        };
        let predecessor_index = frame_guard.prev();
        let adjacent_to_head =
            matches!((predecessor_index, current_head), (Some(prev), Some(head)) if prev == head);

        if adjacent_to_head {
            let Some(mut current_head_guard) =
                current_head.and_then(|current_head| buffer_pool[current_head].try_lock_meta())
            else {
                return false;
            };
            let next_index = frame_guard.next();
            let mut next_guard = match next_index {
                Some(idx) => match buffer_pool[idx].try_lock_meta() {
                    Some(guard) => Some(guard),
                    None => return false,
                },
                None => None,
            };
            intrusive_list_guard.promote_successor_to_head(
                &mut current_head_guard,
                &mut frame_guard,
                next_guard.as_mut(),
            );
            return true;
        }

        let mut current_head_guard = match current_head {
            Some(idx) => match buffer_pool[idx].try_lock_meta() {
                Some(guard) => Some(guard),
                None => return false,
            },
            None => None,
        };
        let mut prev_guard = match predecessor_index {
            Some(idx) => match buffer_pool[idx].try_lock_meta() {
                Some(guard) => Some(guard),
                None => return false,
            },
            None => None,
        };
        let next_index = frame_guard.next();
        let mut next_guard = match next_index {
            Some(idx) => match buffer_pool[idx].try_lock_meta() {
                Some(guard) => Some(guard),
                None => return false,
            },
            None => None,
        };
        intrusive_list_guard.move_to_head(
            frame_idx,
            &mut frame_guard,
            current_head_guard.as_mut(),
            prev_guard.as_mut(),
            next_guard.as_mut(),
        );
        true
    }
}
