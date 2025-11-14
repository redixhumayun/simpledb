use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{
    intrusive_dll::{IntrusiveList, IntrusiveNode},
    BlockId, BufferFrame,
};

#[derive(Debug)]
struct ListState {
    intrusive_list: IntrusiveList,
    hand: Option<usize>,
}

#[derive(Debug)]
pub struct PolicyState {
    list_state: Mutex<ListState>,
    pool_len: usize,
}

impl PolicyState {
    pub fn new(buffer_pool: &[Arc<Mutex<BufferFrame>>]) -> Self {
        let mut guards = buffer_pool
            .iter()
            .map(|frame| frame.lock().unwrap())
            .collect::<Vec<MutexGuard<'_, BufferFrame>>>();
        let intrusive_list = IntrusiveList::from_nodes(&mut guards);
        Self {
            list_state: Mutex::new(ListState {
                hand: intrusive_list.peek_tail(),
                intrusive_list,
            }),
            pool_len: buffer_pool.len(),
        }
    }

    pub fn record_hit<'a>(
        &self,
        buffer_pool: &[Arc<Mutex<BufferFrame>>],
        frame_ptr: &'a Arc<Mutex<BufferFrame>>,
        block_id: &BlockId,
        resident_table: &Mutex<HashMap<BlockId, Weak<Mutex<BufferFrame>>>>,
    ) -> Option<MutexGuard<'a, BufferFrame>> {
        let mut frame_guard = frame_ptr.lock().unwrap();
        if let Some(frame_block_id) = frame_guard.block_id.as_ref() {
            if frame_block_id != block_id {
                resident_table.lock().unwrap().remove(block_id);
                return None;
            }
        }
        frame_guard.ref_bit = true;
        Some(frame_guard)
    }

    pub fn on_frame_assigned(&self, buffer_pool: &[Arc<Mutex<BufferFrame>>], frame_idx: usize) {
        let mut list_guard = self.list_state.lock().unwrap();
        let current_head = list_guard.intrusive_list.peek_head();
        match current_head {
            Some(head) => {
                if frame_idx == head {
                    return;
                }
                let mut frame_guard = buffer_pool[frame_idx].lock().unwrap();
                frame_guard.ref_bit = true;
                let mut current_head_guard = buffer_pool[head].lock().unwrap();
                list_guard.intrusive_list.insert_at_head(
                    frame_idx,
                    &mut frame_guard,
                    Some(&mut current_head_guard),
                );
            }
            None => {
                let mut frame_guard = buffer_pool[frame_idx].lock().unwrap();
                frame_guard.ref_bit = true;
                list_guard
                    .intrusive_list
                    .insert_at_head(frame_idx, &mut frame_guard, None);
            }
        }
    }

    pub fn evict_frame<'a>(
        &self,
        buffer_pool: &'a [Arc<Mutex<BufferFrame>>],
    ) -> Option<(usize, MutexGuard<'a, BufferFrame>)> {
        let mut list_guard = self.list_state.lock().unwrap();

        for _ in 0..self.pool_len {
            match list_guard.hand {
                Some(hand) => {
                    let mut current_guard = buffer_pool[hand].lock().unwrap();
                    if current_guard.is_pinned() {
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
                        .map(|prev| buffer_pool[prev].lock().unwrap());
                    let mut next_node = current_guard
                        .next()
                        .map(|next| buffer_pool[next].lock().unwrap());
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
