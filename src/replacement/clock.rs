use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{BlockId, BufferFrame};

#[derive(Debug)]
pub struct PolicyState {
    hand: Mutex<usize>,
    pool_len: usize,
}

impl PolicyState {
    pub fn new(buffer_pool: &[Arc<Mutex<BufferFrame>>]) -> Self {
        assert!(
            !buffer_pool.is_empty(),
            "Clock policy requires at least one buffer frame"
        );
        Self {
            hand: Mutex::new(0),
            pool_len: buffer_pool.len(),
        }
    }

    pub fn record_hit<'a>(
        &self,
        _buffer_pool: &'a [Arc<Mutex<BufferFrame>>],
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
        let mut frame_guard = buffer_pool[frame_idx].lock().unwrap();
        frame_guard.ref_bit = true;
    }

    pub fn evict_frame<'a>(
        &self,
        buffer_pool: &'a [Arc<Mutex<BufferFrame>>],
    ) -> Option<(usize, MutexGuard<'a, BufferFrame>)> {
        let mut hand = self.hand.lock().unwrap();
        for _ in 0..self.pool_len {
            let idx = *hand;
            let mut frame_guard = buffer_pool[idx].lock().unwrap();
            if frame_guard.is_pinned() {
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
