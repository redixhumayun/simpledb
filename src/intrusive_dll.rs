pub trait IntrusiveNode {
    fn prev(&self) -> Option<usize>;
    fn set_prev(&mut self, prev: Option<usize>);
    fn next(&self) -> Option<usize>;
    fn set_next(&mut self, next: Option<usize>);
}

#[derive(Debug)]
pub struct IntrusiveList {
    head: Option<usize>,
    tail: Option<usize>,
}

impl IntrusiveList {
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }

    pub fn from_nodes<T: IntrusiveNode>(nodes: &mut [T]) -> Self {
        let mut intrusive_list = IntrusiveList::new();
        intrusive_list.head = None;
        intrusive_list.tail = None;

        if nodes.is_empty() {
            return intrusive_list;
        }

        let last = nodes.len() - 1;
        for (index, node) in nodes.iter_mut().enumerate() {
            node.set_prev(if index == 0 { None } else { Some(index - 1) });
            node.set_next(if index == last { None } else { Some(index + 1) });
        }
        intrusive_list.head = Some(0);
        intrusive_list.tail = Some(last);
        intrusive_list
    }

    /// Insert the given node into the head of the list
    pub fn insert_at_head<T: IntrusiveNode>(
        &mut self,
        index: usize,
        node: &mut T,
        head_node: Option<&mut T>,
    ) {
        match head_node {
            Some(head_node) => {
                node.set_next(self.head);
                node.set_prev(None);
                head_node.set_prev(Some(index));
                self.head = Some(index);
                assert!(self.head != self.tail);
            }
            None => {
                //  ensure invariants are maintained
                assert!(self.head.is_none());
                assert!(self.tail.is_none());
                node.set_prev(None);
                node.set_next(None);
                self.head = Some(index);
                self.tail = Some(index);
            }
        }
    }

    /// Move the given node to the head of the list.
    #[cfg(feature = "replacement_lru")]
    pub fn move_to_head<T: IntrusiveNode>(
        &mut self,
        index: usize,
        node: &mut T,
        curr_head: Option<&mut T>,
        prev_node: Option<&mut T>,
        next_node: Option<&mut T>,
    ) {
        assert!(
            self.head.is_some(),
            "Invariant broken: meaningless to move to head of an empty list"
        );
        assert!(
            self.tail.is_some(),
            "Invariant broken: no tail on a non-empty list"
        );
        assert!(
            curr_head.is_some(),
            "Invariant broken: moving a node to the head is meaningless if there is no head currently"
        );
        if self.head == Some(index) {
            return;
        }
        let was_tail = self.tail == Some(index);
        let prev_idx = node.prev();
        let next_idx = node.next();

        if let Some(prev_node) = prev_node {
            prev_node.set_next(next_idx)
        }
        if let Some(next_node) = next_node {
            next_node.set_prev(prev_idx)
        }
        if let Some(curr_head) = curr_head {
            curr_head.set_prev(Some(index))
        }
        node.set_prev(None);
        node.set_next(self.head);
        self.head = Some(index);
        if was_tail {
            self.tail = prev_idx;
        }
    }

    /// Move the node that currently succeeds the head into the head position.
    ///
    /// # Panics
    /// * If the list is empty or has only one element.
    /// * If the provided nodes do not reflect the list's current ordering.
    #[cfg(feature = "replacement_lru")]
    pub fn promote_successor_to_head<T: IntrusiveNode>(
        &mut self,
        head_node: &mut T,
        successor_node: &mut T,
        next_node: Option<&mut T>,
    ) {
        assert!(
            self.head.is_some(),
            "Invariant broken: cannot promote successor when list is empty"
        );
        assert!(
            head_node.prev().is_none(),
            "Invariant broken: head node must not have a predecessor"
        );
        assert!(
            head_node.next().is_some(),
            "Invariant broken: head must have a successor to promote"
        );
        let head_index = self.head.unwrap();
        assert!(
            successor_node.prev() == Some(head_index),
            "Invariant broken: provided successor is not the head's next node"
        );
        let successor_index = head_node.next().unwrap();
        let next_idx = successor_node.next();
        head_node.set_next(next_idx);
        if let Some(next_node) = next_node {
            next_node.set_prev(Some(head_index))
        }

        successor_node.set_prev(None);
        successor_node.set_next(Some(head_index));
        head_node.set_prev(Some(successor_index));

        self.head = Some(successor_index);
        if self.tail == Some(successor_index) {
            self.tail = Some(head_index);
        }
    }

    /// Remove the node at the provided index from the list regardless of position.
    pub fn remove_node<T: IntrusiveNode>(
        &mut self,
        index: usize,
        node: &mut T,
        prev_node: Option<&mut T>,
        next_node: Option<&mut T>,
    ) {
        assert!(
            self.head.is_some() && self.tail.is_some(),
            "Invariant broken: cannot remove from an empty list"
        );

        let prev_idx = node.prev();
        let next_idx = node.next();

        match (prev_idx, &prev_node) {
            (Some(_), Some(_)) => {}
            (None, None) => {}
            (Some(idx), None) => panic!(
                "Invariant broken: node {} reported prev {:?} but prev node not supplied",
                index,
                Some(idx)
            ),
            (None, Some(_)) => panic!(
                "Invariant broken: node {} reported no prev but prev node supplied",
                index
            ),
        }

        match (next_idx, &next_node) {
            (Some(_), Some(_)) => {}
            (None, None) => {}
            (Some(idx), None) => panic!(
                "Invariant broken: node {} reported next {:?} but next node not supplied",
                index,
                Some(idx)
            ),
            (None, Some(_)) => panic!(
                "Invariant broken: node {} reported no next but next node supplied",
                index
            ),
        }

        if let Some(prev) = prev_node {
            prev.set_next(next_idx)
        }
        if let Some(next) = next_node {
            next.set_prev(prev_idx)
        }

        if self.head == Some(index) {
            self.head = next_idx;
        }
        if self.tail == Some(index) {
            self.tail = prev_idx;
        }

        if self.head.is_none() {
            assert!(
                self.tail.is_none(),
                "Invariant broken: head cleared but tail still set"
            );
        }

        node.set_prev(None);
        node.set_next(None);
    }

    /// Get the index of the node at the head
    pub fn peek_head(&self) -> Option<usize> {
        self.head
    }

    /// Get the index of the node at the tail
    pub fn peek_tail(&self) -> Option<usize> {
        self.tail
    }
}

#[cfg(test)]
mod intrusive_dll_tests {
    use std::{cell::RefCell, collections::HashSet};

    use crate::intrusive_dll::{IntrusiveList, IntrusiveNode};

    struct Node<T> {
        _data: T,
        prev: Option<usize>,
        next: Option<usize>,
    }

    impl<T> Node<T> {
        fn new(data: T) -> Self {
            Self {
                _data: data,
                prev: None,
                next: None,
            }
        }
    }

    impl<T> IntrusiveNode for Node<T> {
        fn prev(&self) -> Option<usize> {
            self.prev
        }

        fn set_prev(&mut self, prev: Option<usize>) {
            self.prev = prev
        }

        fn next(&self) -> Option<usize> {
            self.next
        }

        fn set_next(&mut self, next: Option<usize>) {
            self.next = next
        }
    }

    struct NodeHandle<'a, T> {
        cell: &'a RefCell<Node<T>>,
    }

    impl<'a, T> NodeHandle<'a, T> {
        fn new(cell: &'a RefCell<Node<T>>) -> Self {
            Self { cell }
        }
    }

    impl<T> IntrusiveNode for NodeHandle<'_, T> {
        fn prev(&self) -> Option<usize> {
            self.cell.borrow().prev()
        }

        fn set_prev(&mut self, prev: Option<usize>) {
            self.cell.borrow_mut().set_prev(prev)
        }

        fn next(&self) -> Option<usize> {
            self.cell.borrow().next()
        }

        fn set_next(&mut self, next: Option<usize>) {
            self.cell.borrow_mut().set_next(next)
        }
    }

    fn create_list_with_values<T: Clone>(values: &[T]) -> (IntrusiveList, Vec<RefCell<Node<T>>>) {
        let mut list = IntrusiveList::new();
        let nodes: Vec<_> = values
            .iter()
            .cloned()
            .map(|value| RefCell::new(Node::new(value)))
            .collect();

        for (idx, node) in nodes.iter().enumerate() {
            let head_idx = list.head;
            let mut new_node_handle = NodeHandle::new(node);
            match head_idx {
                Some(head_idx) => {
                    let mut head_node_handle = NodeHandle::new(nodes.get(head_idx).unwrap());
                    list.insert_at_head(idx, &mut new_node_handle, Some(&mut head_node_handle));
                }
                None => {
                    list.insert_at_head(idx, &mut new_node_handle, None);
                }
            }
        }

        (list, nodes)
    }

    fn assert_list_integrity<T>(list: &IntrusiveList, nodes: &[RefCell<Node<T>>]) {
        match (list.head, list.tail) {
            (None, None) => return,
            (Some(head), Some(tail)) => {
                assert!(
                    head < nodes.len(),
                    "Invariant broken: head index {} out of bounds",
                    head
                );
                assert!(
                    tail < nodes.len(),
                    "Invariant broken: tail index {} out of bounds",
                    tail
                );
            }
            _ => panic!("Invariant broken: head and tail must both be defined or both be None"),
        }

        let mut visited = Vec::new();
        let mut seen = HashSet::new();
        let mut current_idx = list.head;
        let mut expected_prev = None;

        while let Some(idx) = current_idx {
            assert!(
                idx < nodes.len(),
                "Invariant broken: node index {} out of bounds",
                idx
            );
            assert!(
                seen.insert(idx),
                "Invariant broken: cycle detected at index {}",
                idx
            );

            let node_ref = nodes[idx].borrow();
            assert_eq!(
                node_ref.prev, expected_prev,
                "Invariant broken: node {} has prev {:?}, expected {:?}",
                idx, node_ref.prev, expected_prev
            );

            visited.push(idx);
            current_idx = node_ref.next;
            expected_prev = Some(idx);
        }

        assert_eq!(
            visited.last().copied(),
            list.tail,
            "Invariant broken: tail does not match last visited node"
        );

        if let Some(tail_idx) = list.tail {
            let tail_ref = nodes[tail_idx].borrow();
            assert!(
                tail_ref.next.is_none(),
                "Invariant broken: tail node {} has next pointer {:?}",
                tail_idx,
                tail_ref.next
            );
        }

        let mut reverse_seen = HashSet::new();
        let mut current_idx = list.tail;
        let mut expected_next = None;

        while let Some(idx) = current_idx {
            assert!(
                reverse_seen.insert(idx),
                "Invariant broken: cycle detected in reverse at index {}",
                idx
            );

            let node_ref = nodes[idx].borrow();
            assert_eq!(
                node_ref.next, expected_next,
                "Invariant broken: node {} has next {:?}, expected {:?}",
                idx, node_ref.next, expected_next
            );

            current_idx = node_ref.prev;
            expected_next = Some(idx);
        }

        assert_eq!(
            expected_next, list.head,
            "Invariant broken: head does not match reverse traversal"
        );

        assert_eq!(
            seen.len(),
            reverse_seen.len(),
            "Invariant broken: forward and reverse traversals visited different counts"
        );
    }

    #[test]
    fn test_basic_functionality() {
        let (list, nodes) = create_list_with_values(&[1, 2, 3, 4, 5]);
        assert_list_integrity(&list, &nodes);
        assert_eq!(list.head, Some(4));
        assert_eq!(list.tail, Some(0));
        assert_eq!(list.peek_head(), Some(4));
        assert_eq!(list.peek_tail(), Some(0));
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_list_modifications() {
        let (mut list, nodes) = create_list_with_values(&[1, 2, 3, 4, 5]);
        assert_list_integrity(&list, &nodes);
        let mut promoted_idx = 2;
        let mut current_head_idx = list.head.unwrap();
        let mut prev_idx = nodes[promoted_idx].borrow().prev();
        let mut next_idx = nodes[promoted_idx].borrow().next();
        {
            let mut curr_head_handle = NodeHandle::new(&nodes[current_head_idx]);
            let mut node_handle = NodeHandle::new(&nodes[promoted_idx]);
            let mut prev_handle = prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            let mut next_handle = next_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.move_to_head(
                promoted_idx,
                &mut node_handle,
                Some(&mut curr_head_handle),
                prev_handle.as_mut(),
                next_handle.as_mut(),
            );
        }
        assert_eq!(list.head, Some(promoted_idx));
        assert_eq!(nodes[promoted_idx].borrow().prev(), None);
        assert_list_integrity(&list, &nodes);

        let tail_idx = list.peek_tail().unwrap();
        let tail_prev_idx = nodes[tail_idx].borrow().prev();
        let evicted_idx = {
            let mut tail_handle = NodeHandle::new(&nodes[tail_idx]);
            let mut prev_handle = tail_prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.remove_node(tail_idx, &mut tail_handle, prev_handle.as_mut(), None);
            tail_idx
        };
        assert_eq!(evicted_idx, tail_idx);
        assert_list_integrity(&list, &nodes);

        current_head_idx = list.head.unwrap();
        {
            let mut new_frame_handle = NodeHandle::new(&nodes[evicted_idx]);
            let mut head_handle = NodeHandle::new(&nodes[current_head_idx]);
            list.insert_at_head(evicted_idx, &mut new_frame_handle, Some(&mut head_handle));
        }
        assert_eq!(list.head, Some(evicted_idx));
        assert_list_integrity(&list, &nodes);

        promoted_idx = 2;
        prev_idx = nodes[promoted_idx].borrow().prev();
        next_idx = nodes[promoted_idx].borrow().next();
        {
            let mut curr_head_handle = NodeHandle::new(&nodes[list.head.unwrap()]);
            let mut node_handle = NodeHandle::new(&nodes[promoted_idx]);
            let mut prev_handle = prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            let mut next_handle = next_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.move_to_head(
                promoted_idx,
                &mut node_handle,
                Some(&mut curr_head_handle),
                prev_handle.as_mut(),
                next_handle.as_mut(),
            );
        }
        assert_eq!(list.head, Some(promoted_idx));
        assert_list_integrity(&list, &nodes);

        prev_idx = nodes[evicted_idx].borrow().prev();
        next_idx = nodes[evicted_idx].borrow().next();
        {
            let mut curr_head_handle = NodeHandle::new(&nodes[list.head.unwrap()]);
            let mut node_handle = NodeHandle::new(&nodes[evicted_idx]);
            let mut prev_handle = prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            let mut next_handle = next_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.move_to_head(
                evicted_idx,
                &mut node_handle,
                Some(&mut curr_head_handle),
                prev_handle.as_mut(),
                next_handle.as_mut(),
            );
        }
        assert_eq!(list.head, Some(evicted_idx));
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_tail_eviction() {
        let (mut list, nodes) = create_list_with_values(&[1, 2, 3, 4, 5]);
        assert_list_integrity(&list, &nodes);
        let tail_idx = list.peek_tail().unwrap();
        let prev_idx = nodes[tail_idx].borrow().prev();
        let evicted_idx = {
            let mut tail_handle = NodeHandle::new(&nodes[tail_idx]);
            let mut prev_handle = prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.remove_node(tail_idx, &mut tail_handle, prev_handle.as_mut(), None);
            tail_idx
        };
        assert_eq!(evicted_idx, tail_idx);
        assert_eq!(list.tail, prev_idx);
        assert_eq!(nodes[tail_idx].borrow().prev(), None);
        assert_eq!(nodes[tail_idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_remove_middle_node() {
        let (mut list, nodes) = create_list_with_values(&[1, 2, 3, 4, 5]);
        assert_list_integrity(&list, &nodes);

        let idx = 2;
        let prev_idx = nodes[idx].borrow().prev();
        let next_idx = nodes[idx].borrow().next();
        {
            let mut node_handle = NodeHandle::new(&nodes[idx]);
            let mut prev_handle = prev_idx.map(|i| NodeHandle::new(&nodes[i]));
            let mut next_handle = next_idx.map(|i| NodeHandle::new(&nodes[i]));
            list.remove_node(
                idx,
                &mut node_handle,
                prev_handle.as_mut(),
                next_handle.as_mut(),
            );
        }

        assert_eq!(nodes[idx].borrow().prev(), None);
        assert_eq!(nodes[idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_remove_head_node() {
        let (mut list, nodes) = create_list_with_values(&[10, 20, 30]);
        assert_list_integrity(&list, &nodes);

        let head_idx = list.peek_head().unwrap();
        let next_idx = nodes[head_idx].borrow().next();
        {
            let mut node_handle = NodeHandle::new(&nodes[head_idx]);
            let mut next_handle = next_idx.map(|i| NodeHandle::new(&nodes[i]));
            list.remove_node(head_idx, &mut node_handle, None, next_handle.as_mut());
        }

        assert_ne!(list.peek_head(), Some(head_idx));
        assert_eq!(nodes[head_idx].borrow().prev(), None);
        assert_eq!(nodes[head_idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_remove_tail_node() {
        let (mut list, nodes) = create_list_with_values(&[7, 8, 9]);
        assert_list_integrity(&list, &nodes);

        let tail_idx = list.peek_tail().unwrap();
        let prev_idx = nodes[tail_idx].borrow().prev();
        {
            let mut node_handle = NodeHandle::new(&nodes[tail_idx]);
            let mut prev_handle = prev_idx.map(|i| NodeHandle::new(&nodes[i]));
            list.remove_node(tail_idx, &mut node_handle, prev_handle.as_mut(), None);
        }

        assert_ne!(list.peek_tail(), Some(tail_idx));
        assert_eq!(nodes[tail_idx].borrow().prev(), None);
        assert_eq!(nodes[tail_idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_remove_last_node() {
        let (mut list, nodes) = create_list_with_values(&[42]);
        assert_list_integrity(&list, &nodes);

        let idx = list.peek_head().unwrap();
        {
            let mut node_handle = NodeHandle::new(&nodes[idx]);
            list.remove_node(idx, &mut node_handle, None, None);
        }

        assert_eq!(list.peek_head(), None);
        assert_eq!(list.peek_tail(), None);
        assert_eq!(nodes[idx].borrow().prev(), None);
        assert_eq!(nodes[idx].borrow().next(), None);
    }

    #[test]
    #[should_panic(expected = "Invariant broken: meaningless to move to head of an empty list")]
    fn test_move_to_head_empty_list_panics() {
        let mut list = IntrusiveList::new();
        let node = RefCell::new(Node::new(1));
        let mut node_handle = NodeHandle::new(&node);
        list.move_to_head(0, &mut node_handle, None, None, None);
    }

    #[test]
    fn test_move_to_head_single_node() {
        let (mut list, nodes) = create_list_with_values(&[42]);
        assert_list_integrity(&list, &nodes);
        let idx = list.head.unwrap();
        {
            let mut curr_head_handle = NodeHandle::new(&nodes[idx]);
            let mut node_handle = NodeHandle::new(&nodes[idx]);
            list.move_to_head(
                idx,
                &mut node_handle,
                Some(&mut curr_head_handle),
                None,
                None,
            );
        }
        assert_eq!(list.head, Some(idx));
        assert_eq!(list.tail, Some(idx));
        assert_eq!(nodes[idx].borrow().prev(), None);
        assert_eq!(nodes[idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_evict_tail_single_node() {
        let (mut list, nodes) = create_list_with_values(&[7]);
        assert_list_integrity(&list, &nodes);
        let idx = list.tail.unwrap();
        let evicted_idx = {
            let mut tail_handle = NodeHandle::new(&nodes[idx]);
            list.remove_node(idx, &mut tail_handle, None, None);
            idx
        };
        assert_eq!(evicted_idx, idx);
        assert_eq!(list.head, None);
        assert_eq!(list.tail, None);
        assert_eq!(nodes[idx].borrow().prev(), None);
        assert_eq!(nodes[idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    #[should_panic(expected = "Invariant broken: cannot remove from an empty list")]
    fn test_remove_node_empty_list_panics() {
        let mut list = IntrusiveList::new();
        let node = RefCell::new(Node::new(1));
        let mut node_handle = NodeHandle::new(&node);
        // attempt to remove from an empty list should panic
        list.remove_node(0, &mut node_handle, None, None);
    }

    #[test]
    fn test_move_tail_to_head() {
        let (mut list, nodes) = create_list_with_values(&[1, 2, 3, 4, 5]);
        assert_list_integrity(&list, &nodes);

        let tail_idx = list.peek_tail().unwrap();
        let head_idx = list.peek_head().unwrap();
        let prev_idx = nodes[tail_idx].borrow().prev();
        assert!(prev_idx.is_some());

        {
            let mut current_head_handle = NodeHandle::new(&nodes[head_idx]);
            let mut tail_handle = NodeHandle::new(&nodes[tail_idx]);
            let mut prev_handle = prev_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.move_to_head(
                tail_idx,
                &mut tail_handle,
                Some(&mut current_head_handle),
                prev_handle.as_mut(),
                None,
            );
        }

        assert_eq!(list.head, Some(tail_idx));
        assert_eq!(list.tail, prev_idx);
        assert_eq!(nodes[tail_idx].borrow().prev(), None);
        assert_eq!(nodes[tail_idx].borrow().next(), Some(head_idx));
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_promote_successor_to_head_general_case() {
        let (mut list, nodes) = create_list_with_values(&[10, 20, 30]);
        assert_list_integrity(&list, &nodes);

        let head_idx = list.peek_head().unwrap();
        let successor_idx = nodes[head_idx].borrow().next().unwrap();
        let next_idx = nodes[successor_idx].borrow().next();

        {
            let mut head_handle = NodeHandle::new(&nodes[head_idx]);
            let mut successor_handle = NodeHandle::new(&nodes[successor_idx]);
            let mut next_handle = next_idx.map(|idx| NodeHandle::new(&nodes[idx]));
            list.promote_successor_to_head(
                &mut head_handle,
                &mut successor_handle,
                next_handle.as_mut(),
            );
        }

        assert_eq!(list.peek_head(), Some(successor_idx));
        assert_eq!(nodes[successor_idx].borrow().prev(), None);
        assert_eq!(nodes[successor_idx].borrow().next(), Some(head_idx));
        assert_eq!(nodes[head_idx].borrow().prev(), Some(successor_idx));
        assert_eq!(nodes[head_idx].borrow().next(), next_idx);
        if let Some(next_idx) = next_idx {
            assert_eq!(nodes[next_idx].borrow().prev(), Some(head_idx));
        }
        assert_list_integrity(&list, &nodes);
    }

    #[test]
    fn test_promote_successor_to_head_when_successor_is_tail() {
        let (mut list, nodes) = create_list_with_values(&[1, 2]);
        assert_list_integrity(&list, &nodes);

        let head_idx = list.peek_head().unwrap();
        let successor_idx = nodes[head_idx].borrow().next().unwrap();

        {
            let mut head_handle = NodeHandle::new(&nodes[head_idx]);
            let mut successor_handle = NodeHandle::new(&nodes[successor_idx]);
            list.promote_successor_to_head(&mut head_handle, &mut successor_handle, None);
        }

        assert_eq!(list.peek_head(), Some(successor_idx));
        assert_eq!(list.peek_tail(), Some(head_idx));
        assert_eq!(nodes[successor_idx].borrow().prev(), None);
        assert_eq!(nodes[successor_idx].borrow().next(), Some(head_idx));
        assert_eq!(nodes[head_idx].borrow().prev(), Some(successor_idx));
        assert_eq!(nodes[head_idx].borrow().next(), None);
        assert_list_integrity(&list, &nodes);
    }
}
