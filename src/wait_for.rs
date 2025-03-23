struct LockTable {
    lock_table: Mutex<HashMap<BlockId, LockState>>,
    wait_for_graph: Mutex<HashMap<TransactionID, HashSet<TransactionID>>>,
    cond_var: Condvar,
    timeout: u64,
}

fn new(timeout: u64) -> Self {
    Self {
        lock_table: Mutex::new(HashMap::new()),
        wait_for_graph: Mutex::new(HashMap::new()),
        cond_var: Condvar::new(),
        timeout,
    }
}

// Add waiting relationship to the graph
fn add_wait_edge(&self, waiter: TransactionID, holder: TransactionID) {
    let mut graph = self.wait_for_graph.lock().unwrap();
    graph
        .entry(waiter)
        .or_insert_with(HashSet::new)
        .insert(holder);
}

// Remove a transaction from the wait-for graph (when it's done waiting)
fn remove_wait_edges(&self, tx_id: TransactionID) {
    let mut graph = self.wait_for_graph.lock().unwrap();
    graph.remove(&tx_id);
    // Also remove any edges pointing to this transaction
    for waiters in graph.values_mut() {
        waiters.remove(&tx_id);
    }
}

// Check for deadlocks using cycle detection
fn check_for_deadlock(&self, tx_id: TransactionID) -> Option<TransactionID> {
    let graph = self.wait_for_graph.lock().unwrap();

    // DFS cycle detection
    fn has_cycle(
        graph: &HashMap<TransactionID, HashSet<TransactionID>>,
        node: TransactionID,
        visited: &mut HashSet<TransactionID>,
        path: &mut HashSet<TransactionID>,
    ) -> Option<Vec<TransactionID>> {
        if !visited.insert(node) {
            return None;
        }

        path.insert(node);

        if let Some(neighbors) = graph.get(&node) {
            for &neighbor in neighbors {
                if path.contains(&neighbor) {
                    // Found a cycle - collect the transactions involved
                    let mut cycle = vec![neighbor];
                    return Some(cycle);
                }

                if let Some(mut cycle) = has_cycle(graph, neighbor, visited, path) {
                    cycle.push(node);
                    return Some(cycle);
                }
            }
        }

        path.remove(&node);
        None
    }

    let mut visited = HashSet::new();
    let mut path = HashSet::new();

    // Start DFS from the current transaction
    if let Some(cycle) = has_cycle(&graph, tx_id, &mut visited, &mut path) {
        // Choose victim - usually the youngest transaction
        return cycle.into_iter().max_by_key(|&id| id);
    }

    None
}

fn acquire_write_lock(
    &self,
    tx_id: TransactionID,
    block_id: &BlockId,
) -> Result<(), Box<dyn Error>> {
    // ... existing code ...

    loop {
        let state = lock_table_guard.get_mut(block_id).unwrap();

        let should_wait = state.readers.len() > 1
            || state.writer.is_some()
            || state
                .upgrade_requests
                .front()
                .is_some_and(|id| *id != tx_id);

        if !should_wait {
            break;
        }

        // Add waiting edges for deadlock detection
        if state.readers.len() > 1 {
            for &reader_id in &state.readers {
                if reader_id != tx_id {
                    self.add_wait_edge(tx_id, reader_id);
                }
            }
        }

        if let Some(writer_id) = state.writer {
            self.add_wait_edge(tx_id, writer_id);
        }

        // Check for deadlocks
        if let Some(victim) = self.check_for_deadlock(tx_id) {
            if victim == tx_id {
                // Current transaction chosen as victim - abort it
                self.remove_wait_edges(tx_id);
                return Err("Deadlock detected, transaction aborted".into());
            }
            // Otherwise continue waiting
        }

        // ... existing waiting code ...
    }

    // Remove from wait-for graph when we're no longer waiting
    self.remove_wait_edges(tx_id);

    // ... existing code to acquire the lock ...
}

fn release_locks(&self, tx_id: TransactionID, block_id: &BlockId) -> Result<(), Box<dyn Error>> {
    // ... existing code ...

    // Clean up wait-for graph
    self.remove_wait_edges(tx_id);

    self.cond_var.notify_all();
    Ok(())
}
