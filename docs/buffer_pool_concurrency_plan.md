[# Buffer Pool Concurrency Plan

## Summary

We reintroduced a global mutex inside `BufferManager::pin` to plug a race that allowed two frames to cache the same `BlockId`. While correct, that mutex serializes every miss path and nullifies the concurrency gains the PR set out to deliver. This note captures stdlib-only designs that restore safety **without** a coarse lock, so we can iteratively move the buffer pool back to a scalable foundation.

> **Update (2024-06-05):** Option A (global residency table guarded by a single mutex) was prototyped in commit `9cb50888c77f8be5767c1a5f36a1db39abac50c2`. Benchmarks showed ~75% slower pin/unpin hits and ~30% lower 8-thread throughput because every pin still grabs the map lock and performs extra hash bookkeeping. We reverted to the baseline and are now focusing on per-block latching (Option B).

## Current State

- `BufferManager::pin` acquires `self.global_lock` before running `try_to_pin`.
- The lock guarantees only one thread at a time can assign a frame to a block.
- Throughput now scales poorly when many transactions miss on distinct blocks.
- We need correctness (single frame per block, coherent contents) and we must keep the dependency footprint to Rust stdlib only.

## Goals & Constraints

1. **Correctness:** At most one frame may be associated with a given `BlockId` at any moment.
2. **Progress:** Readers must observe the most recent committed contents after the writer flushes.
3. **Concurrency:** Threads missing on different blocks should run in parallel.
4. **Stdlib-only:** No third-party crates; use `Mutex`, `RwLock`, `Atomic*`, and collections from `std`.

## Option A — Residency Table (HashMap + Mutex)

Maintain a table from `BlockId` → `Arc<Mutex<Buffer>>` to publish ownership as soon as a frame starts serving a block.

### Sketch

```rust
struct BufferManager {
    resident: Mutex<HashMap<BlockId, Weak<Mutex<Buffer>>>>,
    // existing fields…
}

pub fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
    if let Some(buffer) = self.lookup_resident(block_id) {
        buffer.lock().unwrap().pin();
        return Ok(buffer);
    }

    let frame = self.choose_unpinned_buffer()?.ok_or("no buffer")?;
    {
        let mut guard = frame.lock().unwrap();
        guard.assign_to_block(block_id);
        guard.pin();
    }

    self.publish_resident(block_id, &frame);
    Ok(frame)
}
```

### Pros
- Minimal locking: only the table mutex, per-block granularity while in use.
- Simple to reason about; integrates with existing borrow semantics.
- Table can store `Weak` references to avoid leaks when frames are recycled.

### Cons
- Additional bookkeeping during `pin` and `unpin`.
- Needs careful cleanup in `unpin_all` to drop stale entries.
- Hash lookups add overhead to the hot path.
- A single map mutex becomes a new global lock; our prototype showed severe contention under load.

## Option B — Per-Block Latch Table

Create latches per `BlockId` on demand. Only threads contending on the same block serialize; others proceed independently.

### Sketch

```rust
struct BufferManager {
    block_locks: Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    // …
}

fn with_block_lock<T>(&self, block_id: &BlockId, f: impl FnOnce() -> T) -> T {
    let latch = {
        let mut map = self.block_locks.lock().unwrap();
        Arc::clone(map.entry(block_id.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
    };
    let _guard = latch.lock().unwrap();
    let result = f();
    // Optional: prune latch when refcount == 1 to avoid unbounded growth.
    result
}

pub fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
    self.with_block_lock(block_id, || self.pin_within_block(block_id))
}
```

### Pros
- Threads touching different blocks never contend.
- Localizes contention where it truly exists (single hot block).
- Keeps implementation close to current control flow.

### Cons
- Requires latch lifecycle management to avoid unbounded hashmap growth.
- Still uses a mutex for each block access (but only shared by threads on that block).
- Slightly more complexity when evicting/reseting frames to release latches.

## Option C — Frame Claim with Atomics

Teach each frame to advertise its owner using an atomic slot. Threads race on the frame itself; only the winner proceeds to load the block.

### Sketch

```rust
struct Buffer {
    owner: AtomicU64, // encode BlockId hash or stable id
    // …
}

fn try_claim(&self, block_id: &BlockId) -> bool {
    let tag = block_id_hash(block_id);
    self.owner
        .compare_exchange(0, tag, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

fn release(&self) {
    self.owner.store(0, Ordering::Release);
}
```

`pin()` becomes:
1. Search for an existing frame whose `owner` matches the block → return it.
2. Otherwise iterate over frames:
   - If `try_claim` succeeds, lock the frame, `assign_to_block`, and publish.
   - If it fails, someone else either already loaded the block or grabbed the frame; restart the search to reuse the winner.

### Pros
- No global or per-block mutexes; only atomic ops.
- Highest potential concurrency—threads on different blocks rarely interfere.
- Claim/release is constant time.

### Cons
- Needs a reversible encoding of `BlockId` (e.g., stable 64-bit hash) and collision handling.
- Must guard against ABA: ensure frame isn’t reassigned before we consume it (combine atomic owner with pin count checks).
- Harder to reason about; requires extra tests to prove races are covered.

## Recommendation

Pivot to Option B (Per-Block Latch Table):
- Localizes contention to a single `BlockId`; distinct blocks no longer serialize on a global lock.
- Keeps the residency map for O(1) lookups while providing the ordering guarantees we struggled to encode in Option A.
- Keeps the implementation close to the current control flow and remains stdlib-only.

Once Option B stabilizes and benchmarks well, revisit Option C for a lock-free variant if necessary.

## Next Steps

1. Implement per-block latch table and integrate it into `BufferManager::pin`.
2. Keep the residency map but shard or prune latches to avoid unbounded growth.
3. Re-run the benchmark suite (`cargo bench --bench buffer_pool -- 50 12`) and document the deltas.
4. If contention remains high on hot blocks, explore sharding the residency map or moving to Option C.
5. Optimize the `Condvar` path: replace `Mutex<usize>` with an `AtomicUsize` fast path and a dedicated `Mutex<()>`/`Condvar` pair solely for exhaustion waits. This removes the global counter mutex from the hot path while preserving correctness.

Tracking Issue: [#38](https://github.com/redixhumayun/simpledb/issues/38).
](# Buffer Pool Concurrency Plan

## Summary

We reintroduced a global mutex inside `BufferManager::pin` to plug a race that allowed two frames to cache the same `BlockId`. While correct, that mutex serializes every miss path and nullifies the concurrency gains the PR set out to deliver. This note captures stdlib-only designs that restore safety **without** a coarse lock, so we can iteratively move the buffer pool back to a scalable foundation.

> **Update (2024-06-05):** Option A (global residency table guarded by a single mutex) was prototyped in commit `9cb50888c77f8be5767c1a5f36a1db39abac50c2`. Benchmarks showed ~75% slower pin/unpin hits and ~30% lower 8-thread throughput because every pin still grabs the map lock and performs extra hash bookkeeping. We reverted to the baseline and are now focusing on per-block latching (Option B).

## Current State

- `BufferManager::pin` acquires `self.global_lock` before running `try_to_pin`.
- The lock guarantees only one thread at a time can assign a frame to a block.
- Throughput now scales poorly when many transactions miss on distinct blocks.
- We need correctness (single frame per block, coherent contents) and we must keep the dependency footprint to Rust stdlib only.

## Goals & Constraints

1. **Correctness:** At most one frame may be associated with a given `BlockId` at any moment.
2. **Progress:** Readers must observe the most recent committed contents after the writer flushes.
3. **Concurrency:** Threads missing on different blocks should run in parallel.
4. **Stdlib-only:** No third-party crates; use `Mutex`, `RwLock`, `Atomic*`, and collections from `std`.

## Option A — Residency Table (HashMap + Mutex)

Maintain a table from `BlockId` → `Arc<Mutex<Buffer>>` to publish ownership as soon as a frame starts serving a block.

### Sketch

```rust
struct BufferManager {
    resident: Mutex<HashMap<BlockId, Weak<Mutex<Buffer>>>>,
    // existing fields…
}

pub fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
    if let Some(buffer) = self.lookup_resident(block_id) {
        buffer.lock().unwrap().pin();
        return Ok(buffer);
    }

    let frame = self.choose_unpinned_buffer()?.ok_or("no buffer")?;
    {
        let mut guard = frame.lock().unwrap();
        guard.assign_to_block(block_id);
        guard.pin();
    }

    self.publish_resident(block_id, &frame);
    Ok(frame)
}
```

### Pros
- Minimal locking: only the table mutex, per-block granularity while in use.
- Simple to reason about; integrates with existing borrow semantics.
- Table can store `Weak` references to avoid leaks when frames are recycled.

### Cons
- Additional bookkeeping during `pin` and `unpin`.
- Needs careful cleanup in `unpin_all` to drop stale entries.
- Hash lookups add overhead to the hot path.
- A single map mutex becomes a new global lock; our prototype showed severe contention under load.

## Option B — Per-Block Latch Table

Create latches per `BlockId` on demand. Only threads contending on the same block serialize; others proceed independently.

### Sketch

```rust
struct BufferManager {
    block_locks: Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    // …
}

fn with_block_lock<T>(&self, block_id: &BlockId, f: impl FnOnce() -> T) -> T {
    let latch = {
        let mut map = self.block_locks.lock().unwrap();
        Arc::clone(map.entry(block_id.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
    };
    let _guard = latch.lock().unwrap();
    let result = f();
    // Optional: prune latch when refcount == 1 to avoid unbounded growth.
    result
}

pub fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
    self.with_block_lock(block_id, || self.pin_within_block(block_id))
}
```

### Execution Flow (Pin)

```
Thread T
│
├─ look up / create latch for block B     (briefly hold the latch table mutex)
│   └─ lock latch_B                       (serialize with other threads on B)
│      ├─ lock resident map (short critical section)
│      │   ├─ if hit: upgrade Weak, drop map lock
│      │   │    ├─ lock frame mutex (per-frame)
│      │   │    └─ bump pins / update metadata, unlock frame
│      │   └─ if miss: choose frame, lock frame mutex, assign, publish
│      └─ unlock resident map
└─ unlock latch_B
```

### Pros
- Threads touching different blocks never contend.
- Localizes contention where it truly exists (single hot block).
- Keeps implementation close to current control flow.

### Cons
- Requires latch lifecycle management to avoid unbounded hashmap growth.
- Still uses a mutex for each block access (but only shared by threads on that block).
- Slightly more complexity when evicting/reseting frames to release latches.

## Option C — Frame Claim with Atomics

Teach each frame to advertise its owner using an atomic slot. Threads race on the frame itself; only the winner proceeds to load the block.

### Sketch

```rust
struct Buffer {
    owner: AtomicU64, // encode BlockId hash or stable id
    // …
}

fn try_claim(&self, block_id: &BlockId) -> bool {
    let tag = block_id_hash(block_id);
    self.owner
        .compare_exchange(0, tag, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

fn release(&self) {
    self.owner.store(0, Ordering::Release);
}
```

`pin()` becomes:
1. Search for an existing frame whose `owner` matches the block → return it.
2. Otherwise iterate over frames:
   - If `try_claim` succeeds, lock the frame, `assign_to_block`, and publish.
   - If it fails, someone else either already loaded the block or grabbed the frame; restart the search to reuse the winner.

### Pros
- No global or per-block mutexes; only atomic ops.
- Highest potential concurrency—threads on different blocks rarely interfere.
- Claim/release is constant time.

### Cons
- Needs a reversible encoding of `BlockId` (e.g., stable 64-bit hash) and collision handling.
- Must guard against ABA: ensure frame isn’t reassigned before we consume it (combine atomic owner with pin count checks).
- Harder to reason about; requires extra tests to prove races are covered.

## Recommendation

Pivot to Option B (Per-Block Latch Table):
- Localizes contention to a single `BlockId`; distinct blocks no longer serialize on a global lock.
- Keeps the residency map for O(1) lookups while providing the ordering guarantees we struggled to encode in Option A.
- Keeps the implementation close to the current control flow and remains stdlib-only.

Once Option B stabilizes and benchmarks well, revisit Option C for a lock-free variant if necessary.

## Next Steps

1. Implement per-block latch table and integrate it into `BufferManager::pin`.
2. Keep the residency map but shard or prune latches to avoid unbounded growth.
3. Re-run the benchmark suite (`cargo bench --bench buffer_pool -- 50 12`) and document the deltas.
4. If contention remains high on hot blocks, explore sharding the residency map or moving to Option C.
5. Optimize the `Condvar` path: replace `Mutex<usize>` with an `AtomicUsize` fast path and a dedicated `Mutex<()>`/`Condvar` pair solely for exhaustion waits. This removes the global counter mutex from the hot path while preserving correctness.

Tracking Issue: [#38](https://github.com/redixhumayun/simpledb/issues/38).
)