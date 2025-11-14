# Buffer Replacement Policy Design Notes

This doc captures design-level guidance for three candidate replacement policies we plan to implement in the buffer pool. It focuses on trade-offs, metadata requirements, and profiling goals; no implementation decisions are locked in yet.

## Shared Context

- Current code (`BufferManager::choose_unpinned_frame` in `src/main.rs`) returns the first unpinned frame, causing pathological thrashing under sequential reuse.
- Buffer frames already expose `pin`, `unpin`, `assign_to_block`, and stats hooks; any policy must plug into that surface without widening public APIs.
- Benchmarks of interest: `benches/buffer_pool.rs` (phase breakdown already exercises cold/warm patterns) plus any bespoke regression we add later.
- We want policies with contrasting metadata and concurrency behavior to generate distinct profiles when we instrument.

## Policy 1: LRU (Exact)

- **Production usage**: canonical in many databases/caches (e.g., PostgreSQL buffer lists, JVM caches, Redis `allkeys-lru` variant).
- **Metadata shape**:
  - We will store per-frame `prev_idx`/`next_idx` integers so frames form an intrusive doubly linked list inside the fixed `buffer_pool`. No separate nodes or recency counters are needed because the list order itself encodes LRU.
  - Pure timestamp-only scans were considered but rejected: they still require O(n) search on every eviction, so they offer little benefit compared to the current “first unpinned” scan while adding bookkeeping overhead.
- **Key operations**:
  - _Hit_ (pin count transitions from 0→1): remove frame from its current slot in the logical list and splice it at the head (most recently used). Because indices never change, list operations are just a handful of assignments protected by the frame mutex (or a small manager-level lock if needed).
  - _Miss_: evict the tail index (least recently used) after verifying it is unpinned. If tail is pinned, walk backward until an unpinned frame appears (still expected to be short because pins should be transient).
  - New frames enter at the head immediately after `assign_to_block`.
- **Locking considerations**: we only mutate two frames’ linkage per hit/miss. A dedicated `Mutex<()>` protects the list. Order for both hit and miss paths (block latch held the entire time; indentation shows additional locks acquired within that scope):
  - `Block latch`
    - `Resident-table mutex`
    - (Release resident-table lock)
    - `LRU mutex`
    - `Frame mutex`
  - After the list pointers are rewired, release the LRU mutex first, then the frame mutex once assignment/pin bookkeeping completes (hit/miss continue to use the existing logic outside the policy helper).
- **Why not a pool-wide mutex**: wrapping the entire buffer pool with one lock would serialize every `pin`/`unpin`, undoing the concurrency work already in place (per-frame mutexes + block latches). The dedicated list mutex protects only `prev_idx`/`next_idx` swaps; it is released before we lock the frame, so page operations still happen under each frame’s own mutex without additional contention.
- **Implementation notes**:
  - The LRU helper runs only the victim-selection/list-updates portion of the miss path. It removes the tail under the policy mutex, releases that mutex immediately after rewiring pointers, and returns the `Arc<Mutex<BufferFrame>>` for the caller to process (assignment, pin count updates) exactly as today. No extra metadata—detaching the node from the list is enough to “reserve” it for the current miss.

## Policy 2: Clock / Second Chance

- **Production usage**: PostgreSQL’s buffer pool (“clock sweep”), many OS page replacement strategies.
- **Metadata shape**:
  - Single reference bit per frame (boolean inside `BufferFrame`).
  - Global hand index on `BufferManager` indicating the next candidate.
- **Key operations**:
  - _Hit_: set reference bit to `true`; no structural change.
  - _Miss_: advance hand circularly, clearing reference bit when encountering `true`, evict first frame with `false`.
  - Skip pinned frames entirely; hand keeps moving until it finds an unpinned victim.
- **Locking considerations**: hand index guarded by a light mutex. Miss-path order (block latch held throughout):
  - `Block latch`
    - `Resident-table mutex`
    - (Release resident-table lock)
    - `Hand mutex`
    - (Release hand mutex)
    - `Frame mutex`
  - `num_available` updates remain tied to pin-count transitions; hand sweeps do not touch it.

## Policy 3: SIEVE

- **Production usage**: Adopted post-NSDI’24 in systems like BIND 9; academic focus on web-cache workloads.
- **Metadata shape**:
  - Maintain logical order via per-frame `prev_idx`/`next_idx` integers (indices into the fixed buffer-pool vector).
  - One-bit “visited” flag per frame stored inside `BufferFrame`.
  - Hand pointer on `BufferManager` progresses tail→head between evictions.
- **Key operations**:
  - _Hit_: set visited flag, leave frame in place.
  - _Miss_: starting at hand, move toward head:
    - If frame visited==1 → clear to 0 and advance hand.
    - First frame with visited==0 becomes victim.
  - New frames insert at head immediately after assignment.
- **Locking considerations**: queue metadata guarded by a short mutex. Miss-path order (block latch held throughout):
  - `Block latch`
    - `Resident-table mutex`
    - (Release resident-table lock)
    - `Queue mutex`
    - (Release queue mutex)
    - `Frame mutex`
  - Hand advancement skips pinned frames and keeps their visited bits untouched until they become eligible.
- **Expected behavior (benchmarks)**:
  - Single-thread or lightly contended locality-heavy workloads (Repeated Access, Random K=10, Zipf) should match LRU hit rates and throughput, potentially edging slightly ahead of Clock thanks to the double-pass eviction (visited bit only cleared at the tail).
  - Mid-sized working sets (Random K=50/100) should show modest improvement over Clock by reducing “one bad miss” evictions—expect hit rates slightly closer to LRU with overhead closer to Clock.
  - Scan-heavy workloads (Sequential, Sequential MT) should remain comparable to Clock; no gains expected beyond the shared intrusive-list ordering.
  - Highly concurrent workloads (pin:t8, Sequential MT) should behave like LRU or better because SIEVE doesn’t serialize on a global clock hand; no single mutex guards the sweep aside from the existing list lock.
  - If benchmarks show regressions on locality-heavy or contention-heavy workloads relative to LRU/Clock, revisit the locking order or visited-bit handling.

## Cross-Policy Considerations

- **Configurability**: Policy selection happens at compile time via mutually exclusive Cargo features (e.g., `replacement-lru`, `replacement-clock`, `replacement-sieve`). Only the enabled module is built, so there’s zero runtime dispatch overhead. Rebuilding between benchmark runs is acceptable—real systems don’t hot-swap replacement policies either.
- **Testing**: Regression scenario from docs (10 blocks, 12 buffers) should run under each policy; expect different hit distributions but all >> current 0%.
- **Profiling**: Rely on existing benchmark stats initially; add policy-specific counters only if later investigations demand them.
- **Extensibility**: Keep API boundaries clean so we can later slot in LFU/ARC without refactoring BufferManager surface.

1. **Metadata placement**: Per-frame metadata (LRU indices, clock ref bit, SIEVE visited bit) lives inside `BufferFrame`; manager-level state (clock hand index, SIEVE hand pointer) lives on `BufferManager`. Each field is guarded by `#[cfg(feature = "...")]`, so only the active policy’s metadata exists in a given build.
2. **Abstraction**: Policy is chosen at compile time via feature flags; only the enabled replacement module is compiled, so no runtime dispatch or trait objects.

## Implementation Notes

- **Intrusive metadata**: Policy-specific fields live directly on `BufferFrame`; we are not introducing wrapper node structs or parallel metadata arrays. That keeps existing `Arc<Mutex<BufferFrame>>` APIs untouched and minimizes indirection.
- **Intrusive list module**: For LRU/SIEVE we will add a dedicated module (e.g., `intrusive_list.rs`) defining:
  - `IntrusiveNode` trait exposing `prev_idx`/`next_idx` getters/setters (implemented by `BufferFrame` behind the relevant feature).
  - `IntrusiveList` struct tracking `head`/`tail` indices and providing operations such as `move_to_head`, `take_tail`, and `insert_head`.
  This keeps pointer-arithmetic centralized, testable with dummy node types, and shared between policies that need it.
- **Block latch scope**: The block latch acquired at the start of `try_to_pin` remains held until the operation succeeds or times out. Policy locks and frame locks nest underneath it following the sequences above.

## Replacement Policy Benchmark Impact

**macOS (aarch64, pool=12, block=4 KiB) — raw runs captured in `docs/benchmarks/replacement_policies/macos_buffer_pool.md`**

|Benchmark|Replacement LRU|Replacement Clock|Replacement SIEVE|
|---|---|---|---|
|Pin/Unpin hit latency|0.918 µs|0.973 µs|1.32 µs|
|Cold pin latency|7.03 µs|7.38 µs|10.14 µs|
|Repeated Access throughput|3.531 M ops/s (100 % hits)|3.792 M ops/s (100 % hits)|3.799 M ops/s (100 % hits)|
|Random K=10 throughput|3.500 M ops/s (100 % hits)|3.798 M ops/s (100 % hits)|3.773 M ops/s (100 % hits)|
|Zipf 80/20 throughput|1.741 M ops/s (82 % hits)|1.532 M ops/s (76 % hits)|1.261 M ops/s (70 % hits)|
|2-thread pin/unpin throughput|1.338 M ops/s|1.501 M ops/s|1.295 M ops/s|
|8-thread pin/unpin throughput|0.189 M ops/s|0.160 M ops/s|0.191 M ops/s|



**Linux (x86_64, pool=12, block=4 KiB) — raw runs captured in `docs/benchmarks/replacement_policies/linux_buffer_pool.md`**

|Benchmark|Replacement LRU|Replacement Clock|Replacement SIEVE|
|---|---|---|---|
|Pin/Unpin hit latency|1.09 µs|0.800 µs|1.06 µs|
|Cold pin latency|4.49 µs|4.11 µs|4.05 µs|
|Repeated Access throughput|1.180 M ops/s (100 % hits)|1.189 M ops/s (100 % hits)|1.224 M ops/s (100 % hits)|
|Random K=10 throughput|1.201 M ops/s (100 % hits)|1.138 M ops/s (100 % hits)|1.204 M ops/s (100 % hits)|
|Zipf 80/20 throughput|0.756 M ops/s (76 % hits)|0.566 M ops/s (77 % hits)|0.537 M ops/s (68 % hits)|
|2-thread pin/unpin throughput|0.220 M ops/s|0.213 M ops/s|0.217 M ops/s|
|8-thread pin/unpin throughput|0.184 M ops/s|0.125 M ops/s|0.182 M ops/s|


Clock figures captured via `cargo bench --bench buffer_pool -- 100 12` on the respective macOS (M-series) and Linux (x86_64) hosts.

Use these snapshots as baselines when comparing future replacement policies or tuning metadata overheads.
