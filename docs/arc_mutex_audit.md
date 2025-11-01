# Arc/Mutex Usage Audit

## Threading Model
- Transactions are the sole concurrency boundary: each `Transaction` executes on one thread while multiple transactions may run in parallel.
- Shared infrastructure (`FileManager`, `LogManager`, `BufferManager`, `LockTable`) services many transactions simultaneously and must remain thread-safe.
- Metadata, planner, and catalog helpers are constructed during boot and then reused inside individual transactions; most of these helpers do not cross transaction threads except through read-only access.

## Inventory

| Component | Location | Sharing model | Mutability | Notes | Action |
|-----------|----------|---------------|------------|-------|--------|
| `SharedFS = Arc<Mutex<Box<dyn FileSystemInterface>>>` | `src/main.rs:40` | `Arc<Mutex<...>>` | Mutates OS file handles and block cache | All transactions funnel file IO through this trait object. Mutex protects underlying `FileManager` which is not thread-safe. | **Keep** |
| `LogManager` handles | `src/main.rs:46`, `10300` | `Arc<Mutex<LogManager>>` | Appends WAL records, flushes | Multiple transactions append/log concurrently; the internal data structures are not lock-free. | **Keep** |
| `BufferManager` handle | `src/main.rs:47` | `Arc<BufferManager>` | Internal state guarded by mutexes and atomics | Shared across threads; `BufferManager` owns its own locking. Exterior does not need an extra mutex. | **Keep** |
| `LockTable` | `src/main.rs:50` | `Arc<LockTable>` | Interior mutex per lock map | Coordinates 2PL across transactions. Outer `Arc` enables sharing; locking handled internally. | **Keep** |
| `Transaction` | numerous (`src/main.rs:8572`, plans/scans) | `Arc<Transaction>` | Logical txn state mutates | Shared within a single thread across plan/scans to satisfy ownership requirements. No cross-thread use observed. Candidate for `Rc` once non-`Send` consumers confirmed. | Investigate (low priority) |
| CLI transaction references | `src/bin/simpledb-cli.rs` | `Arc<Transaction>` | Mutable txn state | CLI uses one transaction per session; single-threaded. Could become `Rc` if `Transaction` stays single-thread. | Follow `Transaction` decision |
| Metadata stack (`MetadataManager`, `TableManager`, `ViewManager`, `IndexManager`) | `src/main.rs:6857` onwards | `Arc<T>` (except `Arc<Mutex<StatManager>>`) | Construction-time writes; steady-state reads | These managers become read-only after boot (except statistics). Mutex wrapping stats is under review. | Simplify (planned) |
| `StatManager` | `src/main.rs:7330` | `Arc<StatManager>` (internal `Mutex`) | Caches stats, maintains refresh counter | Only mutable component inside metadata. Accessed by query planning during optimization, so interior mutex guards state; outer wrappers no longer lock metadata manager. | **Keep (interior lock)** |
| Planner (`Planner`, `BasicQueryPlanner`, `IndexUpdatePlanner`) | `src/main.rs:3037`, `3700` | `Arc<Planner>`, internals use `Arc<dyn Plan>` | Plans assembled per transaction; mostly read-only structures | Determine if `Arc<dyn Plan>` is required for shared ownership or if `Box<dyn Plan>` suffices within single-threaded execution. | Simplify if safe |
| Execution plans/scans (`MultiBufferProductPlan`, `TablePlan`, etc.) | `src/main.rs:120+` | Frequent `Arc<dyn Plan>` and `Arc<Transaction>` | Plans compose other plans; scans mutate per-iterator state | `Arc` currently compensates for trait-object cloning. Need evaluation before changing. | Evaluate |
| Buffer frames & latches | `src/main.rs:10143` onward | `Arc<Mutex<BufferFrame>>`, latch tables with `Arc<Mutex<()>>` | Mutable buffer state | Shared between transactions; critical for correctness. | **Keep** |
| Index structures (`BTreeIndex`, `BTreePage`, etc.) | `src/btree.rs` | `Arc<Transaction>` | Pages pinned/unpinned via txn | Same ownership story as other operators—single-thread use but shared references. | Follow `Transaction` decision |
| Tests/utilities | scattered | `Arc` for convenience | Usually single-thread | Can often be relaxed, but low impact. | N/A |

## Observations
- Mutex usage is concentrated in IO/locking subsystems where true concurrency exists.
- Metadata subsystem is mostly immutable; only statistics layer currently forces `Arc<Mutex<T>>`.
- Planner and execution tree rely on `Arc<dyn Plan>` for structural sharing despite single-threaded evaluation; benchmarking shows plan builders frequently clone child plans (e.g. heuristic join enumeration), so downgrading to `Box<dyn Plan>` would require redesigning those algorithms around move semantics. Switching to `Rc` could clarify the single-thread contract but is a broader refactor.
- `Transaction` objects are cloned via `Arc` purely for ergonomic sharing inside the same thread. Confirming lack of cross-thread use would allow downgrading to `Rc`, clarifying the single-threaded contract.

## StatManager Concurrency Notes
- `StatManager` now wraps its mutable cache inside an internal `Mutex`, so callers share it through `Arc<StatManager>` without layering extra synchronization in `MetadataManager`.
- `get_stat_info` still mutates (`num_calls`, cache refresh); the mutex serializes refreshes while keeping the external API read-only.
- Refresh iterates the catalog and may hold the lock briefly, but planner workloads are light enough that this coarse-grained protection is acceptable.
- More granular approaches (per-table locks, atomics) remain on the table if contention shows up in profiles, but current measurements don’t justify the added complexity.

## Next Steps
1. Remove unnecessary mutexes from metadata helpers once immutability is enforced/documented.
2. Investigate replacing `Arc<dyn Plan>` with `Box<dyn Plan>` (and adjusting APIs) if plans never escape the owning transaction thread.
3. Profile `StatManager` access patterns; keep the mutex if contention is negligible and race-free semantics are required, otherwise pursue finer-grained or interior mutability.
