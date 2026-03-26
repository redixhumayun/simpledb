# Planner / Execution Boundary Plan

Tracking issue: [#96](https://github.com/redixhumayun/simpledb/issues/96)

## Goal

Separate planning from execution so plan objects are metadata-only and runtime transactional authority is bound when execution starts, not while plans are being built.

Before doing that boundary work, first replace the current universal scan/update abstraction with execution interfaces that match the operators we actually have.

This plan is deliberately staged so we can improve the planner/executor boundary **before** forcing the larger transaction-session refactor tracked in [#63](https://github.com/redixhumayun/simpledb/issues/63).

## Why this exists

Today the engine mixes planning and execution:

- planner-owned objects can carry `Arc<Transaction>`
- `Plan::open()` creates live executable scans without taking an explicit runtime context
- leaf execution/storage objects (`TableScan`, `RecordPage`, index scans) directly own broad transaction authority

It also forces an executor abstraction that is too broad for the current operator set:

- `Plan::open()` always returns `Box<dyn UpdateScan>`
- read-only operators (`SortScan`, `MergeJoinScan`, `ChunkScan`, multi-buffer product, etc.) still implement update methods
- several operators panic / `todo!()` / `unimplemented!()` for parts of `UpdateScan`
- some plans immediately downcast the generic result back to concrete scan types

That shape is simple, but it makes it much harder to later introduce tighter transaction ownership or session-based execution.

## Scope

This plan covers three layers:

1. execution storage objects (`TableScan`, `RecordPage`, index/table access helpers)
2. executor capability layer (row cursors, table-backed mutation cursors, insert targets, wrapper scans)
3. planner/executor boundary (`QueryPlan`, table-backed source plans, `SelectPlan`, etc.)

## Non-goals

- Do not introduce session-borrowed scans in the first phases.
- Do not remove `Arc<Transaction>` from execution objects immediately.
- Do not combine this work with optimizer redesign.
- Do not require intra-query parallelism as part of the first step.
- Do not require the new executor traits to be final or exhaustive; they can evolve after the current operator set is modeled cleanly.

## Design direction

The target boundary is:

- plans describe operator structure, schema, and estimates
- execution binds runtime context explicitly
- leaf operators receive runtime authority when opened, not while being planned

But that boundary should not be built around the current universal `UpdateScan` API. The first step is to make execution capabilities explicit.

Conceptually, the executor should separate:

- row-producing operators (`RowCursor`-like)
- table-backed row-location / row-mutation operators (`TableCursor`-like)
- insert targets / temp-table writers (`InsertSink`-like)
- special stateful capabilities needed by specific operators (for example sort save/restore position)

The important staging decision: first separate **planner-owned state** from **execution-owned state**. Only after that boundary is clean should we decide whether execution context stays `Arc<Transaction>`-based or moves to transaction sessions.

An additional staging decision: finish the executor capability split before threading a new planner/executor boundary through the old `UpdateScan` interface.

## Current pressure points

- `Plan::open()` has no runtime context parameter: `src/main.rs:5014`
- `Plan::open()` always returns `Box<dyn UpdateScan>`: `src/main.rs:5014`
- `UpdateScan` bundles unrelated capabilities into one trait: `src/main.rs:8803`
- `TablePlan::new(...)` currently receives a transaction during planning
- `TableScan` stores `Arc<Transaction>` directly: `src/main.rs:8480`
- `RecordPage` stores `Arc<Transaction>` directly: `src/main.rs:9012`
- wrapper scans are built assuming the leaf scans already own transactional authority
- several plans downcast generic scans back into concrete executor types (`TableScan`, `SortScan`), which is a sign the trait boundary is not expressing the real contracts

## Target end state

Conceptually:

```rust
pub trait QueryPlan {
    fn open(&self, ctx: &ExecutionContext) -> Result<Box<dyn RowCursor>, ExecError>;
}

pub trait TablePlan {
    fn open_table(&self, ctx: &ExecutionContext) -> Result<Box<dyn TableCursor>, ExecError>;
}

pub trait InsertSink {
    fn insert_values(&mut self, values: &[Constant]) -> Result<RID, ExecError>;
}
```

where:

- plan objects are metadata-only
- `ExecutionContext` is the runtime binding point
- most operators are row-producing only
- only table-backed operators expose row identity / row mutation
- insert targets are not modeled as generic update-capable query nodes
- leaf execution objects are created inside `open(...)` / `open_table(...)`

The exact trait names are not important yet; the important point is to stop forcing every operator through a universal `UpdateScan` abstraction.

Early phases do **not** require `ExecutionContext` to borrow transaction sessions yet. It can still carry `Arc<Transaction>` initially if that keeps the migration incremental.

## Phase plan

### Phase 0: lock the boundary ✅ DONE

- Document that plan objects should stop gaining new runtime fields.
- Treat `Arc<Transaction>` in planner-owned objects as debt, not a permanent pattern.
- Keep issue #63 and issue #96 separate: transaction session enforcement is follow-on work, not phase 1 of this plan.

Exit criteria:

- boundary documented ✅

### Phase 1: replace universal `Scan` / `UpdateScan` boundary ✅ DONE

- Introduce executor traits that match the current operator set instead of forcing every node through `UpdateScan`.
- Make row-producing operators the default execution abstraction.
- Restrict row identity / row mutation to table-backed operators.
- Split insert-target behavior out of generic query execution.
- Preserve special-purpose capabilities as narrow traits instead of overloading generic cursor APIs.

Minimum success criteria for this phase:

- read-only operators no longer implement fake update methods ✅
- `Plan::open()` no longer requires returning `Box<dyn UpdateScan>` for all nodes ✅
- update/delete paths use a narrower table-row mutation interface ✅
- plans stop downcasting generic scan trait objects just to recover concrete capabilities ✅

Implementation notes:

- `UpdateScan` replaced by `TableCursor: Scan + Any` (only `TableScan` implements it)
- `Plan::open()` now returns `Box<dyn Scan>`
- Removed fake `UpdateScan` impls from 9 read-only scan types
- `MergeJoinPlan` stores `plan_2: Arc<SortPlan>` directly; `SortPlan::open_sort_scan()` added
- `MultiBufferProductPlan` stores `lhs: Arc<MaterializePlan>`; `MaterializePlan::open_table_scan()` added
- `IndexSelectPlan` and `IndexJoinPlan` store `Arc<TablePlan>`; `TablePlan::open_table_scan()` added
- Mutation planners apply predicates inline against `TableScan` (no more `SelectPlan` wrapping for mutations)
- Design decisions recorded in `docs/decisions/phase1_executor_capability_split.md`

Exit criteria:

- no universal `UpdateScan`-style boundary remains ✅
- executor capabilities are split in a way that fits the current operators ✅
- planner/executor boundary work can target the new execution interfaces, not the legacy ones ✅

### Phase 2: introduce explicit execution context ✅ DONE

- Add a documented `ExecutionContext` type.
- Initially allow it to wrap the current runtime authority (`Arc<Transaction>`) so behavior stays stable.
- Add context-taking execution entry points against the new executor traits.

Implemented shape:

```rust
pub struct ExecutionContext {
    txn: Arc<Transaction>,
}
impl ExecutionContext {
    pub fn new(txn: Arc<Transaction>) -> Self { Self { txn } }
    pub fn txn(&self) -> &Arc<Transaction> { &self.txn }
}
```

Exit criteria:

- a runtime context type exists ✅
- new execution entry points can take it explicitly ✅

### Phase 3: migrate leaf storage/execution objects first ✅ DONE

- Change table-backed planning so planning does not need to retain a live transaction.
- Move transaction binding into context-taking open paths.
- `TableScan`/`RecordPage` still internally store `Arc<Transaction>` (per the plan); the win is the planner no longer owns it.

Implementation notes:

- `TablePlan` is now metadata-only (`table_name`, `layout`, `stat_info`, `table_id` — no `txn`)
- `TablePlan::open_table_scan(&self, ctx)` creates `TableScan` from `ctx.txn()`
- `MaterializePlan` stores `block_size: usize` (stable config value captured at construction) instead of `txn`
- `SortPlan` drops `txn`; `split_into_runs`, `merge`, `do_merge_iters` take `&ExecutionContext`
- Design decisions in `docs/decisions/phase2_5_execution_context.md`

Exit criteria:

- `TablePlan` is metadata-only ✅
- `TableScan`/`RecordPage` are constructed from execution context ✅

### Phase 4: migrate wrapper scans/operators ✅ DONE

- `SelectPlan`, `ProjectPlan`, `ProductPlan`, `MergeJoinPlan`, `MultiBufferProductPlan`, `IndexSelectPlan`, `IndexJoinPlan` all open children via `ctx`.
- `MergeJoinPlan` and `MultiBufferProductPlan` drop their `txn` fields.
- `TablePlanner` drops its `txn` field.

Exit criteria:

- all major plan types open through explicit runtime context ✅
- no planner-owned operator requires broad transaction state ✅

### Phase 5: retire context-free `open()` ✅ DONE

- Context-free `Plan::open()` removed entirely.
- `Plan::open(&self, ctx: &ExecutionContext) -> Box<dyn Scan>` is the only execution entry.
- All call sites (tests, CLI, benchmarks, mutation planners) updated.

Exit criteria:

- the plan/execution boundary is explicit everywhere ✅

### Phase 5.5: introduce `TableSource` trait ✅ DONE

- Introduced `pub trait TableSource: Plan` with `open_table_scan(&self, ctx) -> TableScan`.
- `TablePlan` and `MaterializePlan` implement it; `open_table_scan` moved from inherent `impl` blocks to trait impls.
- `IndexSelectPlan`, `IndexJoinPlan`, `MultiBufferProductPlan` now store `Arc<dyn TableSource>` instead of `Arc<TablePlan>` / `Arc<MaterializePlan>`.
- `TablePlanner` private helpers still take `Arc<TablePlan>` (always concrete); coercion to `Arc<dyn TableSource>` happens at call sites.
- `SortPlan::open_sort_scan()` stays as a concrete method — `MergeJoinPlan` still needs `SortScan` directly for `save_position`/`restore_position`.

Exit criteria:

- `Arc<TablePlan>` and `Arc<MaterializePlan>` no longer appear as field types in parent plan structs ✅
- `Plan` methods remain callable on `Arc<dyn TableSource>` via supertrait dispatch ✅

### Phase 6: evaluate follow-on transaction/session integration

- Revisit issue [#63](https://github.com/redixhumayun/simpledb/issues/63) after the planner/executor boundary is clean.
- Decide whether `ExecutionContext` should remain `Arc<Transaction>`-based or become session-based.
- Only now reconsider scan/storage lifetimes if the ownership payoff is worth the churn.

Exit criteria:

- a clean handoff exists from #96 to #63

## Why this staging is preferable

This plan avoids trying to solve two hard problems at once.

- first, stop forcing the new boundary through a broken universal executor trait
- `#96` cleans the planner/executor boundary
- `#63` can later tighten transaction ownership on top of that cleaner boundary

If we skip this separation and jump straight to session-borrowed scans, we likely have to rewrite planner traits, scan traits, and transaction APIs all at once.

## Key migration hazards

- If we thread `ExecutionContext` through the current `UpdateScan` API first, we will likely have to redo the boundary immediately after the executor cleanup.
- Returning borrowed execution objects from planner open methods may force trait/lifetime changes; do not take that on until the explicit execution context seam exists.
- `Arc<dyn Plan>` cleanup and plan ownership cleanup are related but not identical; avoid conflating this plan with issue [#50](https://github.com/redixhumayun/simpledb/issues/50).
- `TableScan`/`RecordPage` are the real first movers; if they are not migrated early, the boundary change stays cosmetic.

## Validation after each phase

After each phase, run:

```bash
cargo build
cargo test --no-default-features --features replacement_lru --features page-4k
cargo test --no-default-features --features replacement_clock --features page-4k
cargo test --no-default-features --features replacement_sieve --features page-4k
cargo test --no-default-features --features replacement_lru --features page-4k --features direct-io
```

For nontrivial execution-path phases, also run benchmark smoke checks:

```bash
SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench buffer_pool --no-default-features --features replacement_clock --features page-4k
cargo bench --bench simple_bench --no-default-features --features replacement_clock --features page-4k
```

The purpose is not just performance comparison; it is to catch deadlocks, hangs, or accidental execution regressions early.

## Immediate next step

Start with phase 1 only, and complete it before any planner/execution boundary changes:

- replace the universal `UpdateScan` boundary with narrower executor capabilities
- remove fake update implementations from read-only operators
- stop depending on downcasts from generic scan trait objects to concrete executor types
- do not change transaction semantics yet

That is the smallest step that makes later execution/storage cleanup and planner/execution boundary cleanup plausible.

## References

- PostgreSQL planner/optimizer overview: <https://www.postgresql.org/docs/current/planner-optimizer.html>
- PostgreSQL executor overview: <https://www.postgresql.org/docs/current/executor.html>
- DuckDB internals overview: <https://duckdb.org/docs/stable/internals/overview.html>
- Goetz Graefe, "Volcano - An Extensible and Parallel Query Evaluation System" (1994)
- Goetz Graefe, "The Cascades Framework for Query Optimization" (1995)
