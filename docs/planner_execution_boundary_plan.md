# Planner / Execution Boundary Plan

Tracking issue: [#96](https://github.com/redixhumayun/simpledb/issues/96)

## Goal

Separate planning from execution so plan objects are metadata-only and runtime transactional authority is bound when execution starts, not while plans are being built.

This plan is deliberately staged so we can improve the planner/executor boundary **before** forcing the larger transaction-session refactor tracked in [#63](https://github.com/redixhumayun/simpledb/issues/63).

## Why this exists

Today the engine mixes planning and execution:

- planner-owned objects can carry `Arc<Transaction>`
- `Plan::open()` creates live executable scans without taking an explicit runtime context
- leaf execution/storage objects (`TableScan`, `RecordPage`, index scans) directly own broad transaction authority

That shape is simple, but it makes it much harder to later introduce tighter transaction ownership or session-based execution.

## Scope

This plan covers three layers:

1. execution storage objects (`TableScan`, `RecordPage`, index/table access helpers)
2. scan/operator layer (`Scan`, `UpdateScan`, wrapper scans)
3. planner/executor boundary (`Plan`, `TablePlan`, `SelectPlan`, etc.)

## Non-goals

- Do not introduce session-borrowed scans in phase 1.
- Do not remove `Arc<Transaction>` from execution objects immediately.
- Do not combine this work with optimizer redesign.
- Do not require intra-query parallelism as part of the first step.

## Design direction

The target boundary is:

- plans describe operator structure, schema, and estimates
- execution binds runtime context explicitly
- leaf operators receive runtime authority when opened, not while being planned

The important staging decision: first separate **planner-owned state** from **execution-owned state**. Only after that boundary is clean should we decide whether execution context stays `Arc<Transaction>`-based or moves to transaction sessions.

## Current pressure points

- `Plan::open()` has no runtime context parameter: `src/main.rs:5014`
- `TablePlan::new(...)` currently receives a transaction during planning
- `TableScan` stores `Arc<Transaction>` directly: `src/main.rs:8480`
- `RecordPage` stores `Arc<Transaction>` directly: `src/main.rs:9012`
- wrapper scans are built assuming the leaf scans already own transactional authority

## Target end state

Conceptually:

```rust
pub trait Plan {
    fn open<'a>(&'a self, ctx: &'a mut ExecutionContext) -> Box<dyn UpdateScan + 'a>;
}
```

where:

- `Plan` is metadata-only
- `ExecutionContext` is the runtime binding point
- leaf scans are created inside `open(...)`

Phase 1 does **not** require `ExecutionContext` to borrow transaction sessions yet. It can still carry `Arc<Transaction>` initially if that keeps the migration incremental.

## Phase plan

### Phase 0: lock the boundary

- Document that plan objects should stop gaining new runtime fields.
- Treat `Arc<Transaction>` in planner-owned objects as debt, not a permanent pattern.
- Keep issue #63 and issue #96 separate: transaction session enforcement is follow-on work, not phase 1 of this plan.

Exit criteria:

- boundary documented

### Phase 1: introduce explicit execution context

- Add a documented `ExecutionContext` type.
- Initially allow it to wrap the current runtime authority (`Arc<Transaction>`) so behavior stays stable.
- Add `open_with_context(...)`-style APIs in parallel with existing `open()` where needed.

Suggested minimal shape:

```rust
pub struct ExecutionContext {
    txn: Arc<Transaction>,
}
```

Why this phase matters:

- it separates planner-owned data from execution-owned data without forcing session lifetimes yet
- it creates the seam that later transaction/session work can plug into

Exit criteria:

- a runtime context type exists
- new execution entry points can take it explicitly

### Phase 2: migrate leaf storage/execution objects first

- Change `TablePlan` so planning does not need a live transaction.
- Move transaction binding into `TablePlan::open_with_context(...)`.
- Migrate `TableScan`, `RecordPage`, and index/table access constructors so they are created from runtime context, not planner-owned transaction fields.

Important point:

- phase 2 still allows `TableScan` / `RecordPage` to internally store `Arc<Transaction>` if that keeps churn manageable
- the key win is that the planner no longer owns that runtime state

Exit criteria:

- `TablePlan` is metadata-only
- `TableScan`/`RecordPage` are constructed from execution context

### Phase 3: migrate wrapper scans/operators

- Update `SelectPlan`, `ProjectPlan`, `ProductPlan`, and other operator plans to open children using explicit runtime context.
- Keep wrapper execution objects owning child scans, not planner-time transaction state.
- Preserve current semantics and operator ordering.

Exit criteria:

- all major plan types open through explicit runtime context
- no planner-owned operator requires broad transaction state

### Phase 4: retire context-free `open()`

- Remove or deprecate `Plan::open()` with implicit runtime binding.
- Make the explicit execution context path the only supported runtime entry.
- Update planner tests, CLI entry points, and helper APIs.

Exit criteria:

- the plan/execution boundary is explicit everywhere

### Phase 5: evaluate follow-on transaction/session integration

- Revisit issue [#63](https://github.com/redixhumayun/simpledb/issues/63) after the planner/executor boundary is clean.
- Decide whether `ExecutionContext` should remain `Arc<Transaction>`-based or become session-based.
- Only now reconsider scan/storage lifetimes if the ownership payoff is worth the churn.

Exit criteria:

- a clean handoff exists from #96 to #63

## Why this staging is preferable

This plan avoids trying to solve two hard problems at once.

- `#96` cleans the planner/executor boundary
- `#63` can later tighten transaction ownership on top of that cleaner boundary

If we skip this separation and jump straight to session-borrowed scans, we likely have to rewrite planner traits, scan traits, and transaction APIs all at once.

## Key migration hazards

- Returning borrowed scans from `Plan::open(...)` may force trait/lifetime changes; do not take that on until the explicit execution context seam exists.
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

Start with phase 1 only:

- add `ExecutionContext`
- add explicit context-taking open paths
- do not change transaction semantics yet

That is the smallest step that makes later execution/storage cleanup plausible.

## References

- PostgreSQL planner/optimizer overview: <https://www.postgresql.org/docs/current/planner-optimizer.html>
- PostgreSQL executor overview: <https://www.postgresql.org/docs/current/executor.html>
- DuckDB internals overview: <https://duckdb.org/docs/stable/internals/overview.html>
- Goetz Graefe, "Volcano - An Extensible and Parallel Query Evaluation System" (1994)
- Goetz Graefe, "The Cascades Framework for Query Optimization" (1995)
