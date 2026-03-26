# 005 — Logical/Physical Planner Boundary

## Decision

Introduce a formal logical/physical split in the query planner:

- `LogicalPlan` enum (`TableScan`, `Filter`, `Project`, `Join`) as a pure relational IR
- `LogicalPlanner`, `LogicalOptimizer`, `PhysicalPlanner` boundary traits
- `PipelineQueryPlanner` chains all three stages and replaces `BasicQueryPlanner` as the default in `SimpleDB`

## Context

Before this change, `HeuristicQueryPlanner` and `TablePlanner` mixed logical reasoning (join ordering, predicate pushdown) and physical implementation choices (index selection, join algorithm) in a single pass over `Arc<dyn Plan>` objects. `BasicQueryPlanner` built physical plan nodes directly from `QueryData`. Neither had a clear logical/physical seam.

This made rule-based logical rewrites, memoization, and DP join ordering impractical: any new logical IR would have immediately collapsed back into the same mixed-layer structure.

## What changed

- `LogicalPlan` enum with pre-computed cost stats (records_output, blocks_accessed, distinct_values) derived from catalog data at build time.
- `BasicLogicalPlanner`: naive `QueryData → LogicalPlan` translation (cross-join tree + Filter + Project).
- `HeuristicLogicalOptimizer`: predicate pushdown and left-deep join ordering over `LogicalPlan` nodes only — no physical plan objects.
- `DefaultPhysicalPlanner`: recursive `LogicalPlan → Arc<dyn Plan>` lowering. Physical choices (IndexSelectPlan, IndexJoinPlan, MultiBufferProductPlan) are made only here.
- `Predicate::reduction_factor_fn`: generic selectivity estimation via closure, used by the logical layer without requiring `Arc<dyn Plan>`.
- `SimpleDB::new_with_options` updated to use `PipelineQueryPlanner`.
- `BasicQueryPlanner` and `HeuristicQueryPlanner` retained as `pub` alternative implementations but no longer used in the default path.

## What did not change

- Physical plan node types (`TablePlan`, `SelectPlan`, `ProjectPlan`, `IndexSelectPlan`, `IndexJoinPlan`, `MultiBufferProductPlan`, etc.) are unchanged.
- Execution semantics, scan iterators, transaction handling are unchanged.
- The heuristics themselves (join ordering, predicate pushdown, index selection) produce identical results — they are restructured, not changed.

## Phase 6 (not yet done)

The plan doc (`docs/logical_optimizer_ir_plan.md`) describes a Phase 6 for memo/DP readiness:
- Stable `Hash`/`Eq` on `LogicalPlan` nodes
- Tests for structural rewrites independent of execution
- Explicit logical attribute types for memo keys

These are deferred. The boundary is sufficient for rule-based rewrites to begin.

## Alternatives considered

- Keep `HeuristicQueryPlanner` and add a `LogicalPlan` wrapper on top: rejected because the new IR would have immediately become cosmetic, inheriting the same mixed-layer problems.
- Introduce a Cascades-style memo framework immediately: out of scope; the boundary must be stable first.
