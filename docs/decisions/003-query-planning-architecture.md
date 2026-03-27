# 003 — Query Planning Architecture

## Decision

Use a two-stage query planner:

- a logical stage that produces and rewrites logical query IR
- a physical stage that lowers logical IR into executable `Plan` nodes

The logical IR uses a wrapper-node shape:

- `LogicalPlanKind`
- `children: Vec<Arc<LogicalPlan>>`
- `LogicalPlanData`
- `LogicalPlanProps`

## Context

The old planner mixed logical reasoning and physical implementation choice in one pass over `Arc<dyn Plan>` objects. That made it hard to:

- rewrite logical queries without dragging executable plan nodes along
- add memoization or rule matching later
- experiment with DP join ordering or other cost-based optimizer work

At the same time, the physical executor remains easiest to understand and evolve as typed plan nodes. There is no need to force the physical layer into the same generic shape yet.

## Why this shape

The logical layer needs uniform structural operations:

- inspect children
- rebuild nodes
- match patterns
- later memoize equivalent expressions

That is why the logical IR is a wrapper node instead of a large typed enum tree. The shape is chosen now so future optimizer work does not require another logical-IR migration after more rewrite logic accumulates.

This choice is inspired by Alex Chi's write-up on optimizer plan representation in Rust:

- <https://www.skyzh.dev/blog/2025-02-06-optimizer-lesson-01/>

## What this means in SimpleDB

- `LogicalPlanner` builds `LogicalPlan`
- `LogicalOptimizer` rewrites `LogicalPlan`
- `PhysicalPlanner` lowers `LogicalPlan` into `Arc<dyn Plan>`
- physical operator choices like `IndexSelectPlan`, `IndexJoinPlan`, `MultiBufferProductPlan`, `SortPlan`, and `MergeJoinPlan` happen only in the physical stage
- `PipelineQueryPlanner` is the default query-planning path

## What this does not imply

- the physical layer does not need a Cascades-style representation yet
- the executor does not need to change shape just because the logical IR did
- this does not commit the project to a full memo engine immediately; it only keeps that path open
