# Query Engine Architecture

## Goal

This document is the authoritative overview of how query processing is structured in SimpleDB today.

It describes:

- the query pipeline from SQL to execution
- the logical, physical, and execution layers
- the main runtime interfaces
- where the architecture is already clean and where it is still intentionally transitional

## High-level pipeline

```text
SQL
 |
 v
Parser
 |
 v
QueryData
 |
 v
BasicLogicalPlanner
 |
 v
LogicalPlan
 |
 v
HeuristicLogicalOptimizer
 |
 v
LogicalPlan
 |
 v
DefaultPhysicalPlanner
 |
 v
Arc<dyn Plan>
 |
 v
ExecutionContext
 |
 v
Plan::open(&ExecutionContext)
 |
 v
Scan / TableCursor runtime objects
 |
 v
Rows / updates / temp state
```

The default query path is the pipeline planner:

- `LogicalPlanner`: `QueryData -> LogicalPlan`
- `LogicalOptimizer`: `LogicalPlan -> LogicalPlan`
- `PhysicalPlanner`: `LogicalPlan -> Arc<dyn Plan>`

## Core layers

### 1. Logical layer

The logical layer represents query meaning.

It answers:

- what tables are involved?
- what predicates apply?
- what projections apply?
- what join structure does the query have?

It should not answer:

- whether we use an index
- whether we use merge join or index join
- how temp state is allocated at runtime

Current logical IR shape:

```text
LogicalPlan
  - kind
  - children
  - data
  - props
```

Current logical node kinds:

- `TableScan`
- `Filter`
- `Project`
- `Join`

The logical IR intentionally uses a wrapper-node shape rather than a large typed enum tree because future optimizer work wants uniform structural operations over children and node payloads.

## 2. Physical layer

The physical layer represents chosen implementation strategy.

It answers:

- which access path do we use?
- which join algorithm do we use?
- do we sort?
- do we materialize?

Current physical plan nodes include:

- `TablePlan`
- `SelectPlan`
- `ProjectPlan`
- `ProductPlan`
- `IndexSelectPlan`
- `IndexJoinPlan`
- `SortPlan`
- `MergeJoinPlan`
- `MaterializePlan`
- `MultiBufferProductPlan`

These are executable plan descriptors. They are not themselves the live execution state.

## 3. Execution layer

The execution layer is created when a physical plan is opened.

It holds live runtime state such as:

- cursor position
- open child scans
- temp tables
- merge state
- chunk state
- runtime resources tied to one execution

Current execution-side objects include:

- `TableScan`
- `SelectScan`
- `ProjectScan`
- `ProductScan`
- `IndexSelectScan`
- `IndexJoinScan`
- `SortScan`
- `MergeJoinScan`
- `ChunkScan`

## Plan / execution seam

The key seam is:

```rust
plan.open(&ExecutionContext)
```

This is where:

- physical plan metadata stops
- live execution state begins

That is the main physical/execution boundary in the engine.

## One useful mental model

For a given operator, the mapping is usually:

```text
logical operator
  -> physical plan node
      -> runtime execution object
```

Examples:

```text
Logical Join
  -> IndexJoinPlan
      -> IndexJoinScan

Logical Join
  -> MergeJoinPlan
      -> MergeJoinScan

Logical Filter
  -> SelectPlan
      -> SelectScan
```

This is approximately one-to-one at the operator-instance level, not at the internal-resource level.

Example for sort:

```text
SortPlan
  -> one runtime sort operator instance
      -> temp runs
      -> temp-table scans
      -> merge state
      -> output cursor position
```

So a single physical sort operator may allocate several runtime artifacts once opened.

## Current logical query shape

The current logical planner and heuristic optimizer work with a fairly normalized shape:

```text
Project
  |
Filter? 
  |
Join tree
 / | \
TableScan leaves
```

That is sufficient for the current heuristic optimizer.

## Current runtime contexts

### `ExecutionContext`

`ExecutionContext` is the runtime binding point for execution.

It currently wraps transaction authority and is the object passed into `Plan::open(...)`.

This is where future transaction-session work is expected to plug in.

### `PlanningContext`

`PlanningContext` exists to separate planning-time metadata access from execution-time authority.

Today it is still an early seam rather than a final abstraction. The intended long-term direction is:

- planner code depends on planning-specific queries
- execution code depends on runtime authority

## What is already clean

- logical plans are separate from physical plans
- physical plans are separate from runtime scan objects
- runtime authority is bound at execution via `ExecutionContext`
- the logical optimizer rewrites logical IR, not executable plans

## What is still transitional

Two areas are still intentionally incomplete.

### 1. Planning context is not yet a narrow planning-only interface

`PlanningContext` exists, but the planning API surface is not yet fully narrowed to planner-safe accessors.

### 2. Some physical plans still know concrete executor classes too directly

The remaining blur between the physical and execution layers is mostly about execution capabilities vs concrete executor types.

Examples:

- `TableSource` currently returns a concrete `TableScan`
- merge join currently depends on `SortScan`-specific behavior rather than only on a narrower runtime capability

The intended long-term direction is:

- physical plans depend on execution capabilities
- not concrete executor classes

## Architecture status summary

```text
Logical <-> Physical   : mostly clean
Physical <-> Execution : real boundary exists; still some capability leakage
Planning <-> Execution : started, not yet fully narrowed
```

That is already much better than the earlier mixed-layer planner/executor design.

## Why this architecture matters

This structure is what makes future work tractable:

- cost-based logical optimization
- DP join ordering
- cleaner physical/execution abstraction boundaries
- transaction-session refactor work bound to runtime execution instead of planner traits

In particular, session-based transaction authority becomes easier once:

- logical plans are pure query IR
- physical plans are operator descriptors
- execution is the only place where runtime authority and lifetimes matter

## Related docs

- `docs/architecture/WAL.md`
- `docs/transaction_session_refactor.md`
- `docs/planning_context_and_snapshot.md`
- `docs/planning_context_interface_note.md`
- `docs/physical_execution_boundary_plan.md`
- `docs/execution_capability_boundary_notes.md`
- `docs/value_based_scan_interface_plan.md`

## External references

- PostgreSQL planner/optimizer overview: <https://www.postgresql.org/docs/current/planner-optimizer.html>
- PostgreSQL executor overview: <https://www.postgresql.org/docs/current/executor.html>
- Alex Chi, "Plan Representation: #1 Lesson Learned from Building an Optimizer": <https://www.skyzh.dev/blog/2025-02-06-optimizer-lesson-01/>
- Goetz Graefe, "Volcano - An Extensible and Parallel Query Evaluation System" (1994)
- Goetz Graefe, "The Cascades Framework for Query Optimization" (1995)
