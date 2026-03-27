# Physical Plan / Execution Boundary Plan

## Goal

Make the physical plan layer and the execution layer distinct:

- physical plans describe the chosen execution strategy
- execution objects hold live runtime state
- runtime allocation and stateful algorithms begin only after `open(ctx)`

This is a follow-on cleanup after the logical/physical planner split.

## Why this matters

The codebase already has a real seam at:

```rust
plan.open(&ctx) -> Box<dyn Scan>
```

That is good, but the separation is still not fully clean.

Some physical plan structs still contain executor logic or planning-time transaction/config access:

- `SortPlan` contains execution-oriented methods like `split_into_runs`, `merge`, and `do_merge_iters`
- `MultiBufferProductPlan` contains execution-oriented temp-table creation helpers
- `TablePlan::new` still reads txn-backed metadata directly
- `MaterializePlan::new` and `MultiBufferProductPlan::new` still cache txn-derived configuration directly

So the code already has a boundary, but not yet a pure one.

## Target model

The intended shape is:

```text
LogicalPlan
  -> PhysicalPlan
      -> Execution objects / scans
```

with these responsibilities:

- logical layer: query meaning and rewrites
- physical plan layer: chosen operator implementations and plan metadata
- execution layer: open scans, temp tables, cursor position, merge state, chunk state, runtime buffers

One useful mental model is that physical plan to execution is often approximately one execution-operator instance per physical plan node, but not one runtime resource per plan node.

For example:

```text
SortPlan
  -> one runtime sort operator instance
      -> many internal runtime artifacts if needed
         (temp runs, temp-table scans, merge state, cursor position)
```

So the mapping is close to 1:1 at the operator level, not at the internal resource level.

## What belongs in the physical plan layer

Physical plans should contain only descriptor-style information such as:

- chosen child plans
- sort keys
- join keys
- chosen access path / join algorithm
- schema
- cost / cardinality estimates
- planning-time configuration or metadata that is safe to cache as plain values

Examples:

- `SortPlan` = input + sort fields + schema/cost metadata
- `MergeJoinPlan` = left + right + join fields + schema/cost metadata
- `MultiBufferProductPlan` = lhs + rhs + schema/cost metadata

## What belongs in the execution layer

Execution objects should own:

- open child scans
- temp tables
- cursor position
- merge state
- chunk iteration state
- any runtime allocation derived from `ExecutionContext`

Examples:

- sort run generation and merge loops
- temp table materialization
- index cursor state
- table scan positioning

Concrete example for sort:

```text
Physical layer:
  SortPlan {
    input,
    sort_keys,
    schema,
    estimates,
  }

Execution layer after open(ctx):
  SortExec / SortScan {
    child_scan,
    comparator,
    temp_runs,
    merge_state,
    output_position,
  }
```

In that model, methods like `split_into_runs` and `merge` belong to the runtime sort operator, not to `SortPlan` itself.

## Current pressure points

- `SortPlan` mixes physical description and execution algorithm helpers
- `MultiBufferProductPlan` mixes physical description and runtime temp-table creation
- some plan constructors still take raw `Arc<Transaction>` only to fetch planning metadata/config

These make the physical layer partially executor-like.

## Design direction

### 1. Make plan constructors planning-only

Move planner-facing constructors toward planning inputs only.

Longer term, prefer planning-time context/snapshot over raw transaction handles for:

- schema / layout lookup
- stats lookup
- block size
- available buffers

### 2. Move executor algorithms below `open(ctx)`

Methods that actually perform runtime work should live on execution-side types or helpers, not on plan structs.

Examples of code that should move below the boundary:

- sort run splitting / merge logic
- temp-table creation for multi-buffer product
- other runtime helper flows that allocate execution state

### 3. Keep physical plans as descriptors

After cleanup, plan structs should read more like:

```text
SortPlan { input, sort_keys, schema, estimates }
```

not like mini-executors.

## Suggested migration order

### Phase 1: remove stale txn/config coupling in plan constructors

- remove constructor params that are no longer used
- replace raw txn access with planning-time cached values where appropriate
- prefer planning context/snapshot for plan construction inputs

### Phase 2: isolate runtime helper logic

- move `SortPlan` execution helpers into execution-side code
- move `MultiBufferProductPlan` temp-table creation into execution-side code
- keep plan structs as metadata holders

### Phase 3: make execution-side types explicit

- keep `Plan::open(&ctx)` as the seam if desired
- but ensure the returned runtime objects own all live execution state
- avoid runtime allocation methods on plan structs except as thin delegation to executor construction

## Standardness

This separation is standard in query engines.

The exact execution model varies:

- Volcano iterators
- vectorized execution
- pipelines / tasks
- compiled fragments

But the common pattern is the same:

- planning chooses the implementation strategy
- execution instantiates and runs live state from that strategy

So this cleanup is not inventing a novel architecture. It is pushing the codebase closer to the usual planner/executor split found in mature engines.

## Immediate next step

Start with the most obvious cases:

- remove stale constructor args like the old `SortPlan` txn parameter
- identify physical plan methods that allocate temp/runtime state
- move those methods below the `open(ctx)` boundary one operator at a time
