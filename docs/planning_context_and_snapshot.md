# Planning Context and Planning Snapshot

## Goal

Separate planning-time metadata access from execution-time authority.

Today many planning APIs still take `Arc<Transaction>` directly. That works, but it blurs two different roles:

- planning needs catalog, stats, and configuration inputs
- execution needs runtime authority to read, write, lock, and allocate temp state

This doc proposes making the planning role explicit.

## Why this matters

After the logical/physical split, the codebase now has a cleaner executor boundary, but planning still often receives raw `Arc<Transaction>`.

That is broader than what planning conceptually needs.

Examples of planning-time needs:

- table schema / layout lookup
- table and index stats lookup
- table id lookup
- block size / available buffer configuration
- snapshot-consistent metadata while building or costing a plan

Examples of execution-time needs:

- opening scans
- writing temp tables
- taking locks
- mutating records
- creating runtime state tied to one execution

Those should not be modeled as the same kind of input long-term.

## Distinction

### Execution context

Execution context answers:

- what authority does this execution run with?

This is the role of `ExecutionContext` today.

### Planning context

Planning context answers:

- what metadata and costing inputs may planning consult?

It may still be backed by a transaction initially, but that transaction is used only as a means to read planning inputs.

### Planning snapshot

A planning snapshot is a narrower form of planning context:

- immutable extracted planning inputs
- detached from the live transaction handle once built

Conceptually:

```rust
pub struct PlanningContext {
    txn: Arc<Transaction>,
    metadata: Arc<MetadataManager>,
}

pub struct PlanningSnapshot {
    schemas: ...,
    stats: ...,
    indexes: ...,
    block_size: usize,
    available_buffs: usize,
}
```

The context is a convenient first step. The snapshot is the cleaner long-term form if we want planning to depend less on live transactional state.

## Why not pass `Arc<Transaction>` forever

Using raw `Arc<Transaction>` in planning APIs has a few downsides:

- it suggests planner code has broad runtime authority, even when it only needs metadata
- it makes planner dependencies less explicit
- it makes it harder to later replace live metadata reads with cached snapshot data
- it keeps planner signatures coupled to transaction internals even when the planner only needs a few derived values

This is the same reason execution now uses `ExecutionContext` instead of smearing runtime authority through every plan node.

## What should move into planning context first

First-pass candidates:

- schema / layout lookup
- stats lookup
- index metadata lookup
- block size
- available buffers

These are the places where planner code still often takes `Arc<Transaction>` only to read metadata or cache planning properties.

## What should not move into planning context

Planning context should not become a second execution context.

Avoid putting runtime operations behind it, such as:

- opening scans
- record mutation
- temp table creation for execution
- buffer pinning as part of plan execution

If planning context exposes everything a transaction can do, then it is just `Transaction` with a different name.

## Suggested direction

Start with a thin wrapper:

```rust
pub struct PlanningContext {
    txn: Arc<Transaction>,
    metadata_manager: Arc<MetadataManager>,
}
```

Then migrate planner-facing APIs from:

```rust
fn build(&self, query: QueryData, txn: Arc<Transaction>) -> SimpleDBResult<LogicalPlan>
```

to something like:

```rust
fn build(&self, query: QueryData, ctx: &PlanningContext) -> SimpleDBResult<LogicalPlan>
```

and similarly for logical optimization / physical lowering if they still need planning-time metadata.

Later, if useful, replace some live reads with an immutable planning snapshot.

## Long-term payoff

This gives the codebase a cleaner three-way separation:

- logical planning / optimization consumes planning inputs
- physical execution consumes runtime authority
- plans themselves remain metadata

That should make future optimizer work easier, especially if we later want:

- memoized logical optimization
- more stable costing inputs
- plan caching experiments
- less coupling between planner code and transaction internals
