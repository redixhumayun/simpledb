# PlanningContext Interface Note

## Point

`PlanningContext` does not need immutable snapshot data on day one to be useful.

The first important step is to narrow the interface that planner code sees.

## Intended progression

### Step 1: narrow the planner-facing interface

Planner code should depend on planning-specific accessors like:

- `layout(table)`
- `stat_info(table, layout)`
- `table_id(table)`
- `index_info(table)`
- `block_size()`
- `available_buffs()`

and should stop depending on raw:

- `Arc<Transaction>`
- `MetadataManager`

Internally, `PlanningContext` may still use a live transaction and metadata manager to answer those calls.

### Step 2: change the implementation later

Once the interface is narrow, the implementation can evolve without changing planner call sites.

For example, `PlanningContext` can later switch from:

- live txn-backed metadata reads

to:

- cached planning inputs
- immutable snapshot data
- narrower catalog/config handles

## Why this matters

The key boundary is the interface seen by planner code, not whether snapshot data already exists.

So the immediate goal is:

- hide raw transaction access from planner code
- expose planner-specific queries only

That gives the project a real planning boundary now, and leaves snapshotting as an implementation improvement later.
