# Execution Capability Boundary Notes

## Point

The remaining blur between the physical plan layer and the execution layer is no longer mainly that plans perform runtime work themselves.

The more important issue now is that some physical plans know too much about the exact executor types they require.

There are two concrete cases.

## Case 1: `TableSource` returns concrete `TableScan`

Today:

```rust
pub trait TableSource: Plan {
    fn open_table_scan(&self, ctx: &ExecutionContext) -> TableScan;
}
```

This leaks a concrete execution type into the physical-plan layer.

That means plans like:

- `IndexSelectPlan`
- `IndexJoinPlan`
- `MultiBufferProductPlan`

do not really depend on a table-backed execution capability. They depend on `TableScan` specifically.

### Why this is a problem

- it hard-codes one executor implementation into the physical layer
- it makes the physical layer less explicit about what capability it truly needs
- it reduces freedom to add a different table-backed executor later

The real requirement is not "I need `TableScan`".

The real requirement is something closer to:

- "I need a table-backed cursor"
- "I need row-location / mutation capability"

### Solution

Change `TableSource` to return an execution capability instead of a concrete executor type.

Conceptually:

```rust
pub trait TableSource: Plan {
    fn open_table_cursor(&self, ctx: &ExecutionContext) -> Box<dyn TableCursor>;
}
```

Then the physical plans depend on:

- `TableCursor`

instead of:

- `TableScan`

That expresses the true contract of the boundary.

## Case 2: `MergeJoinPlan` knows it needs `SortScan`

Today `MergeJoinPlan` stores `Arc<SortPlan>` and opens a concrete `SortScan`.

That happens because the merge-join executor needs additional runtime behavior:

- `save_position()`
- `restore_position()`

### Why this is a problem

Again, the issue is not that `MergeJoinPlan` needs runtime machinery.

That is normal.

The issue is that the physical plan knows the exact executor class it wants instead of the execution capability it needs.

The real requirement is something like:

- sorted input with mark/restore support

not:

- `SortScan`

### Solution

Introduce a narrow execution capability for the runtime behavior merge join requires.

Conceptually:

```rust
pub trait MarkRestoreScan: Scan {
    fn save_position(&mut self) -> SimpleDBResult<()>;
    fn restore_position(&mut self) -> SimpleDBResult<()>;
}
```

Then the physical layer should depend on that capability rather than on `SortScan` specifically.

The physical contract becomes:

- merge join requires a sorted, mark/restore-capable right input

instead of:

- merge join requires `SortPlan -> SortScan`

## Summary

The fix in both cases is the same pattern:

- physical plans should depend on execution capabilities
- not concrete executor classes

So the rule of thumb is:

- if a physical node says "I need `TableScan`" or "I need `SortScan`", that is too concrete
- it should instead say "I need a table-backed cursor" or "I need a mark/restore-capable sorted scan"

That keeps the physical/execution boundary real while reducing concrete executor leakage across it.
