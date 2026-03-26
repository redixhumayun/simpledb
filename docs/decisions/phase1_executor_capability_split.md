# Phase 1 Executor Capability Split — Design Decisions

Related issue: [#96](https://github.com/redixhumayun/simpledb/issues/96)

## Context

Phase 1 of the planner/execution boundary plan replaces the universal `UpdateScan`
abstraction with narrower capability interfaces. This document records the concrete
design decisions made before implementation began.

---

## Decision 1: New trait names and hierarchy

**Choice:** Introduce `TableCursor` as the new mutation trait, keep `Scan` as-is.

**Rationale:** `Scan` already captures what the plan called "RowCursor" — row
iteration and field reads. Renaming it would be high-churn with no semantic gain at
this stage. The plan acknowledges "The exact trait names are not important yet."
`TableCursor: Scan + Any` adds mutation capability (set, delete, insert, get_rid,
move_to_rid) and is only implemented by `TableScan`.

**`UpdateScan` fate:** Removed entirely. It is replaced by `TableCursor` for
table-backed mutation and `Scan` for read-only operators.

---

## Decision 2: `Plan::open()` return type

**Choice:** Change `Plan::open(&self) -> Box<dyn Scan>`.

**Rationale:** Most plan nodes are read-only. Returning `Box<dyn UpdateScan>` from
every plan was the root cause of fake `unimplemented!()` / `todo!()` implementations
on every read-only scan type.

---

## Decision 3: Accessing concrete scan types without downcasts

Four plans were downcasting `plan.open()` to concrete scan types:

| Plan | Was doing | Fix |
|---|---|---|
| `MergeJoinPlan` | downcast `plan_2.open()` → `SortScan` | store `plan_2: Arc<SortPlan>`, call `open_sort_scan()` |
| `MultiBufferProductPlan` | downcast `lhs.open()` → `TableScan` | store `lhs: Arc<MaterializePlan>`, call `open_table_scan()` |
| `IndexSelectPlan` | downcast `plan.open()` → `TableScan` | store `plan: Arc<TablePlan>`, call `open_table_scan()` |
| `IndexJoinPlan` | downcast `plan_2.open()` → `TableScan` | store `plan_2: Arc<TablePlan>`, call `open_table_scan()` |

**Choice:** Change plan field types to concrete plan types and add non-trait
`open_X()` methods that return the concrete scan type.

**New methods:**
- `TablePlan::open_table_scan() -> TableScan`
- `MaterializePlan::open_table_scan() -> TableScan`
- `SortPlan::open_sort_scan() -> SortScan`

**For `MergeJoinPlan`:** Also requires `SortScan::save_position()` and
`restore_position()`. These stay as inherent methods on `SortScan` (not put in a
trait). `MergeJoinScan` holds `scan_2: SortScan` directly; `MergeJoinPlan` calls
`sort_plan_2.open_sort_scan()`. No narrow "PositionSaveable" trait is introduced
at this stage — the coupling is explicit and there is only one use site.

**For `TablePlanner` in the query optimizer:** `TablePlanner::plan` field changes
from `Arc<dyn Plan>` to `Arc<TablePlan>`, since it is always constructed as a
`TablePlan` in `TablePlanner::new()`. Methods `make_index_select_plan` and
`make_index_join_plan` pass `Arc<TablePlan>` directly.

---

## Decision 4: Mutation planner paths

The update planners (`BasicUpdatePlanner`, `IndexUpdatePlanner`) previously called
`plan.open()` on `SelectPlan(TablePlan)` to get a mutable scan. After the change,
`Plan::open()` returns `Box<dyn Scan>` — no mutation methods available.

**Choice:** Mutation planners bypass `SelectPlan` and work with `TableScan` directly.
For DELETE and MODIFY, the predicate is applied inline:

```rust
let mut scan = table_plan.open_table_scan();
scan.before_first()?;
while let Some(result) = scan.next() {
    result?;
    if predicate.is_satisfied(&scan)? {
        // do mutation
    }
}
```

For INSERT, `table_plan.open_table_scan()` is called directly.

**Rationale:** Mutation planners always target a single base table. The `SelectPlan`
wrapping was only needed to pass through `UpdateScan`. Inline filtering is correct
and more explicit about what is happening. This is also the pattern that will
eventually use `ExecutionContext` (phase 3).

---

## Decision 5: `SortPlan::split_into_runs` and `copy`

These internal methods used `Box<dyn UpdateScan>` and `Dest: UpdateScan`.

**Choice:**
- `split_into_runs` takes `Box<dyn Scan>` (it only reads the source)
- `copy` changes to `Dest: TableCursor` (it writes to a `TableScan`)

---

## Decision 6: Generic scan constraint changes

All generic scan wrappers had `where S: UpdateScan + 'static`. Since those types
no longer implement `UpdateScan`, the constraints drop to `where S: Scan + 'static`.

Affected types: `ProductScan`, `ProjectScan`, `SelectScan`, `MultiBufferProductScan`,
`MergeJoinScan`, `IndexJoinScan`.

---

## Decision 7: `Box<dyn TableCursor>` blanket impls

`impl Scan for Box<dyn TableCursor>` and `impl TableCursor for Box<dyn TableCursor>`
are added for future use (phase 2-3 will need `Box<dyn TableCursor>` at the
planner/executor boundary). Phase 1 does not require these for correctness but they
are low-cost and make the trait usable as a trait object.

---

## What is NOT in phase 1

- `Arc<Transaction>` is NOT removed from any plan or scan
- `TableScan`/`RecordPage` still own `Arc<Transaction>` directly
- No `ExecutionContext` type is introduced
- `SortScan` and `ChunkScan` retain `Arc<Transaction>` ownership
- `InsertSink` trait is not introduced (insert stays on `TableCursor`)
