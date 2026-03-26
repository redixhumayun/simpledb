# Phases 2–5: ExecutionContext Threading — Design Decisions

Related issue: [#96](https://github.com/redixhumayun/simpledb/issues/96)

## Scope

Phases 2–5 are implemented together because they form a single coherent
change: thread `ExecutionContext` through `Plan::open()` and remove
`Arc<Transaction>` from planner-owned objects.

---

## Decision 1: `ExecutionContext` shape

```rust
pub struct ExecutionContext {
    txn: Arc<Transaction>,
}

impl ExecutionContext {
    pub fn new(txn: Arc<Transaction>) -> Self { Self { txn } }
    pub fn txn(&self) -> &Arc<Transaction> { &self.txn }
}
```

Initially wraps `Arc<Transaction>` verbatim. Later phases (issue #63) may
change the interior to a session-based authority type without disturbing
callers.

---

## Decision 2: `Plan::open()` signature

```rust
fn open(&self, ctx: &ExecutionContext) -> Box<dyn Scan>;
```

The context-free `open()` is removed entirely (phase 5 goal). All callers
must provide an `ExecutionContext`. There is no shim/deprecated path — the
migration is done in one pass.

---

## Decision 3: Which plan structs lose their `txn` field

| Struct | txn removed? | Reason |
|---|---|---|
| `TablePlan` | YES | Metadata fetched at construction; txn not needed at open time |
| `MaterializePlan` | YES | Gets txn from ctx during open |
| `SortPlan` | YES | Gets txn from ctx during open |
| `MergeJoinPlan` | YES | Only needed to pass to child plans/sorts; children now take ctx |
| `MultiBufferProductPlan` | YES | Same |
| `TablePlanner` | YES | Only needed for plan construction; not stored after phase 4 |
| `TempTable` | NO | Execution object; keeps Arc<Transaction> internally (phase 3 policy) |
| `ChunkScan` | NO | Execution object; same policy |
| `TableScan` | NO | Execution object; same policy |
| `RecordPage` | NO | Execution object; same policy |

---

## Decision 4: `MaterializePlan::blocks_accessed()` after txn removal

Currently uses `self.txn.block_size()`. Fix: store `block_size: usize` as a
plain field, set at construction from `txn.block_size()`. This is a static
property of the database configuration, not a runtime value, so caching it
at plan creation is correct.

---

## Decision 5: `open_table_scan` / `open_sort_scan` signatures

These concrete-type-returning methods also take `ctx`:

```rust
// TablePlan
pub fn open_table_scan(&self, ctx: &ExecutionContext) -> TableScan

// MaterializePlan
pub fn open_table_scan(&self, ctx: &ExecutionContext) -> TableScan

// SortPlan
pub fn open_sort_scan(&self, ctx: &ExecutionContext) -> SortScan
```

Callers that were already using these methods (IndexSelectPlan, IndexJoinPlan,
MultiBufferProductPlan, MergeJoinPlan) pass through the ctx they receive in
their own `open(ctx)`.

---

## Decision 6: `SortPlan` internal methods

`split_into_runs`, `merge`, and `do_merge_iters` all create `TempTable`s.
After removing `self.txn`:

- `split_into_runs(&self, ctx: &ExecutionContext, source_scan: Box<dyn Scan>)`
- `merge(&self, ctx: &ExecutionContext, table_1: TempTable, table_2: TempTable)`
- `do_merge_iters(&self, ctx: &ExecutionContext, temp_tables: Vec<TempTable>)`
- `copy` stays `copy<Source: Scan, Dest: TableCursor>` — no ctx needed

---

## Decision 7: `MultiBufferProductPlan::create_temp_table`

```rust
fn create_temp_table(&self, ctx: &ExecutionContext, plan: &Arc<dyn Plan>) -> SimpleDBResult<TempTable>
```

---

## Decision 8: `MergeJoinPlan::new()` and `MultiBufferProductPlan::new()`

Remove `txn` parameter entirely. The schema merging that happens in `new()`
does not require a transaction.

```rust
// Before
MergeJoinPlan::new(plan_1, plan_2, txn, field_name_1, field_name_2)
// After
MergeJoinPlan::new(plan_1, plan_2, field_name_1, field_name_2)

// Before
MultiBufferProductPlan::new(lhs, rhs, txn)
// After
MultiBufferProductPlan::new(lhs, rhs)
```

---

## Decision 9: Mutation planner call sites

Update planners create `ExecutionContext` locally from their `txn` parameter:

```rust
let ctx = ExecutionContext::new(Arc::clone(&txn));
let scan = table_plan.open_table_scan(&ctx);
```

---

## Decision 10: `QueryPlanner::create_plan` trait

Keeps taking `txn: Arc<Transaction>` — plans still need txn during the
planning phase for metadata lookups. The trait signature does not change.
The returned `Arc<dyn Plan>` is now metadata-only (no txn stored inside).

---

## Decision 11: `TablePlanner`

Drops `txn: Arc<Transaction>` field. The `txn` is still passed to `new()`
for the initial `get_index_info` metadata query and `TablePlan::new()` call,
but is not stored. `make_product_plan` and the sort-plan helpers in
`HeuristicQueryPlanner` no longer pass txn to plan constructors.

---

## What phase 6 sees after this work

- `ExecutionContext` exists and is the only path to execution
- Plan objects carry zero runtime authority
- `ExecutionContext::txn()` is the sole seam — issue #63 can change what
  lives inside it without touching any plan code
