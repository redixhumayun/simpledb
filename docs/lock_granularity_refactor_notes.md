# Lock Granularity Refactor Notes

## Context

Current design conflates:
1. Logical concurrency control (transaction locks)
2. Physical page protection (latches)

Both are currently acquired via transaction pin APIs at page (`BlockId`) granularity. This over-serializes concurrent work on different rows in the same page.

## Target Direction

Separate concerns:
1. **Logical locks**: table/row scope, held to commit/rollback (strict 2PL).
2. **Physical latches**: page scope, held only during page access.

Practical outcome:
- More concurrency for same-page, disjoint-row operations.
- Better foundation for future B-tree concurrency work.

## Why Intent Locks Are Needed

If locks exist at both table and row level, intent locks are required for correct cross-level conflict checks.

Without intent locks:
- A transaction can hold row `X` locks while another transaction is incorrectly granted table `S`/`X`.
- Or the engine must scan all row locks to decide table-lock compatibility (too expensive).

With intent locks:
- Row readers/writers declare intent at table scope (`IS`/`IX`).
- Table-level compatibility checks remain cheap and correct.

## Simple Example

1. Txn A updates one row in table `users`.
2. Txn A takes row `X(users, rid=...)`.
3. Txn B requests table `S(users)` (or `X(users)`).

If Txn A first took table `IX(users)`, Txn B is blocked correctly by table-level compatibility.

## Proposed Lock Modes

Table-level:
1. `IS` (intent shared)
2. `IX` (intent exclusive)
3. `S` (shared table)
4. `X` (exclusive table)

Row-level:
1. `S`
2. `X`

Compatibility (table-level, standard):
1. `IS` with `IS`, `IX`, `S`
2. `IX` with `IS`, `IX`
3. `S` with `IS`, `S`
4. `X` with none

## Layering Decision

`Transaction` still owns lock lifetime/release, but lock acquisition call-sites move up:

1. Record/executor layer decides logical lock target and mode.
2. Record/executor layer calls `txn.lock_*` API.
3. Pin/latch APIs do not acquire logical locks.

So: policy at record/executor layer, ownership/lifetime in transaction.

## API Sketch

```rust
enum LockMode { IS, IX, S, X }

enum LockTarget {
    Table { table_id: u32 },
    Row { table_id: u32, block: u32, slot: u16 },
}

impl Transaction {
    fn lock_table_s(&self, table_id: u32) -> Result<(), Error>;
    fn lock_table_x(&self, table_id: u32) -> Result<(), Error>;
    fn lock_row_s(&self, table_id: u32, rid: Rid) -> Result<(), Error>;
    fn lock_row_x(&self, table_id: u32, rid: Rid) -> Result<(), Error>;
}
```

Expected behavior:
1. `lock_row_s` ensures table `IS` first.
2. `lock_row_x` ensures table `IX` first.
3. Locks released at commit/rollback only.

## What Changes in Current Code Paths

1. Remove logical `slock/xlock` from `pin_read_guard` / `pin_write_guard`.
2. Update `LockTable` + `ConcurrencyManager` to key locks by table/row targets (not `BlockId`).
3. In record/executor operations:
   1. row reads acquire row `S`
   2. row updates/deletes acquire row `X`
4. Keep operation order:
   1. logical lock
   2. pin/latch
   3. read/write
   4. unlatch/unpin
   5. logical unlock at commit/rollback

## Is Table + RID Context Already Available?

Mostly yes:
1. Table identity exists in scan/record paths.
2. RID is known when positioned on a row (block + slot context).

Main gap is architectural usage, not missing data: lock keys and lock call-sites still assume page-level logical locking.

## Cardinality / Scale Considerations

Row-level locking increases lock count substantially.

Operational controls to add:
1. Per-transaction lock count metric.
2. Lock wait/timeout counters.
3. Optional escalation policy (`many row locks -> table lock`) if needed later.

## Execution Order Recommendation

1. Implement logical-vs-physical separation + table/row locks first.
2. Validate with new concurrent SQL benchmarks (same-page/disjoint-RID).
3. Then proceed to deeper B-tree concurrency changes.
