# No-Force Recovery Plan

Goal: move SimpleDB from `steal + force + undo-only recovery` to `steal + no-force + redo+undo recovery`.

## Why

- `force` couples commit latency to data-page flush.
- `no-force` lets commit return after WAL durability only.
- correctness then depends on recovery being able to redo committed changes that were still only in memory at crash time.

## Required changes

1. Commit protocol
- stop flushing data pages in `RecoveryManager::commit()`
- commit becomes: append `Commit`, flush WAL through commit LSN, return

2. Recovery algorithm
- current backward undo-only pass is insufficient
- recovery must do:
  - analysis over the WAL to identify committed and unfinished txns
  - forward redo of committed records when `page_lsn < record_lsn`
  - backward undo of unfinished txns

3. Page LSN discipline
- page LSN must be reliable on every flushed page
- flush path should stamp page/header LSN from frame metadata before writeout instead of relying on scattered callers

4. Checkpoint semantics
- current bare `Checkpoint` record is not enough for fuzzy checkpoints
- minimal correct first step: do not rely on checkpoint to truncate recovery work; full-log recovery is acceptable

5. WAL payload gaps
- redo needs each WAL record to carry enough post-image information
- known gaps to fix:
  - `BTreeMetaFormatFresh`
  - `BTreePageSplit`

## First implementation shape

1. Keep `steal` and WAL-before-data flush.
2. Remove force-at-commit.
3. Add a forward log scan helper.
4. Implement `redo(record, lsn)` for all redo-capable log records.
5. Make full recovery scan the entire WAL instead of stopping at checkpoint.
6. Keep checkpoint conservative/minimal for now.

## Validation

Must pass:

1. crash test: committed update survives restart without pre-commit data flush
2. crash test: committed changes are redone and uncommitted changes are undone in the same restart
3. existing rollback/recovery tests
4. required cargo matrix from `AGENTS.md`

## Non-goals for this step

- fuzzy checkpoint metadata
- background flusher policy redesign
- direct-I/O writeback batching
