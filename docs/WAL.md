# WAL and Recovery

## Overview

SimpleDB uses write-ahead logging (WAL) with physical/page-oriented mutation records for heap and B-tree operations.
The implementation currently follows a **steal + force** buffer policy with an **undo-only** recovery pass.

Core components:
- `LogManager` (`src/main.rs`): append-only WAL writer/iterator over WAL pages.
- `LogRecord` (`src/main.rs`): typed record enum for heap, B-tree, split/root/free-list, and page-format operations.
- Page/view mutators (`src/page.rs`, `src/btree.rs`): emit WAL records and track page LSN.
- `RecoveryManager` (`src/main.rs`): commit, rollback, and crash recovery logic.
- Buffer manager flush path (`src/buffer_manager/mod.rs`): enforces WAL-before-data flush ordering.

## Durability Model

### Steal + Force

- **Steal**: dirty pages may be flushed before transaction commit (e.g. replacement/assignment flush paths).
- **Force**: commit calls `flush_all(txn)` before writing/flushing the commit record.

Code points:
- Force on commit: `RecoveryManager::commit()` in `src/main.rs`.
- Flush ordering enforcement: `BufferFrame::flush_locked()` calls `flush_lsn(lsn)` before data page write in `src/buffer_manager/mod.rs`.

### Recovery Strategy

- Recovery is **undo-focused** for unfinished transactions.
- WAL is scanned backward using `LogIterator`.
- Undo eligibility uses page LSN gating: `page_lsn >= record_lsn`.

Code points:
- Recovery loop: `RecoveryManager::recover()` in `src/main.rs`.
- LSN gate: `LogRecord::should_undo_during_recovery()` in `src/main.rs`.

## WAL Write Path

1. Mutation path emits a `LogRecord` and gets LSN (`record.write_log_record(...)`).
2. Mutable page views track highest emitted page LSN (`page_lsn`).
3. On drop of dirty mutable view:
   - page header LSN is written,
   - frame marked modified with that LSN.
4. On frame flush, WAL is flushed to that LSN before writing page bytes.

This ties page bytes to a concrete WAL position and preserves WAL ordering on disk flush.

## Invariants

1. **WAL-before-data flush**
   - If a page frame has LSN `L`, flush must force WAL to `>= L` before writing page.

2. **Dirty mutable view must have page LSN**
   - Mutable view `Drop` now treats `dirty && page_lsn.is_none()` as invariant violation (panic), not silent fallback.

3. **Undo records carry sufficient preimage for supported operations**
   - Heap tuple and B-tree entry/header operations include enough old state for implemented undo paths.

4. **Meta/root mutations use real WAL LSN**
   - Root update flow propagates emitted record LSN into meta-page dirty marking.

## Tradeoffs

1. **Simple undo-focused recovery**
   - Easier to reason about than full ARIES+CLR design.
   - Limitation: less robust for crash-during-recovery scenarios (no CLRs yet).

2. **Single-page WAL record constraint**
   - Record payload must fit one WAL page payload (`PAGE_SIZE - wal_header`).
   - With 4KB pages and current WAL header, max payload is 4088 bytes.
   - No multi-page record support today.

3. **Physical-ish logging for page/view mutations**
   - Good for deterministic undo of implemented operations.
   - Large before-images (e.g. generic full page images) are constrained by record size cap.

## Known Shortcomings / Open Work

1. **Reclaimed/reused non-fresh pages need explicit lifecycle semantics**
   - Current `*FormatFresh` model assumes fresh append intent.
   - Future page reclaim/reuse must choose and enforce one model:
     - strict lifecycle boundary semantics, or
     - richer preimage logging (potentially needing multi-page WAL).
   - Tracking issue: https://github.com/redixhumayun/simpledb/issues/69

2. **Recovery/checkpoint semantics are minimal**
   - Recovery currently stops at first checkpoint seen in backward scan (assumes quiescent marker semantics).
   - No CLRs; crash-during-recovery handling is limited.

3. **WAL format scalability**
   - No spanning/multi-page log record support.
   - This constrains future features that require large physical images.

The main remaining design risk is future page reclaim/reuse semantics for non-fresh repurpose paths (issue #69).