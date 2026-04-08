# 005 — Recovery Uses Steal + No-Force With Redo+Undo

## Context

The previous runtime used `steal + force` with an undo-only crash recovery pass.
That made commit latency depend on flushing data pages before acknowledging commit.

The direct-I/O and batched-write work exposed that this coupling is expensive and awkward: commit threads end up sitting on data-page flush behavior instead of only WAL durability.

## Decision

Move to `steal + no-force`.

- commit durability is WAL-only: append `Commit`, flush WAL, return
- data pages are no longer forced at commit
- crash recovery now does forward redo for committed records and backward undo for unfinished records
- startup recovery runs before metadata bootstrapping on reopen
- page-header LSNs are stamped from frame metadata on flush so redo gating is reliable across page types

## Consequences

- committed changes may exist only in WAL at crash time, so redo recovery is required for correctness
- recovery work is larger than before because restart can no longer rely on force-at-commit
- WAL record payloads for recovery-sensitive operations must remain redo-sufficient
- commit latency is decoupled from data-page flush latency

## Notes

- This change does not introduce fuzzy-checkpoint metadata yet; recovery conservatively scans the full log
- crash-during-recovery semantics remain limited because the system still does not use CLRs
