# 001 — Deadlock Handling: Timeout-Only (Wait-Die Removed)

## Context

`LockTable::acquire` resolves deadlocks purely by timeout — a transaction waiting for a lock returns an error after `lock_timeout_ms` and the caller retries. The timeout is set to 1000ms in benchmarks and varies per test.

Wait-die was proposed as an improvement: instead of waiting up to the full timeout, a younger transaction (higher `tx_id`, which is a monotonically increasing `u64`) that encounters an older incompatible holder aborts immediately. This eliminates worst-case timeout latency and resolves deadlocks faster.

Wait-die was implemented in PR #85 (`LockError` typed enum + wait-die checks in both the new-lock and upgrade paths of `acquire`), benchmarked, and then removed.

- **Implemented:** commit `52b376b`
- **Removed:** commit `56c84c9`

## Decision

Remove the wait-die checks. Keep the typed `LockError` enum (`Timeout` | `WaitDieAbort`) as it is a strict improvement over string matching regardless of deadlock strategy.

## Alternatives Considered

**Keep wait-die:** Correct implementation, small code footprint. Rejected because benchmarking showed it actively hurts the current workload (see below).

**Feature-gate wait-die behind `--features wait-die`:** Keeps the code available without it being on by default. Rejected as unnecessary complexity for an optimization with no demonstrated benefit on any workload in this codebase.

## Evidence

Benchmarked against `simple_bench` concurrent UPDATE workload (10 workers, same page, disjoint IDs, 1000ms timeout):

- **Master (timeout-only):** `retries=0, timeouts=0` consistently across 100 samples — every transaction acquired its lock on the first attempt by waiting briefly on the condvar
- **Wait-die branch:** `retries=~57, aborts=~82` per 100-op run — wait-die fired aggressively, converting cheap condvar waits into expensive abort+rollback+retry cycles

The regression occurs because this workload is "brief-lock": holders finish and release within milliseconds, so a brief condvar wait always succeeds. Wait-die aborts younger transactions immediately even when waiting a few milliseconds would have succeeded at no extra cost.

## Root Cause of Wait-Die Regression

Two compounding factors:

1. **Lock hold times are short** relative to the timeout (microseconds to low milliseconds vs 1000ms timeout). Timeouts essentially never fire on this workload, so there is no wasted wait time for wait-die to eliminate.

2. **Retry cost is high**: the retry loop re-scans the table from the start to find the target row. Resuming from the aborted position would require storing row location (BlockId + slot) across transaction boundaries — non-trivial given the pull-based iterator model where scan state is tied to a transaction's lifetime.

## When Wait-Die Would Help

Wait-die is net positive when lock hold times are long enough that timeouts fire frequently. This codebase has no such workload currently. If one is added (e.g., long-running analytical queries holding locks during a full table scan), wait-die should be reconsidered alongside a fix to retry cost (resume from known row position rather than re-scanning).

## Consequences

- Deadlock resolution remains timeout-only. Worst-case latency for a blocked transaction is `lock_timeout_ms`.
- `LockError` enum stays: callers use `downcast_ref::<LockError>()` rather than string matching on `"Timeout"`, which is cleaner and extensible.
- `LockError::WaitDieAbort` variant exists but is never constructed. It documents the intent for future implementors.
