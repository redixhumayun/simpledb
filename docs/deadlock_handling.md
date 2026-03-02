# Deadlock Handling

## The Problem

A deadlock occurs when two or more transactions form a circular wait-for dependency:

```
T1 holds S on row R1, wants X on row R2
T2 holds S on row R2, wants X on row R1
```

Neither can proceed. The current implementation resolves this purely by timeout — transactions
waiting beyond a threshold return an error and must retry. This works but means a deadlocked
transaction wastes the full timeout duration before making progress.

There are two principled alternatives: cycle detection and eager abort.

---

## Option 1: Cycle Detection (Reactive)

Maintain a wait-for graph alongside the lock table. Each time a transaction blocks, add a
directed edge from the waiter to every holder it is waiting on. When a transaction is granted
its lock or aborts, remove its edges.

```
T1 waiting for T2  →  edge T1 → T2
T2 waiting for T1  →  edge T2 → T1
cycle: T1 → T2 → T1
```

On each blocked request, run a DFS from the waiting transaction. If a cycle is found, choose a
victim (typically the youngest transaction) and abort it.

**Pros:**
- Only aborts transactions when a deadlock actually exists — no false positives.

**Cons:**
- Graph maintenance adds overhead to every acquire and release.
- Cycle detection is O(V + E) over the wait-for graph on every blocked request.
- Concurrent graph updates require careful locking — the graph itself becomes a contention point.
- Victim selection and signalling the victim mid-execution adds implementation complexity.

The deleted `src/wait_for.rs` was a sketch of this approach.

---

## Option 2: Wait-Die (Eager Abort, Non-Preemptive)

Assign each transaction a timestamp at start time. Older timestamp = higher priority.

Rule: when a transaction T requests a lock held by transaction H:
- If T is **older** than H → T waits.
- If T is **younger** than H → T dies (aborts immediately and retries).

```
T1 (older) requests lock held by T2 (younger) → T1 waits
T2 (younger) requests lock held by T1 (older) → T2 aborts
```

A cycle can never form: it would require a younger transaction to be waiting on an older one,
but the rule kills younger transactions before they can wait.

**Pros:**
- No graph required — one timestamp comparison per blocked request.
- Simple to implement: on block, compare timestamps, return error if younger.
- Non-preemptive — the holder is never interrupted.

**Cons:**
- False positives: a younger transaction aborts even if no deadlock would have formed.
- Younger transactions may starve under high contention if they keep losing to older ones
  (mitigated by retaining the original timestamp on retry, so the retried transaction ages).

---

## Option 3: Wound-Wait (Eager Abort, Preemptive)

Same timestamp scheme, opposite rule:

- If T is **older** than H → T wounds H (H is forced to abort).
- If T is **younger** than H → T waits.

Older transactions never wait — they always preempt. This reduces unnecessary aborts compared
to wait-die but requires a mechanism to interrupt a running holder mid-execution (e.g. a flag
the holder checks periodically, or an error returned on its next lock/IO operation).

**Pros:**
- Fewer unnecessary aborts than wait-die.
- Older (longer-running) transactions are protected from being killed.

**Cons:**
- Requires preemption infrastructure — harder to implement correctly.
- A wounded transaction may have done significant work before it checks the wound flag.

---

## Recommendation for This Codebase

Wait-die is the right fit here. It requires no graph, no preemption, and fits naturally into
the existing `acquire` loop: before waiting, compare the requesting transaction's timestamp
against all incompatible holders — if younger, return an error immediately. The transaction
layer above already handles abort-and-retry on lock failure. Retried transactions must keep
their original timestamp so they eventually become the oldest waiter and make progress.
