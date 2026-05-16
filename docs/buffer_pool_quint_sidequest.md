# Buffer Pool Quint Side Quest

This is a side quest for the main buffer-pool protocol redesign.

Goal:

- verify protocol correctness before or alongside implementation work
- model the directory + frame state machine in Quint
- check safety invariants with model checking

This is **not** the main implementation path.
It is a correctness aid for the main redesign work described in [buffer_pool_scaling_targets.md](/home/ci/worktree-buffer-pool-opt/docs/buffer_pool_scaling_targets.md:1).

## Why Quint

The protocol we are discussing is a small concurrent state machine:

- directory states
- frame states
- hit / miss / install / evict transitions
- retry on stale observations

That is a good fit for Quint.

Useful references:

- Quint overview: <https://quint-lang.org/docs/what-does-quint-do>
- Model checkers: <https://quint-lang.org/docs/model-checkers>
- CLI / verification: <https://quint-lang.org/docs/quint>
- Checking invariants: <https://quint-lang.org/docs/checking-properties>

## Scope

First model only the core residency protocol:

- directory:
  - `Absent`
  - `Installing`
  - `Resident(frame, generation)`

- frame:
  - `Free`
  - `ResidentStable`
  - `Evicting`
  - `Loading`
  - `Writeback`

- fields:
  - `pin_count`
  - `generation`
  - `ref_bit`
  - maybe `dirty`

- actions:
  - hit lookup
  - hit pin success / retry
  - miss claim install
  - eviction claim
  - publish resident
  - unpublish old
  - unpin

Do **not** start with the full flusher/writeback machinery. Keep the first model small.

## Invariants To Check

- at most one installer per block
- at most one resident mapping per block
- successful pin implies block/frame/generation match
- pinned frame is never reused
- `evicting` or `loading` frame cannot be pinned successfully
- generation changes when a frame is reused
- stale directory observations can cause retry, but not wrong pin

## Model Size

Keep the first model tiny:

- `2` frames
- `2` blocks
- `2` threads

If that works, expand carefully.

## Expected Value

Quint will not tell us whether the protocol is fast.

It **will** help answer:

- is the optimistic validation protocol actually safe?
- are there stale lookup races that lead to incorrect pinning?
- do the state transitions preserve the intended invariants?

## Suggested Workflow

1. Write a tiny Quint model for hit/miss/install/evict.
2. Check the invariants above.
3. Refine the protocol if Quint finds a counterexample.
4. Only then push the corresponding implementation structure in Rust.
