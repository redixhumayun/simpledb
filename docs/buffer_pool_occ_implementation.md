# Buffer Pool OCC Implementation Notes

This document tracks the first correctness-first OCC implementation for the buffer pool scaling work.

## Goal

Implement a simpler residency protocol that:

- removes `latch_shards` and per-block latches from the resident-hit path
- replaces `resident_shards` with a single global directory
- uses directory lookup plus frame-local validation
- keeps enough of the old flusher/page infrastructure to reduce change surface

This is **not** the final performance design. It is the first protocol milestone.

## Milestone 1 Decisions

1. Directory shape

- use one global `Mutex<HashMap<BlockId, DirectoryEntry>>`
- `DirectoryEntry` states:
  - `Installing`
  - `Resident { frame_idx, generation }`

Reason:

- simplest way to get the protocol correct first
- if scaling is still limited, shard or switch to `RwLock` later

2. Frame control state

- split frame state in two pieces:
  - hot atomics on `BufferFrame`
    - `pin_count`
    - Clock `ref_bit`
    - control word for `generation/loading/evicting`
  - cold/flush metadata on `FrameMeta`
    - `BlockId`
    - dirty/writeback state
    - replacement-list links where applicable

Reason:

- validating residency through `FrameMeta` still left the hit path serialized on the per-frame mutex
- moving `generation/loading/evicting` out too was necessary to make the hit path meaningfully OCC-shaped

Status:

- implemented
- `BufferFrame` now carries:
  - `pin_count: AtomicUsize`
  - `ref_bit: AtomicBool`
  - `control: AtomicU64`
- `FrameMeta` no longer stores pin count, ref bit, or residency-control flags

3. Generation source of truth

- add a dedicated residency generation to the frame metadata
- do not reuse dirty/writeback generation for residency identity

Reason:

- dirty generation and residency generation represent different correctness domains

Status:

- implemented as dedicated residency generation on the frame

4. Hit validation style

- lookup directory
- drop directory lock
- validate atomic frame control against directory generation
- pin only if still stable

Reason:

- this is the actual OCC step that replaces block-latch protection on hits

Status:

- implemented for `pin()` and `pin_fast()`
- hit validation now uses the atomic control word
- successful pins still consult `FrameMeta` when `pin_count` transitions `0 -> 1`

5. Installing wait policy

- for milestone 1, use retry/yield rather than a dedicated waiter queue

Reason:

- simpler to implement
- enough to prove the protocol
- more targeted wait/wake can come later

Status:

- implemented as retry + `yield_now()` on `Installing`

6. Eviction coordination

- eviction claim is a CAS on the atomic control word
- after the CAS, re-check writeback state under `FrameMeta`

Reason:

- new pins must see `evicting=1` before reuse proceeds
- writeback state still lives in `FrameMeta`, so eviction must reconcile with both layers

Status:

- implemented
- `Clock` now observes atomic `pin_count` / control flags / `ref_bit`

7. Prefetch handling

- adapt prefetch to the same directory protocol instead of keeping an old side protocol

Reason:

- two residency protocols in one manager would be harder to reason about than one simplified protocol

Status:

- implemented
- targeted prefetch tests pass

## What Changed

- removed `latch_shards`
- removed per-block latch participation from pin paths
- removed `resident_shards`
- added one global directory with:
  - `Installing`
  - `Resident { frame_idx, generation }`
- changed hit path to:
  - lookup directory
  - validate atomic frame-local residency generation/state
  - increment atomic pin count
  - revalidate and roll back on race
- changed miss path to:
  - publish `Installing`
  - claim victim
  - unpublish old resident mapping
  - load new residency
  - publish `Resident`

## Remaining Hot-Path Caveat

`pin_count` and residency validation are now atomic, but the common benchmark still does
`0 -> 1 -> 0` transitions on every operation. Those transitions still lock `FrameMeta`
to answer:

- did this leave the clean-unpinned pool?
- did this become clean-unpinned again?
- should this dirty frame be queued for flush?

That means this change removes shared residency serialization, but it does **not** yet
remove all per-operation metadata locking from `Concurrent Pin`.

## Open Questions To Revisit Later

- shard directory
- switch directory reads to `RwLock`
- add a better install waiter mechanism
- partition Clock eviction domains
- if needed, add one more atomic summary bit for clean/flushable state so `0 <-> 1`
  pin transitions stop locking `FrameMeta`

## Validation

Builds completed:

```bash
cargo build --release --bin profile_buffer_pool_pin --no-default-features --features replacement_clock --features page-4k
cargo bench --bench buffer_pool --no-run --no-default-features --features replacement_clock --features page-4k
```

Targeted tests passed:

```bash
cargo test --no-default-features --features replacement_clock --features page-4k buffer_manager_tests::
```

Quick spot check from the profiling harness:

```bash
cargo run --release --bin profile_buffer_pool_pin --no-default-features --features replacement_clock --features page-4k -- --threads 4 --duration-secs 3 --buffers 4096 --blocks-per-thread 10
cargo run --release --bin profile_buffer_pool_pin --no-default-features --features replacement_clock --features page-4k -- --threads 8 --duration-secs 3 --buffers 4096 --blocks-per-thread 10
```

Observed:

- `4t`: about `1.40M ops/s`
- `8t`: about `1.82M ops/s`
- `8t / 4t`: about `1.30x`

Interpretation:

- correctness is intact
- the protocol is cleaner
- but this step alone does not yet materially change scaling
- the remaining `0 <-> 1` transition lock on `FrameMeta` is the next obvious target

Notes:

- I started one full `cargo test --no-default-features --features replacement_clock --features page-4k` run, but it stopped producing fresh output while long-running unrelated tests were still consuming CPU.
- I chose to stop there and rely on focused buffer-manager validation for this first milestone.
