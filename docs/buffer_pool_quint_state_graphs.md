# Buffer Pool Quint State Graphs

This document collects the state-transition graphs for the Quint buffer-pool model in [specs/buffer_pool.qnt](/home/ci/worktree-buffer-pool-opt/specs/buffer_pool.qnt:42).

## Thread Phases

This graph covers all `ThreadPhase` states and the actions that transition between them in [specs/buffer_pool.qnt](/home/ci/worktree-buffer-pool-opt/specs/buffer_pool.qnt:82).

```mermaid
stateDiagram-v2
    direction LR

    [*] --> PhIdle

    PhIdle --> PhLookedUp: do_dir_lookup
    PhLookedUp --> PhValidated: do_first_validate_ok
    PhLookedUp --> PhIdle: do_first_validate_fail
    PhValidated --> PhPinPending: do_pin_attempt
    PhPinPending --> PhPinned: do_pin_revalidate_ok
    PhPinPending --> PhIdle: do_pin_revalidate_fail
    PhPinned --> PhIdle: do_unpin

    PhIdle --> PhInstalling: do_miss_install
    PhInstalling --> PhIdle: do_publish

    PhIdle --> PhEvictClaiming: do_evict_claim
    PhEvictClaiming --> PhIdle: do_evict_complete
```

Notes:

- This is only the `thread_phase` graph. It does not show `frame_state` or `dir` transitions.
- An edge means "this action may move a thread between these phases when its guards hold."
- If an action's guards do not hold, that action is simply not enabled; there is no separate failure state.

## Frame States

This graph covers all `FrameState` values and the actions that transition between them in [specs/buffer_pool.qnt](/home/ci/worktree-buffer-pool-opt/specs/buffer_pool.qnt:65).

```mermaid
stateDiagram-v2
    direction LR

    [*] --> FrameFree

    FrameFree --> FrameLoading: do_miss_install
    FrameLoading --> FrameResidentStable: do_publish
    FrameResidentStable --> FrameEvicting: do_evict_claim
    FrameEvicting --> FrameFree: do_evict_complete
```

Notes:

- This is only the `frame_state` graph. It does not show `thread_phase` or directory transitions.
- `do_dir_lookup`, `do_first_validate_*`, `do_pin_attempt`, `do_pin_revalidate_*`, and `do_unpin` do not change `frame_state`.
- The design doc discusses a future `Writeback` state, but this first Quint model does not include it.

## Directory States

This graph covers all `DirState` values and the actions that transition between them in [specs/buffer_pool.qnt](/home/ci/worktree-buffer-pool-opt/specs/buffer_pool.qnt:55).

```mermaid
stateDiagram-v2
    direction LR

    [*] --> DirAbsent

    DirAbsent --> DirInstalling: do_miss_install
    DirInstalling --> DirResident: do_publish
    DirResident --> DirAbsent: do_evict_complete
```

Notes:

- This is only the directory-state graph. It does not show `thread_phase` or `frame_state` transitions.
- The `DirResident` node stands for `DirResident { frame, gen }`.
- `do_dir_lookup` reads the directory but does not change directory state.
