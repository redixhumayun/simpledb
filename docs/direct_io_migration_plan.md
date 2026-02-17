# Direct I/O Migration Plan

## Scope

Enable direct I/O for **data files only**. WAL remains on the current buffered path.

- Data files: direct I/O (`O_DIRECT` on Linux)
- WAL (`simpledb.log`): existing buffered I/O + fsync behavior

## Goals

1. Eliminate OS page-cache double buffering for heap/index data pages.
2. Preserve existing WAL durability and ordering invariants.
3. Keep rollout low-risk with explicit fallback and validation gates.

## Non-Goals

1. WAL direct I/O experiments.
2. Reworking WAL format or recovery model.
3. Broad cross-platform direct I/O support in phase 1.

## Current-State Notes

1. Disk access is centralized behind `FileSystemInterface`/`FileManager`, so integration is localized.
2. WAL and data writes already follow WAL-before-data ordering in buffer flush path.
3. Page-size assumptions must be feature-safe (`page-4k`/`page-8k`/`page-1m`).

## Target Policy

### File-class policy split

1. All database files are classified by file class (`Wal` or `Data`).
2. Data files:
   - Attempt direct I/O mode.
   - Fallback to buffered mode if unsupported.

### Runtime behavior

1. If direct open fails with unsupported-flag errors, degrade that file to buffered.
2. Emit a one-line fallback diagnostic when degradation occurs:
   - `[direct-io:fallback] file=<name> requested=direct effective=buffered reason="<io error>"`

## Migration Phases

## Phase 0: Baseline hardening

Goal: remove correctness footguns before enabling any direct-I/O path.

Required tasks:
1. Replace fixed-size array assumptions with feature-sized arrays in file I/O paths.
   - Example class of bug: `&[u8; 4096]` in `FileManager::read` must become `&[u8; PAGE_SIZE_BYTES as usize]`.
2. Verify every file read/write path uses exactly `PAGE_SIZE_BYTES`-sized buffers.
   - `read`, `write`, `read_raw`, `write_raw`, `append` in `FileManager`.
   - WAL read/write paths that use `read_raw`/`write_raw`.
3. Add/keep assertions that raw I/O buffers are page-sized where applicable.
4. Keep I/O behavior buffered in this phase (no direct-I/O behavior changes).

Exit criteria:
- No remaining hardcoded page-size constants in runtime file I/O code.

## Phase 1: I/O mode abstraction

1. Add internal file class + mode model:
   - `FileClass::{Wal, Data}`
   - `IoMode::{Buffered, Direct}`
2. Route open behavior through a single file-open helper that chooses mode by file class.
3. Enable direct mode by compile-time feature only (`direct-io`):
   - feature enabled: `Data -> Direct`, `Wal -> Buffered`
   - feature disabled: `Data -> Buffered`, `Wal -> Buffered`

Exit criteria:
- No behavioral change in default builds.
- Unit coverage for file-class classification.

## Phase 2: Direct I/O data path

1. For `IoMode::Direct` files, use page-size-aligned I/O buffers for read/write/read_raw/write_raw/append.
2. Ensure all offsets and lengths are multiples of page size.

Implementation notes:
1. Introduce `OpenFile { file: File, mode: IoMode }` and store it in `FileManager.open_files`.
2. Use file classification (`Wal` vs `Data`) when opening files:
   - `open_with_fallback(path, class) -> io::Result<OpenFile>`
   - For `Data`, try direct mode first, then fallback to buffered mode when direct mode is unsupported.
   - Prefer explicit file-class argument at call sites (WAL paths pass `Wal`, data paths pass `Data`).
3. `open_with_fallback` determines mode by return value (the returned `OpenFile.mode` reflects the mode that actually opened successfully); no mutable `FileClass` is needed.
4. Keep aligned page buffer allocation page-sized and page-aligned via `PAGE_SIZE_BYTES`:
   - `size = PAGE_SIZE_BYTES as usize`
   - `align = PAGE_SIZE_BYTES as usize`
5. Mode selection should be centralized in `desired_mode_for_class(class)` and keyed only on Cargo feature state:
   - feature `direct-io` enabled:
     - `Data -> IoMode::Direct`
     - `Wal -> IoMode::Buffered`
   - feature `direct-io` disabled:
     - `Data -> IoMode::Buffered`
     - `Wal -> IoMode::Buffered`
6. Keep raw file helpers in `FileManager` returning `io::Result<_>` and convert at existing trait boundaries where needed.
7. Linux-only support in phase 1:
   - direct-I/O code paths are compiled only on Linux targets.

Exit criteria:
- Data files can run in direct mode without `EINVAL` on supported Linux setups.

## Phase 3: Fallback + observability

1. Open-time fallback only:
   - If direct `open(..., O_DIRECT)` fails for a data file with unsupported direct-I/O errors, retry buffered mode.
   - Fallback-eligible errors apply to **open-time only** (`EINVAL`, `EOPNOTSUPP`, `ENOTSUP`).
   - Do **not** fallback on read/write `EINVAL`; treat those as implementation bugs (alignment/offset/length issues).
2. Emit one diagnostic line at fallback time:
   - `[direct-io:fallback] file=<name> requested=direct effective=buffered reason="<io error>"`
3. Store effective mode in `OpenFile.mode`; do not retry direct mode for that file handle.

Exit criteria:
- Process does not fail hard on unsupported FS/device; degrades safely.

## Phase 4: Validation and performance signoff

1. Run A/B performance comparison:
   - A: buffered mode
   - B: data-direct mode
2. Use existing benchmark tooling for deltas:
   - `scripts/run_all_benchmarks.py`
   - `scripts/compare_benchmarks.py`
3. Primary performance signal:
   - `io_patterns` Phase 1 workloads (`seq_read`, `seq_write`, `rand_read`, `rand_write`)
4. Record signoff metadata manually with the results:
   - OS/kernel, CPU, storage device/filesystem, page size, enabled features

Exit criteria:
- Performance impact documented and acceptable for target workloads.

## Rollout Strategy

1. Land behind an opt-in feature/config gate first.
2. Keep current buffered path as default for one stabilization cycle.
3. Promote to default only after benchmark + stability evidence.

## Risks and Mitigations

1. Risk: filesystem/device rejects direct I/O.
   - Mitigation: per-file fallback to buffered mode.

2. Risk: alignment bugs cause `EINVAL` or partial I/O issues.
   - Mitigation: strict aligned buffer helpers + invariant asserts.

3. Risk: hidden page-size assumptions break non-4k builds.
   - Mitigation: add compile/test checks for alternate page sizes early.

4. Risk: throughput regression for mixed workloads.
   - Mitigation: keep feature-gated rollout and benchmark before defaulting.
