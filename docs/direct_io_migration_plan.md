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

---

## Phase 4 Signoff — Benchmark Results

### Environment

| Property | Value |
|---|---|
| OS / kernel | Linux 6.8.0-100-generic |
| CPU | Intel Xeon E3-1275 v5 @ 3.60GHz |
| Storage / filesystem | ext4 on `/dev/md2` (RAID) |
| Page size | 4096 bytes (`page-4k` feature) |
| Buffer replacement | LRU (`replacement_lru` feature) |
| Buffered build flags | `--no-default-features --features replacement_lru --features page-4k` |
| Direct build flags | above + `--features direct-io` |
| Iterations | 50 (fsync-heavy phases capped at 5) |
| O_DIRECT fallback | None — O_DIRECT engaged on all data files |

### A/B Comparison

Base = buffered, PR = direct-io.

| Benchmark | Buffered | Direct | Change | Status |
|---|---|---|---:|---|
| Sequential Read (1000 blocks) | 5.05ms | 25.08ms | +396% | ⚠️ slower |
| Sequential Write (1000 blocks) | 4.39ms | 26.87ms | +512% | ⚠️ slower |
| Random Read (K=1000, 1000 ops) | 4.67ms | 93.01ms | +1893% | ⚠️ slower |
| Random Write (K=1000, 1000 ops) | 4.41ms | 26.46ms | +500% | ⚠️ slower |
| WAL append (no fsync) | 230.81ms | 227.22ms | -2% | ✅ |
| WAL append + immediate fsync | 892.31ms | 900.53ms | +1% | ✅ |
| WAL group commit (batch=10) | 1.11s | 1.09s | -2% | ✅ |
| WAL group commit (batch=50) | 402.51ms | 424.03ms | +5% | ✅ |
| WAL group commit (batch=100) | 310.10ms | 307.33ms | -1% | ✅ |
| Mixed 70/30R/W no-fsync | 44.09ms | 76.11ms | +73% | ⚠️ slower |
| Mixed 70/30R/W immediate-fsync | 1.37s | 1.34s | -2% | ✅ |
| Mixed 50/50R/W no-fsync | 68.80ms | 100.54ms | +46% | ⚠️ slower |
| Mixed 50/50R/W immediate-fsync | 2.22s | 2.53s | +14% | ⚠️ slower |
| Mixed 10/90R/W no-fsync | 110.95ms | 139.86ms | +26% | ⚠️ slower |
| Mixed 10/90R/W immediate-fsync | 3.93s | 4.11s | +5% | ✅ |
| Random Write durability (WAL fsync, no data fsync) | 8.83s | 9.14s | +3% | ✅ |
| Random Write durability (WAL fsync + data fsync) | 12.21s | 9.65s | -21% | 🚀 faster |

WAL benchmarks omitted from table body above; all WAL results were within ±5% (expected — WAL always runs in buffered mode regardless of feature).

### Analysis

**Why Phase 1 shows large regressions:**
The benchmark working set (1000 blocks × 4 KB = 4 MB) fits entirely in the OS page cache.
With buffered I/O, after the first iteration the entire working set is cache-resident and subsequent
reads are served from RAM at memory bandwidth speeds. With O_DIRECT the page cache is bypassed
on every iteration, so every read and write traverses the full I/O stack to the storage device.
The regression is an artifact of the micro-benchmark structure, not a signal about production behaviour.

**WAL is unaffected:** WAL always uses buffered I/O regardless of the `direct-io` feature.
All WAL-only benchmarks are within noise (±5%).

**Random Write durability with data fsync is 21% faster:** When data pages are fsynced after
every write, buffered I/O must write dirty pages through the page cache and then flush them to
disk (two passes through the I/O subsystem). O_DIRECT writes go directly to the device in one
pass, making the fsync effectively a no-op for already-durable data. This is the workload
profile where direct I/O is expected to provide a real benefit.

**Production expectation:** In a database with a working set larger than available RAM,
buffered-I/O hit rates drop and direct I/O becomes competitive or faster by eliminating
double-buffering overhead. This benchmark does not represent that regime.

### Signoff Decision

Direct I/O is **not yet suitable as the default** for this codebase:

- The current buffer pool is small (tunable via `num_buffers`); most reads bypass the pool
  and the page cache is doing effective caching work that direct I/O discards.
- Benefit materialises only when working sets exceed RAM or when data fsyncs are frequent.

**Recommended next steps before promoting to default:**

1. Run A/B with a working set larger than available RAM to observe the cache-pressure regime.
2. Evaluate buffer pool sizing — a larger pool reduces the advantage of OS caching and makes
   direct I/O more competitive.
3. Re-run after any buffer pool improvements to get an updated signal.

### Implementation Notes: Regime-Based Validation

To validate direct-I/O benefits under realistic memory pressure, benchmark each workload across
three working-set regimes, then compare buffered vs direct-io in each regime.

#### Regime definitions

- `cache-hot`: working set ~= `0.25 x RAM`
- `cache-pressure`: working set ~= `1.0 x RAM`
- `cache-thrashing`: working set ~= `2.0 x RAM`

#### Working-set sizing formula

Use page-size-aware sizing:

- `ram_bytes = host_total_memory_bytes`
- `page_bytes = PAGE_SIZE_BYTES`
- `target_bytes = ram_bytes * regime_ratio`
- `working_set_blocks = floor(target_bytes / page_bytes)`

#### Benchmark harness updates required

`benches/io_patterns.rs` should be parameterized so workloads no longer rely on hardcoded
working sets (`100`, `1000`, `10_000` blocks). Add either:

- `--working-set-blocks <N>` (preferred for reproducibility), or
- `--regime hot|pressure|thrash` (resolved to block count at runtime using RAM detection).

Recommended precedence:

1. If `--working-set-blocks` is provided, use it exactly.
2. Else if `--regime` is provided, derive `working_set_blocks` from RAM and page size.
3. Else keep current default sizes.

Rationale: `--regime` makes routine runs easy and machine-adaptive; `--working-set-blocks`
preserves exact reproducibility for comparisons and CI reruns.

Apply this to:

- Phase 1: `seq_read`, `seq_write`, `rand_read`, `rand_write`
- Mixed R/W workloads
- Random write durability workload

#### Execution matrix

For each selected workload, run:

1. `buffered x cache-hot`
2. `buffered x cache-pressure`
3. `buffered x cache-thrashing`
4. `direct-io x cache-hot`
5. `direct-io x cache-pressure`
6. `direct-io x cache-thrashing`

This yields `workload x 3 regimes x 2 modes`, with repeated iterations per cell for stable stats.

#### Run hygiene and reporting

- Use unique data filenames per run cell to avoid cross-run cache contamination.
- Capture and report: RAM size, computed `working_set_blocks`, page size feature, filesystem/device,
  and whether any direct-I/O fallback occurred.
- Report p50/p95 and standard deviation in addition to mean.

---

### Running the Regime Matrix

#### Prerequisites

- Rust toolchain with the project's required feature flags available.
- Python 3 (no third-party packages required).
- Run from the repository root.

#### Command

```bash
python3 scripts/run_regime_matrix.py [iterations] [output_dir]
```

| Argument | Default | Description |
|---|---|---|
| `iterations` | 50 | Iterations per benchmark cell (fsync-heavy workloads are capped at 5 internally) |
| `output_dir` | `regime_matrix_results` | Directory where JSON and comparison files are written |

Example:

```bash
python3 scripts/run_regime_matrix.py 50 results/regime_$(date +%Y%m%d)
```

#### What the script does

For each regime in `[hot, pressure, thrash]`:

1. Runs the buffered build of `io_patterns` and saves results to `<output_dir>/buffered_<regime>.json`.
2. Runs the direct-io build of `io_patterns` and saves results to `<output_dir>/direct_<regime>.json`.
3. Compares the pair with `compare_benchmarks.py` and writes `<output_dir>/compare_<regime>.md`.

After all regimes, prints an aggregate summary (faster / neutral / slower counts per regime).

#### Output files

| File | Contents |
|---|---|
| `buffered_<regime>.json` | Raw benchmark results for buffered mode in that regime |
| `direct_<regime>.json` | Raw benchmark results for direct-io mode in that regime |
| `compare_<regime>.md` | Markdown comparison table with mean, p50, p95, std_dev columns |

#### Runtime estimate

Working-set sizes are derived from total RAM at runtime (Linux: `/proc/meminfo`).
On this machine (62.6 GiB RAM, ext4 on `/dev/md2`):

| Regime | Working set | Approx. runtime per cell (50 iters) |
|---|---|---|
| `hot` | ~15.6 GiB (0.25 × RAM) | several minutes |
| `pressure` | ~62.6 GiB (1.0 × RAM) | long — requires evicting page cache |
| `thrash` | ~125 GiB (2.0 × RAM) | very long — exceeds RAM, heavy swap/I/O |

> **Note:** The `thrash` regime may take an extremely long time or OOM on machines where
> `2 × RAM` worth of blocks cannot be pre-created. Consider using `--working-set-blocks`
> to set an explicit cap.

#### Running a single cell manually

To run one (regime, mode) pair outside the matrix script:

```bash
# Buffered, hot regime
cargo bench --bench io_patterns \
  --no-default-features --features replacement_lru --features page-4k \
  -- 50 12 --regime hot --json > buffered_hot.json

# Direct-io, hot regime
cargo bench --bench io_patterns \
  --no-default-features --features replacement_lru --features page-4k --features direct-io \
  -- 50 12 --regime hot --json > direct_hot.json

# Compare
python3 scripts/compare_benchmarks.py buffered_hot.json direct_hot.json compare_hot.md
```

Use `--working-set-blocks <N>` instead of `--regime` for a reproducible fixed size:

```bash
cargo bench --bench io_patterns \
  --no-default-features --features replacement_lru --features page-4k \
  -- 10 12 --working-set-blocks 500000
```

Optional knobs to keep runtime bounded while using large working sets:

- `--phase1-ops <N>`: operation count for Phase 1 (`seq_*`, `rand_*`)
- `--mixed-ops <N>`: operation count for mixed R/W phase
- `--durability-ops <N>`: operation count for durability phase

Example:

```bash
cargo bench --bench io_patterns \
  --no-default-features --features replacement_lru --features page-4k --features direct-io \
  -- 10 12 --regime pressure --phase1-ops 20000 --mixed-ops 10000 --durability-ops 5000 --json
```

#### Interpreting results

- **hot regime:** working set fits in page cache. Buffered I/O benefits from cache warmth across
  iterations; direct-io hits storage every time. Expect buffered to win.
- **pressure regime:** working set ~= RAM. Cache hit rate degrades; the gap between modes narrows.
- **thrash regime:** working set exceeds RAM. Page cache provides no benefit; direct-io eliminates
  double-buffering overhead. Expect direct-io to be competitive or faster.

Direct-io fallback status is printed at the end of each human-readable run. If any file fell back
to buffered mode, a `[direct-io:fallback]` diagnostic is emitted to stderr at open time.
