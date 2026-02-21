# FileManager Positional I/O Migration Plan (Layer 1)

## Context

This is the prerequisite to all async/queue-depth work described in
`async_io_and_queue_depth_plan.md`. It moves `FileManager` from quadrant A
(global lock, sync/blocking) to quadrant C (no hot lock, sync/blocking) in the
concurrency matrix, enabling multiple threads to have I/O requests in-flight
simultaneously.

## Problem

`SharedFS` is currently `Arc<Mutex<Box<dyn FileSystemInterface + Send>>>`. Every
`read` and `write` call acquires the outer Mutex and holds it across the full
`seek → read/write` sequence — the entire NVMe round-trip (~70-100µs for 4K
random on PM9A1). From the storage device's perspective this is always QD=1
regardless of thread count.

Even switching to `read_at`/`write_at` internally does not help as long as the
outer Mutex is held across the call. The Mutex must be released before the
blocking syscall.

## Goal

Release the outer lock before the I/O syscall. Allow N concurrent threads to
have N requests in-flight simultaneously (QD=N from the device perspective).

## New SharedFS type

```rust
// Before
type SharedFS = Arc<Mutex<Box<dyn FileSystemInterface + Send + 'static>>>;

// After
type SharedFS = Arc<dyn FileSystemInterface + Send + Sync + 'static>;
```

The outer `Mutex` is removed entirely. Locking moves inside `FileManager`.

## Trait signature changes

All methods that currently take `&mut self` become `&self`. Interior mutability
is the responsibility of each implementor.

```rust
pub trait FileSystemInterface: Send + Sync {
    fn block_size(&self) -> usize;
    fn length(&self, filename: String) -> usize;
    fn read(&self, block_id: &BlockId, page: &mut Page);
    fn write(&self, block_id: &BlockId, page: &Page);
    fn read_raw(&self, block_id: &BlockId, buf: &mut [u8]);
    fn write_raw(&self, block_id: &BlockId, buf: &[u8]);
    fn append(&self, filename: String) -> BlockId;
    fn sync(&self, filename: &str);
    fn sync_directory(&self);
}
```

## FileManager internal structure

Two levels of locking replace the single outer Mutex:

```rust
struct FileManager {
    db_directory: PathBuf,
    open_files: Mutex<HashMap<String, Arc<ManagedFile>>>,  // brief: lookup/insert only
    directory_fd: File,
}

struct ManagedFile {
    file: File,
    mode: IoMode,
    append_mu: Mutex<()>,  // held across length→write_at for append atomicity
    #[cfg(target_os = "linux")]
    // no scratch field here — see scratch buffer section below
}
```

### read/write flow

```
read:
  lock(open_files) → get Arc<ManagedFile> → unlock
  read_at(fd, buf, offset)                 ← no lock held during syscall

write:
  lock(open_files) → get Arc<ManagedFile> → unlock
  write_at(fd, buf, offset)                ← no lock held during syscall

append:
  lock(open_files) → get Arc<ManagedFile> → unlock
  lock(managed_file.append_mu)
  length = file.seek(End(0))               ← or metadata().len()
  write_at(fd, zeros, length_offset)
  unlock(managed_file.append_mu)
```

`append` is the only method that still requires serialization — it is an
inherently stateful sequence (query length → write at that offset) and two
concurrent appenders would corrupt the file without the per-file lock.

## Scratch buffer for direct I/O

Direct I/O reads and writes require a 4K-aligned intermediate buffer. Currently
this is `OpenFile.scratch: Option<AlignedBuf>` — one buffer per open file,
protected by the outer Mutex. With `&self` and concurrent readers, a per-file
buffer would need its own lock and would reintroduce serialization for
same-file concurrent reads.

**Decision: `thread_local! { AlignedBuf }`**

One aligned buffer per thread, reused across calls, zero contention.

Rejected alternatives:
- `Mutex<AlignedBuf>` per file — serializes concurrent reads on the same file,
  defeating the purpose of this migration.
- Allocate per call — 4K aligned allocation on every direct I/O operation adds
  overhead that obscures the storage benefit we are trying to measure.

## MockFileManager

`MockFileManager` is test-only. The trait now requires `&self`, so its internal
`HashMap` must be wrapped in a `Mutex`. No performance constraint applies.

```rust
struct MockFileManager {
    inner: Mutex<MockInner>,
}

struct MockInner {
    files: HashMap<String, MockFile>,
    directory_synced: bool,
    crashed: bool,
}
```

## Call site simplification

Every caller currently acquires the outer lock before calling into the FM:

```rust
// Before
self.file_manager.lock().unwrap().read(block_id, page);
```

After this migration, the lock is gone at every call site:

```rust
// After
self.file_manager.read(block_id, page);
```

Affected: `LogManager`, `Transaction`, `BufferFrame`, all benchmarks.
`LogManager` and `Transaction` also hold `Arc<Mutex<LogManager>>` separately —
that is unrelated and unchanged.

## Implementation steps

Three steps, each independently testable:

### Step 1 — Positional I/O internally, trait unchanged

- Change `OpenFile::read_page` and `write_page` to accept an explicit `offset`
  parameter and use `FileExt::read_at` / `write_at`.
- Move direct I/O scratch buffer from `OpenFile` field to `thread_local!`.
- `FileManager` still has `&mut self` on trait methods; outer Mutex still exists.
- **Verify**: `cargo test` passes. No observable behavior change.

### Step 2 — Trait to `&self`, two-level locking in FileManager

- Change all `FileSystemInterface` methods to `&self`.
- Restructure `FileManager`: `open_files` becomes
  `Mutex<HashMap<String, Arc<ManagedFile>>>`. Add `append_mu: Mutex<()>` to
  `ManagedFile`.
- Wrap `MockFileManager` internals in `Mutex<MockInner>`.
- `SharedFS` type alias changes; outer `Mutex` removed.
- **Verify**: `cargo test` passes.

### Step 3 — Call site cleanup

- Remove all `self.file_manager.lock().unwrap()` chains at call sites.
- Update benchmarks (`io_patterns.rs`, `buffer_pool.rs`) similarly.
- **Verify**: `cargo test` + `cargo clippy` pass.

## Expected benchmark impact

- Single-threaded benchmarks (`sequential_read`, `random_read`, etc.): **no
  improvement**. One thread, no contention. `read_at` vs `seek+read` is
  equivalent in throughput.
- Concurrent benchmarks (`concurrent_io_shared`, `concurrent_io_sharded`,
  `multi_stream_scan`): **meaningful improvement for direct I/O**, where there
  is no page cache to absorb misses. Threads will actually reach the NVMe
  simultaneously.
- Buffered I/O concurrent benchmarks: minimal improvement. The OS page cache
  absorbs most reads; lock hold time was already near-zero for cache hits.

Use `--features direct-io` with `--concurrent-ops 1000` and `--regime thrash`
to observe the improvement signal clearly.
