## To-do list

### Transactions
1. Implement a deadlock detection strategy (either wait-for or wait-die)

### Storage
1. Store a bitmap for presence checking
2. Store an ID table to manage offsets so that its easier to support variable length strings (similar to B-tree pages)


### 📝 Converting “manual pin/unpin” to an RAII Buffer Guard  

Note: [Another conversation](https://claude.ai/chat/60c71699-fd13-48dc-ae25-34cc0c5d5eb3) with Claude about how to do this well

---

#### 0 · Goal
Replace every `pin(&blk)` / `unpin(&blk)` pair with an object that:
* pins in its constructor,
* un‑pins in `Drop`,
* carries the `Arc<Mutex<Buffer>>` for safe page access,  
  eliminating double‑unpin and forgotten unpins.

---

#### 1 · Add the guard type (transaction layer)

```rust
// transaction/buffer_guard.rs
pub struct BufferGuard<'a> {
    bm:   &'a BufferManager,   // ONLY buffer pool reference
    blk:  BlockId,
    pub buf: Arc<Mutex<Buffer>>,   // expose if callers need it
}

impl<'a> Drop for BufferGuard<'a> {
    fn drop(&mut self) { self.bm.unpin(&self.blk); }
}
```

---

#### 2 · Expose `Transaction::pin` that returns the guard

```rust
impl Transaction {
    pub fn pin<'a>(&'a self, blk: &BlockId) -> BufferGuard<'a> {
        let bm  = self.buffer_manager.lock().unwrap();
        let buf = bm.pin(blk);                 // existing method, bumps pin‑cnt
        BufferGuard { bm: &*bm, blk: blk.clone(), buf }
    }
}
```

*Remove the old `fn unpin(&self, blk: &BlockId)` from `Transaction`.*

---

#### 3 · Replace call‑sites

| Before | After |
|--------|-------|
| `tx.pin(&blk); /*…*/ tx.unpin(&blk);` | `let guard = tx.pin(&blk); /* use guard.buf */` |

> **Tip**: a 2‑pass `sed` / search‑replace:  
> 1. replace `tx.pin(` with `let _g = tx.pin(`  
> 2. delete `tx.unpin(` lines.

---

#### 4 · RecordPage tweaks (optional)

*Change constructor to accept `&Arc<Mutex<Buffer>>` instead of pinning itself;*  
if you prefer minimal churn, leave it unchanged—double‑pin is harmless.

---

#### 5 · TableScan integration

```rust
pub struct TableScan<'tx> {
    guard: BufferGuard<'tx>,              // keeps page pinned
    page:  RecordPage<'tx>,
    /* rest unchanged */
}

fn jump_to_block(&mut self, n: usize) {
    let blk = BlockId::new(self.file.clone(), n);
    self.guard = self.tx.pin(&blk);       // old guard drops ⇒ old page unpinned
    self.page  = RecordPage::new(&self.guard.buf, self.layout.clone());
}
```

(Covers `move_to_block`, `move_to_new_block`, `move_to_row_id`.)

---

#### 6 · Delete every `close()` used only for unpinning
* Traits `Scan` / `UpdateScan`  
* Concrete scans’ noop impls  
* Each lingering call site (compiler will flag them).

---

#### 7 · Run tests  
All “unpin of non‑pinned buffer” assertions should vanish; no new leaks.

---

##### Return later
* Split read‑ vs write‑guards if you want mutability discipline.  
* Push pin‑logic fully into BufferManager (`pin_handle()` variant) for even cleaner layering.

---


### BTree
Support range scans for BTree