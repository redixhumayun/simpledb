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


### Iterator Design Overhaul: Value-Based vs Zero-Copy Scans

Currently, the `Scan` trait mixes iteration, data access, and cursor control into one interface:

```rust
trait Scan: Iterator<Item = Result<(), Box<dyn Error>>> {
    fn next(&mut self) -> Option<Result<(), Box<dyn Error>>>;  // Navigation
    fn get_int(&self, field: &str) -> Result<i32, Box<dyn Error>>;  // Data access
    fn before_first(&mut self) -> Result<(), Box<dyn Error>>;  // Cursor control
    fn close(&mut self);  // Resource management
}
```

**Problems:**
- `next()` returns `()` - no actual data, just cursor positioning
- Data access depends on hidden iterator state
- Cannot pass records to other functions
- Violates single responsibility principle

This involves two independent design decisions:

1. **Trait separation**: Split mixed concerns into separate traits
2. **Data ownership**: Whether records are cloned or zero-copy references

**Trait separation should be done regardless of which data ownership approach is chosen**, as it improves code organization and testability.

Two data ownership approaches to consider:

---

#### Option 1: Value-Based Iterator

Replace stateful data access with records that own their data:

```rust
#[derive(Debug, Clone)]
struct Record {
    values: HashMap<String, Constant>,
}

impl Record {
    fn get_int(&self, field: &str) -> Result<i32, DatabaseError> { /* ... */ }
    fn get_string(&self, field: &str) -> Result<String, DatabaseError> { /* ... */ }
}

trait Scan: Iterator<Item = Result<Record, DatabaseError>> {
    fn close(&mut self);
}
```

**Usage becomes intuitive:**
```rust
for record in scan {
    let record = record?;
    let name = record.get_string("name")?;
    process_record(&record);  // Records can be passed around!
}
```

**Pros:**
- Clean, obvious API - `next()` actually returns data
- Records are self-contained - no hidden state dependencies  
- Easy to test, debug, and compose
- Simple lifetime management

**Cons:**
- **Memory overhead**: Every string gets cloned, primitives get copied
- **Allocation cost**: New `HashMap` per record
- Performance impact for large result sets

---

#### Option 2: Zero-Copy Iterator

Use lifetime-bound views that reference original page data:

```rust
struct RecordView<'a> {
    page: &'a RecordPage,
    slot: usize,
    layout: &'a Layout,
}

impl<'a> RecordView<'a> {
    fn get_int(&self, field: &str) -> Result<i32, DatabaseError> {
        let offset = self.layout.offset(field)?;
        Ok(self.page.get_int(self.slot, offset))  // Direct page access
    }
}

trait Scan {
    type Record<'a> where Self: 'a;
    fn next(&mut self) -> Option<Result<Self::Record<'_>, DatabaseError>>;
}
```

**Pros:**
- **Zero allocation**: No copying of strings or primitives
- **Maximum performance**: Data accessed directly from pages
- **Memory efficient**: References existing buffer pool data

**Cons:**
- **Complex lifetimes**: Views tied to scan lifetime
- **Cannot store records**: Views become invalid when scan advances
- **Harder to compose**: Lifetime propagation through query operators

---

#### Implementation Strategy

**Gradual Migration:**
1. Add `Record` type alongside current trait
2. Implement `RecordScan` trait for new operators
3. Convert existing operators one by one
4. Remove old `Scan` trait when complete

**Recommendation for SimpleDB:**
Start with **Option 1 (Value-Based)** for its clean API and educational value. The allocation cost is acceptable for a pedagogical database, and the cleaner design makes the codebase easier to understand and extend.

Consider **Option 2 (Zero-Copy)** only if performance profiling reveals record allocation as a bottleneck, or as an advanced exercise in lifetime management.

---

### BTree
Support range scans for BTree


