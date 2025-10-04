# Type System and Serialization Format

## Overview

This document outlines the design and implementation of an expanded type system for SimpleDB, including serialization formats for each type, NULL value handling, and the relationship between type-level and page-level structures.

## Motivation

Currently, SimpleDB supports only two types:
- `INT` (i32, 4 bytes)
- `VARCHAR(n)` (variable-length string with maximum size)

This limitation prevents storing common data types like booleans, floating-point numbers, dates, and binary data. Expanding the type system is essential for SimpleDB to handle real-world data while maintaining its pedagogical clarity.

**Goals:**
1. Add fixed set of common types (no user-defined types)
2. Define clear serialization formats for each type
3. Implement NULL value support
4. Maintain fixed-length record slots for simplicity
5. Ensure cross-platform compatibility

---

## Current Type System Architecture

### Type Flow

```
SQL QUERY → PARSER → SCHEMA → FIELDTYPE → CONSTANT → PAGE
```

**1. Parser** (parser.rs:180-195)
```rust
fn field_def(&mut self) -> Result<Schema, ParserError> {
    let field_name = self.lexer.eat_identifier()?;
    if self.lexer.match_keyword("int") {
        schema.add_int_field(&field_name);
    } else if self.lexer.match_keyword("varchar") {
        let size = self.lexer.eat_int_constant()?;
        schema.add_string_field(&field_name, size as usize);
    }
}
```

**2. FieldType Enum** (main.rs:8417)
```rust
enum FieldType {
    Int = 0,
    String = 1,
}

impl From<i32> for FieldType {
    fn from(value: i32) -> Self {
        match value {
            0 => FieldType::Int,
            1 => FieldType::String,
            _ => panic!("Invalid field type"),
        }
    }
}
```

**3. Constant Enum** (main.rs:8058) - Runtime values
```rust
pub enum Constant {
    Int(i32),
    String(String),
}
```

**4. Page Serialization** (main.rs:10860)
```rust
impl Page {
    const INT_BYTES: usize = 4;

    fn get_int(&self, offset: usize) -> i32 {
        let bytes = self.contents[offset..offset + 4];
        i32::from_be_bytes(bytes)
    }

    fn get_string(&self, offset: usize) -> String {
        let len = u32::from_be_bytes(...);
        let bytes = self.contents[...];
        String::from_utf8(bytes).unwrap()
    }
}
```

### Current Record Layout

```
CURRENT RECORD (INT, VARCHAR(20), INT)
═══════════════════════════════════════════════════════════════════

┌──────┬──────┬────────────┬──────┐
│ Flag │  id  │    name    │ age  │
│  4B  │  4B  │  4B + 20B  │  4B  │
└──────┴──────┴────────────┴──────┘
Total: 36 bytes

String format: [4-byte length][up to 20 bytes UTF-8 data]
Problem: "Hi" (2 bytes) still reserves full 20 bytes!
```

---

## Proposed Type System

### Supported Types

```
TYPE CATALOG
═══════════════════════════════════════════════════════════════════

Type            Size      Serialization               Range/Precision
──────────────────────────────────────────────────────────────────
BOOLEAN         1 byte    0x00=false, 0x01=true      true/false
INT             4 bytes   i32 big-endian             ±2.1 billion
BIGINT          8 bytes   i64 big-endian             ±9.2 quintillion
FLOAT           8 bytes   f64 IEEE 754 big-endian    ±1.7e±308
DECIMAL(p,s)    8 bytes   i64 fixed-point            Exact, scale in schema
DATE            4 bytes   i32 days since 1970-01-01  ±5.8 million years
TIMESTAMP       8 bytes   i64 μs since epoch         ±292,471 years
VARCHAR(n)      4+n bytes [4B length][n bytes UTF-8] Up to n characters
BLOB(n)         4+n bytes [4B length][n bytes data]  Up to n bytes
──────────────────────────────────────────────────────────────────
```

### FieldType Enum Expansion

```rust
enum FieldType {
    Int = 0,
    String = 1,
    Boolean = 2,
    BigInt = 3,
    Float = 4,
    Decimal = 5,
    Date = 6,
    Timestamp = 7,
    Blob = 8,
}

impl From<i32> for FieldType {
    fn from(value: i32) -> Self {
        match value {
            0 => FieldType::Int,
            1 => FieldType::String,
            2 => FieldType::Boolean,
            3 => FieldType::BigInt,
            4 => FieldType::Float,
            5 => FieldType::Decimal,
            6 => FieldType::Date,
            7 => FieldType::Timestamp,
            8 => FieldType::Blob,
            _ => panic!("Invalid field type: {}", value),
        }
    }
}
```

### Constant Enum Expansion

```rust
pub enum Constant {
    Int(i32),
    String(String),
    Boolean(bool),
    BigInt(i64),
    Float(f64),
    Decimal(i64),        // Fixed-point with scale in schema
    Date(i32),           // Days since epoch
    Timestamp(i64),      // Microseconds since epoch
    Blob(Vec<u8>),
    Null,                // NEW: Explicit NULL value
}

impl Constant {
    fn as_int(&self) -> i32 {
        match self {
            Constant::Int(v) => *v,
            Constant::Null => panic!("NULL value"),
            _ => panic!("Expected Int"),
        }
    }

    // ... similar accessors for each type

    fn is_null(&self) -> bool {
        matches!(self, Constant::Null)
    }
}
```

---

## Serialization Formats

### 1. Boolean (1 byte)

```
LAYOUT
┌──────┐
│ bool │
└──────┘
1 byte

Values:
  0x00 = false
  0x01 = true

Implementation:
fn get_bool(&self, offset: usize) -> bool {
    self.contents[offset] != 0
}

fn set_bool(&mut self, offset: usize, b: bool) {
    self.contents[offset] = if b { 1 } else { 0 };
}
```

**Alternative considered:** Bit packing (8 bools in 1 byte)
- ✓ 8x space savings
- ✗ Bit manipulation complexity
- ✗ Read-modify-write for single bool update
- **Decision:** Use full byte for simplicity

---

### 2. Integer Types

```
INT (i32) - 4 bytes
┌────────────────┐
│  i32 (4 bytes) │
└────────────────┘

BIGINT (i64) - 8 bytes
┌────────────────┐
│  i64 (8 bytes) │
└────────────────┘

Format: Big-endian (network byte order)

Example: 305,419,896 (0x12345678)
Big-endian bytes: [0x12, 0x34, 0x56, 0x78]

Implementation:
fn get_int(&self, offset: usize) -> i32 {
    let bytes: [u8; 4] = self.contents[offset..offset+4]
        .try_into().unwrap();
    i32::from_be_bytes(bytes)
}

fn get_bigint(&self, offset: usize) -> i64 {
    let bytes: [u8; 8] = self.contents[offset..offset+8]
        .try_into().unwrap();
    i64::from_be_bytes(bytes)
}
```

**Why big-endian?**
- ✓ Network byte order standard
- ✓ Cross-platform consistency
- ✓ Database files portable between machines
- ✓ Easier debugging (reads left-to-right)

---

### 3. Floating-Point (8 bytes)

```
FLOAT (f64) - 8 bytes
┌──────┬────────────┬──────────────────────────┐
│ Sign │  Exponent  │       Mantissa           │
│  1b  │    11b     │          52b             │
└──────┴────────────┴──────────────────────────┘

IEEE 754 double-precision format

Implementation:
fn get_float(&self, offset: usize) -> f64 {
    let bytes: [u8; 8] = self.contents[offset..offset+8]
        .try_into().unwrap();
    f64::from_be_bytes(bytes)
}

fn set_float(&mut self, offset: usize, f: f64) {
    self.contents[offset..offset+8]
        .copy_from_slice(&f.to_be_bytes());
}
```

**Important considerations:**
- NaN and Infinity are valid IEEE 754 values
- Use `f64::total_cmp()` for comparisons (handles NaN correctly)
- No alternative to IEEE 754 (universal standard)

---

### 4. Decimal (Fixed-Point, 8 bytes)

```
DECIMAL(p, s) stored as i64
p = precision (total digits)
s = scale (digits after decimal point)

Example: DECIMAL(10, 2)
Value: 1234.56
Storage: 123456 (i64)
Scale: 2 (stored in schema metadata, not per-value)

┌─────────────────────┐
│  i64 (8 bytes)      │
│  Value × 10^scale   │
└─────────────────────┘

Implementation:
fn get_decimal(&self, offset: usize, scale: u32) -> Decimal {
    let raw = self.get_bigint(offset);
    Decimal { value: raw, scale }
}

// Arithmetic example:
// 1234.56 (scale=2) + 78.90 (scale=2)
// = 123456 + 7890 = 131346
// = 1313.46 (scale=2)
```

**Why fixed-point over arbitrary precision?**
- ✓ Fast integer arithmetic
- ✓ Exact precision
- ✓ Compact (8 bytes)
- ✓ Simple implementation
- ✗ Fixed precision per column (acceptable for pedagogical DB)

---

### 5. Date (4 bytes)

```
DATE - Days since 1970-01-01 (Unix epoch)
┌──────────────────────┐
│  i32 (4 bytes)       │
│  Days since epoch    │
└──────────────────────┘

Range: ±5.8 million years from epoch
Precision: 1 day

Example: 2024-01-15
= 19,737 days since 1970-01-01

Implementation:
fn get_date(&self, offset: usize) -> NaiveDate {
    let days = self.get_int(offset);
    NaiveDate::from_num_days_from_ce(EPOCH_DAYS + days)
}

fn set_date(&mut self, offset: usize, date: &NaiveDate) {
    let days = date.num_days_from_ce() - EPOCH_DAYS;
    self.set_int(offset, days);
}
```

**Arithmetic examples:**
- Date difference: 19,737 - 19,700 = 37 days
- Add days: 19,737 + 7 = 19,744 (one week later)

---

### 6. Timestamp (8 bytes)

```
TIMESTAMP - Microseconds since 1970-01-01 00:00:00 UTC
┌─────────────────────────────┐
│  i64 (8 bytes)              │
│  Microseconds since epoch   │
└─────────────────────────────┘

Range: ±292,471 years from epoch
Precision: 1 microsecond

Example: 2024-01-15 14:30:45.123456
= 1,705,329,045,123,456 microseconds since epoch

Implementation:
fn get_timestamp(&self, offset: usize) -> DateTime<Utc> {
    let micros = self.get_bigint(offset);
    let secs = micros / 1_000_000;
    let nanos = (micros % 1_000_000) * 1000;
    DateTime::from_timestamp(secs, nanos as u32).unwrap()
}
```

**Why microseconds instead of seconds?**
- ✓ High precision for timestamps
- ✓ Matches PostgreSQL TIMESTAMP
- ✓ Still simple integer arithmetic
- ✓ Sufficient range for practical use

---

### 7. VARCHAR (Variable, 4 + n bytes)

```
VARCHAR(n) - Length-prefixed UTF-8 string
┌──────────────┬────────────────────────┐
│ Length (4B)  │  UTF-8 data (up to n)  │
└──────────────┴────────────────────────┘

Current implementation (UNCHANGED)

Example: VARCHAR(20) storing "Alice"
┌──────────┬───────────────┬───────────────────┐
│ 0x000005 │  Alice (5B)   │  Padding (15B)    │
└──────────┴───────────────┴───────────────────┘

Total: 4 + 20 = 24 bytes (reserves full 20 even for short strings)
```

**Trade-off:**
- ✗ Wastes space on short strings
- ✓ Fixed-length slots (simple implementation)
- ✓ In-place updates (if new value fits)

**Future improvement:** With Page redesign (#18), can use ID table for true variable-length strings

---

### 8. BLOB (Binary Large Object, 4 + n bytes)

```
BLOB(n) - Length-prefixed binary data
┌──────────────┬────────────────────────┐
│ Length (4B)  │  Binary data (up to n) │
└──────────────┴────────────────────────┘

Identical to VARCHAR, but:
  • No UTF-8 validation
  • Stores arbitrary binary data
  • Same length-prefix format

Implementation:
fn get_blob(&self, offset: usize) -> Vec<u8> {
    self.get_bytes(offset)  // Reuse existing implementation!
}

fn set_blob(&mut self, offset: usize, data: &[u8]) {
    self.set_bytes(offset, data)  // Reuse existing implementation!
}
```

---

## NULL Value Handling

### The Problem

Currently, SimpleDB has **no NULL support**. This is a critical limitation.

**What NOT to do: Sentinel values**
```
✗ INT NULL:    i32::MIN (-2,147,483,648)
✗ STRING NULL: length = 0xFFFFFFFF
✗ BOOL NULL:   0xFF
✗ FLOAT NULL:  NaN

Problems:
  • i32::MIN is a valid integer!
  • Empty string ≠ NULL string
  • Can't distinguish NULL from actual value
```

### Solution: NULL Bitmap

Add a bitmap at the start of each record showing which fields are NULL.

```
RECORD WITH NULL BITMAP
═══════════════════════════════════════════════════════════════════

Schema: (id INT, name VARCHAR(20), salary DECIMAL(10,2), hired DATE)
4 fields → 4 bits → 1 byte bitmap

┌──────┬──────────┬──────┬────────────┬─────────┬───────┐
│ Flag │ NULL Map │  id  │    name    │ salary  │ hired │
│  4B  │   1B     │  4B  │  4B+20B    │   8B    │  4B   │
└──────┴──────────┴──────┴────────────┴─────────┴───────┘
   0       4         5         9          33        41

NULL Map (1 byte for 4 fields):
┌─────────────────────────────────────┐
│ bit 7-4 │ bit 3  │ bit 2  │bit 1│bit0│
│ unused  │ hired  │ salary │name │ id │
└─────────────────────────────────────┘

Example: If salary is NULL, bit 2 = 1
Bitmap byte = 0b00000100 = 0x04
```

**Size overhead:**
- 1 byte for up to 8 fields
- 2 bytes for 9-16 fields
- ⌈n/8⌉ bytes for n fields

**Implementation:**
```rust
struct NullBitmap {
    bytes: Vec<u8>,
}

impl NullBitmap {
    fn is_null(&self, field_index: usize) -> bool {
        let byte_index = field_index / 8;
        let bit_index = field_index % 8;
        (self.bytes[byte_index] & (1 << bit_index)) != 0
    }

    fn set_null(&mut self, field_index: usize) {
        let byte_index = field_index / 8;
        let bit_index = field_index % 8;
        self.bytes[byte_index] |= 1 << bit_index;
    }

    fn clear_null(&mut self, field_index: usize) {
        let byte_index = field_index / 8;
        let bit_index = field_index % 8;
        self.bytes[byte_index] &= !(1 << bit_index);
    }
}
```

**Record access with NULL checking:**
```rust
impl TableScan {
    fn get_value(&self, field_name: &str) -> Result<Constant, Error> {
        let field_index = self.layout.field_index(field_name)?;

        // Check NULL bitmap first
        if self.is_null(field_index) {
            return Ok(Constant::Null);
        }

        // Not NULL, read actual value
        match self.layout.field_type(field_name)? {
            FieldType::Int => Ok(Constant::Int(self.get_int(field_name)?)),
            FieldType::Boolean => Ok(Constant::Boolean(self.get_bool(field_name)?)),
            // ... etc
        }
    }
}
```

---

## Complete Record Layout

### Example Schema

```sql
CREATE TABLE employees (
    id INT,
    name VARCHAR(50),
    active BOOLEAN,
    salary DECIMAL(10, 2),
    hired DATE,
    photo BLOB(1000)
)
```

### Record Layout with All Features

```
COMPLETE RECORD LAYOUT
═══════════════════════════════════════════════════════════════════

┌──────┬──────┬──────┬────────┬──────┬────────┬──────┬──────────┐
│ Flag │ NULL │  id  │  name  │active│ salary │hired │  photo   │
│  4B  │  1B  │  4B  │ 4+50B  │  1B  │   8B   │  4B  │ 4+1000B  │
└──────┴──────┴──────┴────────┴──────┴────────┴──────┴──────────┘
   0     4      5      9        63     64       72     76

Total slot_size = 1080 bytes

Offsets:
  Flag:    0 (slot presence marker)
  NULL:    4 (NULL bitmap, 1 byte for 6 fields)
  id:      5 (INT, 4 bytes)
  name:    9 (VARCHAR(50), 4 + 50 bytes)
  active:  63 (BOOLEAN, 1 byte)
  salary:  64 (DECIMAL, 8 bytes)
  hired:   72 (DATE, 4 bytes)
  photo:   76 (BLOB(1000), 4 + 1000 bytes)
```

### Layout Calculation Logic

```rust
impl Layout {
    fn new(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut offset = Page::INT_BYTES;  // Start after slot flag (4B)

        // Calculate NULL bitmap size
        let null_bitmap_size = (schema.fields.len() + 7) / 8;  // ⌈n/8⌉
        offset += null_bitmap_size;

        // Calculate field offsets
        for field in schema.fields.iter() {
            let field_info = schema.info.get(field).unwrap();
            offsets.insert(field.clone(), offset);

            match field_info.field_type {
                FieldType::Boolean => offset += 1,
                FieldType::Int | FieldType::Date => offset += 4,
                FieldType::BigInt | FieldType::Float
                    | FieldType::Decimal | FieldType::Timestamp => offset += 8,
                FieldType::String | FieldType::Blob =>
                    offset += Page::INT_BYTES + field_info.length,
            }
        }

        Self {
            schema,
            offsets,
            slot_size: offset,
        }
    }
}
```

---

## Memory Alignment Considerations

### Why Alignment Matters

Modern CPUs prefer aligned memory access:
- i32/f32: 4-byte aligned (address % 4 == 0)
- i64/f64: 8-byte aligned (address % 8 == 0)

Unaligned access = slower (or crashes on some architectures)

### Field Ordering Strategy

```
BAD: Unaligned layout
┌──────┬──────┬──────┬──────────┬──────────┐
│ Flag │ NULL │ bool │  i64     │  i32     │
│  4B  │  1B  │  1B  │   8B     │   4B     │
└──────┴──────┴──────┴──────────┴──────────┘
   0     4      5      6          14
                       ↑ i64 at offset 6 (not 8-byte aligned!)


GOOD: Aligned layout with padding
┌──────┬──────┬─────┬──────────┬──────────┬──────┐
│ Flag │ NULL │ PAD │  i64     │  i32     │ bool │
│  4B  │  1B  │ 3B  │   8B     │   4B     │  1B  │
└──────┴──────┴─────┴──────────┴──────────┴──────┘
   0     4      5     8          16         20
                      ↑ i64 at offset 8 (aligned!)
```

**Recommended field ordering:**
1. Slot flag (4B)
2. NULL bitmap (1-2B + padding to 4B boundary)
3. 8-byte fields (i64, f64, timestamp, decimal)
4. 4-byte fields (i32, date)
5. 1-byte fields (boolean)
6. Variable-length fields (varchar, blob)

This minimizes padding and ensures natural alignment.

---

## Relationship with Page Format Redesign

The type system and Page format redesign (Issue #18) are **complementary**:

### Two Levels of Structure

```
PAGE LEVEL (Issue #18)                  RECORD LEVEL (This doc)
═══════════════════════════             ═══════════════════════

┌─────────────────────────┐              ┌────────────────┐
│ Page Header             │              │ Record Flag    │
│ - slot_count            │              │ Record NULL map│
│ - free_ptr              │              │ Field data     │
├─────────────────────────┤              └────────────────┘
│ Page Bitmap (slots)     │                      ↑
├─────────────────────────┤                      │
│ ID Table (offsets)      │              Points to records
├─────────────────────────┤                      │
│ Record Space            │──────────────────────┘
│   Record 0 (slot 0)     │
│   Record 1 (slot 1)     │
│   ...                   │
└─────────────────────────┘
```

**Page-level bitmap:** Which slots are occupied (256 bits)
**Record-level NULL bitmap:** Which fields are NULL (per record)

**These are DIFFERENT:**
- Page bitmap: slot occupancy tracking
- NULL bitmap: field NULL tracking

---

## Implementation Strategy

### Phase 1: Add New FieldType Variants ⏱️ 1 day
- [ ] Expand FieldType enum with new types
- [ ] Update From<i32> implementation
- [ ] Add to parser keyword matching
- [ ] Add Schema helper methods (add_bool_field, etc.)

### Phase 2: Expand Constant Enum ⏱️ 1 day
- [ ] Add new Constant variants
- [ ] Implement accessor methods (as_bool, as_float, etc.)
- [ ] Add Constant::Null variant
- [ ] Implement is_null() method

### Phase 3: Page Serialization ⏱️ 2-3 days
- [ ] Implement Page::get_bool/set_bool
- [ ] Implement Page::get_bigint/set_bigint
- [ ] Implement Page::get_float/set_float
- [ ] Implement Page::get_date/set_date
- [ ] Implement Page::get_timestamp/set_timestamp
- [ ] Implement Page::get_decimal/set_decimal
- [ ] Add unit tests for each type

### Phase 4: NULL Bitmap Support ⏱️ 2-3 days
- [ ] Implement NullBitmap struct with bit operations
- [ ] Update Layout to include NULL bitmap size
- [ ] Add NULL bitmap to record layout
- [ ] Update RecordPage to check/set NULL bits
- [ ] Update TableScan::get_value to check NULL
- [ ] Add NULL handling tests

### Phase 5: RecordPage/TableScan Updates ⏱️ 3-4 days
- [ ] Add get/set methods for all new types
- [ ] Update get_value match statement
- [ ] Update set_value match statement
- [ ] Handle NULL values in all operations
- [ ] Update all scan types (SelectScan, ProjectScan, etc.)

### Phase 6: Parser Integration ⏱️ 2 days
- [ ] Add keyword matching for new types
- [ ] Parse type-specific syntax (DECIMAL(10,2), etc.)
- [ ] Update field_def() method
- [ ] Add parser tests for new types

### Phase 7: Testing & Validation ⏱️ 3-4 days
- [ ] End-to-end tests for each type
- [ ] NULL value insertion and retrieval
- [ ] Cross-platform serialization tests
- [ ] Performance benchmarks
- [ ] All existing tests still pass

---

## Acceptance Criteria

- [ ] All 9 types supported (Boolean through Blob)
- [ ] NULL values work correctly for all types
- [ ] Serialization is big-endian and cross-platform
- [ ] NULL bitmap correctly tracks field nullability
- [ ] Layout calculation includes NULL bitmap size
- [ ] Parser recognizes all type keywords
- [ ] All type-specific operations work (arithmetic, comparisons)
- [ ] No data corruption in serialization/deserialization
- [ ] All tests pass

---

## Related Issues

- **Prerequisite:** [#18 - Redesign Page format](https://github.com/redixhumayun/simpledb/issues/18) (for optimal variable-length support)
- **Enables:** More realistic query examples and use cases
- **Enables:** Better pedagogical demonstrations of type systems

---

## References

### Type System Design

- **PostgreSQL Type System:** Comprehensive example with 40+ built-in types
- **MySQL Type System:** ~30 types with good balance of simplicity and power
- **SQLite Type Affinity:** Minimalist approach with only 5 storage classes

### Serialization Standards

- **IEEE 754:** Floating-point standard (universal)
- **ISO 8601:** Date/time format standard
- **Network Byte Order:** Big-endian for cross-platform compatibility

### NULL Handling

- **SQL NULL Semantics:** Three-valued logic (TRUE, FALSE, NULL)
- **PostgreSQL NULL Implementation:** Bitmap approach similar to proposed design

---

## Appendix: Full Example

```
EXAMPLE: Employee record with all features
═══════════════════════════════════════════════════════════════════

SQL:
INSERT INTO employees VALUES
  (42, 'Alice', true, 50000.00, '2024-01-15', NULL);

Record in memory (hex dump):

Offset | Bytes              | Meaning
───────┼────────────────────┼────────────────────────────────
0      | 00 00 00 01        | Flag: slot used
4      | 20                 | NULL bitmap: 0b00100000 (photo is NULL)
5      | 00 00 00 2A        | id: 42
9      | 00 00 00 05        | name length: 5
13     | 41 6C 69 63 65 ... | "Alice" + padding
63     | 01                 | active: true
64     | 00 00 00 00 00 4C  | salary: 5000000 (scale=2)
       | 4B 40              | = 50000.00
72     | 00 00 4E 94        | hired: 20116 days (2024-01-15)
76     | 00 00 00 00        | photo: length 0 (NULL, don't read)

Note: Even though photo is NULL, we still reserve the 1004 bytes
for it in fixed-length record layout.
```

This type system provides SimpleDB with essential data types while maintaining pedagogical clarity and implementation simplicity.
