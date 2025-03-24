# SimpleDB Record Management System

This is an artefact prepared by Claude based on a conversation about chapter 6 of the book. Find the chat here - https://claude.ai/chat/8dce4c6d-507f-4b8d-98ce-61a819d3c88f

## Core Abstractions

We've discussed SimpleDB's record management system using a "lens" analogy, where each layer provides a different way of looking at the same underlying data. The system is organized in layers of increasing abstraction:

### Raw Storage Layer
The lowest level deals with raw bytes stored in files. These bytes have no inherent meaning until interpreted through higher-level abstractions.

### Block Layer
A Block represents a fixed-size chunk of the file, identified by:
- A filename (e.g., "students.tbl")
- A block number within that file
This layer provides location-based access to data.

### Page Layer
A Page provides an in-memory view of a block's contents with methods to:
- Read and write basic types (integers, strings)
- Handle byte ordering and type conversion
- Work with the raw bytes in a structured way

### RecordPage Layer
RecordPage understands how records are organized within a block:
- Uses slots to organize records
- Knows about record structure through Layout
- Provides record-level operations within a single block
- Works directly with slot numbers

### TableScan Layer
The highest-level abstraction that provides:
- Iterator interface over all records
- Handles movement between blocks automatically
- Manages complexity of finding records
- Abstracts away slots and blocks from clients

## Key Components

### Schema
Represents the logical structure of records:
- Defines what fields exist in a record
- Specifies the type of each field
- Declares size constraints (e.g., string lengths)
- Acts as a "legend" telling us what data to expect

Example:
```rust
let mut schema = Schema::new();
schema.add_int_field("student_id");
schema.add_string_field("name", 20);
schema.add_int_field("grade");
```

### Layout
Translates schema into physical organization:
- Calculates where each field is stored in a record
- Determines total record size
- Manages field offsets
- Acts as a "map" telling us where to find data

Example:
```rust
let layout = Layout::new(schema);
// layout knows:
// - student_id starts at offset 4 (after slot header)
// - name starts at offset 8
// - grade starts at offset 28
```

### Record ID (RID)
Uniquely identifies a record's location:
- Block number (which block contains the record)
- Slot number (where in the block the record is stored)
Used for:
- Direct record access
- Record references
- Index implementations

## Implementation Details

SimpleDB makes several specific implementation choices:

1. File Organization:
- Uses homogeneous files (one table per file)
- No record spanning across blocks
- Fixed-length records within a block

2. String Storage:
- Fixed-length representation
- Allocates maximum declared space
- No external storage or variable-length handling

3. Record Access:
- Slot-based organization
- No ID table implementation
- Direct slot numbering within blocks

4. Resource Management:
- RAII-style resource handling
- Automatic block pinning/unpinning
- Transaction-based access control

## Usage Patterns

The system can be used at different abstraction levels:

1. High-level (TableScan):
```rust
let table = TableScan::new("students", layout);
while table.next() {
    println!("{}", table.get_string("name"));
}
```

2. Mid-level (RID-based):
```rust
let rid = table.get_rid();
table.move_to_rid(rid);
```

3. Low-level (RecordPage):
```rust
let record_page = RecordPage::new(tx, block_id, layout);
let name = record_page.get_string(slot, "name");
```

## Key Concepts Discussed

1. Layer Abstractions:
- Each layer provides a different view of the same data
- Higher layers hide complexity from clients
- Lower layers handle physical organization

2. Record Organization:
- Fixed-size slots within blocks
- Record identification through RIDs
- Block and slot-based navigation

3. Schema and Layout Relationship:
- Schema defines logical structure
- Layout implements physical organization
- Together they enable field access in records


## Page Layout

┌───────────────────────────────────────────────────────────────┐
│                         4096-byte Page                         │
├────────┬────────┬─────────────────┬────────────────────────────┤
│ Header │ Bitmap │    ID Table     │       Record Space         │
│8 bytes │32 bytes│   512 bytes     │       3544 bytes           │
├────────┼────────┼─────────────────┼────────────────────────────┤
│        │        │                 │                            │
│ - Slot │ - 1 bit│ - 2-byte offset │  - Records grow from here  │
│  count │  per   │   per slot      │    toward the left         │
│ - Free │  slot  │ - Points to     │  - Each record has a       │
│  ptr   │ 256 max│   record start  │    4-byte header           │
└────────┴────────┴─────────────────┴────────────────────────────┘

The idea behind the page layout is simple:
* The header contains the slot count (4 bytes) and the free pointer (4 bytes). The free pointer points to the beginning of the free space in the record space. This needs to be updated after each record is written.
* The next 32 bytes are where the bitmap is stored. This bitmap is used to mark the presence/absence of each record slot.
* The next 512 bytes are for the ID table which serves a similar purpose as in a B-tree where it marks the offset of its respective record.

The calculations for this were done as follows:
┌────────┬────────────────────────────┐
│ Header │ Field Values               │
│4 bytes │ (variable, schema-defined) │
└────────┴────────────────────────────┘
* First, each record will have a 4-byte header (2 bytes for the record length and 2 bytes reserved for future flags)
* Second, at minimum each record will also contain a 4-byte integer field (giving a minimum total of 8 bytes per record)
* Assuming a 4,096(4KB) bytes page, the number of slots for records = 4,096/8 = 512. Take 256 slots instead of 512 because 512 is a thereotical limit
* For 256 slots, we need 256 bits in the bitmap and 256 bits = 32 bytes
* Each entry in the ID table will contain the offset for a record, so 2 bytes should allow us to address offsets up to 2^16=65,536 which is >> 4,096. So, each entry in the ID table will be 2 bytes giving us 2 * 256 = 512 bytes for the ID table

## Notes On Implementation
Upon trying to implement this approach, I noticed that I have to do a bunch of nonsense to get around the design of the `Page` and `Transaction` objects because those don't have the concept & affordances of bit maps and ID tables.
If I truly want to modify this, I should change the actual `Page` format so that it has knowledge of the bitmap and the ID table. That way, all these changes can go through the `RecoveryManager` correctly.
Refer to [this chat](https://claude.ai/chat/b36c7658-2311-479b-acc9-945e95ea2ba5) for more details.