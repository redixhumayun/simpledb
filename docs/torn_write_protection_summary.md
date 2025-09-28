# Torn Write Protection - Implementation Guide

## Overview

This document summarizes the key concepts, hardware realities, and implementation strategies for torn write protection in database systems. This serves as the foundation for implementing torn write protection in a pedagogical database implementation.

## The Fundamental Problem

### What is a Torn Write?

A torn write occurs when a power failure or system crash interrupts a multi-sector write operation, leaving only some sectors written while others remain in their previous state.

```
Database Page (8KB):
┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
│ 512B │ 512B │ 512B │ 512B │ 512B │ 512B │ 512B │ 512B │
│ Sec0 │ Sec1 │ Sec2 │ Sec3 │ Sec4 │ Sec5 │ Sec6 │ Sec7 │
└──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘

Normal write: All sectors written ✓
Torn write:   Only some sectors written ✗

Power fails here ↓
┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
│ NEW  │ NEW  │ NEW  │ OLD  │ OLD  │ OLD  │ OLD  │ OLD  │
│ Data │ Data │ Data │ Data │ Data │ Data │ Data │ Data │
└──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘
```

### The Storage Stack Gap

```
Application Layer
     ↓
┌─────────────────┐
│   Database      │ ← Works with 4KB-16KB pages
│     Pages       │
└─────────────────┘
     ↓ write()
┌─────────────────┐
│  OS Page Cache  │ ← OS buffers writes
└─────────────────┘
     ↓ fsync()
┌─────────────────┐
│   Disk Sectors  │ ← Hardware guarantees atomicity
│   (512B/4KB)    │   only at sector level
└─────────────────┘
```

**Key insight**: Databases work with large pages (4KB+), but disks only promise atomicity at the sector level.

## Hardware Reality

### Test System Analysis
- **Hardware**: Dell XPS 13 9370 with Netac PCIe 3 NVMe SSD (DRAM-less)
- **Filesystem**: ext4
- **Logical Block Size**: 512 bytes
- **Physical Block Size**: 512 bytes
- **Atomic Write Support**: 512 bytes only (no multi-sector atomicity)

### SSD vs HDD Sector Concepts

```
HDD Reality:
Physical Sector (512B) = Storage Unit = Atomicity Unit

SSD Reality:
┌─────────────────┐
│ Logical Sector  │ ← What OS sees (512B or 4KB)
├─────────────────┤
│ Physical Page   │ ← What SSD writes (4KB-16KB)
├─────────────────┤  
│ Erase Block     │ ← What SSD erases (128KB-2MB)
└─────────────────┘
```

**Important**: Even modern NVMe SSDs may only guarantee 512B atomicity to the OS, regardless of internal architecture.

### Logical Block Addressing (LBA)

LBA is how the OS addresses locations on storage:

```
LBA Addressing Scheme:
┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│LBA 0│LBA 1│LBA 2│LBA 3│LBA 4│LBA 5│LBA 6│LBA 7│...
│512B │512B │512B │512B │512B │512B │512B │512B │
└─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘

8KB page at offset 16384:
→ LBA 32 through LBA 47 (16384 ÷ 512 = 32, 8192 ÷ 512 = 16 blocks)
```

**Key insight**: Logical contiguity ≠ physical contiguity. The SSD controller may map these to different physical locations.

## Torn Write Protection Strategies

### 1. Detection Only

**Approach**: Detect torn pages during recovery but don't prevent them.

### 2. Log Full Pages (SQLite approach)

**Approach**: Write entire updated page to WAL for each modification.

```
SQLite WAL Process:
┌─────────────────────┐
│ 1. Modify page      │ ← Change page in memory
├─────────────────────┤
│ 2. Write full page  │ ← Entire page → WAL
│    to WAL           │
├─────────────────────┤
│ 3. fsync WAL        │ ← Make durable
├─────────────────────┤
│ 4. Return COMMIT    │ ← Transaction complete
├─────────────────────┤
│ 5. Checkpoint       │ ← Apply WAL → main file
│    (later)          │
└─────────────────────┘
```

### 3. Log Page on First Write (PostgreSQL approach)

**Approach**: Log full page only on first modification after checkpoint, then log deltas.

```
PostgreSQL Process:
┌─────────────────────┐
│ After checkpoint:   │
│ 1st write → full    │ ← Full page to WAL
│     page to WAL     │
├─────────────────────┤
│ Subsequent writes   │ ← Only deltas to WAL
│ → deltas to WAL     │
├─────────────────────┤
│ fsync WAL on commit │ ← Single fsync for durability
└─────────────────────┘
```

### 4. Double-Write Buffer (MySQL/InnoDB approach)

**Approach**: Write pages to scratch space first, then to final location.

```
MySQL Double-Write Process:
┌─────────────────────┐
│ 1. Write page to    │ ← Pages → scratch space
│    double-write     │
│    buffer           │
├─────────────────────┤
│ 2. fsync buffer     │ ← First fsync
├─────────────────────┤
│ 3. Write page to    │ ← Pages → final location
│    B-Tree location  │
├─────────────────────┤
│ 4. fsync B-Tree     │ ← Second fsync
└─────────────────────┘
```

### 5. Copy-on-Write (LMDB approach)

**Approach**: Never update pages in-place; always create new pages.

```
Copy-on-Write Process:
┌─────────────────────┐
│ 1. Allocate new     │ ← New page for modified data
│    page             │
├─────────────────────┤
│ 2. Copy + modify    │ ← Update in new location
├─────────────────────┤
│ 3. Update parent    │ ← Parent points to new page
│    (recursively)    │   (may recurse to root)
├─────────────────────┤
│ 4. Update root      │ ← Atomic root update
│    (atomic commit)  │
└─────────────────────┘
```

### 6. Atomic Block Writes

**Approach**: Use hardware/OS support for multi-block atomic writes.

**Linux 6.11+ Support**:
- `pwritev2()` with `RWF_ATOMIC` flag
- `statx()` to query atomic write capabilities
- XFS and ext4 support

## Key Diagnostic Commands

### Linux Storage Analysis
```bash
# List block devices
lsblk -f -t

# Check atomic write support
ls /sys/block/nvme0n1/queue/atomic_write_*
cat /sys/block/nvme0n1/queue/atomic_write_unit_max_bytes

# Check NVMe capabilities  
sudo nvme id-ns /dev/nvme0n1

# Check if SSD (0) or HDD (1)
cat /sys/block/nvme0n1/queue/rotational
```

## References

1. [Torn Write Detection and Protection](https://transactional.blog/blog/2025-torn-writes)