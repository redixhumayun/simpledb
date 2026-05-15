# Architecture Decisions

One-line summaries with pointers to detail files. See `docs/decisions/` for full context, alternatives, and reasoning.

| # | Decision | Detail |
|---|----------|--------|
| 001 | Deadlock handling uses timeout-only; wait-die was evaluated and removed | [docs/decisions/001-wait-die.md](docs/decisions/001-wait-die.md) |
| 002 | B-tree latch crabbing uses a no-wait fast-pin path; contention forces restart | [docs/decisions/002-btree-fast-pin-no-wait.md](docs/decisions/002-btree-fast-pin-no-wait.md) |
| 003 | Query planning is split into logical and physical stages; logical IR uses a wrapper-node shape for future optimizer work | [docs/decisions/003-query-planning-architecture.md](docs/decisions/003-query-planning-architecture.md) |
| 004 | Transaction authority is intentionally asymmetric: shared read path, explicit write session, and narrow lower-layer capabilities | [docs/decisions/004-transaction-write-session-asymmetry.md](docs/decisions/004-transaction-write-session-asymmetry.md) |
| 005 | Recovery uses steal + no-force with WAL-only commit durability and redo+undo restart recovery | [docs/decisions/005-no-force-recovery.md](docs/decisions/005-no-force-recovery.md) |
| 006 | Direct-I/O page writeback uses snapshot-first batched flush with generation-based completion | [docs/decisions/006-snapshot-writeback-protocol.md](docs/decisions/006-snapshot-writeback-protocol.md) |
| 007 | TPC-C driver is single-threaded, uses driver-level arithmetic, and defers multi-terminal parallelism | [docs/decisions/007-tpcc-driver.md](docs/decisions/007-tpcc-driver.md) |
