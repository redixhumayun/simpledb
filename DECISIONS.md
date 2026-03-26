# Architecture Decisions

One-line summaries with pointers to detail files. See `docs/decisions/` for full context, alternatives, and reasoning.

| # | Decision | Detail |
|---|----------|--------|
| 001 | Deadlock handling uses timeout-only; wait-die was evaluated and removed | [docs/decisions/001-wait-die.md](docs/decisions/001-wait-die.md) |
| 002 | B-tree latch crabbing uses a no-wait fast-pin path; contention forces restart | [docs/decisions/002-btree-fast-pin-no-wait.md](docs/decisions/002-btree-fast-pin-no-wait.md) |
| 003 | Executor capability split: `UpdateScan` replaced by `TableCursor` + `Scan`; `TableSource` trait unifies table-producing plans | [docs/decisions/phase1_executor_capability_split.md](docs/decisions/phase1_executor_capability_split.md) |
| 004 | `ExecutionContext` threads runtime authority through `Plan::open()`; plan structs are now metadata-only | [docs/decisions/phase2_5_execution_context.md](docs/decisions/phase2_5_execution_context.md) |
| 005 | Logical/physical planner boundary introduced: `LogicalPlan` IR, `PipelineQueryPlanner` replaces mixed-layer planners | [docs/decisions/005-logical-physical-boundary.md](docs/decisions/005-logical-physical-boundary.md) |
