# 007 — TPC-C Driver Architecture

## Single-threaded sequential execution

TPC-C normally requires multiple concurrent "terminals." The driver runs them sequentially
for now. Reason: `Planner` holds `Box<dyn QueryPlanner>` without `Send + Sync` bounds,
so sharing a `SimpleDB` across OS threads via `Arc` doesn't compile without modifying the
library. The sequential path validates correctness and produces a tpmC number that reflects
single-terminal throughput. Multi-terminal parallelism requires extracting a `TpccRuntime`
of raw components (file/log/buffer/lock managers) and re-wiring transactions without the
planner—the same pattern used in `simple_bench.rs` concurrency tests.

## Standalone binary, not Criterion

TPC-C is a macro-workload (minutes-long, measures steady-state tpmC) rather than a
microbenchmark. Criterion's iteration model doesn't map to it. `src/bin/tpcc.rs` runs
for a configurable duration, counts committed NewOrder transactions, and prints tpmC at
the end.

## Arithmetic in Rust, literals in SQL

The engine's UPDATE parser accepts `SET field = constant` or `SET field = field_name`, but
no arithmetic expressions. All computed values (new balance, new stock quantity, order
total) are calculated in Rust and passed as literals in the SQL string.

## Multiple UPDATE statements per logical row update

The parser handles exactly one `SET field = value` per UPDATE statement. Multi-field
updates require one UPDATE call per field. The extra round-trips are wasteful but
unavoidable without changing the parser.

## Dates stored as i32 epoch seconds

SimpleDB has no DATE type. Timestamps are stored as `INT` (i32 seconds since Unix epoch).
This is valid until 2038; adequate for a test environment.

## NULL sentinels

`o_carrier_id = 0` means "not yet assigned" (spec allows NULL). `ol_delivery_d = 0` means
"not yet delivered." Queries that need to find undelivered order lines filter `ol_delivery_d
= 0`; the spec says NULL, same intent.

## StockLevel uses driver-side dedup and per-item stock queries

The spec join (order_line ⋈ stock with COUNT DISTINCT below threshold) needs a subquery
or join—neither is supported. Instead: scan order_line for the last 20 orders, collect
distinct item IDs in a `HashSet`, then query stock for each. The inner loop is bounded by
20 orders × 15 max lines = 300 unique item IDs at most.

## Customer-by-last-name: ORDER BY + driver-side middle pick

Payment and OrderStatus can look up customers by last name. The spec says take the row at
position `floor(n/2)` in first-name order. The driver issues `SELECT ... ORDER BY c_first`
and iterates to the middle row. Uses the ORDER BY support added in PR #114.

## Table named `orders`, not `order`

`ORDER` is a SQL keyword. The TPC-C spec uses `ORDER` for the table name; we rename it to
`orders` to avoid parser ambiguity.

## Index-before-load (not after)

Indexes must be created BEFORE any data is loaded. `CREATE INDEX` in SimpleDB only
registers the index in the catalog; it does not backfill existing rows. The
`IndexUpdatePlanner` then maintains btree entries on every subsequent INSERT. Creating
indexes before load means every insert pays the btree maintenance cost, which is slower,
but it's the only correct path.

## Reduced default item count

Full TPC-C W=1 requires 100k items, 100k stock rows, and 30k customers. Loading at
~1k INSERTs/second (WAL + direct-IO path) would take ~4–5 minutes before any transaction
runs. The default `--items 10000` trades spec fidelity for practical iteration speed.
Pass `--items 100000 --customers-per-district 3000` for a spec-compliant load.
