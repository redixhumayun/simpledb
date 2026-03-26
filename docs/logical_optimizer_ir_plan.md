# Logical Optimizer IR Plan

Tracking issue: planner / optimizer follow-on from [#96](https://github.com/redixhumayun/simpledb/issues/96)

## Goal

Introduce a logical query representation that is optimizer-friendly in Rust:

- logical plans are pure query IR, not executable plans
- logical rewrites operate on a uniform tree shape
- physical planning remains a separate lowering step
- the physical layer can stay typed and executor-focused for now

This is intentionally narrower than a full optimizer rewrite. The focus is the logical layer only.

However, the logical/physical planning boundary must be implemented first. The logical IR work should not begin until query meaning and physical implementation choice are separated in code.

## Why this exists

Today the codebase does not have a clean logical/physical split.

- `BasicQueryPlanner` builds `TablePlan` / `ProductPlan` / `SelectPlan` / `ProjectPlan` directly: `src/main.rs:4152`
- `HeuristicQueryPlanner` performs logical and physical choices in lockstep: `src/main.rs:4007`
- `TablePlanner` chooses physical operators while still doing predicate and join reasoning: `src/main.rs:3860`
- physical choices like `IndexSelectPlan`, `IndexJoinPlan`, and `MultiBufferProductPlan` are introduced during planner search: `src/main.rs:3954`, `src/main.rs:3917`, `src/main.rs:3897`
- the `Plan` trait is both optimizer-facing metadata and executable runtime entry: `src/main.rs:4975`

That shape is acceptable for the current heuristic planner, but it is a poor foundation for:

- rule-based logical rewrites
- memoization
- DP join ordering
- separating query meaning from implementation choice

## What changes and what does not

This plan changes:

- how logical query trees are represented in memory
- where logical rewrites happen
- where physical operator selection happens

This plan does not require, initially:

- changing executor node implementations
- rewriting the physical layer into the same generic wrapper shape
- introducing a Cascades-style physical optimizer immediately
- changing transaction / execution-context semantics

## Design direction

The logical layer should move toward the shape suggested in the blog post:

- logical planning returns a logical IR, not executable plans
- logical optimization rewrites that IR
- physical planning lowers that IR into executable plan structs

The exact in-memory shape of `LogicalPlan` does not need to be frozen yet. The boundary matters more than the final logical-node representation.

For the first implementation, it is acceptable for `LogicalPlan` to be a single enum with embedded children and schema. If later memo/rule work needs a framework-owned wrapper shape with uniform `children`, we can evolve the logical representation then.

But that only works if the logical layer has a real contract to target. So the first implementation step is not the wrapper node itself; it is the boundary between:

- logical query representation and logical rewrites
- physical operator selection and executable plan construction

## Why use the wrapper shape only for logical plans

The logical optimizer needs uniform structural operations:

- get children
- rebuild with new children
- match patterns
- insert into a memo representation later

The current physical layer does not need that yet. Physical plans can stay as typed Rust enums / structs until physical optimization itself becomes rule- and memo-driven.

So the intended split is:

- logical layer: optimizer-friendly logical IR
- physical layer: typed plan / executor representation

## Current logical concepts already present in the codebase

The existing planner already reasons about several logical concepts, even though it expresses them through physical `Plan` structs:

- base relation access: `TablePlan`: `src/main.rs:4885`
- filtering: `SelectPlan`: `src/main.rs:4832`
- projection: `ProjectPlan`: `src/main.rs:4724`
- join / product structure: `ProductPlan`: `src/main.rs:4653`
- join predicate extraction and select predicate extraction in `TablePlanner`: `src/main.rs:3971`, `src/main.rs:3987`

Those concepts should move into a true logical IR.

## What should not appear in logical IR

These are physical concerns and should not be logical node kinds:

- `IndexSelectPlan`: `src/main.rs:4781`
- `IndexJoinPlan`: `src/main.rs:5453`
- `MultiBufferProductPlan`: `src/main.rs:190`
- `SortPlan`: `src/main.rs:1925`
- `MergeJoinPlan`: `src/main.rs:1169`
- `MaterializePlan`: `src/main.rs:2824`

Those belong in physical planning / lowering.

## Suggested logical node shape for this codebase

Start small. The logical IR only needs to model operators we already have query semantics for. For the first implementation, a single enum is enough.

```rust
pub enum LogicalPlan {
    TableScan {
        table: String,
        schema: Schema,
    },
    Filter {
        input: Arc<LogicalPlan>,
        predicate: Predicate,
        schema: Schema,
    },
    Project {
        input: Arc<LogicalPlan>,
        fields: Vec<String>,
        schema: Schema,
    },
    Join {
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        predicate: Predicate,
        schema: Schema,
    },
}
```

Notes:

- storing `schema` directly is fine for the first step; it avoids repeated recomputation
- separate `LogicalNodeKind` / `LogicalNodeData` and `LogicalProps` types are not required yet
- logical `Join` should represent relational join meaning, not a particular implementation
- an inner join with no extracted join predicate can still exist as `Join { predicate: Predicate::empty() }`
- if later memo/rule work wants the blog post's wrapper-style representation, this enum can be migrated then

## Mapping from current planner to the new logical IR

Current code should map roughly like this:

- `TablePlan` -> logical `TableScan`
- `SelectPlan` -> logical `Filter`
- `ProjectPlan` -> logical `Project`
- `ProductPlan` plus later join filter -> logical `Join`

Important consequence:

- `TablePlanner::make_index_select_plan(...)` and `TablePlanner::make_index_join_plan(...)` should stop constructing plan nodes in the logical phase
- those functions become physical-lowering decisions later, after logical optimization

## Planner pipeline after this change

Target shape:

1. parse SQL into `QueryData`
2. build initial logical tree
3. apply logical rewrites / join-order search on logical IR
4. lower logical IR into physical plan nodes
5. execute physical plan with `ExecutionContext`

The key change is that steps 2-3 do not create executable plan structs.

## Required boundary before logical IR work

Before introducing the logical wrapper representation, implement a minimal boundary in code:

- one stage produces a logical query representation only
- a later stage lowers that logical representation into the current physical `Plan` family
- physical operators like `IndexSelectPlan`, `IndexJoinPlan`, `SortPlan`, `MergeJoinPlan`, `MaterializePlan`, and `MultiBufferProductPlan` are forbidden from being created during logical planning

This is required because otherwise the new logical IR will immediately become a cosmetic wrapper around the current mixed planner.

The API surface to lock first should be:

```rust
pub trait LogicalPlanner {
    fn build(
        &self,
        query: QueryData,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<LogicalPlan>;
}

pub trait LogicalOptimizer {
    fn optimize(
        &self,
        plan: LogicalPlan,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<LogicalPlan>;
}

pub trait PhysicalPlanner {
    fn lower(
        &self,
        logical: &LogicalPlan,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Arc<dyn Plan>>;
}
```

The exact trait names are not important. The important points are:

- logical planning no longer returns `Arc<dyn Plan>` directly
- logical optimization consumes and produces `LogicalPlan`
- physical planning is the first stage allowed to produce executable `Plan` nodes

This doc intentionally does not lock down catalog helper traits, context objects, or ownership details beyond this seam.

## Heuristics to move into logical space first

The existing heuristics are good candidates for the first logical pass:

- predicate extraction from `Predicate`
- select pushdown by table schema
- join predicate extraction by unioned schema
- left-deep join order heuristic currently in `HeuristicQueryPlanner`: `src/main.rs:4023`
- projection placement

These should operate over logical nodes, not physical `Plan` trait objects.

## Why this matches the blog post's recommendation

The blog post's main point is not that enums are bad. The point is that optimizer internals want one uniform structural representation.

That matters here because future logical optimization likely needs:

- recursive rewrites over arbitrary logical subtrees
- memo groups keyed by operator kind + children + attributes
- rule matching without hand-written conversion code for every node type

Using one wrapper shape for logical nodes avoids baking the optimizer into a large set of typed logical-plan structs that later need parallel memo / binding representations.

## Phased plan

### Phase 0: document the split

- document that logical and physical planning are separate stages
- document that current `Plan` structs are physical or mixed, not the long-term logical IR

Exit criteria:

- logical/physical split is documented

### Phase 1: implement the logical/physical boundary

- introduce separate logical-planning and physical-lowering stages in code
- stop returning physical `Plan` trait objects directly from the logical-planning stage
- make physical operator selection an explicit follow-on step
- keep the first logical representation minimal if necessary, but enforce the stage boundary first

Minimum success criteria for this phase:

- there is a code path that constructs query meaning before physical operator choice
- physical nodes are created only in lowering, not in logical planning
- `HeuristicQueryPlanner` / `BasicQueryPlanner` stop mixing logical reasoning and physical node construction in one pass

Important point:

- this phase must be completed before introducing the wrapper-style logical optimizer IR
- otherwise the new logical representation will inherit the same mixed-layer problems as the current planner

Exit criteria:

- logical and physical planning are separate implemented stages
- the boundary exists in code, not just in documentation

### Phase 2: add logical IR types

- add `LogicalPlan` with a small initial operator set: table scan, filter, project, join
- embed `schema` directly in the logical variants for now
- do not over-design the logical representation before the boundary is stable
- keep open the option of later migrating this enum to a wrapper-style representation if memo/rule work needs it

Exit criteria:

- logical trees can be constructed without using `Arc<dyn Plan>`

### Phase 3: build initial logical plans from `QueryData`

- replace `BasicQueryPlanner`'s direct construction of physical/mixed plans with logical IR construction
- construct table scans first
- attach filter / join / project semantics as logical nodes
- keep schema derivation explicit during construction

Exit criteria:

- SQL query planning can produce a logical tree without choosing physical operators

### Phase 4: move current heuristic rewrites onto logical IR

- migrate predicate extraction and pushdown logic from `TablePlanner`
- migrate the current left-deep join heuristic from `HeuristicQueryPlanner`
- keep the first implementation heuristic-driven; do not require memoization yet

Exit criteria:

- current heuristic planner works over logical IR instead of `Arc<dyn Plan>`

### Phase 5: add physical lowering

- introduce a lowering stage from logical IR to the current physical plan family
- choose access paths and join implementations here
- keep the physical plan representation typed for now

Examples of lowering decisions:

- logical table scan -> `TablePlan` or `IndexSelectPlan`
- logical join -> `IndexJoinPlan`, `MultiBufferProductPlan`, `MergeJoinPlan`, etc.
- logical sort-sensitive requirements -> `SortPlan` / `MaterializePlan` where needed

Exit criteria:

- logical planning and physical selection are distinct steps

### Phase 6: prepare for memo / DP work

- add stable logical-node hashing / equality conventions
- make logical attributes explicit enough for memo keys
- avoid embedding executor-only state in logical nodes
- add tests for structural rewrites independent of execution

Exit criteria:

- logical IR is usable as the front-end tree for memoization or DP join ordering

## Immediate non-goals for the first implementation

- do not add physical properties to logical nodes yet
- do not build a generic memo engine in the same PR as the new logical IR
- do not rewrite physical executor nodes into the wrapper shape
- do not block on having the perfect final logical-node taxonomy

## Risks and tradeoffs

- A generic logical wrapper is less ergonomic to hand-read than typed logical structs; helper constructors and pretty-printers will matter.
- If `Predicate` remains a monolithic payload everywhere, some later rewrites may still be awkward; that is acceptable initially.
- If we keep physical selection mixed into logical construction during the migration, the new IR will become cosmetic. The real win only appears once lowering is a separate step.

## Validation

After each phase, run:

```bash
cargo build
cargo test --no-default-features --features replacement_lru --features page-4k
cargo test --no-default-features --features replacement_clock --features page-4k
cargo test --no-default-features --features replacement_sieve --features page-4k
cargo test --no-default-features --features replacement_lru --features page-4k --features direct-io
```

Focus additional tests on:

- planner tests
- logical rewrite tests that do not execute queries
- end-to-end query equivalence before and after logical rewrites

## Immediate next step

Start with phase 1 only, and complete it before any logical IR work:

- define separate logical-planning, logical-optimization, and physical-lowering interfaces
- make logical planning stop returning `Arc<dyn Plan>` directly
- forbid physical operator construction during the logical-planning stage
- keep the physical plan family as-is for now

That is the minimum step that prevents the future logical IR from collapsing back into the current mixed-layer planner.

## References

- Alex Chi, "Plan Representation: #1 Lesson Learned from Building an Optimizer": <https://www.skyzh.dev/blog/2025-02-06-optimizer-lesson-01/>
- PostgreSQL planner/optimizer overview: <https://www.postgresql.org/docs/current/planner-optimizer.html>
- Goetz Graefe, "The Cascades Framework for Query Optimization" (1995)
