# Single-Pipeline LeanStore Lessons

This file records what the final single-pipeline LeanStore implementations teach
the next Calcite-to-LeanStore phase. It should be read as an implementation
handoff, not as the old Calcite single-MI roadmap.

The published VLDB paper remains the conceptual reference. This file captures
the later engineering adaptations in `../leanstore/frontend/tpch/q3`,
`../leanstore/frontend/tpch/q5`, and `../leanstore/frontend/tpch/q10`.

## Main Lesson

The final LeanStore implementations do not follow the old Calcite Q3 framing of
only indexing ORDERS/LINEITEM by `orderkey` and joining CUSTOMER later. They use
the COL family:

```text
CUSTOMER -> ORDERS -> LINEITEM
custkey     custkey,orderkey     custkey,orderkey,linenumber
```

ORDERS and LINEITEM are extended with `custkey` through the FK chain, turning the
query into a strict hierarchical prefix walk. The MI implementation is a single
custkey-prefixed `col_group_walk` over co-located CUSTOMER, ORDERS, and LINEITEM
records. Filters, side-table probes, aggregation, and TopN are fused into the
walker or attached immediately around it.

That adaptation is important for future manual LeanStore work: Calcite's logical
pipeline may identify a valid order-sharing region, but the C++ plan may need an
extended physical key, FD-attached payload fields, or a different aggregation
grain to make the implementation competitive and semantically clean.

## Implemented Reference Queries

| Query | LeanStore shape | Key adaptation for future work |
|-------|-----------------|--------------------------------|
| Q3 | `MergedAdapter<customer_col_t, orders_coli_t, lineitem_col_t>` with `col_group_walk` | Use custkey-prefixed COL, not only ORDERS/LINEITEM by orderkey. Keep mktsegment, orderdate, and shipdate live so the structure is reusable across parameters. |
| Q5 | Same COL chain plus REGION/NATION/SUPPLIER side structures | Keep small dimensions outside the MI. Fuse `c_nationkey` and `l_suppkey` into a composite `supplier_nation_set` probe during the walk. |
| Q10 | Same COL chain, plus a bespoke per-customer visitor and TopN sink | The group key is `c_custkey`, the leading MI key, so per-customer aggregation finalizes naturally at group end. NATION is an INL lookup at emit time. |

The shared pattern is stronger than the historical Calcite single-MI candidate
enumeration because it models the physical FD extension explicitly. Future
manual plans should ask first whether an apparently independent join key can be
made hierarchical by carrying a parent key into child records.

## Q3 Adaptation

LeanStore Q3 is the cleanest COL example. It implements the canonical TPC-H Q3
over CUSTOMER, ORDERS, and LINEITEM with all parameterized predicates applied at
query time.

Main choices:

- Store the 3-table chain in custkey order.
- Extend ORDERS with `custkey` and LINEITEM with `(custkey, orderkey)`.
- Use one `col_group_walk` rather than an ORDERS/LINEITEM MI followed by a
  separate CUSTOMER join.
- Keep `c_mktsegment`, `o_orderdate`, and `l_shipdate` live in the walker.
- Accumulate revenue per orderkey inside the visitor, then apply TopN.

This is the most important correction to carry back to Calcite planning: the
useful physical MI is a 3-table hierarchical layout even though a purely logical
orderkey pipeline does not show that layout by itself.

## Q5 Adaptation

Q5 reuses the COL chain and attaches REGION, NATION, and SUPPLIER as small side
structures.

Main choices:

- Keep CUSTOMER/ORDERS/LINEITEM in the same custkey-prefixed MI shape as Q3.
- Build an in-region `nation_set` from REGION/NATION.
- Build `supplier_nation_set` keyed by `(s_nationkey, s_suppkey)`.
- During the COL walk, prune customers by `c_nationkey`, filter orders by
  `o_orderdate`, then probe `(c_nationkey, l_suppkey)` for each surviving
  lineitem.
- Resolve `n_name` once per surviving nationkey and aggregate by that name.

The adaptation lesson is that dimension tables do not always belong in the MI.
For Q5, they are better treated as small reduce-side structures around the COL
walk. A future manual plan should decide which tables are chain participants and
which are side probes.

## Q10 Adaptation

Q10 again reuses the COL chain, but its group key is the leading MI key:
`c_custkey`.

Main choices:

- Use a bespoke `Q10GroupWalkVisitor` over the COL MI.
- Apply the orderdate and returnflag predicates inside the walk.
- Accumulate returned revenue per customer and emit one aggregate row at the
  custkey group boundary.
- Resolve NATION by primary-key INL at emit time.
- Feed rows into a bounded TopN(20) sink.

Q10 also added an S5 aCOL implementation after the final audit. The important
insight is grain selection: per-customer returned revenue cannot be baked
because the date window is parameterized, but per-order returned revenue can be
baked because `l_returnflag = 'R'` is fixed by the query and the date predicate
applies at order granularity. The aCOL MI stores CUSTOMER plus per-order
returned revenue, drops LINEITEM bulk, and was the fastest structure in the
recorded LeanStore 5L sweep.

For later LeanStore work, this is the template for deciding when a pre-aggregated
MI is sound: bake constants and parameter-independent work, keep parameterized
filters live at the right grain.

## What Calcite Should Learn

The Calcite artifact is still valuable, but future manual LeanStore plans should
expect these adaptations:

- Model FD-extended physical keys, not just SQL join keys.
- Treat the root MI scan as the entry point to remaining query execution.
- Keep parameterized predicates live unless the value is fixed by the benchmark
  query text.
- Put tiny dimension tables in side structures when that is simpler and cheaper
  than interleaving them into the MI.
- Decide aggregation grain carefully. A pre-aggregation can be invalid at one
  grain and valid at a lower grain.
- Compare against fair baselines: separate sorted indexes, materialized pipeline
  views, merged indexes, hash plans, and when relevant pre-aggregated MIs.

## Historical Side Notes

The earlier Calcite single-MI attempt found useful candidates for Q9 and Q12,
but those notes should not drive the next implementation plan.

- **Q12:** `{ORDERS, LINEITEM}` by orderkey was a reasonable structural
  candidate. The old Calcite test still needs semantic cleanup around the
  high-priority predicate (`1-URGENT` and `2-HIGH`). It should be revisited only
  after the Q3/Q5/Q10 LeanStore adaptation pattern is reflected in Calcite.
- **Q9:** historical candidates such as `{LINEITEM, PART, PARTSUPP}` and
  `{LINEITEM, PARTSUPP, SUPPLIER}` exposed useful compound-key issues. They were
  tie-breaking and plan-shape experiments, not final LeanStore designs.

Use [SESSION_PROGRESS.md](SESSION_PROGRESS.md) for the active multi-pipeline
status and [CALCITE_LEANSTORE_INTEGRATION.md](CALCITE_LEANSTORE_INTEGRATION.md)
for the manual C++ handoff workflow.
