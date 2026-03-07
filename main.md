# Storing and Indexing Multiple Tables by Interesting Orderings

**Wenhui Lyu** (UW–Madison) · **Goetz Graefe** (Google)
PVLDB Vol. 19, 2026 — [Artifact](https://github.com/alicia-lyu/vldb19-artifact)

---

## Abstract

Relational databases face a tension between fast multi-table queries and frequent updates. Materialized join views accelerate queries but slow updates and waste space. Query-time joins over traditional indexes favor updates but limit query speed. This paper generalizes **merged indexes** — a multi-table B-tree/LSM structure — from two-table joins to multi-way joins and grouping operations that share a sort order. Multi-table merged indexes match materialized views for query performance (sometimes 2× faster) while matching traditional single-table indexes for update performance and space efficiency.

---

## 1. Problem Scope

This paper targets query pipelines of the form:

**Q = γ(A, F) ( σ(P) ( R₁ ⋈_A R₂ ⋈_A … ⋈_A Rn ) )**

where the join keys and grouping key A form a consistent prefix chain compatible with a single shared sort order. This pattern appears in 14 of 22 TPC-H queries and all 33 JOB queries.

**Performance trade-off (Table 1):**

| Structure | Query | Update | Space |
|---|---|---|---|
| Traditional indexes (query-time) | moderate | fast | high |
| Materialized views | fast | slow | low |
| **Multi-table merged indexes** | **fast** | **fast** | **high** |

---

## 2. Background: Interesting Orderings (Selinger 1979)

When multiple operators in a query plan share a sort order, intermediate sorted results can be reused, avoiding redundant sorting. For a 3-way merge join of Customer ⋈ Orders ⋈ Invoice on `custkey`, only **3 sorts** are needed (not 4): the first join's output is already sorted on `custkey`, so the second join consumes it directly.

This naturally divides complex queries into **order-based pipelines** — groups of join and aggregation operators sharing a common key and sort order.

---

## 3. Physical Structure: "Data at Rest"

### 3.1 Multi-Table Joins with Identical Join Keys

A merged index stores records from all participating tables **interleaved by a shared sort key**. For Customer ⋈ Orders ⋈ Invoice on `custkey`, the merged index stores the primary index of Customer and the secondary indexes of Orders and Invoice, all sorted by `custkey`. This physically co-locates each customer record with all its orders and invoices — "master-detail clustering."

A traditional query plan using interesting orderings:
```
Sort(Customer) ──→ MergeJoin(custkey) ──→ MergeJoin(custkey)
Sort(Orders)  ──→/                     ↑
Sort(Invoice) ────────────────────────/
                              ↑ output already sorted — no extra sort!
```

A merged index **embeds this sort order into physical storage**, so the entire pipeline collapses into one sequential scan.

### 3.2 Hierarchical Join Keys

Merged indexes also support queries where join keys form a **prefix chain** (e.g., Nation ⋈ States ⋈ County ⋈ City on successively longer keys). A single merged index sorted on the maximal key `(nationkey, statekey, countykey, citykey)` satisfies all merge join requirements because lexicographic ordering on a composite key preserves the order of every prefix.

### 3.3 Record Structure

Each record in a merged index has three parts:

1. **Sort key with domain tags** — 1-byte tags before each key field act as separators and domain identifiers, enabling byte-wise comparison of composite variable-length keys.
2. **Index identifier** — a special domain tag (`I`) followed by a 1-byte identifier naming the source table (e.g., `IN` for Nation, `IS` for States).
3. **Payload** — remaining non-key columns stored as binary data.

The domain tags and index identifier are the only storage overhead vs. traditional indexes (~1 byte each per field), making merged indexes nearly as space-efficient as the constituent single-table indexes.

### 3.4 Multi-Table Aggregations

Merged indexes also support queries where joins and a grouping operator share a sort order. For example, counting cities per state (grouping on `(nationkey, statekey)`) over the geographic four-table join: the merged index sorted on `(nationkey, statekey, countykey)` already satisfies the grouping requirement because it is a prefix of the sort order.

**TPC-H example** — Query aggregating order quantities by priority:
```sql
SELECT o_orderpriority, SUM(l_quantity)
FROM orders JOIN lineitem ON o_orderkey = l_orderkey
GROUP BY o_orderpriority
```
`o_orderpriority` is functionally dependent on `o_orderkey`. A merged index on (Orders, Lineitem) sorted by `orderkey` supports this query: one scan clusters line items by order, and `o_orderpriority` is read from the co-located Orders record.

### 3.5 Comparison with Row and Columnar Storage

| Storage | Groups data by |
|---|---|
| Row-oriented | Row |
| Columnar | Column |
| Merged index | Record cluster (matching rows for a join key) |

Merged indexes specialize in multi-table OLTP and operational analytics workloads. They are not optimal for queries accessing only a subset of the interleaved tables (read overhead from traversing unneeded record types) or low-selectivity inner joins.

---

## 4. Query Execution: "Data in Flight"

### 4.1 Single-Pass Scan for Multi-Table Joins

A merged index scan is a **non-blocking iterator** that processes the interleaved record stream and produces joined results on the fly:

1. Scan records in sort-key order, accumulating each record into a per-table buffer `B[source]`.
2. On a **key change**: compute the Cartesian product of all non-empty buffers (yielding the joined rows for the old key), enqueue results, then clear buffers that don't match the new key.
3. Repeat until the scan range is exhausted; flush remaining buffers.

**Outer/semi/anti joins**: replace the Cartesian product step with the appropriate join-type function. Because potential matches are co-located within the scan range, absence of a partner is determined locally — all join types work within this single-pass scan.

**Many-to-one joins** (e.g., geographic hierarchy): buffers for singleton tables (Nation, State) persist across key changes at finer granularities (County, City) and are cleared only when their own key changes. This handles mixed multiplicities naturally.

### 4.2 Multi-Table Aggregation

Same algorithm, with the Cartesian product step replaced by a query-specific aggregate function. For example, counting cities per state reads the single buffered Nation and State records, counts City buffer entries, and emits the aggregate — no materialization of the full join is needed.

### 4.3 Execution of Complex Queries

A complex query may contain a pipeline describable by interesting orderings as only **part** of the plan. The pipeline is replaced by a single merged-index scan; the rest of the plan executes conventionally.

**TPC-H Q3 example** — the merge join of Orders ⋈ Lineitem on `orderkey` plus the subsequent grouping share a sort order. In the plan:

```
Sort(Lineitem, l_orderkey)
  └─ GroupBy(l_orderkey)
       └─ MergeJoin(o_orderkey)
            └─ Sort(Orders, o_orderkey)
```

This inner pipeline (dashed red box in the paper's Figure 5) collapses into one scan of a merged index on (Orders, Lineitem) by `orderkey`. The outer semi-join with Customer and the final ORDER BY execute as normal.

---

## 5. Index Maintenance

For any base-table insert/update/delete, the database updates **one corresponding merged index entry** — the same 1-to-1 cost as a traditional single-table index.

This contrasts with materialized join views, where one base-table update may trigger multiple view updates (1-to-N) because matching rows from other tables must be joined and the view delta computed.

**Additional benefits of physical clustering:**
- Inserting a child record allows a **local parent-existence check** (no random lookup).
- Cascading deletes become a single **range-delete** since child records are contiguous.

Merged indexes store more data than single-table indexes, so individual operations are marginally slower, but experiments show comparable overall update performance.

---

## 6. Spectrum of Pre-computation

| Structure | Update-time workload | Query-time workload | Query coverage |
|---|---|---|---|
| Traditional indexes | Light (1-to-1) | Heavy | Wide |
| **Merged indexes** | **Light (1-to-1)** | **Light (scan + assemble)** | **Moderate** |
| Materialized views | Heavy (1-to-N) | Light | Narrow |

Merged indexes occupy the middle ground: they shift **sorting and clustering** to update time (like a traditional index), leaving only a sequential range scan and record assembly at query time (like a materialized view retrieval). Unlike materialized views, they remain reusable across multiple queries sharing the same sort order.

---

## 7. Key Experimental Results

**(B-tree backend, LeanStore; LSM-tree backend, RocksDB)**

- **Query performance**: Merged indexes match materialized views; for range join queries they outperform them by up to **2×** (materialized views store redundant data, incurring larger scan volume).
- **Update performance**: Merged indexes match traditional single-table indexes. Traditional IVM is significantly slower. DBToaster is marginally faster but requires **10× more memory**.
- **Space**: Merged indexes ≈ traditional indexes (0.37 GiB vs. 0.35 GiB). Materialized join views: 1.57 GiB. DBToaster (with intermediate views): 4.55 GiB.
- **LSM-tree**: Results hold; performance gap with materialized views narrows for range queries due to LSM sequential scan speed.

---

## 8. Trade-offs and Limitations

- **Partial-table access**: Queries touching only a subset of the interleaved tables incur overhead traversing the rest.
- **Low-selectivity inner joins**: Filtering non-matching records adds overhead; future work may partition merged indexes to concentrate inner join candidates.
- **Outer joins**: Supported natively (merged index captures outer join result); materialized inner joins cannot.
- **Query coverage**: Merged indexes are order-specific — reusable only for pipelines sharing the same interesting ordering.
- **Storing views**: The merged index can store aggregate views instead of (or in addition to) base-table records for wider design flexibility; full scope left to future work.

---

## 9. Connection to This Calcite Implementation

The Calcite proof-of-concept demonstrates **PATH A** (substitution): the planner identifies an order-based pipeline

```
EnumerableMergeJoin
  EnumerableSort → EnumerableTableScan(A)
  EnumerableSort → EnumerableTableScan(B)
```

and replaces it wholesale with `EnumerableMergedIndexScan`, which represents the single-pass scan of Algorithm 1 above. The rule (`PipelineToMergedIndexScanRule`) fires when a registered `MergedIndex` covers the tables, and the cost model reflects that all sorts are eliminated (O(N log N) → O(N)) and N table scans collapse to one.

TPC-H tests exercise:
- **Q12**: full 2-table pipeline (ORDERS ⋈ LINEITEM) replacement.
- **Q3**: partial 3-table plan — inner (CUSTOMER ⋈ ORDERS) replaced; outer join with LINEITEM remains.
- **Q3-OL**: inner (ORDERS ⋈ LINEITEM) replaced; outer join with CUSTOMER remains.
- **Q9**: 6-table plan — all qualifying leaf joins replaced simultaneously.
