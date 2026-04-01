# Mar 27 Update

## Background

A cost-based query optimizer that favors **order-based algorithms** and optimizes for interesting orderings produces query plans that naturally decompose into *pipelines*: groups of operators that all share a common sort order. Within each pipeline, a single sort at the input feeds a cascade of joins and aggregations without any additional sorting. Pipeline boundaries are exactly where one sort order ends and another begins.

## Key Idea: Merged Index

Sorts of each boundary can be replaced by a **merged index** — a B-tree that physically stores records from multiple tables interleaved in sorted order. Because the data arrives pre-sorted from the index, the sort is eliminated. More importantly, the *entire pipeline* — sort, join, and aggregation together — collapses to a single sequential scan over the merged index, which assembles and combines records on the fly.

## How to Read the Query Examples

Each query below is shown as a set of plan diagrams. There are two roles a pipeline can play:

- **Root pipeline** — the outermost pipeline that produces the final query result. At query time this executes as an ordinary query plan (a scan or a join over merged index scans). There is no separate maintenance step for the root pipeline because it is not stored.
- **Non-root pipeline** — an inner pipeline whose output is stored in a merged index. It appears in two contexts: building the index initially (*index creation plan*) and keeping it current when base-table rows change (*maintenance plan*).

| | root pipeline | non-root pipeline |
|-|---------------|-------------------|
|data flow| query plan | index creation plan |
|delta flow | N/A | maintenance plan |

The **traditional query plan** (shown first for each query) is what a standard optimizer produces before applying the merged-index substitution.

The **maintenance plan** processes only the *changed rows* (a delta), not the full table. For nested indexes — where an inner merged index feeds an outer one — a base-table delta cascades level by level through the maintenance plans, each step still 1-to-1.

The three queries below range from simple (Q12: 2 tables, 1 non-root pipeline) to complex (Q9: 6 tables, 5 non-root pipelines), demonstrating that the same substitution scales uniformly.

---

## TPC-H Q12

```sql
select l_shipmode,
    sum(case when o_orderpriority ='1-URGENT' or o_orderpriority ='2-HIGH' then 1 else 0 end) as high_line_count,
    sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders, lineitem
where o_orderkey = l_orderkey
    and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '[DATE]'
    and l_receiptdate < date '[DATE]' + interval '1' year
group by l_shipmode
order by l_shipmode;
```

### Traditional Query Plan

![q12/before-pipeline](./q12/before-pipeline_color.png)

| | root pipeline | non-root pipeline |
|-|---------------|-------------------|
|data flow| query plan | index creation plan |
|delta flow | N/A | maintenance plan |

### Query Plan (for pipeline 1)

![q12/root-pipeline-query-plan](./q12/root-pipeline-query-plan_color.png)

### Index creation plan (for pipeline 0 only)

![q12/pipeline-0-index-creation-plan](./q12/pipeline-0-index-creation-plan_color.png)

### Maintenance plan (for pipeline 0 only)

![q12/maintenance-tree](./q12/maintenance-tree_color.png)

Outer join

Galindo Lagaria

## TPCH Q3

```sql
select l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from customer, orders, lineitem
where c_mktsegment = '[SEGMENT]'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '[DATE]'
    and l_shipdate > date '[DATE]'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate;
```

### Traditional Query Plan

![q3ol/before-pipeline](./q3ol/before-pipeline_color.png)

### Query Plan (for pipeline 1)

![q3ol/root-pipeline-query-plan](./q3ol/root-pipeline-query-plan_color.png)

### Index Creation Plan (for pipeline 0)

![q3ol/leaf-1-index-creation-plan](./q3ol/leaf-1-index-creation-plan_color.png)

### Maintenance Plan (for pipeline 0)

![q3ol/maintenance-0-tree](./q3ol/maintenance-0-tree_color.png)

\delta Orders \join lineitem \union \delta Lineitem \join Orders(new)

## TPCH Q9

```sql
select nation, o_year, sum(amount) as sum_profit
from ( select n_name as nation,
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from part, supplier, lineitem, partsupp, orders, nation
    where s_suppkey = l_suppkey 
        and ps_suppkey = l_suppkey 
        and ps_partkey = l_partkey 
        and p_partkey = l_partkey 
        and o_orderkey = l_orderkey
        and s_nationkey = n_nationkey
        and p_name like '%[COLOR]%'
) as profit
group by nation, o_year
order by nation, o_year desc;
```

### Traditional Query Plan

![q9/before-pipeline](./q9/before-pipeline_color.png)

partkey or suppkey first (query vs maintenance)

shared sort order between (partkey, suppkey) and partkey

| | root pipeline | non-root pipeline |
|-|---------------|-------------------|
|data flow| query plan | index creation plan |
|delta flow | N/A | maintenance plan |

### Query Plan (for root pipeline)

![q9/root-pipeline-query-plan](./q9/root-pipeline-query-plan_color.png)



### Index Creation Plans (for non-root pipelines)

![q9/branch-1-index-creation-plan](./q9/branch-1-index-creation-plan_color.png)

![q9/branch-2-index-creation-plan](./q9/branch-2-index-creation-plan_color.png)

![q9/branch-3-index-creation-plan](./q9/branch-3-index-creation-plan_color.png)

![q9/branch-4-index-creation-plan](./q9/branch-4-index-creation-plan_color.png)

![q9/leaf-5-index-creation-plan](./q9/leaf-5-index-creation-plan_color.png)

### Maintenance Plans (for non-root pipelines)

![q9/maintenance-0-tree](./q9/maintenance-0-tree_color.png)

![q9/maintenance-1-tree](./q9/maintenance-1-tree_color.png)

![q9/maintenance-2-tree](./q9/maintenance-2-tree_color.png)

![q9/maintenance-3-tree](./q9/maintenance-3-tree_color.png)

![q9/maintenance-4-tree](./q9/maintenance-4-tree_color.png)

## Query Performance

Compared with materialized views, query execution with merged indexes require the record assembly of the last pipeline.

## Maintenance Performance

- Traditional IVM: Updates must propagate through the entire query.
- DBToaster: Updates propagate through the entire query and update intermediate views along the way. Especially helps with the case where a full intermediate view is required (to be joined with the delta).
- Merged index: Updates propagate through the entire query and update intermediate merged indexes along the way. When a full intermediate view is required (e.g. outcome of pipeline1), we just scan pipeline1.mergedIndex and process the operators of pipeline1, which is similar to scanning a fully computed intermediate view.

Propagate until query: large merge fan-in

---- Multi-level propagation (LSM compaction) 

---- Propagate at updates: fresh up-to-date merged indexes
