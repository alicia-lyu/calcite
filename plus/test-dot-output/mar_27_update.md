# Mar 27 Update

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
