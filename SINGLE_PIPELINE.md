# Single-Pipeline Merged Index Notes

This document is historical context from the published single-pipeline phase.
The active next phase is multiple-pipeline planning and manual LeanStore C++
implementation. Do not treat this file as the current roadmap.

## Historical Scope

The VLDB paper compares:

1. traditional single-table indexes plus query-time joins
2. materialized views
3. one merged index covering one order-sharing pipeline
4. hash-join baselines

In that setting, a query may contain one useful MI pipeline while the rest of the
query executes conventionally. This is different from the current multi-pipeline
Calcite artifact, where nested pipeline descriptors can be registered and
substituted level by level.

## Candidate Enumeration

`testkit/src/main/java/org/apache/calcite/test/SingleMIPipelineIdentifier.java`
enumerates conceptual single-MI candidates from a logical plan:

1. collect sort requirements from joins, aggregates, and ORDER BY
2. build equivalence classes from equi-join predicates
3. consolidate prefix-compatible collations
4. enumerate globally consistent table/key subsets
5. rank candidates, currently by table count

These candidates are useful background for choosing manual LeanStore plans, but
they are not a complete optimizer.

## Historical Results

| Query | Candidates found in historical tests | Notes |
|-------|--------------------------------------|-------|
| Q12 | `{ORDERS, LINEITEM}` by orderkey | Single-MI substitution test existed |
| Q3 | `{CUSTOMER, ORDERS}` by custkey and `{ORDERS, LINEITEM}` by orderkey | No 3-table candidate without modeling extended keys |
| Q9 | `{LINEITEM, PART, PARTSUPP}` and `{LINEITEM, PARTSUPP, SUPPLIER}` style candidates | Tie-breaking needed better cardinality metrics |

Older notes mention Q5 and Q7 as next candidates. Those are no longer the active
documentation target; the current plan is a broader query-by-query worksheet for
manual LeanStore work.

## Relationship to Multi-Pipeline Work

The single-MI candidate finder is still useful for:

- identifying order-sharing subsets inside a larger query
- explaining why independent keys need separate pipelines
- comparing one-MI and cascade-style designs

It does not answer:

- how to maintain nested MIs
- how to share one physical scan across logical source scans
- whether final ordering survives substitution
- which LeanStore physical key layout is best

Use [SESSION_PROGRESS.md](SESSION_PROGRESS.md) for current status and next steps.
