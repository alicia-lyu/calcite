<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core)
[![CI Status](https://github.com/apache/calcite/workflows/CI/badge.svg?branch=main)](https://github.com/apache/calcite/actions?query=branch%3Amain)

# Apache Calcite

## Merged-index research fork

This fork explores rule-based plans for queries with multiple order-sharing
pipelines, extending the [published single-pipeline study](https://www.vldb.org/pvldb/vol19/p3621-lyu.pdf).
Calcite serves as a reproducible planning artifact. Future query execution will
be implemented manually in LeanStore and may adapt the reference plans;
automatic integration and MI-aware cost optimization are deferred.

Start with [current status and TPC-H coverage](SESSION_PROGRESS.md),
[research and agent guidance](CLAUDE.md), and the
[manual LeanStore handoff](CALCITE_LEANSTORE_INTEGRATION.md).
The existing examples validate plan structure, not complete benchmark results
or executable cascade maintenance.

## Upstream project

Apache Calcite is a dynamic data management framework.

It contains many of the pieces that comprise a typical
database management system but omits the storage primitives.
It provides an industry standard SQL parser and validator,
a customisable optimizer with pluggable rules and cost functions,
logical and physical algebraic operators, various transformation
algorithms from SQL to algebra (and the opposite), and many
adapters for executing SQL queries over Cassandra, Druid,
Elasticsearch, MongoDB, Kafka, and others, with minimal
configuration.

For more details, see the [home page](http://calcite.apache.org).

The project uses [JIRA](https://issues.apache.org/jira/browse/CALCITE)
for issue tracking. For further information, please see the [JIRA accounts guide](https://calcite.apache.org/develop/#jira-accounts).
