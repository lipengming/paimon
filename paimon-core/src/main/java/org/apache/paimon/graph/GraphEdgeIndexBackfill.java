/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.graph;

import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Generates the batch backfill SQL statements of the adjacency index tables (design §7.1): one
 * {@code INSERT ... SELECT ... GROUP BY} per planned index table, reading the base table at exactly
 * one snapshot through the {@code OPTIONS('scan.snapshot-id'...)} hint.
 *
 * <p>The batch {@code COUNT(*)} and the streaming "write 1 per event" are equivalent under the sum
 * aggregation ({@code sum(1) = count}), matching the {@code '*:count'} expansion. {@code
 * last_non_null_value} is expressed as {@code MAX} in batch SQL, the equivalence the design doc
 * adopts for monotonic-ish event time columns.
 */
public final class GraphEdgeIndexBackfill {

    private GraphEdgeIndexBackfill() {}

    /**
     * Generates one backfill SQL statement per planned adjacency table. Returns an empty list when
     * the base table has no snapshot yet (nothing to backfill).
     */
    public static List<String> generateSql(
            String database, String baseTableName, TableSchema baseSchema, Long snapshotId) {
        List<String> sqls = new ArrayList<>();
        if (snapshotId == null) {
            return sqls;
        }
        GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan(baseTableName, baseSchema);
        GraphEdgeIndexDecl decl =
                GraphIndexDecls.from(baseSchema)
                        .edge()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Table '%s' has no graph edge declaration.",
                                                        baseTableName)));
        sqls.add(one(database, baseTableName, plan.edgeTableName(), plan.edge(), decl, snapshotId));
        if (plan.reverse()) {
            sqls.add(
                    one(
                            database,
                            baseTableName,
                            plan.edgeRevTableName(),
                            plan.edgeRev(),
                            decl,
                            snapshotId));
        }
        return sqls;
    }

    private static String one(
            String database,
            String baseTableName,
            String indexTableName,
            TableSchema indexSchema,
            GraphEdgeIndexDecl decl,
            long snapshotId) {
        List<String> select = new ArrayList<>();
        for (String endpoint : indexSchema.primaryKeys()) {
            select.add(quoted(endpoint));
        }
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            String column = GraphEdgeIndexPlanner.aggregateColumn(aggregate);
            String expr =
                    aggregate.isWildcard()
                            ? "COUNT(*)"
                            : sqlFunction(aggregate.function())
                                    + "("
                                    + quoted(aggregate.field())
                                    + ")";
            select.add(expr + " AS " + quoted(column));
        }
        return String.format(
                "INSERT INTO `%s`.`%s`\n"
                        + "SELECT %s\n"
                        + "FROM `%s`.`%s` /*+ OPTIONS('scan.snapshot-id'='%d') */\n"
                        + "GROUP BY %s",
                database,
                indexTableName,
                String.join(", ", select),
                database,
                baseTableName,
                snapshotId,
                indexSchema.primaryKeys().stream()
                        .map(GraphEdgeIndexBackfill::quoted)
                        .collect(Collectors.joining(", ")));
    }

    private static String sqlFunction(String function) {
        switch (function) {
            case "sum":
                return "SUM";
            case "min":
                return "MIN";
            case "max":
                return "MAX";
            case "last_non_null_value":
            case "last_not_null_value":
                return "MAX";
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Aggregate function '%s' cannot be expressed in batch backfill SQL"
                                        + " yet; supported functions are sum, min, max and"
                                        + " last_non_null_value.",
                                function));
        }
    }

    private static String quoted(String identifier) {
        return "`" + identifier + "`";
    }
}
