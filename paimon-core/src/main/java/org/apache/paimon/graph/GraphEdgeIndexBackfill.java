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
        if (snapshotId == null) {
            return new ArrayList<>();
        }
        return generate(
                database,
                baseTableName,
                baseSchema,
                "scan.snapshot-id",
                String.valueOf(snapshotId));
    }

    /**
     * Generates the incremental maintenance statements of one micro-batch (design §7.2/§7.3): the
     * same aggregation as the backfill, restricted to the snapshot interval {@code (from, to]}
     * through the {@code OPTIONS('incremental-between'...)} hint. Every statement is idempotent
     * relative to the coverage watermark contract: the caller advances the watermark only after all
     * statements committed.
     */
    public static List<String> generateIncrementalSql(
            String database,
            String baseTableName,
            TableSchema baseSchema,
            long fromExclusive,
            long toInclusive) {
        if (toInclusive <= fromExclusive) {
            return new ArrayList<>();
        }
        return generate(
                database,
                baseTableName,
                baseSchema,
                "incremental-between",
                fromExclusive + "," + toInclusive);
    }

    private static List<String> generate(
            String database,
            String baseTableName,
            TableSchema baseSchema,
            String hintKey,
            String hintValue) {
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
        List<String> sqls = new ArrayList<>();
        sqls.add(
                one(
                        database,
                        baseTableName,
                        plan.edgeTableName(),
                        plan.edge(),
                        decl,
                        hintKey,
                        hintValue));
        if (plan.reverse()) {
            sqls.add(
                    one(
                            database,
                            baseTableName,
                            plan.edgeRevTableName(),
                            plan.edgeRev(),
                            decl,
                            hintKey,
                            hintValue));
        }
        return sqls;
    }

    private static String one(
            String database,
            String baseTableName,
            String indexTableName,
            TableSchema indexSchema,
            GraphEdgeIndexDecl decl,
            String hintKey,
            String hintValue) {
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
            // cast to the expanded column type: SUM(DECIMAL(p, s)) widens the precision on some
            // engines and the sink would otherwise reject the insert
            select.add(
                    "CAST("
                            + expr
                            + " AS "
                            + typeOf(indexSchema, column).asSQLString()
                            + ") AS "
                            + quoted(column));
        }
        return String.format(
                "INSERT INTO `%s`.`%s`\n"
                        + "SELECT %s\n"
                        + "FROM `%s`.`%s` /*+ OPTIONS('%s'='%s') */\n"
                        + "GROUP BY %s",
                database,
                indexTableName,
                String.join(", ", select),
                database,
                baseTableName,
                hintKey,
                hintValue,
                indexSchema.primaryKeys().stream()
                        .map(GraphEdgeIndexBackfill::quoted)
                        .collect(Collectors.joining(", ")));
    }

    private static org.apache.paimon.types.DataType typeOf(TableSchema indexSchema, String column) {
        for (org.apache.paimon.types.DataField field : indexSchema.fields()) {
            if (field.name().equals(column)) {
                return field.type();
            }
        }
        throw new IllegalArgumentException(
                "Expanded adjacency column '" + column + "' is missing from the index schema.");
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
