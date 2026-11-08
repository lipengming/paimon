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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Versioned planner expanding a graph edge declaration into its physical adjacency index tables.
 *
 * <p>For an edge event table the planner derives the target {@link TableSchema} of the forward
 * adjacency table (base table name plus the {@code __edge} suffix, primary key (src, dst)) and,
 * when 'graph.edge.reverse' is true, of the reverse one (suffix {@code __edge_rev}, primary key
 * (dst, src)). Both are unpartitioned fixed-bucket aggregation primary-key tables:
 *
 * <ul>
 *   <li>'*:count' expands into an explicit {@code cnt BIGINT} column aggregated by {@code sum}
 *       (each event contributes 1), because the aggregation merge engine has no count function;
 *   <li>every other 'field:function[:alias]' entry maps to the official {@code
 *       fields.<col>.aggregate-function} option on a column named by the alias or {@code
 *       <function>_<field>}, keeping the base column type;
 *   <li>the bucket key is the leading endpoint (src forward, dst reverse), so one adjacency scan is
 *       a single-bucket primary-key prefix scan.
 * </ul>
 *
 * <p>This planner only derives schemas. Creating and backfilling the tables is the job of the graph
 * index create pipeline. Expansion rules are versioned: changing them is a graph index version
 * evolution, the user declaration stays untouched.
 */
public final class GraphEdgeIndexPlanner {

    /** Suffix of the forward adjacency index table. */
    public static final String EDGE_TABLE_SUFFIX = "__edge";

    /** Suffix of the reverse adjacency index table. */
    public static final String EDGE_REV_TABLE_SUFFIX = "__edge_rev";

    /** Default column expanded from '*:count' unless an explicit alias is declared. */
    public static final String COUNT_COLUMN = "cnt";

    /**
     * Fixed bucket count of the adjacency index tables. Buckets hold full-history accumulations
     * (the tables are unpartitioned), so the default follows the design guidance of 200MB~2GB per
     * bucket; tuning with the create pipeline arrives later.
     */
    public static final int DEFAULT_EDGE_BUCKETS = 1024;

    private static final String AGGREGATION_MERGE_ENGINE = "aggregation";
    private static final String CHANGELOG_PRODUCER_NONE = "none";
    private static final String SUM = "sum";
    private static final String LAST_NON_NULL_VALUE = "last_non_null_value";

    private GraphEdgeIndexPlanner() {}

    /** Name of the forward adjacency index table of the given base table. */
    public static String edgeTableName(String baseTableName) {
        return baseTableName + EDGE_TABLE_SUFFIX;
    }

    /** Name of the reverse adjacency index table of the given base table. */
    public static String edgeRevTableName(String baseTableName) {
        return baseTableName + EDGE_REV_TABLE_SUFFIX;
    }

    /** The physical adjacency column of one aggregate entry: alias wins, wildcard maps to cnt. */
    public static String aggregateColumn(GraphEdgeAggregate aggregate) {
        if (aggregate.alias() != null) {
            return aggregate.alias();
        }
        return aggregate.isWildcard()
                ? COUNT_COLUMN
                : aggregate.function() + "_" + aggregate.field();
    }

    /**
     * Expands the 'graph.edge.*' declaration of the base schema into the adjacency index table
     * schemas. The declaration is expected to be schema-validated already ({@link
     * org.apache.paimon.schema.SchemaValidation}).
     */
    public static GraphEdgeIndexPlan plan(String baseTableName, TableSchema baseSchema) {
        GraphEdgeIndexDecl edge = GraphIndexDecls.from(baseSchema).edge().orElse(null);
        checkArgument(
                edge != null,
                "Table '%s' is not declared as a graph edge: option 'graph.edge.enabled' is not"
                        + " set.",
                baseTableName);
        return new GraphEdgeIndexPlan(
                baseTableName,
                expand(baseTableName, baseSchema, edge, false),
                edge.reverse() ? expand(baseTableName, baseSchema, edge, true) : null);
    }

    private static TableSchema expand(
            String baseTableName,
            TableSchema baseSchema,
            GraphEdgeIndexDecl edge,
            boolean reverse) {
        // Leading endpoint clusters adjacency: (src, dst) forward, (dst, src) reverse.
        String leadingField = reverse ? edge.dstField() : edge.srcField();
        String trailingField = reverse ? edge.srcField() : edge.dstField();

        Map<String, DataType> fieldTypes = new HashMap<>();
        for (DataField field : baseSchema.fields()) {
            fieldTypes.put(field.name(), field.type());
        }

        List<DataField> columns = new ArrayList<>();
        Set<String> usedNames = new HashSet<>();
        Map<String, String> options = new LinkedHashMap<>();

        addColumn(columns, usedNames, leadingField, fieldTypes.get(leadingField));
        addColumn(columns, usedNames, trailingField, fieldTypes.get(trailingField));

        for (GraphEdgeAggregate aggregate : edge.aggregates()) {
            String function;
            DataType type;
            if (aggregate.isWildcard()) {
                type = DataTypes.BIGINT();
                function = SUM;
            } else {
                checkArgument(
                        !GraphEdgeAggregate.COUNT.equals(aggregate.function()),
                        "Aggregate function 'count' in 'graph.edge.aggregates' only supports the"
                                + " wildcard field '*', but got field '%s'.",
                        aggregate.field());
                type = fieldTypes.get(aggregate.field());
                checkArgument(
                        type != null,
                        "Column '%s' in 'graph.edge.aggregates' does not exist in table schema.",
                        aggregate.field());
                function = aggregate.function();
            }
            String column = aggregateColumn(aggregate);

            checkArgument(
                    !usedNames.contains(column),
                    "Aggregate column '%s' expanded from 'graph.edge.aggregates' conflicts with"
                            + " another column of the adjacency index tables of table '%s'.",
                    column,
                    baseTableName);
            addColumn(columns, usedNames, column, type);
            options.put(fieldAggregateFunction(column), function);
        }

        options.put(CoreOptions.MERGE_ENGINE.key(), AGGREGATION_MERGE_ENGINE);
        options.put(defaultAggregateFunction(), LAST_NON_NULL_VALUE);
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), CHANGELOG_PRODUCER_NONE);
        options.put(CoreOptions.BUCKET.key(), String.valueOf(DEFAULT_EDGE_BUCKETS));
        options.put(CoreOptions.BUCKET_KEY.key(), leadingField);

        return new TableSchema(
                0L,
                columns,
                columns.size() - 1,
                Collections.<String>emptyList(),
                Arrays.asList(leadingField, trailingField),
                options,
                String.format(
                        "Graph edge %s adjacency index expanded from table '%s'.",
                        reverse ? "reverse" : "forward", baseTableName));
    }

    private static void addColumn(
            List<DataField> columns, Set<String> usedNames, String name, DataType type) {
        columns.add(new DataField(columns.size(), name, type));
        usedNames.add(name);
    }

    private static String fieldAggregateFunction(String column) {
        return CoreOptions.FIELDS_PREFIX + "." + column + "." + CoreOptions.AGG_FUNCTION;
    }

    private static String defaultAggregateFunction() {
        return CoreOptions.FIELDS_PREFIX + "." + CoreOptions.DEFAULT_AGG_FUNCTION;
    }
}
