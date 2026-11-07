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
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Validates graph index declarations ({@code graph.vertex.*} / {@code graph.edge.*} table options)
 * against the table schema.
 *
 * <p>Declaration syntax is checked while parsing ({@link GraphIndexDecls}); this class adds the
 * schema-aware rules: vertex tables must be single-column primary-key tables whose id field is the
 * primary key, edge tables must be append event tables whose endpoint and aggregate fields exist,
 * aggregate functions must be legal Paimon aggregate functions compatible with the column types,
 * and runtime options must use allowed values.
 */
public final class GraphIndexValidator {

    private GraphIndexValidator() {}

    /** Validates graph declarations of the schema; a no-op for tables without graph options. */
    public static void validate(TableSchema schema) {
        GraphIndexDecls decls = GraphIndexDecls.from(schema);
        if (decls.isEmpty()) {
            return;
        }

        checkArgument(
                !(decls.vertex().isPresent() && decls.edge().isPresent()),
                "A table cannot be declared as both a graph vertex ('graph.vertex.enabled') and"
                        + " a graph edge ('graph.edge.enabled').");

        decls.vertex().ifPresent(decl -> validateVertex(schema, decl));
        decls.edge().ifPresent(decl -> validateEdge(schema, decl));
        validateRuntimeOptions(schema);
    }

    private static void validateVertex(TableSchema schema, GraphVertexIndexDecl vertex) {
        List<String> primaryKeys = schema.primaryKeys();
        checkArgument(
                !primaryKeys.isEmpty(),
                "Option 'graph.vertex.enabled' requires a primary-key table.");
        checkArgument(
                primaryKeys.size() == 1,
                "Option 'graph.vertex.enabled' requires a single-column primary key (the node id"
                        + " must be stable), but got %s.",
                primaryKeys);
        checkArgument(
                primaryKeys.get(0).equals(vertex.idField()),
                "Option 'graph.vertex.id-field' is '%s', but it must be the primary key column '%s'.",
                vertex.idField(),
                primaryKeys.get(0));

        Set<String> fieldNames = new HashSet<>(schema.fieldNames());
        for (GraphVertexIndexDecl.NodeMapField nodeMapField : vertex.nodeMapFields()) {
            checkArgument(
                    fieldNames.contains(nodeMapField.field()),
                    "Column '%s' in 'graph.vertex.node-map.fields' does not exist in table schema."
                            + " Existing columns: %s.",
                    nodeMapField.field(),
                    schema.fieldNames());
        }
    }

    private static void validateEdge(TableSchema schema, GraphEdgeIndexDecl edge) {
        checkArgument(
                schema.primaryKeys().isEmpty(),
                "Option 'graph.edge.enabled' requires a table without primary keys (an append"
                        + " event table), but got primary keys %s.",
                schema.primaryKeys());

        Set<String> fieldNames = new HashSet<>(schema.fieldNames());
        checkArgument(
                fieldNames.contains(edge.srcField()),
                "Column '%s' in 'graph.edge.src-field' does not exist in table schema. Existing"
                        + " columns: %s.",
                edge.srcField(),
                schema.fieldNames());
        checkArgument(
                fieldNames.contains(edge.dstField()),
                "Column '%s' in 'graph.edge.dst-field' does not exist in table schema. Existing"
                        + " columns: %s.",
                edge.dstField(),
                schema.fieldNames());

        Map<String, DataType> fieldTypes = new HashMap<>();
        for (DataField field : schema.fields()) {
            fieldTypes.put(field.name(), field.type());
        }

        CoreOptions coreOptions = new CoreOptions(schema.options());
        for (GraphEdgeAggregate aggregate : edge.aggregates()) {
            if (aggregate.isWildcard() || GraphEdgeAggregate.COUNT.equals(aggregate.function())) {
                // 'count' is graph-level semantics: the planner expands it into an explicit cnt
                // column aggregated by 'sum', because the aggregation merge engine has no count
                // function.
                continue;
            }
            checkArgument(
                    fieldTypes.containsKey(aggregate.field()),
                    "Column '%s' in 'graph.edge.aggregates' does not exist in table schema."
                            + " Existing columns: %s.",
                    aggregate.field(),
                    schema.fieldNames());
            try {
                FieldAggregatorFactory.create(
                        fieldTypes.get(aggregate.field()),
                        aggregate.field(),
                        aggregate.function(),
                        coreOptions);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid aggregate '%s' for column '%s' in 'graph.edge.aggregates': %s",
                                aggregate.function(), aggregate.field(), e.getMessage()),
                        e);
            }
        }
    }

    private static void validateRuntimeOptions(TableSchema schema) {
        Options options = new Options(schema.options());
        String backend = options.get(GraphIndexOptions.GRAPH_INDEX_BACKEND);
        checkArgument(
                GraphIndexOptions.BACKEND_AUTO.equals(backend)
                        || GraphIndexOptions.BACKEND_PK_INDEX.equals(backend)
                        || GraphIndexOptions.BACKEND_TABLE.equals(backend),
                "Option '%s' must be one of [%s, %s, %s], but was '%s'.",
                GraphIndexOptions.GRAPH_INDEX_BACKEND.key(),
                GraphIndexOptions.BACKEND_AUTO,
                GraphIndexOptions.BACKEND_PK_INDEX,
                GraphIndexOptions.BACKEND_TABLE,
                backend);

        String searchMode = options.get(GraphIndexOptions.GRAPH_INDEX_SEARCH_MODE);
        checkArgument(
                GraphIndexOptions.SEARCH_MODE_FAST.equals(searchMode)
                        || GraphIndexOptions.SEARCH_MODE_FULL.equals(searchMode)
                        || GraphIndexOptions.SEARCH_MODE_DETAIL.equals(searchMode),
                "Option '%s' must be one of [%s, %s, %s], but was '%s'.",
                GraphIndexOptions.GRAPH_INDEX_SEARCH_MODE.key(),
                GraphIndexOptions.SEARCH_MODE_FAST,
                GraphIndexOptions.SEARCH_MODE_FULL,
                GraphIndexOptions.SEARCH_MODE_DETAIL,
                searchMode);
    }
}
