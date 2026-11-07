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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Parsed graph index declarations ({@code graph.vertex.*} / {@code graph.edge.*} table options) of
 * one table.
 *
 * <p>This class only parses declaration syntax; semantic validation against the table schema is
 * performed by the graph index validator in paimon-core. A malformed declaration value throws
 * {@link IllegalArgumentException} with an option-specific message.
 */
@Public
public class GraphIndexDecls implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Optional<GraphVertexIndexDecl> vertex;
    private final Optional<GraphEdgeIndexDecl> edge;

    private GraphIndexDecls(
            Optional<GraphVertexIndexDecl> vertex, Optional<GraphEdgeIndexDecl> edge) {
        this.vertex = vertex;
        this.edge = edge;
    }

    public static GraphIndexDecls from(TableSchema schema) {
        return from(new Options(schema.options()));
    }

    public static GraphIndexDecls from(Options options) {
        Optional<GraphVertexIndexDecl> vertex = parseVertex(options);
        Optional<GraphEdgeIndexDecl> edge = parseEdge(options);
        return new GraphIndexDecls(vertex, edge);
    }

    public boolean isEmpty() {
        return !vertex.isPresent() && !edge.isPresent();
    }

    public Optional<GraphVertexIndexDecl> vertex() {
        return vertex;
    }

    public Optional<GraphEdgeIndexDecl> edge() {
        return edge;
    }

    // --------------------------------------------------------------------------------------------
    // Vertex parsing
    // --------------------------------------------------------------------------------------------

    private static Optional<GraphVertexIndexDecl> parseVertex(Options options) {
        if (!options.get(GraphIndexOptions.GRAPH_VERTEX_ENABLED)) {
            return Optional.empty();
        }

        String idField = options.get(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD);
        if (StringUtils.isNullOrWhitespaceOnly(idField)) {
            throw new IllegalArgumentException(
                    "Option 'graph.vertex.id-field' must be set when 'graph.vertex.enabled' is true.");
        }

        List<GraphVertexIndexDecl.NodeMapField> nodeMapFields = parseNodeMapFields(options);

        String encoding = options.get(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_ENCODING);
        if (!GraphIndexOptions.ENCODING_ORDERED_BINARY.equals(encoding)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported node-map encoding '%s' in '%s', currently only '%s' is supported.",
                            encoding,
                            GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_ENCODING.key(),
                            GraphIndexOptions.ENCODING_ORDERED_BINARY));
        }

        String updateAction = options.get(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_UPDATE_ACTION);
        if (!GraphIndexOptions.UPDATE_ACTION_MAINTAIN.equals(updateAction)
                && !GraphIndexOptions.UPDATE_ACTION_FREEZE.equals(updateAction)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid node-map update action '%s' in '%s', expected '%s' or '%s'.",
                            updateAction,
                            GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_UPDATE_ACTION.key(),
                            GraphIndexOptions.UPDATE_ACTION_MAINTAIN,
                            GraphIndexOptions.UPDATE_ACTION_FREEZE));
        }

        return Optional.of(
                new GraphVertexIndexDecl(idField.trim(), nodeMapFields, encoding, updateAction));
    }

    private static List<GraphVertexIndexDecl.NodeMapField> parseNodeMapFields(Options options) {
        String raw = options.get(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS);
        if (StringUtils.isNullOrWhitespaceOnly(raw)) {
            return new ArrayList<>();
        }

        Map<String, GraphVertexIndexDecl.NodeMapField> result = new LinkedHashMap<>();
        for (String token : raw.split(",", -1)) {
            String entry = token.trim();
            if (entry.isEmpty()) {
                throw new IllegalArgumentException(
                        "Wrong option 'graph.vertex.node-map.fields': should not contain empty fields.");
            }

            String[] parts = entry.split(":");
            if (parts.length > 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wrong option 'graph.vertex.node-map.fields': entry '%s' should be 'field' or 'field:family'.",
                                entry));
            }

            String field = parts[0].trim();
            String family =
                    parts.length == 2
                            ? parts[1].trim().toLowerCase(Locale.ROOT)
                            : GraphVertexIndexDecl.FAMILY_BTREE;
            if (!GraphVertexIndexDecl.FAMILY_BTREE.equals(family)
                    && !GraphVertexIndexDecl.FAMILY_BITMAP.equals(family)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown node-map index family '%s' for field '%s' in 'graph.vertex.node-map.fields', expected '%s' or '%s'.",
                                parts.length == 2 ? parts[1].trim() : family,
                                field,
                                GraphVertexIndexDecl.FAMILY_BTREE,
                                GraphVertexIndexDecl.FAMILY_BITMAP));
            }

            if (result.containsKey(field)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Duplicate field '%s' in 'graph.vertex.node-map.fields'.", field));
            }
            result.put(field, new GraphVertexIndexDecl.NodeMapField(field, family));
        }
        return new ArrayList<>(result.values());
    }

    // --------------------------------------------------------------------------------------------
    // Edge parsing
    // --------------------------------------------------------------------------------------------

    private static Optional<GraphEdgeIndexDecl> parseEdge(Options options) {
        if (!options.get(GraphIndexOptions.GRAPH_EDGE_ENABLED)) {
            return Optional.empty();
        }

        String srcField = requireEdgeOption(options, GraphIndexOptions.GRAPH_EDGE_SRC_FIELD);
        String dstField = requireEdgeOption(options, GraphIndexOptions.GRAPH_EDGE_DST_FIELD);
        String srcVertex = requireEdgeOption(options, GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX);
        String dstVertex = requireEdgeOption(options, GraphIndexOptions.GRAPH_EDGE_DST_VERTEX);
        boolean reverse = options.get(GraphIndexOptions.GRAPH_EDGE_REVERSE);
        List<GraphEdgeAggregate> aggregates =
                parseAggregates(options.get(GraphIndexOptions.GRAPH_EDGE_AGGREGATES));

        return Optional.of(
                new GraphEdgeIndexDecl(
                        srcField, dstField, srcVertex, dstVertex, reverse, aggregates));
    }

    private static String requireEdgeOption(Options options, ConfigOption<String> option) {
        String value = options.get(option);
        if (StringUtils.isNullOrWhitespaceOnly(value)) {
            throw new IllegalArgumentException(
                    "Option '" + option.key() + "' must be set when 'graph.edge.enabled' is true.");
        }
        return value.trim();
    }

    private static List<GraphEdgeAggregate> parseAggregates(@Nullable String raw) {
        if (StringUtils.isNullOrWhitespaceOnly(raw)) {
            throw new IllegalArgumentException(
                    "Option 'graph.edge.aggregates' must not be empty when 'graph.edge.enabled' is true.");
        }

        Map<String, GraphEdgeAggregate> result = new LinkedHashMap<>();
        for (String token : raw.split(";", -1)) {
            String entry = token.trim();
            if (entry.isEmpty()) {
                throw new IllegalArgumentException(
                        "Wrong option 'graph.edge.aggregates': should not contain empty aggregate entries.");
            }

            String[] parts = entry.split(":", -1);
            if (parts.length < 2 || parts.length > 3) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wrong option 'graph.edge.aggregates': entry '%s' should be 'field:function[:alias]'.",
                                entry));
            }

            String field = parts[0].trim();
            String function = parts[1].trim().toLowerCase(Locale.ROOT);
            String alias = parts.length == 3 ? parts[2].trim() : null;
            if (field.isEmpty() || function.isEmpty() || (alias != null && alias.isEmpty())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wrong option 'graph.edge.aggregates': entry '%s' should be 'field:function[:alias]'.",
                                entry));
            }

            if (GraphEdgeAggregate.WILDCARD.equals(field)
                    && !GraphEdgeAggregate.COUNT.equals(function)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Wrong option 'graph.edge.aggregates': wildcard '*' only supports aggregate function 'count', but got '%s'.",
                                function));
            }

            if (result.containsKey(field)) {
                throw new IllegalArgumentException(
                        String.format("Duplicate field '%s' in 'graph.edge.aggregates'.", field));
            }
            result.put(field, new GraphEdgeAggregate(field, function, alias));
        }
        return new ArrayList<>(result.values());
    }
}
