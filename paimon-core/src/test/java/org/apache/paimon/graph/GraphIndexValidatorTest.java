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
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests of graph index declaration validation through {@link
 * SchemaValidation#validateTableSchema}.
 */
class GraphIndexValidatorTest {

    // --------------------------------------------------------------------------------------------
    // Vertex declarations
    // --------------------------------------------------------------------------------------------

    @Test
    void testValidVertex() {
        TableSchema schema = vertexSchema("name,phone,city:bitmap", "id");
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(schema));
    }

    @Test
    void testVertexWithoutNodeMapFields() {
        Map<String, String> options = vertexOptions("id");
        TableSchema schema =
                schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        emptyList(),
                        singletonList("id"),
                        options);
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(schema));
    }

    @Test
    void testVertexRequiresPrimaryKeyTable() {
        // no fixed bucket here: an append table with 'bucket' > 0 fails earlier on bucket-key
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(), "id");
        TableSchema schema =
                schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        emptyList(),
                        emptyList(),
                        options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Option 'graph.vertex.enabled' requires a primary-key table.");
    }

    @Test
    void testVertexRequiresSingleColumnPrimaryKey() {
        Map<String, String> options = vertexOptions("id");
        TableSchema schema =
                schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        emptyList(),
                        Arrays.asList("id", "name"),
                        options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.vertex.enabled' requires a single-column primary key (the"
                                + " node id must be stable), but got [id, name].");
    }

    @Test
    void testVertexIdFieldMustBePrimaryKey() {
        Map<String, String> options = vertexOptions("phone");
        TableSchema schema = vertexSchemaFields(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.vertex.id-field' is 'phone', but it must be the primary key column 'id'.");
    }

    @Test
    void testVertexNodeMapFieldNotExist() {
        Map<String, String> options = vertexOptions("id");
        options.put(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), "name,addr");
        TableSchema schema = vertexSchemaFields(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Column 'addr' in 'graph.vertex.node-map.fields' does not exist in table schema.");
    }

    // --------------------------------------------------------------------------------------------
    // Edge declarations
    // --------------------------------------------------------------------------------------------

    @Test
    void testValidEdge() {
        Map<String, String> options = edgeOptions("amount:sum;tx_time:last_non_null_value;*:count");
        TableSchema schema = edgeSchema(options);
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(schema));
    }

    @Test
    void testEdgeOnPrimaryKeyTable() {
        Map<String, String> options = edgeOptions("*:count");
        TableSchema schema = schema(edgeFields(), emptyList(), singletonList("src_id"), options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.edge.enabled' requires a table without primary keys (an append"
                                + " event table), but got primary keys [src_id].");
    }

    @Test
    void testEdgeEndpointFieldNotExist() {
        Map<String, String> options = edgeOptions("*:count");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "target_id");
        TableSchema schema = edgeSchema(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Column 'target_id' in 'graph.edge.dst-field' does not exist in table schema.");
    }

    @Test
    void testEdgeUnknownAggregateFunction() {
        Map<String, String> options = edgeOptions("amount:countt");
        TableSchema schema = edgeSchema(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid aggregate 'countt' for column 'amount'")
                .hasMessageContaining("Could not find any factory for identifier 'countt'");
    }

    @Test
    void testEdgeAggregateTypeMismatch() {
        Map<String, String> options = edgeOptions("name:sum");
        TableSchema schema = edgeSchema(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid aggregate 'sum' for column 'name'");
    }

    @Test
    void testEdgeAggregateFieldNotExist() {
        Map<String, String> options = edgeOptions("quantity:sum");
        TableSchema schema = edgeSchema(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Column 'quantity' in 'graph.edge.aggregates' does not exist in table schema.");
    }

    @Test
    void testEdgeCountOnAnyType() {
        Map<String, String> options = edgeOptions("*:count");
        assertThatNoException()
                .isThrownBy(() -> SchemaValidation.validateTableSchema(edgeSchema(options)));
    }

    // --------------------------------------------------------------------------------------------
    // Cross and runtime rules
    // --------------------------------------------------------------------------------------------

    @Test
    void testVertexAndEdgeExclusive() {
        Map<String, String> options = vertexOptions("id");
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "person");
        TableSchema schema = schema(vertexFields(), emptyList(), singletonList("id"), options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "A table cannot be declared as both a graph vertex ('graph.vertex.enabled')"
                                + " and a graph edge ('graph.edge.enabled').");
    }

    @Test
    void testInvalidBackend() {
        Map<String, String> options = vertexOptions("id");
        options.put(GraphIndexOptions.GRAPH_INDEX_BACKEND.key(), "es");
        TableSchema schema = vertexSchemaFields(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.index.backend' must be one of [auto, pk-index, table], but was 'es'.");
    }

    @Test
    void testInvalidSearchMode() {
        Map<String, String> options = vertexOptions("id");
        options.put(GraphIndexOptions.GRAPH_INDEX_SEARCH_MODE.key(), "instant");
        TableSchema schema = vertexSchemaFields(options);
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.index.search-mode' must be one of [fast, full, detail], but was 'instant'.");
    }

    @Test
    void testRuntimeOptionsIgnoredWithoutGraphDecls() {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_INDEX_BACKEND.key(), "es");
        TableSchema schema =
                schema(
                        singletonList(new DataField(0, "id", DataTypes.BIGINT())),
                        emptyList(),
                        singletonList("id"),
                        options);
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(schema));
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static Map<String, String> vertexOptions(String idField) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(), idField);
        options.put(CoreOptions.BUCKET.key(), "256");
        return options;
    }

    private static List<DataField> vertexFields() {
        return Arrays.asList(
                new DataField(0, "id", DataTypes.BIGINT()),
                new DataField(1, "name", DataTypes.STRING()),
                new DataField(2, "phone", DataTypes.STRING()),
                new DataField(3, "city", DataTypes.STRING()));
    }

    private static TableSchema vertexSchema(String nodeMapFields, String idField) {
        Map<String, String> options = vertexOptions(idField);
        options.put(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), nodeMapFields);
        return vertexSchemaFields(options);
    }

    private static TableSchema vertexSchemaFields(Map<String, String> options) {
        return schema(vertexFields(), emptyList(), singletonList("id"), options);
    }

    private static Map<String, String> edgeOptions(String aggregates) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "merchant");
        options.put(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), aggregates);
        return options;
    }

    private static List<DataField> edgeFields() {
        return Arrays.asList(
                new DataField(0, "src_id", DataTypes.BIGINT()),
                new DataField(1, "dst_id", DataTypes.BIGINT()),
                new DataField(2, "amount", DataTypes.DECIMAL(18, 2)),
                new DataField(3, "tx_time", DataTypes.TIMESTAMP(3)),
                new DataField(4, "name", DataTypes.STRING()),
                new DataField(5, "dt", DataTypes.STRING()));
    }

    private static TableSchema edgeSchema(Map<String, String> options) {
        return schema(edgeFields(), singletonList("dt"), emptyList(), options);
    }

    private static TableSchema schema(
            List<DataField> fields,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options) {
        return new TableSchema(
                1,
                new ArrayList<>(fields),
                fields.size() - 1,
                partitionKeys,
                primaryKeys,
                options,
                "");
    }
}
