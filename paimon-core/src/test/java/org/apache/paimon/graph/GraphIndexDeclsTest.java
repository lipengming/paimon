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

import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for parsing {@code graph.vertex.*} / {@code graph.edge.*} declarations. */
class GraphIndexDeclsTest {

    @Test
    void testNoDeclarations() {
        assertThat(GraphIndexDecls.from(new Options()).isEmpty()).isTrue();
    }

    @Test
    void testParseVertex() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_VERTEX_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD, "id");
        options.set(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS, "name,phone,city:bitmap");

        GraphIndexDecls decls = GraphIndexDecls.from(options);
        Optional<GraphVertexIndexDecl> vertex = decls.vertex();
        assertThat(vertex.isPresent()).isTrue();
        assertThat(decls.edge()).isEmpty();

        GraphVertexIndexDecl parsed = vertex.get();
        assertThat(parsed.idField()).isEqualTo("id");
        List<GraphVertexIndexDecl.NodeMapField> fields = parsed.nodeMapFields();
        assertThat(fields)
                .containsExactly(
                        new GraphVertexIndexDecl.NodeMapField("name", "btree"),
                        new GraphVertexIndexDecl.NodeMapField("phone", "btree"),
                        new GraphVertexIndexDecl.NodeMapField("city", "bitmap"));
        assertThat(parsed.encoding()).isEqualTo(GraphIndexOptions.ENCODING_ORDERED_BINARY);
        assertThat(parsed.updateAction()).isEqualTo(GraphIndexOptions.UPDATE_ACTION_MAINTAIN);
    }

    @Test
    void testParseVertexTrimsAndNormalizesFamily() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_VERTEX_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD, "id");
        options.set(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS, " name , city:BITMAP ");

        GraphVertexIndexDecl parsed = GraphIndexDecls.from(options).vertex().get();
        assertThat(parsed.nodeMapFields())
                .containsExactly(
                        new GraphVertexIndexDecl.NodeMapField("name", "btree"),
                        new GraphVertexIndexDecl.NodeMapField("city", "bitmap"));
    }

    @Test
    void testParseEdge() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_EDGE_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD, "src_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_FIELD, "dst_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX, "merchant");
        options.set(
                GraphIndexOptions.GRAPH_EDGE_AGGREGATES,
                "amount:sum;tx_time:last_non_null_value;*:count");
        options.set(GraphIndexOptions.GRAPH_EDGE_REVERSE, false);

        GraphIndexDecls decls = GraphIndexDecls.from(options);
        assertThat(decls.vertex()).isEmpty();
        Optional<GraphEdgeIndexDecl> edge = decls.edge();
        assertThat(edge.isPresent()).isTrue();

        GraphEdgeIndexDecl parsed = edge.get();
        assertThat(parsed.srcField()).isEqualTo("src_id");
        assertThat(parsed.dstField()).isEqualTo("dst_id");
        assertThat(parsed.srcVertex()).isEqualTo("person");
        assertThat(parsed.dstVertex()).isEqualTo("merchant");
        assertThat(parsed.reverse()).isFalse();
        assertThat(parsed.aggregates())
                .containsExactly(
                        new GraphEdgeAggregate("amount", "sum", null),
                        new GraphEdgeAggregate("tx_time", "last_non_null_value", null),
                        new GraphEdgeAggregate("*", "count", null));
    }

    @Test
    void testParseEdgeDefaults() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_EDGE_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD, "src_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_FIELD, "dst_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX, "person");

        GraphEdgeIndexDecl parsed = GraphIndexDecls.from(options).edge().get();
        assertThat(parsed.reverse()).isTrue();
        assertThat(parsed.aggregates()).containsExactly(new GraphEdgeAggregate("*", "count", null));
    }

    @Test
    void testParseEdgeAggregateAlias() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_EDGE_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD, "src_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_FIELD, "dst_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_AGGREGATES, "amount:sum:amt");

        GraphEdgeIndexDecl parsed = GraphIndexDecls.from(options).edge().get();
        assertThat(parsed.aggregates())
                .containsExactly(new GraphEdgeAggregate("amount", "sum", "amt"));
    }

    @Test
    void testVertexMissingIdField() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_VERTEX_ENABLED, true);
        assertThatThrownBy(() -> GraphIndexDecls.from(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.vertex.id-field' must be set when 'graph.vertex.enabled' is true.");
    }

    @Test
    void testNodeMapFieldsEmptyEntry() {
        assertThatThrownBy(() -> parseVertexFields("a,,b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.vertex.node-map.fields': should not contain empty fields.");
    }

    @Test
    void testNodeMapFieldsTooManyParts() {
        assertThatThrownBy(() -> parseVertexFields("a:btree:extra"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.vertex.node-map.fields': entry 'a:btree:extra' should be 'field' or 'field:family'.");
    }

    @Test
    void testNodeMapFieldsUnknownFamily() {
        assertThatThrownBy(() -> parseVertexFields("city:hash"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unknown node-map index family 'hash' for field 'city' in 'graph.vertex.node-map.fields', expected 'btree' or 'bitmap'.");
    }

    @Test
    void testNodeMapFieldsDuplicateField() {
        assertThatThrownBy(() -> parseVertexFields("phone:bitmap,phone"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate field 'phone' in 'graph.vertex.node-map.fields'.");
    }

    @Test
    void testVertexInvalidEncoding() {
        Options options = vertexOptions();
        options.set(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_ENCODING, "json");
        assertThatThrownBy(() -> GraphIndexDecls.from(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unsupported node-map encoding 'json' in 'graph.vertex.node-map.encoding', currently only 'ordered-binary' is supported.");
    }

    @Test
    void testVertexInvalidUpdateAction() {
        Options options = vertexOptions();
        options.set(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_UPDATE_ACTION, "pause");
        assertThatThrownBy(() -> GraphIndexDecls.from(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Invalid node-map update action 'pause' in 'graph.vertex.node-map.update-action', expected 'maintain' or 'freeze'.");
    }

    @Test
    void testEdgeMissingRequiredOptions() {
        List<String> keys =
                Arrays.asList(
                        GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(),
                        GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(),
                        GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(),
                        GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key());
        for (String optionKey : keys) {
            Options options = new Options();
            options.set(GraphIndexOptions.GRAPH_EDGE_ENABLED, true);
            options.set(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD, "src_id");
            options.set(GraphIndexOptions.GRAPH_EDGE_DST_FIELD, "dst_id");
            options.set(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX, "person");
            options.set(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX, "person");
            options.setString(optionKey, "");
            assertThatThrownBy(() -> GraphIndexDecls.from(options))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Option '"
                                    + optionKey
                                    + "' must be set when 'graph.edge.enabled' is true.");
        }
    }

    @Test
    void testAggregatesWildcardOnlyCount() {
        assertThatThrownBy(() -> parseAggregates("*:sum"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.edge.aggregates': wildcard '*' only supports aggregate function 'count', but got 'sum'.");
    }

    @Test
    void testAggregatesMalformedEntry() {
        assertThatThrownBy(() -> parseAggregates("amount"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.edge.aggregates': entry 'amount' should be 'field:function[:alias]'.");

        assertThatThrownBy(() -> parseAggregates("a:b:c:d"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.edge.aggregates': entry 'a:b:c:d' should be 'field:function[:alias]'.");

        assertThatThrownBy(() -> parseAggregates("amount:sum:"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.edge.aggregates': entry 'amount:sum:' should be 'field:function[:alias]'.");
    }

    @Test
    void testAggregatesEmptyEntry() {
        String aggregates = "amount:sum;" + ";" + "*:count";
        assertThatThrownBy(() -> parseAggregates(aggregates))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Wrong option 'graph.edge.aggregates': should not contain empty aggregate entries.");
    }

    @Test
    void testAggregatesDuplicateField() {
        assertThatThrownBy(() -> parseAggregates("amount:sum;amount:max"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate field 'amount' in 'graph.edge.aggregates'.");
    }

    @Test
    void testAggregatesEmptyValue() {
        assertThatThrownBy(() -> parseAggregates(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.edge.aggregates' must not be empty when 'graph.edge.enabled' is true.");
    }

    private static Options vertexOptions() {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_VERTEX_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD, "id");
        return options;
    }

    private static void parseVertexFields(String fields) {
        Options options = vertexOptions();
        options.set(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS, fields);
        GraphIndexDecls.from(options);
    }

    private static void parseAggregates(String aggregates) {
        Options options = new Options();
        options.set(GraphIndexOptions.GRAPH_EDGE_ENABLED, true);
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD, "src_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_FIELD, "dst_id");
        options.set(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX, "person");
        options.set(GraphIndexOptions.GRAPH_EDGE_AGGREGATES, aggregates);
        GraphIndexDecls.from(options);
    }
}
