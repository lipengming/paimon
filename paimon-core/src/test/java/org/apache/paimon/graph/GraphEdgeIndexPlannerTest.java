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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of {@link GraphEdgeIndexPlanner} expansion results. */
class GraphEdgeIndexPlannerTest {

    private static final String DEFAULT_AGGREGATES =
            "amount:sum;tx_time:last_non_null_value;*:count";

    @Test
    void testForwardEdgeExpansion() {
        GraphEdgeIndexPlan plan =
                GraphEdgeIndexPlanner.plan("transfer", edgeSchema(DEFAULT_AGGREGATES, false));

        assertThat(plan.baseTableName()).isEqualTo("transfer");
        assertThat(plan.edgeTableName()).isEqualTo("transfer__edge");
        assertThat(plan.reverse()).isFalse();
        assertThat(plan.edgeRev()).isNull();

        TableSchema edge = plan.edge();
        assertThat(edge.fields())
                .containsExactly(
                        new DataField(0, "src_id", DataTypes.BIGINT()),
                        new DataField(1, "dst_id", DataTypes.BIGINT()),
                        new DataField(2, "sum_amount", DataTypes.DECIMAL(18, 2)),
                        new DataField(3, "last_non_null_value_tx_time", DataTypes.TIMESTAMP(3)),
                        new DataField(4, "cnt", DataTypes.BIGINT()));
        assertThat(edge.highestFieldId()).isEqualTo(4);
        assertThat(edge.primaryKeys()).containsExactly("src_id", "dst_id");
        assertThat(edge.partitionKeys()).isEmpty();
        assertThat(edge.bucketKeys()).containsExactly("src_id");
        assertThat(edge.numBuckets()).isEqualTo(GraphEdgeIndexPlanner.DEFAULT_EDGE_BUCKETS);
        assertThat(edge.options())
                .containsEntry("fields.sum_amount.aggregate-function", "sum")
                .containsEntry(
                        "fields.last_non_null_value_tx_time.aggregate-function",
                        "last_non_null_value")
                .containsEntry("fields.cnt.aggregate-function", "sum")
                .containsEntry("fields.default-aggregate-function", "last_non_null_value")
                .containsEntry("merge-engine", "aggregation")
                .containsEntry("changelog-producer", "none")
                .containsEntry("bucket", "1024")
                .containsEntry("bucket-key", "src_id");
        assertThat(edge.comment()).contains("forward").contains("transfer");
    }

    @Test
    void testReverseEdgeExpansion() {
        GraphEdgeIndexPlan plan =
                GraphEdgeIndexPlanner.plan("transfer", edgeSchema(DEFAULT_AGGREGATES, true));

        assertThat(plan.reverse()).isTrue();
        assertThat(plan.edgeRevTableName()).isEqualTo("transfer__edge_rev");

        TableSchema edgeRev = plan.edgeRev();
        assertThat(edgeRev.fields())
                .containsExactly(
                        new DataField(0, "dst_id", DataTypes.BIGINT()),
                        new DataField(1, "src_id", DataTypes.BIGINT()),
                        new DataField(2, "sum_amount", DataTypes.DECIMAL(18, 2)),
                        new DataField(3, "last_non_null_value_tx_time", DataTypes.TIMESTAMP(3)),
                        new DataField(4, "cnt", DataTypes.BIGINT()));
        assertThat(edgeRev.primaryKeys()).containsExactly("dst_id", "src_id");
        assertThat(edgeRev.bucketKeys()).containsExactly("dst_id");
        assertThat(edgeRev.options())
                .containsEntry("fields.sum_amount.aggregate-function", "sum")
                .containsEntry("fields.cnt.aggregate-function", "sum")
                .containsEntry("bucket-key", "dst_id");
        assertThat(edgeRev.comment()).contains("reverse").contains("transfer");
    }

    @Test
    void testExplicitAlias() {
        GraphEdgeIndexPlan plan =
                GraphEdgeIndexPlanner.plan(
                        "transfer", edgeSchema("amount:sum:total_amount;*:count:events", false));

        assertThat(plan.edge().fields())
                .containsExactly(
                        new DataField(0, "src_id", DataTypes.BIGINT()),
                        new DataField(1, "dst_id", DataTypes.BIGINT()),
                        new DataField(2, "total_amount", DataTypes.DECIMAL(18, 2)),
                        new DataField(3, "events", DataTypes.BIGINT()));
        assertThat(plan.edge().options())
                .containsEntry("fields.total_amount.aggregate-function", "sum")
                .containsEntry("fields.events.aggregate-function", "sum");
    }

    @Test
    void testCountOnConcreteFieldRejected() {
        assertThatThrownBy(
                        () ->
                                GraphEdgeIndexPlanner.plan(
                                        "transfer", edgeSchema("amount:count", false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Aggregate function 'count' in 'graph.edge.aggregates' only supports the"
                                + " wildcard field '*', but got field 'amount'.");
    }

    @Test
    void testExpandedColumnConflictWithEndpoint() {
        assertThatThrownBy(
                        () ->
                                GraphEdgeIndexPlanner.plan(
                                        "transfer", edgeSchema("*:count:src_id", false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Aggregate column 'src_id' expanded from 'graph.edge.aggregates' conflicts"
                                + " with another column of the adjacency index tables of table"
                                + " 'transfer'.");
    }

    @Test
    void testDuplicateExpandedColumn() {
        assertThatThrownBy(
                        () ->
                                GraphEdgeIndexPlanner.plan(
                                        "transfer",
                                        edgeSchema("amount:sum:total;tx_time:max:total", false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Aggregate column 'total' expanded from 'graph.edge.aggregates' conflicts"
                                + " with another column of the adjacency index tables of table"
                                + " 'transfer'.");
    }

    @Test
    void testPlanRequiresEdgeDeclaration() {
        TableSchema schema =
                new TableSchema(
                        0L,
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT())),
                        1,
                        emptyList(),
                        emptyList(),
                        Collections.<String, String>emptyMap(),
                        null);
        assertThatThrownBy(() -> GraphEdgeIndexPlanner.plan("transfer", schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Table 'transfer' is not declared as a graph edge: option"
                                + " 'graph.edge.enabled' is not set.");
    }

    @Test
    void testAggregateFieldNotExist() {
        Map<String, String> options = edgeOptions("amount:sum", false);
        TableSchema schema =
                new TableSchema(
                        0L,
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT())),
                        1,
                        emptyList(),
                        emptyList(),
                        options,
                        null);
        assertThatThrownBy(() -> GraphEdgeIndexPlanner.plan("transfer", schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Column 'amount' in 'graph.edge.aggregates' does not exist in table schema.");
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private TableSchema edgeSchema(String aggregates, boolean reverse) {
        return new TableSchema(
                0L,
                Arrays.asList(
                        new DataField(0, "src_id", DataTypes.BIGINT()),
                        new DataField(1, "dst_id", DataTypes.BIGINT()),
                        new DataField(2, "amount", DataTypes.DECIMAL(18, 2)),
                        new DataField(3, "tx_time", DataTypes.TIMESTAMP(3))),
                3,
                emptyList(),
                emptyList(),
                edgeOptions(aggregates, reverse),
                null);
    }

    private Map<String, String> edgeOptions(String aggregates, boolean reverse) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "account");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "account");
        options.put(GraphIndexOptions.GRAPH_EDGE_REVERSE.key(), String.valueOf(reverse));
        options.put(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), aggregates);
        return options;
    }
}
