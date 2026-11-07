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
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test of the graph edge index declaration chain: declaration parsing and schema
 * validation (F1, {@link SchemaValidation#validateTableSchema}) through adjacency index expansion
 * (F2, {@link GraphEdgeIndexPlanner#plan}).
 */
class GraphEdgeIndexEndToEndTest {

    @Test
    void testTransferEdgeFromDeclarationToAdjacencyIndexTables() {
        // A bank-transfer event table declared as a graph edge (design sections 3.2 / 4.4).
        TableSchema transfer =
                transferSchema(
                        "account",
                        "account",
                        true,
                        "amount:sum;tx_time:last_non_null_value;*:count");

        // F1: the declaration parses and passes schema validation (cross-checks + aggregate
        // function/type validation through the FieldAggregator SPI).
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(transfer));

        // F2: the planner expands the validated declaration into adjacency index table schemas.
        GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan("transfer", transfer);

        // Forward adjacency: primary key (src, dst), clustered by src, 'count' expanded into the
        // sum-aggregated cnt column.
        assertThat(plan.edgeTableName()).isEqualTo("transfer__edge");
        assertThat(plan.edge().primaryKeys()).containsExactly("src_id", "dst_id");
        assertThat(plan.edge().bucketKeys()).containsExactly("src_id");
        assertThat(plan.edge().numBuckets()).isEqualTo(GraphEdgeIndexPlanner.DEFAULT_EDGE_BUCKETS);
        assertThat(plan.edge().fieldNames())
                .containsExactly(
                        "src_id", "dst_id", "sum_amount", "last_non_null_value_tx_time", "cnt");
        assertThat(plan.edge().options())
                .containsEntry("merge-engine", "aggregation")
                .containsEntry("fields.cnt.aggregate-function", "sum")
                .containsEntry("fields.sum_amount.aggregate-function", "sum")
                .containsEntry(
                        "fields.last_non_null_value_tx_time.aggregate-function",
                        "last_non_null_value");

        // Reverse adjacency: same aggregates, primary key (dst, src), clustered by dst.
        assertThat(plan.edgeRevTableName()).isEqualTo("transfer__edge_rev");
        assertThat(plan.edgeRev().primaryKeys()).containsExactly("dst_id", "src_id");
        assertThat(plan.edgeRev().bucketKeys()).containsExactly("dst_id");
        assertThat(plan.edgeRev().fieldNames())
                .containsExactly(
                        "dst_id", "src_id", "sum_amount", "last_non_null_value_tx_time", "cnt");

        // Invariant: the expanded tables are themselves valid Paimon schemas, i.e. they can be
        // created as ordinary aggregation primary-key tables by the create pipeline.
        assertThatNoException().isThrownBy(() -> SchemaValidation.validateTableSchema(plan.edge()));
        assertThatNoException()
                .isThrownBy(() -> SchemaValidation.validateTableSchema(plan.edgeRev()));

        // F1 guards F2: an illegal aggregate declaration fails validation and never reaches the
        // planner.
        TableSchema illegal = transferSchema("account", "account", true, "amount:countt");
        assertThatThrownBy(() -> SchemaValidation.validateTableSchema(illegal))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid aggregate 'countt' for column 'amount'");
    }

    private TableSchema transferSchema(
            String srcVertex, String dstVertex, boolean reverse, String aggregates) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), srcVertex);
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), dstVertex);
        options.put(GraphIndexOptions.GRAPH_EDGE_REVERSE.key(), String.valueOf(reverse));
        options.put(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), aggregates);

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
                options,
                null);
    }
}
