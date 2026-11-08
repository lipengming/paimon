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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of the adjacency backfill SQL generation (design §7.1). */
class GraphEdgeIndexBackfillTest {

    @Test
    void testGenerateSqlMatchesDesignDoc() {
        TableSchema base = transferSchema("amount:sum;tx_time:last_non_null_value;*:count", true);

        List<String> sqls = GraphEdgeIndexBackfill.generateSql("db", "transfer", base, 17L);

        assertThat(sqls).hasSize(2);
        assertThat(sqls.get(0))
                .isEqualTo(
                        "INSERT INTO `db`.`transfer__edge`\n"
                                + "SELECT `src_id`, `dst_id`, SUM(`amount`) AS `sum_amount`,"
                                + " MAX(`tx_time`) AS `last_non_null_value_tx_time`, COUNT(*) AS"
                                + " `cnt`\n"
                                + "FROM `db`.`transfer` /*+ OPTIONS('scan.snapshot-id'='17') */\n"
                                + "GROUP BY `src_id`, `dst_id`");
        assertThat(sqls.get(1))
                .isEqualTo(
                        "INSERT INTO `db`.`transfer__edge_rev`\n"
                                + "SELECT `dst_id`, `src_id`, SUM(`amount`) AS `sum_amount`,"
                                + " MAX(`tx_time`) AS `last_non_null_value_tx_time`, COUNT(*) AS"
                                + " `cnt`\n"
                                + "FROM `db`.`transfer` /*+ OPTIONS('scan.snapshot-id'='17') */\n"
                                + "GROUP BY `dst_id`, `src_id`");
    }

    @Test
    void testAliasOverridesColumnName() {
        TableSchema base = transferSchema("amount:sum:amt", false);

        List<String> sqls = GraphEdgeIndexBackfill.generateSql("db", "transfer", base, 1L);

        assertThat(sqls).hasSize(1);
        assertThat(sqls.get(0)).contains("SUM(`amount`) AS `amt`").doesNotContain("sum_amount");
    }

    @Test
    void testNoSnapshotNoSql() {
        TableSchema base = transferSchema("*:count", false);
        assertThat(GraphEdgeIndexBackfill.generateSql("db", "transfer", base, null)).isEmpty();
    }

    @Test
    void testUnsupportedAggregateFunction() {
        TableSchema base = transferSchema("name:listagg", false);
        assertThatThrownBy(() -> GraphEdgeIndexBackfill.generateSql("db", "transfer", base, 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Aggregate function 'listagg' cannot be expressed");
    }

    private static TableSchema transferSchema(String aggregates, boolean reverse) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "merchant");
        options.put(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), aggregates);
        options.put(GraphIndexOptions.GRAPH_EDGE_REVERSE.key(), String.valueOf(reverse));
        return new TableSchema(
                0L,
                Arrays.asList(
                        new DataField(0, "src_id", DataTypes.BIGINT()),
                        new DataField(1, "dst_id", DataTypes.BIGINT()),
                        new DataField(2, "amount", DataTypes.DECIMAL(18, 2)),
                        new DataField(3, "tx_time", DataTypes.TIMESTAMP(3)),
                        new DataField(4, "name", DataTypes.STRING())),
                4,
                emptyList(),
                emptyList(),
                options,
                "");
    }
}
