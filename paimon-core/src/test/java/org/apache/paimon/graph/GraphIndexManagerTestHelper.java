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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/** Shared fixture: a bank-transfer edge event table declared via {@code graph.edge.*}. */
final class GraphIndexManagerTestHelper {

    private GraphIndexManagerTestHelper() {}

    /** Creates the 'mydb.transfer' base table and optionally writes one event row. */
    static Identifier createTransferBase(Catalog catalog, boolean writeRow) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "merchant");
        options.put(
                GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(),
                "amount:sum;tx_time:last_non_null_value;*:count");

        Identifier id = Identifier.create("mydb", "transfer");
        catalog.createTable(
                id,
                new Schema(
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT()),
                                new DataField(2, "amount", DataTypes.DECIMAL(18, 2)),
                                new DataField(3, "tx_time", DataTypes.TIMESTAMP(3)),
                                new DataField(4, "dt", DataTypes.STRING())),
                        singletonList("dt"),
                        emptyList(),
                        options,
                        ""),
                false);

        if (writeRow) {
            Table table = catalog.getTable(id);
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite()) {
                write.write(
                        GenericRow.of(
                                1L,
                                2L,
                                Decimal.fromBigDecimal(new BigDecimal("10.00"), 18, 2),
                                Timestamp.fromEpochMillis(1699000000000L),
                                BinaryString.fromString("x"),
                                BinaryString.fromString("2026-11-08")));
                try (BatchTableCommit commit = builder.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }
        }
        return id;
    }
}
