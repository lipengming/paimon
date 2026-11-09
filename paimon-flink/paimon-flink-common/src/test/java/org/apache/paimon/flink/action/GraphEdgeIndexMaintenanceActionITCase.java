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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.graph.GraphIndexManager;
import org.apache.paimon.graph.GraphIndexStatusTable;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases of the Flink graph adjacency maintenance micro-batch (design §7.2/§7.3). */
public class GraphEdgeIndexMaintenanceActionITCase extends ActionITCaseBase {

    private static final RowType EDGE_ROW_TYPE =
            RowType.of(
                    new org.apache.paimon.types.DataType[] {
                        DataTypes.BIGINT(),
                        DataTypes.BIGINT(),
                        DataTypes.DECIMAL(18, 2),
                        DataTypes.TIMESTAMP(3)
                    },
                    new String[] {"src_id", "dst_id", "amount", "tx_time"});

    @Test
    public void testMaintenanceConsumesIncrementAndAdvancesWatermark() throws Exception {
        Identifier base = createEdgeTable();
        // snapshot 1 is the backfill scope of the create pipeline
        append(base, event(1L, 2L, "10.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);

        // snapshot 2 is the increment consumed by the Flink micro-batch
        append(base, event(1L, 4L, "3.00"), event(4L, 1L, "6.00"), event(2L, 1L, "2.00"));

        runMaintenance();

        Map<String, String> forward = readAdjacency("transfer__edge");
        assertThat(forward)
                .hasSize(3)
                .containsEntry("1,4", "3.00|1")
                .containsEntry("4,1", "6.00|1")
                .containsEntry("2,1", "2.00|1");

        Map<String, String> reverse = readAdjacency("transfer__edge_rev");
        assertThat(reverse)
                .hasSize(3)
                .containsEntry("4,1", "3.00|1")
                .containsEntry("1,4", "6.00|1")
                .containsEntry("1,2", "2.00|1");

        FileStoreTable baseTable = getFileStoreTable("transfer");
        assertThat(baseTable.schema().options())
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "2")
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge_rev", "2")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_ACTIVE);

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, database);
        assertThat(rows).hasSize(2);
        for (GraphIndexStatusTable.StatusRow row : rows) {
            assertThat(row.status()).isEqualTo(GraphIndexStatusTable.STATUS_ACTIVE);
            assertThat(row.buildSnapshotId()).isEqualTo(1L);
            assertThat(row.coverageWatermark()).isEqualTo(2L);
        }
    }

    @Test
    public void testMaintenanceAccumulatesAcrossRuns() throws Exception {
        Identifier base = createEdgeTable();
        append(base, event(9L, 9L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);

        append(base, event(3L, 1L, "5.00"), event(3L, 1L, "1.00"));
        runMaintenance();
        assertThat(readAdjacency("transfer__edge")).containsEntry("3,1", "6.00|2");

        append(base, event(3L, 1L, "2.00"));
        runMaintenance();
        assertThat(readAdjacency("transfer__edge")).containsEntry("3,1", "8.00|3");

        assertThat(getFileStoreTable("transfer").schema().options())
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "3");
    }

    @Test
    public void testMaintenanceNoopWhenCovered() throws Exception {
        Identifier base = createEdgeTable();
        append(base, event(9L, 9L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);

        runMaintenance();

        assertThat(readAdjacency("transfer__edge")).isEmpty();
        // nothing was consumed, so no watermark is recorded; the create pipeline's build
        // snapshot still pins the coverage point
        assertThat(getFileStoreTable("transfer").schema().options())
                .containsEntry(GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + "transfer__edge", "1")
                .doesNotContainKey(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge");
    }

    @Test
    public void testMaintenanceWithExplicitToSnapshot() throws Exception {
        Identifier base = createEdgeTable();
        append(base, event(9L, 9L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);
        append(base, event(1L, 2L, "4.00"));
        append(base, event(1L, 2L, "2.00"));

        new GraphEdgeIndexMaintenanceAction(
                        database, "transfer", Collections.singletonMap("warehouse", warehouse))
                .withToSnapshot(2L)
                .run();

        assertThat(readAdjacency("transfer__edge")).containsEntry("1,2", "4.00|1");
        assertThat(getFileStoreTable("transfer").schema().options())
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "2");

        // the second increment is still pending and is consumed by the next run
        runMaintenance();
        assertThat(readAdjacency("transfer__edge")).containsEntry("1,2", "6.00|2");
    }

    @Test
    public void testMaintenanceRequiresCreatePipeline() throws Exception {
        createEdgeTable();
        append(Identifier.create(database, "transfer"), event(1L, 2L, "1.00"));

        assertThatThrownBy(this::runMaintenance)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("run the graph index create pipeline first");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    private Identifier createEdgeTable() throws Exception {
        catalog.createDatabase(database, true);
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "-1");
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "merchant");
        options.put(
                GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(),
                "amount:sum;tx_time:last_non_null_value;*:count");
        String tableName = "transfer";
        createFileStoreTable(
                tableName,
                EDGE_ROW_TYPE,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                options);
        return Identifier.create(database, tableName);
    }

    private void runMaintenance() throws Exception {
        createAction(
                        GraphEdgeIndexMaintenanceAction.class,
                        "graph_index_maintenance",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        "transfer")
                .run();
    }

    private void append(Identifier base, GenericRow... events) throws Exception {
        Table table = catalog.getTable(base);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (GenericRow event : events) {
                write.write(event);
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }

    private static GenericRow event(long src, long dst, String amount) {
        return GenericRow.of(
                src,
                dst,
                Decimal.fromBigDecimal(new BigDecimal(amount), 18, 2),
                Timestamp.fromEpochMillis(1699000000000L + src * 1000 + dst));
    }

    /** Reads 'src,dst' -> 'sum_amount|cnt' of one adjacency table. */
    private Map<String, String> readAdjacency(String tableName) throws Exception {
        FileStoreTable table =
                (FileStoreTable) catalog.getTable(Identifier.create(database, tableName));
        int cntIndex = -1;
        for (int i = 0; i < table.schema().fields().size(); i++) {
            if (table.schema().fields().get(i).name().equals(GraphEdgeIndexPlanner.COUNT_COLUMN)) {
                cntIndex = i;
            }
        }
        int cnt = cntIndex;
        Map<String, String> result = new HashMap<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(
                    row ->
                            result.put(
                                    row.getLong(0) + "," + row.getLong(1),
                                    row.getDecimal(2, 18, 2).toBigDecimal().toPlainString()
                                            + "|"
                                            + row.getLong(cnt)));
        }
        return result;
    }
}
