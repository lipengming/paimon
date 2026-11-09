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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests of the adjacency maintenance micro-batch: incremental consumption, partial
 * aggregate writes into the aggregation merge engine, coverage watermark advance.
 */
class GraphEdgeIndexMaintainerTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @BeforeEach
    void before() throws Exception {
        String warehouse = tempDir.resolve("warehouse").toString();
        CatalogContext context = CatalogContext.create(new Options());
        FileIO fileIO = FileIO.get(new Path(warehouse), context);
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse), context);
        catalog.createDatabase("mydb", false);
    }

    @Test
    void testMaintainConsumesIncrementAndAdvancesWatermark() throws Exception {
        // snapshot 1: the backfill scope, the index is not built yet
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, false);
        appendEvents(base, event(1L, 2L, "10.00"), event(2L, 3L, "1.00"));

        // build snapshot = 1
        GraphIndexManager.createEdgeIndex(catalog, base);

        // snapshot 2: the increment the maintenance consumes
        appendEvents(base, event(1L, 4L, "3.00"), event(4L, 1L, "6.00"), event(2L, 1L, "2.00"));

        GraphEdgeIndexMaintainer.MaintainResult result =
                GraphEdgeIndexMaintainer.maintain(catalog, base);

        assertThat(result.noop()).isFalse();
        assertThat(result.fromSnapshot()).isEqualTo(1L);
        assertThat(result.toSnapshot()).isEqualTo(2L);
        assertThat(result.eventsConsumed()).isEqualTo(3L);

        // the index only contains the increment (the backfill job has not run in this test);
        // the merge engine combines partial aggregates of repeated pairs by sum
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

        // watermark mirror on the base table
        Map<String, String> options = ((FileStoreTable) catalog.getTable(base)).schema().options();
        assertThat(options)
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "2")
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge_rev", "2")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_ACTIVE);

        // status rows advanced to ACTIVE with the coverage watermark and the build snapshot kept
        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(2);
        for (GraphIndexStatusTable.StatusRow row : rows) {
            assertThat(row.status()).isEqualTo(GraphIndexStatusTable.STATUS_ACTIVE);
            assertThat(row.buildSnapshotId()).isEqualTo(1L);
            assertThat(row.coverageWatermark()).isEqualTo(2L);
        }
    }

    @Test
    void testMaintainNoopWhenNoNewData() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);
        GraphIndexManager.createEdgeIndex(catalog, base);

        GraphEdgeIndexMaintainer.MaintainResult result =
                GraphEdgeIndexMaintainer.maintain(catalog, base);

        assertThat(result.noop()).isTrue();
        assertThat(result.fromSnapshot()).isEqualTo(1L);
        assertThat(result.toSnapshot()).isEqualTo(1L);
        assertThat(readAdjacency("transfer__edge")).isEmpty();
    }

    @Test
    void testMaintainAccumulatesAcrossCheckpoints() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, false);
        appendEvents(base, event(9L, 9L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);

        appendEvents(base, event(3L, 1L, "5.00"), event(3L, 1L, "1.00"));
        GraphEdgeIndexMaintainer.MaintainResult first =
                GraphEdgeIndexMaintainer.maintain(catalog, base);
        assertThat(first.eventsConsumed()).isEqualTo(2L);
        assertThat(readAdjacency("transfer__edge")).containsEntry("3,1", "6.00|2");

        appendEvents(base, event(3L, 1L, "2.00"), event(1L, 3L, "4.00"));
        GraphEdgeIndexMaintainer.MaintainResult second =
                GraphEdgeIndexMaintainer.maintain(catalog, base);
        assertThat(second.fromSnapshot()).isEqualTo(2L);
        assertThat(second.toSnapshot()).isEqualTo(3L);
        assertThat(second.eventsConsumed()).isEqualTo(2L);

        Map<String, String> forward = readAdjacency("transfer__edge");
        // the stored partial (6.00, 2) and the new partial (2.00, 1) are combined by the
        // aggregation merge engine on read
        assertThat(forward).containsEntry("3,1", "8.00|3").containsEntry("1,3", "4.00|1");

        // the build snapshot is pinned to the create pipeline, not to the last checkpoint
        for (GraphIndexStatusTable.StatusRow row : GraphIndexStatusTable.read(catalog, "mydb")) {
            assertThat(row.buildSnapshotId()).isEqualTo(1L);
            assertThat(row.coverageWatermark()).isEqualTo(3L);
        }
    }

    @Test
    void testAdvanceWatermarksKeepsBuildSnapshot() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);
        GraphIndexManager.createEdgeIndex(catalog, base);

        GraphEdgeIndexMaintainer.advanceWatermarks(catalog, base, 1L, 4L);

        Map<String, String> options = ((FileStoreTable) catalog.getTable(base)).schema().options();
        assertThat(options)
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "4")
                .containsEntry(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge_rev", "4")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_ACTIVE);
        for (GraphIndexStatusTable.StatusRow row : GraphIndexStatusTable.read(catalog, "mydb")) {
            assertThat(row.status()).isEqualTo(GraphIndexStatusTable.STATUS_ACTIVE);
            assertThat(row.buildSnapshotId()).isEqualTo(1L);
            assertThat(row.coverageWatermark()).isEqualTo(4L);
        }
    }

    @Test
    void testAdvanceWatermarksRejectsNonEdgeTable() throws Exception {
        Identifier id = Identifier.create("mydb", "plain_advance");
        catalog.createTable(
                id,
                new org.apache.paimon.schema.Schema(
                        java.util.Arrays.asList(
                                new org.apache.paimon.types.DataField(
                                        0, "a", org.apache.paimon.types.DataTypes.BIGINT())),
                        java.util.Collections.emptyList(),
                        java.util.Collections.emptyList(),
                        new HashMap<>(),
                        ""),
                false);

        assertThatThrownBy(() -> GraphEdgeIndexMaintainer.advanceWatermarks(catalog, id, 1L, 2L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph edge");
    }

    @Test
    void testMaintainRequiresCreatePipeline() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);

        assertThatThrownBy(() -> GraphEdgeIndexMaintainer.maintain(catalog, base))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("run the graph index create pipeline first");
    }

    @Test
    void testMaintainRejectsNonEdgeTable() throws Exception {
        Identifier id = Identifier.create("mydb", "plain");
        catalog.createTable(
                id,
                new org.apache.paimon.schema.Schema(
                        java.util.Arrays.asList(
                                new org.apache.paimon.types.DataField(
                                        0, "a", org.apache.paimon.types.DataTypes.BIGINT())),
                        java.util.Collections.emptyList(),
                        java.util.Collections.emptyList(),
                        new HashMap<>(),
                        ""),
                false);

        assertThatThrownBy(() -> GraphEdgeIndexMaintainer.maintain(catalog, id))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph edge");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    private void appendEvents(Identifier base, GenericRow... events) throws Exception {
        Table transfer = catalog.getTable(base);
        BatchWriteBuilder builder = transfer.newBatchWriteBuilder();
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
                Timestamp.fromEpochMillis(1699000000000L + src * 1000 + dst),
                BinaryString.fromString("2026-11-08"));
    }

    /**
     * Reads the adjacency table into 'src,dst' -> 'sum_amount|cnt'. The expanded adjacency schema
     * is (src_id, dst_id, sum_amount, last_non_null_value_tx_time, cnt).
     */
    private Map<String, String> readAdjacency(String tableName) throws Exception {
        FileStoreTable table =
                (FileStoreTable) catalog.getTable(Identifier.create("mydb", tableName));
        TableSchema schema = table.schema();
        int cntIndex = columnIndex(schema, "cnt");
        Map<String, String> result = new HashMap<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            // the reader reuses row instances: materialize every value inside the callback
            reader.forEachRemaining(
                    row ->
                            result.put(
                                    row.getLong(0) + "," + row.getLong(1),
                                    row.getDecimal(2, 18, 2).toBigDecimal().toPlainString()
                                            + "|"
                                            + row.getLong(cntIndex)));
        }
        return result;
    }

    private static int columnIndex(TableSchema schema, String name) {
        for (int i = 0; i < schema.fields().size(); i++) {
            if (schema.fields().get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no column " + name);
    }
}
