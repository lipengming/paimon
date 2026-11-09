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
import org.apache.paimon.options.Options;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of the detail reconciliation of the adjacency indexes (design §5.6/§6.3). */
class GraphEdgeIndexReconcilerTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Identifier base;

    @BeforeEach
    void before() throws Exception {
        String warehouse = tempDir.resolve("warehouse").toString();
        CatalogContext context = CatalogContext.create(new Options());
        FileIO fileIO = FileIO.get(new Path(warehouse), context);
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse), context);
        catalog.createDatabase("mydb", false);
        base = GraphIndexManagerTestHelper.createTransferBase(catalog, false);
    }

    @Test
    void testConsistentAfterProgrammaticBackfill() throws Exception {
        append(event(1L, 2L, "10.00"), event(1L, 2L, "5.00"), event(2L, 3L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);
        backfill();

        List<GraphEdgeIndexReconciler.IndexReconcileResult> results =
                GraphEdgeIndexReconciler.reconcile(catalog, base);

        assertThat(results).hasSize(2);
        for (GraphEdgeIndexReconciler.IndexReconcileResult result : results) {
            assertThat(result.consistent()).isTrue();
            assertThat(result.basePairs()).isEqualTo(2);
            assertThat(result.indexPairs()).isEqualTo(2);
            assertThat(result.samples()).isEmpty();
        }
    }

    @Test
    void testDetectsMissingAndMismatchedPairs() throws Exception {
        append(event(1L, 2L, "10.00"), event(2L, 3L, "1.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);
        backfill();
        // not maintained: one existing pair changes, one new pair appears
        append(event(1L, 2L, "5.00"), event(4L, 5L, "2.00"));

        List<GraphEdgeIndexReconciler.IndexReconcileResult> results =
                GraphEdgeIndexReconciler.reconcile(catalog, base);

        for (GraphEdgeIndexReconciler.IndexReconcileResult result : results) {
            assertThat(result.consistent()).isFalse();
            assertThat(result.missingPairs()).isEqualTo(1L);
            assertThat(result.mismatchedPairs()).isEqualTo(1L);
            assertThat(result.samples()).isNotEmpty();
        }
    }

    @Test
    void testDetectsExtraPairs() throws Exception {
        append(event(1L, 2L, "10.00"));
        GraphIndexManager.createEdgeIndex(catalog, base);
        backfill();
        // an index row that has no matching base pair
        writeIndexRows(
                "transfer__edge",
                java.util.Collections.<InternalRow>singletonList(
                        GenericRow.of(
                                8L,
                                9L,
                                Decimal.fromBigDecimal(new BigDecimal("1.00"), 18, 2),
                                Timestamp.fromEpochMillis(1L),
                                1L)));

        GraphEdgeIndexReconciler.IndexReconcileResult forward =
                GraphEdgeIndexReconciler.reconcile(catalog, base).get(0);

        assertThat(forward.consistent()).isFalse();
        assertThat(forward.extraPairs()).isEqualTo(1L);
        assertThat(String.join(";", forward.samples())).contains("extra (8, 9)");
    }

    @Test
    void testRejectsNonEdgeTable() throws Exception {
        Identifier plain = Identifier.create("mydb", "plain_reconcile");
        catalog.createTable(
                plain,
                new org.apache.paimon.schema.Schema(
                        java.util.Arrays.asList(
                                new org.apache.paimon.types.DataField(
                                        0, "a", org.apache.paimon.types.DataTypes.BIGINT())),
                        java.util.Collections.emptyList(),
                        java.util.Collections.emptyList(),
                        new java.util.HashMap<>(),
                        ""),
                false);

        assertThatThrownBy(() -> GraphEdgeIndexReconciler.reconcile(catalog, plain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph edge");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    /** Writes the full-history aggregation of the base table into both adjacency tables. */
    private void backfill() throws Exception {
        FileStoreTable baseTable = (FileStoreTable) catalog.getTable(base);
        TableSchema schema = baseTable.schema();
        GraphEdgeIndexDecl decl = GraphIndexDecls.from(schema).edge().get();
        GraphEdgeIndexAggregator.AggregatedEdges aggregated =
                GraphEdgeIndexAggregator.snapshot(
                        baseTable,
                        schema,
                        decl,
                        baseTable.store().snapshotManager().latestSnapshot().id());
        writeIndexRows("transfer__edge", aggregated.rows(decl, false));
        writeIndexRows("transfer__edge_rev", aggregated.rows(decl, true));
    }

    private void writeIndexRows(String tableName, List<InternalRow> rows) throws Exception {
        FileStoreTable table =
                (FileStoreTable) catalog.getTable(Identifier.create("mydb", tableName));
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }

    private void append(GenericRow... events) throws Exception {
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
                Timestamp.fromEpochMillis(1699000000000L + src * 1000 + dst),
                BinaryString.fromString("2026-11-08"));
    }
}
