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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of the shared per-pair aggregation engine (maintenance, full mode, reconciliation). */
class GraphEdgeIndexAggregatorTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Identifier base;
    private TableSchema schema;
    private GraphEdgeIndexDecl decl;

    @BeforeEach
    void before() throws Exception {
        String warehouse = tempDir.resolve("warehouse").toString();
        CatalogContext context = CatalogContext.create(new Options());
        FileIO fileIO = FileIO.get(new Path(warehouse), context);
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse), context);
        catalog.createDatabase("mydb", false);
        base = GraphIndexManagerTestHelper.createTransferBase(catalog, false);
        schema = ((FileStoreTable) catalog.getTable(base)).schema();
        decl = GraphIndexDecls.from(schema).edge().get();
    }

    @Test
    void testSnapshotFoldsWholeHistory() throws Exception {
        append(event(1L, 2L, "10.00"), event(2L, 1L, "1.00"));
        append(event(1L, 2L, "5.00"));

        GraphEdgeIndexAggregator.AggregatedEdges aggregated =
                GraphEdgeIndexAggregator.snapshot(base(), schema, decl, 2L);

        assertThat(aggregated.eventCount()).isEqualTo(3L);
        Map<GraphEdgePair, Object[]> values = aggregated.values();
        assertThat(values).hasSize(2);
        assertThat(values.get(new GraphEdgePair(1L, 2L))[0])
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("15.00"), 18, 2));
        assertThat(values.get(new GraphEdgePair(1L, 2L))[2]).isEqualTo(2L);
        assertThat(values.get(new GraphEdgePair(2L, 1L))[2]).isEqualTo(1L);
    }

    @Test
    void testIncrementalOnlyFoldsInterval() throws Exception {
        append(event(1L, 2L, "10.00"));
        append(event(1L, 2L, "5.00"), event(3L, 4L, "2.00"));

        GraphEdgeIndexAggregator.AggregatedEdges aggregated =
                GraphEdgeIndexAggregator.incremental(base(), schema, decl, 1L, 2L);

        assertThat(aggregated.eventCount()).isEqualTo(2L);
        assertThat(aggregated.values()).hasSize(2);
        assertThat(aggregated.values().get(new GraphEdgePair(1L, 2L))[0])
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("5.00"), 18, 2));
        assertThat(aggregated.values().get(new GraphEdgePair(3L, 4L))[0])
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("2.00"), 18, 2));
    }

    @Test
    void testMergeValuesComposesPartialAggregates() throws Exception {
        Object[] stored = new Object[] {decimal("6.00"), timestamp(2000L), 2L};
        Object[] newer = new Object[] {decimal("2.00"), timestamp(3000L), 1L};

        Object[] merged = GraphEdgeIndexAggregator.mergeValues(schema, decl, stored, newer);

        assertThat(merged[0]).isEqualTo(decimal("8.00"));
        assertThat(merged[1]).isEqualTo(timestamp(3000L));
        assertThat(merged[2]).isEqualTo(3L);
    }

    @Test
    void testRowsUseIndexTableOrder() throws Exception {
        append(event(1L, 2L, "10.00"));

        GraphEdgeIndexAggregator.AggregatedEdges aggregated =
                GraphEdgeIndexAggregator.snapshot(base(), schema, decl, 1L);

        List<InternalRow> forward = aggregated.rows(decl, false);
        assertThat(forward).hasSize(1);
        assertThat(forward.get(0).getLong(0)).isEqualTo(1L);
        assertThat(forward.get(0).getLong(1)).isEqualTo(2L);

        List<InternalRow> reverse = aggregated.rows(decl, true);
        assertThat(reverse).hasSize(1);
        assertThat(reverse.get(0).getLong(0)).isEqualTo(2L);
        assertThat(reverse.get(0).getLong(1)).isEqualTo(1L);
    }

    @Test
    void testEmptyIntervalRejected() {
        assertThatThrownBy(() -> GraphEdgeIndexAggregator.incremental(base(), schema, decl, 3L, 3L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be non-empty");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    private FileStoreTable base() throws Exception {
        return (FileStoreTable) catalog.getTable(base);
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

    private static Decimal decimal(String value) {
        return Decimal.fromBigDecimal(new BigDecimal(value), 18, 2);
    }

    private static Timestamp timestamp(long millis) {
        return Timestamp.fromEpochMillis(millis);
    }

    /** Guards the type rendering used by the SQL generator too. */
    @Test
    void testIndexColumnTypes() {
        GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan("transfer", schema);
        DataType type =
                plan.edge().fields().stream()
                        .filter(f -> f.name().equals("sum_amount"))
                        .findFirst()
                        .get()
                        .type();
        assertThat(type).isEqualTo(DataTypes.DECIMAL(18, 2));
    }
}
