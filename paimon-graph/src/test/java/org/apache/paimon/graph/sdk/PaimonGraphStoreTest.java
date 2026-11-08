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

package org.apache.paimon.graph.sdk;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.graph.GraphIndexManager;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests of the traversal SDK over a file system catalog: a pk-index vertex table plus an
 * aggregation edge table are created, backfilled and queried through the design §8.1 primitives
 * (nodeMapScan / adjScan / vertexLookup), including the super-vertex guards.
 */
class PaimonGraphStoreTest {

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @BeforeEach
    void before() throws Exception {
        String warehouse = tempDir.resolve("warehouse").toString();
        CatalogContext context = CatalogContext.create(new Options());
        FileIO fileIO = FileIO.get(new Path(warehouse), context);
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse), context);
        catalog.createDatabase("mydb", false);
        createPersonVertex();
        createTransferEdge();
    }

    @Test
    void testNodeMapEqualityScan() throws Exception {
        GraphStore store = builtStore();

        RoaringNavigableMap64 ids = store.nodeMap("person", "phone").eq("p1").scanToBitmap();

        assertThat(ids.getLongCardinality()).isEqualTo(2);
        assertThat(ids.contains(1L)).isTrue();
        assertThat(ids.contains(4L)).isTrue();
    }

    @Test
    void testNodeMapRangeScanAndConjunction() {
        GraphStore store = builtStore();

        RoaringNavigableMap64 range =
                store.nodeMap("person", "age").between(26L, 35L).scanToBitmap();
        RoaringNavigableMap64 cityA = store.nodeMap("person", "city").eq("cityA").scanToBitmap();
        range.and(cityA);

        assertThat(range.getLongCardinality()).isEqualTo(1);
        assertThat(range.contains(2L)).isTrue();
    }

    @Test
    void testAdjacencyOut() {
        GraphStore store = builtStore();

        Map<Long, BigDecimal> sums = new LinkedHashMap<>();
        Map<Long, Long> counts = new LinkedHashMap<>();
        try (AdjIterator it =
                store.adj("transfer", Direction.OUT).from(1).project("sum_amount", "cnt")) {
            while (it.next()) {
                sums.put(it.dst(), it.getDecimal("sum_amount"));
                counts.put(it.dst(), it.getLong("cnt"));
            }
        }

        assertThat(sums.keySet()).containsExactlyInAnyOrder(2L, 3L);
        assertThat(sums.get(2L)).isEqualByComparingTo(new BigDecimal("15.00"));
        assertThat(sums.get(3L)).isEqualByComparingTo(new BigDecimal("7.00"));
        assertThat(counts.get(2L)).isEqualTo(2L);
        assertThat(counts.get(3L)).isEqualTo(1L);
    }

    @Test
    void testAdjacencyIn() {
        GraphStore store = builtStore();

        Map<Long, BigDecimal> sums = new LinkedHashMap<>();
        try (AdjIterator it =
                store.adj("transfer", Direction.IN).from(2).project("sum_amount", "cnt")) {
            while (it.next()) {
                sums.put(it.dst(), it.getDecimal("sum_amount"));
            }
        }

        assertThat(sums.keySet()).containsExactly(1L);
        assertThat(sums.get(1L)).isEqualByComparingTo(new BigDecimal("15.00"));
    }

    @Test
    void testDegreeCapThrows() {
        GraphStore store = builtStore();

        AdjIterator it = store.adj("transfer", Direction.OUT).from(1).degreeCap(1);
        assertThatThrownBy(it::next)
                .isInstanceOf(GraphCapExceededException.class)
                .hasMessageContaining("hit degreeCap=1");
    }

    @Test
    void testResultCapThrows() {
        GraphStore store = builtStore();

        assertThatThrownBy(
                        () ->
                                store.nodeMap("person", "city")
                                        .eq("cityA")
                                        .resultCap(1)
                                        .scanToBitmap())
                .isInstanceOf(GraphCapExceededException.class)
                .hasMessageContaining("hit resultCap=1");
    }

    @Test
    void testVertexLookup() {
        GraphStore store = builtStore();

        Vertex alice = store.vertex("person").lookup(1L).orElseThrow(IllegalStateException::new);

        assertThat(alice.id()).isEqualTo(1L);
        assertThat(alice.getString("name")).isEqualTo("alice");
        assertThat(alice.getString("phone")).isEqualTo("p1");
        assertThat(alice.getLong("age")).isEqualTo(25L);
        assertThat(store.vertex("person").lookup(99L)).isEmpty();
    }

    @Test
    void testVertexIdTypeMismatch() {
        GraphStore store = builtStore();

        assertThatThrownBy(() -> store.vertex("person").lookup(new GraphVertexId("merchant", 1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type mismatch");
    }

    @Test
    void testBuilderRejectsVertexTypeMismatch() {
        assertThatThrownBy(
                        () ->
                                PaimonGraphStore.builder()
                                        .catalog(catalog)
                                        .defaultDatabase("mydb")
                                        .vertex("person")
                                        .edge("transfer", "person", "merchant")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("declares vertex types (person, person)");
    }

    @Test
    void testBuilderRequiresCreatedIndexTables() {
        assertThatThrownBy(
                        () ->
                                PaimonGraphStore.builder()
                                        .catalog(catalog)
                                        .defaultDatabase("mydb")
                                        .edge("transfer", "person", "person")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("run the graph index create pipeline first");
    }

    @Test
    void testFullModeUnsupported() {
        createAndBackfillIndex();
        GraphStore store =
                PaimonGraphStore.builder()
                        .catalog(catalog)
                        .defaultDatabase("mydb")
                        .vertex("person")
                        .edge("transfer", "person", "person")
                        .searchMode(SearchMode.FULL)
                        .build();

        assertThatThrownBy(() -> store.nodeMap("person", "phone").eq("p1").scanToBitmap())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("SearchMode FULL");
        assertThatThrownBy(() -> store.adj("transfer", Direction.OUT).from(1).next())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("SearchMode FULL");
    }

    @Test
    void testNodeMapUnknownField() {
        GraphStore store = builtStore();

        assertThatThrownBy(() -> store.nodeMap("person", "email"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not declared in 'graph.vertex.node-map.fields'");
    }

    @Test
    void testAdjacencyInRequiresReverse() throws Exception {
        createLikeEdgeWithoutReverse();
        createAndBackfillIndex();
        // 'like' declares reverse = false: only its forward table exists
        createLikeForwardTable();

        GraphStore store =
                PaimonGraphStore.builder()
                        .catalog(catalog)
                        .defaultDatabase("mydb")
                        .vertex("person")
                        .edge("transfer", "person", "person")
                        .edge("like", "person", "person")
                        .build();

        assertThatThrownBy(() -> store.adj("like", Direction.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not declare reverse adjacency");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    /**
     * Batch-writes rows through {@link TableWriteImpl} with an explicit {@link IOManager}:
     * deletion-vector tables run lookup compaction on write and need a temp disk directory, which
     * the plain batch write builder cannot carry.
     */
    private void batchWrite(Table table, List<GenericRow> rows) throws Exception {
        TableWriteImpl<?> write = ((FileStoreTable) table).newWrite("graph-sdk-test");
        write.withIOManager(
                IOManager.create(tempDir.resolve("io-" + System.nanoTime()).toString()));
        try {
            for (GenericRow row : rows) {
                write.write(row);
            }
            List<CommitMessage> messages = write.prepareCommit();
            try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
                commit.commit(messages);
            }
        } finally {
            write.close();
        }
    }

    private void createPersonVertex() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(), "id");
        options.put(
                GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), "name,phone,city:bitmap,age");
        options.put("bucket", "4");
        options.put("deletion-vectors.enabled", "true");
        options.put("deletion-vectors.merge-on-read", "false");

        catalog.createTable(
                Identifier.create("mydb", "person"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING()),
                                new DataField(2, "phone", DataTypes.STRING()),
                                new DataField(3, "city", DataTypes.STRING()),
                                new DataField(4, "age", DataTypes.BIGINT())),
                        emptyList(),
                        singletonList("id"),
                        options,
                        ""),
                false);

        Table person = catalog.getTable(Identifier.create("mydb", "person"));
        batchWrite(
                person,
                Arrays.asList(
                        row(1L, "alice", "p1", "cityA", 25L),
                        row(2L, "bob", "p2", "cityA", 30L),
                        row(3L, "carol", "p3", "cityB", 35L),
                        row(4L, "dave", "p1", "cityB", 28L)));
    }

    private static GenericRow row(long id, String name, String phone, String city, long age) {
        return GenericRow.of(
                id,
                BinaryString.fromString(name),
                BinaryString.fromString(phone),
                BinaryString.fromString(city),
                age);
    }

    private void createTransferEdge() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), "amount:sum;*:count");

        catalog.createTable(
                Identifier.create("mydb", "transfer"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT()),
                                new DataField(2, "amount", DataTypes.DECIMAL(18, 2)),
                                new DataField(3, "dt", DataTypes.STRING())),
                        singletonList("dt"),
                        emptyList(),
                        options,
                        ""),
                false);

        Table transfer = catalog.getTable(Identifier.create("mydb", "transfer"));
        batchWrite(
                transfer,
                Arrays.asList(
                        event(1L, 2L, "10.00"),
                        event(1L, 2L, "5.00"),
                        event(1L, 3L, "7.00"),
                        event(2L, 3L, "1.00"),
                        event(3L, 4L, "2.00")));
    }

    private static GenericRow event(long src, long dst, String amount) {
        return GenericRow.of(
                src,
                dst,
                Decimal.fromBigDecimal(new BigDecimal(amount), 18, 2),
                BinaryString.fromString("2026-11-08"));
    }

    private GraphStore builtStore() {
        createAndBackfillIndex();
        return PaimonGraphStore.builder()
                .catalog(catalog)
                .defaultDatabase("mydb")
                .vertex("person")
                .edge("transfer", "person", "person")
                .build();
    }

    /** Runs the create pipeline and a test-side batch backfill (sum/cnt aggregation in Java). */
    private void createAndBackfillIndex() {
        try {
            GraphIndexManager.createEdgeIndex(catalog, Identifier.create("mydb", "transfer"));

            // aggregate the five events per (src, dst): sum(amount), count(*)
            Map<Long, Map<Long, BigDecimal[]>> agg = new LinkedHashMap<>();
            FileStoreTable base =
                    (FileStoreTable) catalog.getTable(Identifier.create("mydb", "transfer"));
            int[] projection = {0, 1, 2};
            try (RecordReader<InternalRow> reader =
                    base.newReadBuilder()
                            .withProjection(projection)
                            .newRead()
                            .createReader(base.newReadBuilder().newScan().plan())) {
                reader.forEachRemaining(
                        row -> {
                            long src = row.getLong(0);
                            long dst = row.getLong(1);
                            BigDecimal amount = row.getDecimal(2, 18, 2).toBigDecimal();
                            BigDecimal[] slot =
                                    agg.computeIfAbsent(src, k -> new LinkedHashMap<>())
                                            .computeIfAbsent(
                                                    dst,
                                                    k ->
                                                            new BigDecimal[] {
                                                                BigDecimal.ZERO, BigDecimal.ZERO
                                                            });
                            slot[0] = slot[0].add(amount);
                            slot[1] = slot[1].add(BigDecimal.ONE);
                        });
            }

            writeAdjacency("transfer__edge", agg, false);
            writeAdjacency("transfer__edge_rev", agg, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void writeAdjacency(
            String tableName, Map<Long, Map<Long, BigDecimal[]>> agg, boolean reverse)
            throws Exception {
        FileStoreTable table =
                (FileStoreTable) catalog.getTable(Identifier.create("mydb", tableName));
        List<GenericRow> rows = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, BigDecimal[]>> srcEntry : agg.entrySet()) {
            for (Map.Entry<Long, BigDecimal[]> dstEntry : srcEntry.getValue().entrySet()) {
                long first = reverse ? dstEntry.getKey() : srcEntry.getKey();
                long second = reverse ? srcEntry.getKey() : dstEntry.getKey();
                BigDecimal sum = dstEntry.getValue()[0];
                long cnt = dstEntry.getValue()[1].longValue();
                rows.add(GenericRow.of(first, second, Decimal.fromBigDecimal(sum, 18, 2), cnt));
            }
        }
        batchWrite(table, rows);
    }

    private void createLikeEdgeWithoutReverse() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), "src_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), "dst_id");
        options.put(GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), "person");
        options.put(GraphIndexOptions.GRAPH_EDGE_REVERSE.key(), "false");

        catalog.createTable(
                Identifier.create("mydb", "like"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT()),
                                new DataField(2, "dt", DataTypes.STRING())),
                        singletonList("dt"),
                        emptyList(),
                        options,
                        ""),
                false);
    }

    private void createLikeForwardTable() throws Exception {
        // simulates the create pipeline's forward-only expansion of 'like'
        catalog.createTable(
                Identifier.create("mydb", "like__edge"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT())),
                        emptyList(),
                        Arrays.asList("src_id", "dst_id"),
                        new HashMap<>(),
                        ""),
                false);
    }
}
