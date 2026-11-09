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
import org.apache.paimon.graph.GraphEdgeIndexReconciler;
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
import static org.assertj.core.api.Assertions.entry;

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

    @Test
    void testTwoHopOut() {
        GraphStore store = builtStore();

        List<Path2> paths =
                store.twoHop(
                        "transfer", Direction.OUT, 1L, null, DegreeCap.of(10), ResultCap.of(100));

        assertThat(paths).hasSize(2);
        assertThat(paths.toString()).contains("1 -> 2 -> 3", "1 -> 3 -> 4");
        for (Path2 path : paths) {
            assertThat(path.src()).isEqualTo(1L);
            if (path.mid() == 2L) {
                assertThat(path.dst()).isEqualTo(3L);
                assertThat(path.firstHop().getDecimal("sum_amount"))
                        .isEqualByComparingTo(new BigDecimal("15.00"));
                assertThat(path.firstHop().getLong("cnt")).isEqualTo(2L);
                assertThat(path.secondHop().getDecimal("sum_amount"))
                        .isEqualByComparingTo(new BigDecimal("1.00"));
            } else {
                assertThat(path.mid()).isEqualTo(3L);
                assertThat(path.dst()).isEqualTo(4L);
                assertThat(path.firstHop().getDecimal("sum_amount"))
                        .isEqualByComparingTo(new BigDecimal("7.00"));
                assertThat(path.secondHop().getLong("cnt")).isEqualTo(1L);
            }
        }
    }

    @Test
    void testTwoHopIn() {
        GraphStore store = builtStore();

        List<Path2> paths =
                store.twoHop(
                        "transfer", Direction.IN, 4L, null, DegreeCap.of(10), ResultCap.of(100));

        assertThat(paths).hasSize(2);
        assertThat(paths.toString()).contains("4 -> 3 -> 1", "4 -> 3 -> 2");
    }

    @Test
    void testTwoHopEdgeFilterRestrictsFirstHop() {
        GraphStore store = builtStore();

        List<Path2> paths =
                store.twoHop(
                        "transfer",
                        Direction.OUT,
                        1L,
                        EdgeFilter.where("sum_amount").ge(new BigDecimal("10")),
                        DegreeCap.of(10),
                        ResultCap.of(100));

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0).mid()).isEqualTo(2L);
        assertThat(paths.get(0).dst()).isEqualTo(3L);
    }

    @Test
    void testTwoHopEdgeFilterLongColumn() {
        GraphStore store = builtStore();

        List<Path2> paths =
                store.twoHop(
                        "transfer",
                        Direction.OUT,
                        1L,
                        EdgeFilter.where("cnt").gt(1L),
                        DegreeCap.of(10),
                        ResultCap.of(100));

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0).mid()).isEqualTo(2L);
    }

    @Test
    void testTwoHopDegreeCapKeepsTopIntermediateVertices() {
        GraphStore store = builtStore();

        // the first hop is ranked by sum_amount descending, so only 1 -> 2 -> 3 remains
        List<Path2> paths =
                store.twoHop(
                        "transfer", Direction.OUT, 1L, null, DegreeCap.of(1), ResultCap.of(100));

        assertThat(paths).hasSize(1);
        assertThat(paths.get(0).mid()).isEqualTo(2L);
    }

    @Test
    void testTwoHopResultCapThrows() {
        GraphStore store = builtStore();

        assertThatThrownBy(
                        () ->
                                store.twoHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        null,
                                        DegreeCap.of(10),
                                        ResultCap.of(1)))
                .isInstanceOf(GraphCapExceededException.class)
                .hasMessageContaining("exceeded resultCap=1");
    }

    @Test
    void testKHopBfs() {
        GraphStore store = builtStore();

        List<GraphPath> paths =
                store.kHop("transfer", Direction.OUT, 1L, 2, DegreeCap.of(10), ResultCap.of(100));

        assertThat(paths).hasSize(3);
        assertThat(paths.toString()).contains("1 -> 2", "1 -> 3", "1 -> 3 -> 4");
        for (GraphPath path : paths) {
            assertThat(path.start()).isEqualTo(1L);
            assertThat(path.length()).isBetween(1, 2);
        }
    }

    @Test
    void testKHopBreaksCycles() {
        GraphStore store = builtStore();

        // 1 -> 2 -> 3 -> 4 is discoverable, the back edge 2 -> 3 is already visited
        List<GraphPath> paths =
                store.kHop("transfer", Direction.OUT, 1L, 3, DegreeCap.of(10), ResultCap.of(100));

        assertThat(paths).hasSize(3);
        assertThat(paths.stream().anyMatch(path -> path.length() == 3)).isFalse();
    }

    @Test
    void testKHopInDirection() {
        GraphStore store = builtStore();

        List<GraphPath> paths =
                store.kHop("transfer", Direction.IN, 4L, 2, DegreeCap.of(10), ResultCap.of(100));

        assertThat(paths).hasSize(3);
        assertThat(paths.toString()).contains("4 -> 3", "4 -> 3 -> 1", "4 -> 3 -> 2");
    }

    @Test
    void testKHopCapsAndValidation() {
        GraphStore store = builtStore();

        assertThatThrownBy(
                        () ->
                                store.kHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        2,
                                        DegreeCap.of(1),
                                        ResultCap.of(100)))
                .isInstanceOf(GraphCapExceededException.class)
                .hasMessageContaining("hit degreeCap=1");

        assertThatThrownBy(
                        () ->
                                store.kHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        2,
                                        DegreeCap.of(10),
                                        ResultCap.of(1)))
                .isInstanceOf(GraphCapExceededException.class)
                .hasMessageContaining("exceeded resultCap=1");

        assertThatThrownBy(
                        () ->
                                store.kHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        0,
                                        DegreeCap.of(10),
                                        ResultCap.of(100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");

        assertThatThrownBy(
                        () ->
                                store.kHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        6,
                                        DegreeCap.of(10),
                                        ResultCap.of(100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 5 hops");
    }

    @Test
    void testEdgeFilterOnAdjIterator() {
        GraphStore store = builtStore();

        List<Long> matched = new ArrayList<>();
        try (AdjIterator it =
                store.adj("transfer", Direction.OUT).from(1).project("sum_amount", "cnt")) {
            EdgeFilter filter = EdgeFilter.where("sum_amount").ge(new BigDecimal("10"));
            while (it.next()) {
                if (filter.test(it)) {
                    matched.add(it.dst());
                }
            }
        }

        assertThat(matched).containsExactly(2L);
    }

    @Test
    void testCapsRejectNonPositiveValues() {
        assertThatThrownBy(() -> DegreeCap.of(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DegreeCap must be positive");
        assertThatThrownBy(() -> ResultCap.of(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ResultCap must be positive");
    }

    @Test
    void testTwoHopUnknownEdgeAndColumn() {
        GraphStore store = builtStore();

        assertThatThrownBy(
                        () ->
                                store.twoHop(
                                        "unknown",
                                        Direction.OUT,
                                        1L,
                                        null,
                                        DegreeCap.of(10),
                                        ResultCap.of(100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not bound to this graph store");

        assertThatThrownBy(
                        () ->
                                store.twoHop(
                                        "transfer",
                                        Direction.OUT,
                                        1L,
                                        EdgeFilter.where("nope").ge(1L),
                                        DegreeCap.of(10),
                                        ResultCap.of(100)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not part of the adjacency payload");
    }

    @Test
    void testFullModeCatchesUpGap() throws Exception {
        GraphStore fast = builtStore();
        appendEvents(event(1L, 2L, "4.00"), event(2L, 4L, "9.00"), event(5L, 2L, "3.00"));

        GraphStore full = store(SearchMode.FULL);

        // FAST mode reads the index snapshot only
        assertThat(adjacency(fast, Direction.OUT, 1L, "sum_amount", "cnt"))
                .containsOnly(entry(2L, "15.00|2"), entry(3L, "7.00|1"));
        assertThat(adjacency(fast, Direction.OUT, 2L, "sum_amount", "cnt"))
                .containsOnly(entry(3L, "1.00|1"));

        // FULL mode overlays the interval after the coverage watermark at query time
        assertThat(adjacency(full, Direction.OUT, 1L, "sum_amount", "cnt"))
                .containsEntry(2L, "19.00|3");
        assertThat(adjacency(full, Direction.OUT, 2L, "sum_amount", "cnt"))
                .containsEntry(3L, "1.00|1")
                .containsEntry(4L, "9.00|1");
        // the 1 -> 2 append is caught up in the reverse adjacency as well
        assertThat(adjacency(full, Direction.IN, 2L, "sum_amount", "cnt"))
                .containsOnly(entry(1L, "19.00|3"), entry(5L, "3.00|1"));
    }

    @Test
    void testFullModeMatchesFastWhenCovered() throws Exception {
        GraphStore fast = builtStore();
        GraphStore full = store(SearchMode.FULL);

        assertThat(adjacency(full, Direction.OUT, 1L, "sum_amount", "cnt"))
                .isEqualTo(adjacency(fast, Direction.OUT, 1L, "sum_amount", "cnt"));
        assertThat(full.nodeMap("person", "phone").eq("p1").scanToBitmap().getLongCardinality())
                .isEqualTo(2L);
        assertThat(full.vertex("person").lookup(1L)).isPresent();
    }

    @Test
    void testDetailModeDelegatesToReconcile() {
        GraphStore store = builtStore();
        GraphStore detail = store(SearchMode.DETAIL);

        assertThatThrownBy(() -> detail.adj("transfer", Direction.OUT).from(1L).next())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("GraphStore.reconcile");
        assertThatThrownBy(() -> detail.nodeMap("person", "phone").eq("p1").scanToBitmap())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("reconciliation mode");
        assertThatThrownBy(() -> detail.vertex("person").lookup(1L))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("reconciliation mode");
        assertThat(store.reconcile("transfer")).hasSize(2).allMatch(result -> result.consistent());
    }

    @Test
    void testReconcileDetectsMissingPairs() throws Exception {
        builtStore();
        // an existing pair that was not maintained, and a brand new pair
        appendEvents(event(1L, 2L, "4.00"), event(6L, 7L, "1.00"));

        List<GraphEdgeIndexReconciler.IndexReconcileResult> results =
                store(SearchMode.FAST).reconcile("transfer");

        assertThat(results).hasSize(2).noneMatch(result -> result.consistent());
        for (GraphEdgeIndexReconciler.IndexReconcileResult result : results) {
            assertThat(result.missingPairs()).isEqualTo(1L);
            assertThat(result.mismatchedPairs()).isEqualTo(1L);
            assertThat(String.join(";", result.samples()))
                    .contains("missing")
                    .contains("mismatched");
        }
    }

    @Test
    void testReconcileDetectsExtraPairsAndPositiveMismatch() throws Exception {
        builtStore();
        // tamper forward index: an extra pair and a wrong sum for an existing pair
        batchWrite(
                table("transfer__edge"),
                Arrays.asList(
                        GenericRow.of(8L, 9L, decimal("1.00"), 1L),
                        GenericRow.of(1L, 2L, decimal("99.00"), 2L)));

        List<GraphEdgeIndexReconciler.IndexReconcileResult> results =
                store(SearchMode.FAST).reconcile("transfer");

        GraphEdgeIndexReconciler.IndexReconcileResult forward = results.get(0);
        assertThat(forward.consistent()).isFalse();
        assertThat(forward.extraPairs()).isEqualTo(1L);
        assertThat(forward.mismatchedPairs()).isEqualTo(1L);
        assertThat(String.join(";", forward.samples()))
                .contains("extra (8, 9)")
                .contains("mismatched (1, 2)");

        GraphEdgeIndexReconciler.IndexReconcileResult reverse = results.get(1);
        assertThat(reverse.consistent()).isTrue();
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
        return store(SearchMode.FAST);
    }

    private GraphStore store(SearchMode searchMode) {
        return PaimonGraphStore.builder()
                .catalog(catalog)
                .defaultDatabase("mydb")
                .vertex("person")
                .edge("transfer", "person", "person")
                .searchMode(searchMode)
                .build();
    }

    private FileStoreTable table(String name) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("mydb", name));
    }

    private void appendEvents(GenericRow... events) throws Exception {
        batchWrite(table("transfer"), Arrays.asList(events));
    }

    private static Decimal decimal(String value) {
        return Decimal.fromBigDecimal(new BigDecimal(value), 18, 2);
    }

    /** Reads 'dst' -> 'sum|cnt' of one adjacency scan. */
    private static Map<Long, String> adjacency(
            GraphStore store, Direction direction, long from, String... columns) {
        Map<Long, String> result = new LinkedHashMap<>();
        try (AdjIterator it = store.adj("transfer", direction).from(from).project(columns)) {
            while (it.next()) {
                result.put(
                        it.dst(),
                        it.getDecimal("sum_amount").toPlainString() + "|" + it.getLong("cnt"));
            }
        }
        return result;
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
