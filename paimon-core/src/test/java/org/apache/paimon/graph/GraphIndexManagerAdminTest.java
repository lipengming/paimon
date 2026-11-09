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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests of the graph index management API behind the create/drop procedures (design §5.6). */
class GraphIndexManagerAdminTest {

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
    void testCreateNodeMapPkIndex() throws Exception {
        Identifier person = createPersonVertex("person", true);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.createIndex(
                        catalog,
                        person,
                        GraphIndexSpec.vertex().fields("name,phone,city:bitmap").build(),
                        false);

        assertThat(results).hasSize(1);
        GraphIndexOperationResult result = results.get(0);
        assertThat(result.indexType()).isEqualTo(GraphIndexManager.INDEX_TYPE_NODE_MAP);
        assertThat(result.backend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(result.status()).isEqualTo(GraphIndexStatusTable.STATUS_ACTIVE);
        assertThat(result.indexTable()).isEmpty();

        Map<String, String> options = schema(person).options();
        assertThat(options)
                .containsEntry("graph.vertex.enabled", "true")
                .containsEntry("graph.vertex.id-field", "id")
                .containsEntry("graph.vertex.node-map.fields", "name,phone,city:bitmap")
                .containsEntry("global-index.btree.index-columns", "name,phone")
                .containsEntry("global-index.bitmap.index-columns", "city")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "node_map",
                        GraphIndexStatusTable.STATUS_ACTIVE);

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).indexType()).isEqualTo(GraphIndexManager.INDEX_TYPE_NODE_MAP);
        assertThat(rows.get(0).status()).isEqualTo(GraphIndexStatusTable.STATUS_ACTIVE);
        assertThat(rows.get(0).indexTable()).isEmpty();

        // create is idempotent
        GraphIndexManager.createIndex(
                catalog,
                person,
                GraphIndexSpec.vertex().fields("name,phone,city:bitmap").build(),
                false);
        assertThat(schema(person).options())
                .containsEntry("global-index.btree.index-columns", "name,phone");
    }

    @Test
    void testCreateNodeMapTableBackend() throws Exception {
        Identifier person = createPersonVertex("person_dynamic", false);
        appendPerson(person);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.createIndex(
                        catalog,
                        person,
                        GraphIndexSpec.vertex().fields("name,phone").build(),
                        false);

        assertThat(results.get(0).backend()).isEqualTo(GraphIndexOptions.BACKEND_TABLE);
        assertThat(results.get(0).indexTable()).isEqualTo("person_dynamic__node_map");
        assertThat(results.get(0).status()).isEqualTo(GraphIndexStatusTable.STATUS_BUILDING);

        FileStoreTable nodeMap =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("mydb", "person_dynamic__node_map"));
        assertThat(nodeMap.schema().primaryKeys())
                .containsExactly("prop_name", "prop_value", "node_id");
        assertThat(nodeMap.schema().fields())
                .extracting(DataField::name)
                .containsExactly("prop_name", "prop_value", "node_id");

        Map<String, String> options = schema(person).options();
        assertThat(options)
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "node_map",
                        GraphIndexStatusTable.STATUS_BUILDING)
                .containsEntry(GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + "node_map", "1");
    }

    @Test
    void testCreateAdjacencyFromSpec() throws Exception {
        Identifier transfer = createPlainEdgeTable();
        appendEvent(transfer);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.createIndex(
                        catalog,
                        transfer,
                        GraphIndexSpec.edge()
                                .srcField("src_id")
                                .dstField("dst_id")
                                .srcVertex("person")
                                .dstVertex("merchant")
                                .aggregates("amount:sum;*:count")
                                .reverse(true)
                                .build(),
                        false);

        assertThat(results).hasSize(2);
        assertThat(results)
                .extracting(GraphIndexOperationResult::indexTable)
                .containsExactly("transfer__edge", "transfer__edge_rev");
        assertThat(exists("transfer__edge")).isTrue();
        assertThat(exists("transfer__edge_rev")).isTrue();

        Map<String, String> options = schema(transfer).options();
        assertThat(options)
                .containsEntry("graph.edge.enabled", "true")
                .containsEntry("graph.edge.src-field", "src_id")
                .containsEntry("graph.edge.dst-field", "dst_id")
                .containsEntry("graph.edge.src-vertex", "person")
                .containsEntry("graph.edge.dst-vertex", "merchant")
                .containsEntry("graph.edge.aggregates", "amount:sum;*:count")
                .containsEntry("graph.edge.reverse", "true")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_BUILDING);

        assertThat(GraphIndexStatusTable.read(catalog, "mydb")).hasSize(2);
    }

    @Test
    void testDropLogicalKeepsArtifactsAndStopsMaintenance() throws Exception {
        Identifier transfer = createPlainEdgeTable();
        appendEvent(transfer);
        GraphIndexManager.createIndex(
                catalog,
                transfer,
                GraphIndexSpec.edge()
                        .srcField("src_id")
                        .dstField("dst_id")
                        .srcVertex("person")
                        .dstVertex("merchant")
                        .build(),
                false);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.dropIndex(
                        catalog,
                        transfer,
                        GraphIndexSpec.edge().build(),
                        GraphIndexManager.DropMode.LOGICAL,
                        false);

        assertThat(results).hasSize(2);
        assertThat(results)
                .allMatch(
                        result ->
                                result.status()
                                        .equals(GraphIndexStatusTable.STATUS_LOGICAL_DELETED));
        assertThat(exists("transfer__edge")).isTrue();
        assertThat(exists("transfer__edge_rev")).isTrue();
        assertThat(schema(transfer).options())
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_LOGICAL_DELETED);
        assertThat(GraphIndexStatusTable.read(catalog, "mydb"))
                .allMatch(row -> row.status().equals(GraphIndexStatusTable.STATUS_LOGICAL_DELETED));

        assertThatThrownBy(() -> GraphEdgeIndexMaintainer.maintain(catalog, transfer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("logically dropped");

        // re-enable by re-running create
        GraphIndexManager.createIndex(catalog, transfer, GraphIndexSpec.edge().build(), false);
        assertThat(GraphIndexStatusTable.read(catalog, "mydb"))
                .allMatch(row -> row.status().equals(GraphIndexStatusTable.STATUS_BUILDING));
    }

    @Test
    void testDropPurgeRemovesAdjacencyArtifacts() throws Exception {
        Identifier transfer = createPlainEdgeTable();
        appendEvent(transfer);
        GraphIndexManager.createIndex(
                catalog,
                transfer,
                GraphIndexSpec.edge()
                        .srcField("src_id")
                        .dstField("dst_id")
                        .srcVertex("person")
                        .dstVertex("merchant")
                        .build(),
                false);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.dropIndex(
                        catalog,
                        transfer,
                        GraphIndexSpec.edge().build(),
                        GraphIndexManager.DropMode.PURGE,
                        false);

        assertThat(results).hasSize(2);
        assertThat(exists("transfer__edge")).isFalse();
        assertThat(exists("transfer__edge_rev")).isFalse();
        Map<String, String> options = schema(transfer).options();
        assertThat(options)
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_DELETED)
                .doesNotContainKey(GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + "transfer__edge")
                .doesNotContainKey(GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge");
        assertThat(GraphIndexStatusTable.read(catalog, "mydb"))
                .allMatch(row -> row.status().equals(GraphIndexStatusTable.STATUS_DELETED));
    }

    @Test
    void testDropPurgeNodeMapPkIndexRemovesExpandedOptions() throws Exception {
        Identifier person = createPersonVertex("person_purge", true);
        GraphIndexManager.createIndex(
                catalog,
                person,
                GraphIndexSpec.vertex().fields("name,phone,city:bitmap").build(),
                false);

        GraphIndexManager.dropIndex(
                catalog,
                person,
                GraphIndexSpec.vertex().build(),
                GraphIndexManager.DropMode.PURGE,
                false);

        Map<String, String> options = schema(person).options();
        assertThat(options)
                .doesNotContainKey("global-index.btree.index-columns")
                .doesNotContainKey("global-index.bitmap.index-columns")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "node_map",
                        GraphIndexStatusTable.STATUS_DELETED);
        assertThat(GraphIndexStatusTable.read(catalog, "mydb"))
                .allMatch(row -> row.status().equals(GraphIndexStatusTable.STATUS_DELETED));
    }

    @Test
    void testDropDryRunDoesNotMutate() throws Exception {
        Identifier transfer = createPlainEdgeTable();
        appendEvent(transfer);
        GraphIndexManager.createIndex(
                catalog,
                transfer,
                GraphIndexSpec.edge()
                        .srcField("src_id")
                        .dstField("dst_id")
                        .srcVertex("person")
                        .dstVertex("merchant")
                        .build(),
                false);

        List<GraphIndexOperationResult> results =
                GraphIndexManager.dropIndex(
                        catalog,
                        transfer,
                        GraphIndexSpec.edge().build(),
                        GraphIndexManager.DropMode.PURGE,
                        true);

        assertThat(results).hasSize(2).allMatch(GraphIndexOperationResult::dryRun);
        assertThat(exists("transfer__edge")).isTrue();
        assertThat(GraphIndexStatusTable.read(catalog, "mydb"))
                .allMatch(row -> row.status().equals(GraphIndexStatusTable.STATUS_BUILDING));
        assertThat(schema(transfer).options())
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_BUILDING);
    }

    @Test
    void testDropUndeclaredTableRejected() throws Exception {
        Identifier transfer = createPlainEdgeTable();

        assertThatThrownBy(
                        () ->
                                GraphIndexManager.dropIndex(
                                        catalog,
                                        transfer,
                                        GraphIndexSpec.edge().build(),
                                        GraphIndexManager.DropMode.LOGICAL,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph edge");

        Identifier plain = createPlainEdgeTable("plain_vertex_drop");
        assertThatThrownBy(
                        () ->
                                GraphIndexManager.dropIndex(
                                        catalog,
                                        plain,
                                        GraphIndexSpec.vertex().build(),
                                        GraphIndexManager.DropMode.LOGICAL,
                                        false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph vertex");
    }

    @Test
    void testCreateVertexRejectsMultiColumnPrimaryKey() throws Exception {
        Identifier table = Identifier.create("mydb", "bad_vertex");
        catalog.createTable(
                table,
                new Schema(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.BIGINT()),
                                new DataField(1, "b", DataTypes.BIGINT())),
                        emptyList(),
                        Arrays.asList("a", "b"),
                        new HashMap<>(),
                        ""),
                false);

        assertThatThrownBy(
                        () ->
                                GraphIndexManager.createIndex(
                                        catalog,
                                        table,
                                        GraphIndexSpec.vertex().fields("a").build(),
                                        false))
                .hasMessageContaining("single-column primary key");
    }

    // --------------------------------------------------------------------------------------------
    // Fixtures
    // --------------------------------------------------------------------------------------------

    private boolean exists(String tableName) {
        try {
            catalog.getTable(Identifier.create("mydb", tableName));
            return true;
        } catch (Catalog.TableNotExistException e) {
            return false;
        }
    }

    private TableSchema schema(Identifier identifier) throws Exception {
        return ((FileStoreTable) catalog.getTable(identifier)).schema();
    }

    private Identifier createPersonVertex(String name, boolean fixedBucket) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("merge-engine", "deduplicate");
        if (fixedBucket) {
            options.put("bucket", "4");
            options.put("deletion-vectors.enabled", "true");
            options.put("deletion-vectors.merge-on-read", "false");
        }
        Identifier identifier = Identifier.create("mydb", name);
        catalog.createTable(
                identifier,
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING()),
                                new DataField(2, "phone", DataTypes.STRING()),
                                new DataField(3, "city", DataTypes.STRING())),
                        emptyList(),
                        singletonList("id"),
                        options,
                        ""),
                false);
        return identifier;
    }

    private void appendPerson(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(
                    GenericRow.of(
                            1L,
                            BinaryString.fromString("alice"),
                            BinaryString.fromString("p1"),
                            BinaryString.fromString("cityA")),
                    0);
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }

    private Identifier createPlainEdgeTable() throws Exception {
        return createPlainEdgeTable("transfer");
    }

    private Identifier createPlainEdgeTable(String name) throws Exception {
        Identifier identifier = Identifier.create("mydb", name);
        catalog.createTable(
                identifier,
                new Schema(
                        Arrays.asList(
                                new DataField(0, "src_id", DataTypes.BIGINT()),
                                new DataField(1, "dst_id", DataTypes.BIGINT()),
                                new DataField(2, "amount", DataTypes.DECIMAL(18, 2))),
                        emptyList(),
                        emptyList(),
                        new HashMap<>(),
                        ""),
                false);
        return identifier;
    }

    private void appendEvent(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(
                    GenericRow.of(1L, 2L, Decimal.fromBigDecimal(new BigDecimal("10.00"), 18, 2)));
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }
}
