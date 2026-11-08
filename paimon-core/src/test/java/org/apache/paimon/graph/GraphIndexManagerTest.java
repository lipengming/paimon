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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end tests of the edge index create pipeline over a file system catalog. */
class GraphIndexManagerTest {

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
    void testCreateEdgeIndexEndToEnd() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);

        List<Identifier> created = GraphIndexManager.createEdgeIndex(catalog, base);

        assertThat(created).hasSize(2);
        assertThat(created.get(0).getTableName()).isEqualTo("transfer__edge");
        assertThat(created.get(1).getTableName()).isEqualTo("transfer__edge_rev");

        TableSchema edge = ((FileStoreTable) catalog.getTable(created.get(0))).schema();
        assertThat(edge.primaryKeys()).containsExactly("src_id", "dst_id");
        assertThat(edge.fieldNames())
                .containsExactly(
                        "src_id", "dst_id", "sum_amount", "last_non_null_value_tx_time", "cnt");
        assertThat(edge.options())
                .containsEntry("merge-engine", "aggregation")
                .containsEntry("bucket-key", "src_id")
                .containsEntry("bucket", "1024")
                .containsEntry("fields.cnt.aggregate-function", "sum");

        TableSchema rev = ((FileStoreTable) catalog.getTable(created.get(1))).schema();
        assertThat(rev.primaryKeys()).containsExactly("dst_id", "src_id");
        assertThat(rev.options()).containsEntry("bucket-key", "dst_id");

        // status mirror on the base table feeds the $graph_indexes system table
        Map<String, String> baseOptions =
                ((FileStoreTable) catalog.getTable(base)).schema().options();
        assertThat(baseOptions)
                .containsEntry(GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge", "BUILDING")
                .containsEntry(GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + "transfer__edge", "1")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge_rev", "BUILDING")
                .containsEntry(
                        GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + "transfer__edge_rev", "1");

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(2);
        for (GraphIndexStatusTable.StatusRow row : rows) {
            assertThat(row.baseTable()).isEqualTo("mydb.transfer");
            assertThat(row.indexType()).isEqualTo("adjacency");
            assertThat(row.backend()).isEqualTo("merge-engine");
            assertThat(row.status()).isEqualTo("BUILDING");
            assertThat(row.buildSnapshotId()).isEqualTo(1L);
            assertThat(row.coverageWatermark()).isNull();
            assertThat(row.fields()).isEqualTo("amount:sum;tx_time:last_non_null_value;*:count");
        }
    }

    @Test
    void testCreateEdgeIndexIdempotent() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);

        GraphIndexManager.createEdgeIndex(catalog, base);
        GraphIndexManager.createEdgeIndex(catalog, base);

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(2);
    }

    @Test
    void testCreateEdgeIndexWithoutSnapshot() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, false);

        GraphIndexManager.createEdgeIndex(catalog, base);

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(2);
        for (GraphIndexStatusTable.StatusRow row : rows) {
            assertThat(row.buildSnapshotId()).isNull();
        }
    }

    @Test
    void testCreateEdgeIndexRequiresEdgeDeclaration() throws Exception {
        Identifier id = Identifier.create("mydb", "plain");
        catalog.createTable(
                id,
                new Schema(
                        singletonList(new DataField(0, "v", DataTypes.BIGINT())),
                        emptyList(),
                        emptyList(),
                        new HashMap<>(),
                        ""),
                false);

        assertThatThrownBy(() -> GraphIndexManager.createEdgeIndex(catalog, id))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not declared as a graph edge");
    }

    @Test
    void testStatusUpsertOverwrites() throws Exception {
        GraphIndexStatusTable.upsert(
                catalog,
                "mydb",
                new GraphIndexStatusTable.StatusRow(
                        "mydb.transfer",
                        "adjacency",
                        "transfer__edge",
                        "merge-engine",
                        "*:count",
                        GraphIndexStatusTable.STATUS_BUILDING,
                        3L,
                        null,
                        1L));
        GraphIndexStatusTable.upsert(
                catalog,
                "mydb",
                new GraphIndexStatusTable.StatusRow(
                        "mydb.transfer",
                        "adjacency",
                        "transfer__edge",
                        "merge-engine",
                        "*:count",
                        GraphIndexStatusTable.STATUS_ACTIVE,
                        3L,
                        5L,
                        2L));

        List<GraphIndexStatusTable.StatusRow> rows = GraphIndexStatusTable.read(catalog, "mydb");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).status()).isEqualTo("ACTIVE");
        assertThat(rows.get(0).coverageWatermark()).isEqualTo(5L);
    }
}
