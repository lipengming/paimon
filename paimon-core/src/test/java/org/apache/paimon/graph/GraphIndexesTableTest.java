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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests of the {@code $graph_indexes} system table over a file system catalog. */
class GraphIndexesTableTest {

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
    void testEdgeRows() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);
        GraphIndexManager.createEdgeIndex(catalog, base);

        List<InternalRow> rows = readGraphIndexes("transfer");
        assertThat(rows).hasSize(2);

        InternalRow forward = rows.get(0);
        assertThat(forward.getString(0).toString()).isEqualTo("transfer");
        assertThat(forward.getString(1).toString()).isEqualTo("adjacency");
        assertThat(forward.getString(2).toString()).isEqualTo("merge-engine");
        assertThat(forward.getString(3).toString()).isEqualTo("transfer__edge");
        assertThat(forward.getString(4).toString()).isEqualTo("src_id,dst_id");
        assertThat(forward.getString(5).toString()).isEqualTo("out,in");
        assertThat(forward.getString(6).toString()).isEqualTo("person");
        assertThat(forward.getString(7).toString()).isEqualTo("merchant");
        assertThat(forward.getString(8).toString())
                .isEqualTo("amount:sum;tx_time:last_non_null_value;*:count");
        assertThat(forward.getString(9).toString()).isEqualTo("BUILDING");
        assertThat(forward.isNullAt(10)).isTrue();
        assertThat(forward.isNullAt(11)).isTrue();
        assertThat(forward.getString(12).toString()).isEqualTo("bucket=1024,bucket-key=src_id");

        InternalRow reverse = rows.get(1);
        assertThat(reverse.getString(3).toString()).isEqualTo("transfer__edge_rev");
        assertThat(reverse.getString(5).toString()).isEqualTo("out,in");
        assertThat(reverse.getString(12).toString()).isEqualTo("bucket=1024,bucket-key=dst_id");
    }

    @Test
    void testVertexPkIndexRow() throws Exception {
        createVertexBase(256, true, true);
        List<InternalRow> rows = readGraphIndexes("person");
        assertThat(rows).hasSize(1);

        InternalRow row = rows.get(0);
        assertThat(row.getString(0).toString()).isEqualTo("person");
        assertThat(row.getString(1).toString()).isEqualTo("node_map");
        assertThat(row.getString(2).toString()).isEqualTo("pk-index");
        assertThat(row.isNullAt(3)).isTrue();
        assertThat(row.getString(4).toString()).isEqualTo("name,phone,city:bitmap");
        assertThat(row.isNullAt(5)).isTrue();
        assertThat(row.isNullAt(6)).isTrue();
        assertThat(row.isNullAt(7)).isTrue();
        assertThat(row.isNullAt(8)).isTrue();
        assertThat(row.getString(9).toString()).isEqualTo("BUILDING");
        assertThat(row.getString(12).toString())
                .contains("global-index.btree.index-columns=name,phone")
                .contains("global-index.bitmap.index-columns=city");
    }

    @Test
    void testVertexDegradedToTableBackend() throws Exception {
        createVertexBase(-1, false, false);
        List<InternalRow> rows = readGraphIndexes("person");
        assertThat(rows).hasSize(1);

        InternalRow row = rows.get(0);
        assertThat(row.getString(2).toString()).isEqualTo("table");
        assertThat(row.getString(3).toString()).isEqualTo("person__node_map");
        assertThat(row.getString(12).toString()).contains("unmet-requirements=");
    }

    @Test
    void testNoDeclarationsNoRows() throws Exception {
        catalog.createTable(
                Identifier.create("mydb", "plain"),
                new Schema(
                        singletonList(new DataField(0, "v", DataTypes.BIGINT())),
                        emptyList(),
                        emptyList(),
                        new HashMap<>(),
                        ""),
                false);
        assertThat(readGraphIndexes("plain")).isEmpty();
    }

    @Test
    void testWatermarkAndLag() throws Exception {
        Identifier base = GraphIndexManagerTestHelper.createTransferBase(catalog, true);
        GraphIndexManager.createEdgeIndex(catalog, base);

        catalog.alterTable(
                base,
                singletonList(
                        SchemaChange.setOption(
                                GraphIndexOptions.WATERMARK_KEY_PREFIX + "transfer__edge", "1")),
                false);

        List<InternalRow> rows = readGraphIndexes("transfer");
        InternalRow forward = rows.get(0);
        assertThat(forward.getLong(10)).isEqualTo(1L);
        assertThat(forward.getLong(11)).isGreaterThanOrEqualTo(0L);
        // reverse entry stays without a watermark
        assertThat(rows.get(1).isNullAt(10)).isTrue();
    }

    private List<InternalRow> readGraphIndexes(String tableName) throws Exception {
        Table graphIndexes =
                catalog.getTable(Identifier.create("mydb", tableName + "$graph_indexes"));
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                graphIndexes
                        .newReadBuilder()
                        .newRead()
                        .createReader(graphIndexes.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(rows::add);
        }
        return rows;
    }

    private void createVertexBase(int bucket, boolean deletionVectors, boolean mergeOnReadFalse)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(), "id");
        options.put(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), "name,phone,city:bitmap");
        options.put("bucket", String.valueOf(bucket));
        if (deletionVectors) {
            options.put("deletion-vectors.enabled", "true");
            if (mergeOnReadFalse) {
                options.put("deletion-vectors.merge-on-read", "false");
            }
        }
        catalog.createTable(
                Identifier.create("mydb", "person"),
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
    }
}
