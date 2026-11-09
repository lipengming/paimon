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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.graph.GraphIndexStatusTable;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases of the graph index create/drop procedures (design §5.2). */
public class GraphIndexProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testCreateAndDropNodeMapProcedure() throws Exception {
        sql(
                "CREATE TABLE person (id BIGINT, name STRING, phone STRING, city STRING,"
                        + " PRIMARY KEY (id) NOT ENFORCED) WITH ("
                        + "'bucket'='4',"
                        + "'deletion-vectors.enabled'='true',"
                        + "'deletion-vectors.merge-on-read'='false')");

        List<Row> create =
                sql(
                        "CALL sys.create_graph_index('default.person', 'node_map',"
                                + " 'name,phone,city:bitmap')");
        assertThat(create).hasSize(1);
        assertThat(create.get(0).toString()).contains("node_map").contains("pk-index");

        FileStoreTable person = paimonTable("person");
        assertThat(person.schema().options())
                .containsEntry("global-index.btree.index-columns", "name,phone")
                .containsEntry("global-index.bitmap.index-columns", "city")
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "node_map",
                        GraphIndexStatusTable.STATUS_ACTIVE);

        List<Row> rows = sql("SELECT index_type, backend, status FROM `person$graph_indexes`");
        assertThat(rows.stream().map(Row::toString))
                .containsExactly("+I[node_map, pk-index, ACTIVE]");

        // dry run must not mutate
        List<Row> dryRun =
                sql("CALL sys.drop_graph_index('default.person', 'node_map', false, true)");
        assertThat(dryRun.get(0).toString()).contains("(dry run)");
        assertThat(paimonTable("person").schema().options())
                .containsEntry("global-index.btree.index-columns", "name,phone");

        // logical drop keeps the artifacts and stops routing
        sql("CALL sys.drop_graph_index('default.person', 'node_map')");
        assertThat(paimonTable("person").schema().options())
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "node_map",
                        GraphIndexStatusTable.STATUS_LOGICAL_DELETED)
                .containsEntry("global-index.btree.index-columns", "name,phone");

        // purge removes the expanded declarations
        sql("CALL sys.drop_graph_index('default.person', 'node_map', true, false)");
        assertThat(paimonTable("person").schema().options())
                .doesNotContainKey("global-index.btree.index-columns")
                .doesNotContainKey("global-index.bitmap.index-columns");
    }

    @Test
    public void testCreateAndDropAdjacencyProcedure() throws Exception {
        sql("CREATE TABLE transfer (src_id BIGINT, dst_id BIGINT, amount DECIMAL(18, 2))");

        List<Row> create =
                sql(
                        "CALL sys.create_graph_index('default.transfer', 'adjacency',"
                                + " 'src_id', 'dst_id', 'person', 'merchant',"
                                + " 'amount:sum;*:count', true)");
        assertThat(create).hasSize(2);

        assertThat(paimonTable("transfer").schema().options())
                .containsEntry("graph.edge.enabled", "true")
                .containsEntry("graph.edge.src-field", "src_id")
                .containsEntry("graph.edge.dst-field", "dst_id")
                .containsEntry("graph.edge.src-vertex", "person")
                .containsEntry("graph.edge.dst-vertex", "merchant")
                .containsEntry("graph.edge.aggregates", "amount:sum;*:count");
        assertThat(paimonTable("transfer__edge")).isNotNull();
        assertThat(paimonTable("transfer__edge_rev")).isNotNull();

        // logical drop keeps the index tables and marks the state
        sql("CALL sys.drop_graph_index('default.transfer', 'adjacency')");
        assertThat(paimonTable("transfer").schema().options())
                .containsEntry(
                        GraphIndexOptions.STATUS_KEY_PREFIX + "transfer__edge",
                        GraphIndexStatusTable.STATUS_LOGICAL_DELETED);
        assertThat(paimonTable("transfer__edge")).isNotNull();
        assertThat(paimonTable("transfer__edge_rev")).isNotNull();

        // purge removes them
        sql("CALL sys.drop_graph_index('default.transfer', 'adjacency', true, false)");
        assertThat(tableExists("transfer__edge")).isFalse();
        assertThat(tableExists("transfer__edge_rev")).isFalse();
    }

    private boolean tableExists(String tableName) {
        Catalog catalog = flinkCatalog().catalog();
        try {
            catalog.getTable(Identifier.create(tEnv.getCurrentDatabase(), tableName));
            return true;
        } catch (Catalog.TableNotExistException e) {
            return false;
        }
    }
}
