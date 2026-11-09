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

package org.apache.paimon.spark.procedure

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.spark.PaimonSparkTestBase

class GraphIndexProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: create and drop node_map graph index") {
    spark.sql(s"""
                 |CREATE TABLE person (
                 |  id BIGINT,
                 |  name STRING,
                 |  phone STRING,
                 |  city STRING
                 |) TBLPROPERTIES (
                 |  'primary-key' = 'id',
                 |  'bucket' = '4',
                 |  'deletion-vectors.enabled' = 'true',
                 |  'deletion-vectors.merge-on-read' = 'false'
                 |)
                 |""".stripMargin)

    val created =
      spark
        .sql(
          "CALL sys.create_graph_index(table => '`person`', index_type => 'node_map'," +
            " fields => 'name,phone,city:bitmap')")
        .collect()
    assert(created.length == 1)
    assert(created(0).getString(0) == "node_map")
    assert(created(0).getString(2) == "pk-index")
    assert(created(0).getString(3) == "ACTIVE")

    val options = loadTable("person").options()
    assert(options.get("global-index.btree.index-columns") == "name,phone")
    assert(options.get("global-index.bitmap.index-columns") == "city")
    assert(options.get("graph.index.status.node_map") == "ACTIVE")

    val rows = spark
      .sql("SELECT index_type, backend, status FROM `person$graph_indexes`")
      .collect()
    assert(rows.length == 1)
    assert(rows(0).getString(0) == "node_map")
    assert(rows(0).getString(1) == "pk-index")
    assert(rows(0).getString(2) == "ACTIVE")

    // dry run must not mutate
    val dryRun =
      spark
        .sql(
          "CALL sys.drop_graph_index(table => '`person`', index_type => 'node_map'," +
            " purge => false, dry_run => true)")
        .collect()
    assert(dryRun(0).getBoolean(4))
    assert(loadTable("person").options().get("global-index.btree.index-columns") == "name,phone")

    // logical drop keeps the declarations
    spark.sql("CALL sys.drop_graph_index(table => '`person`', index_type => 'node_map')")
    assert(loadTable("person").options().get("graph.index.status.node_map") == "LOGICAL_DELETED")
    assert(loadTable("person").options().get("global-index.btree.index-columns") == "name,phone")

    // purge removes the expanded declarations
    spark.sql(
      "CALL sys.drop_graph_index(table => '`person`', index_type => 'node_map', purge => true)")
    val purged = loadTable("person").options()
    assert(!purged.containsKey("global-index.btree.index-columns"))
    assert(!purged.containsKey("global-index.bitmap.index-columns"))
  }

  test("Paimon Procedure: create and drop adjacency graph index") {
    spark.sql("CREATE TABLE transfer (src_id BIGINT, dst_id BIGINT, amount DECIMAL(18, 2))")

    val created =
      spark
        .sql(
          "CALL sys.create_graph_index(table => '`transfer`', index_type => 'adjacency'," +
            " src_field => 'src_id', dst_field => 'dst_id', src_vertex => 'person'," +
            " dst_vertex => 'merchant', aggregates => 'amount:sum;*:count', reverse => true)")
        .collect()
    assert(created.length == 2)
    assert(created.exists(_.getString(1) == "transfer__edge"))
    assert(created.exists(_.getString(1) == "transfer__edge_rev"))

    val options = loadTable("transfer").options()
    assert(options.get("graph.edge.enabled") == "true")
    assert(options.get("graph.edge.src-field") == "src_id")
    assert(options.get("graph.edge.dst-field") == "dst_id")
    assert(options.get("graph.edge.aggregates") == "amount:sum;*:count")

    // logical drop keeps the index tables
    spark.sql("CALL sys.drop_graph_index(table => '`transfer`', index_type => 'adjacency')")
    assert(
      loadTable("transfer").options().get("graph.index.status.transfer__edge") == "LOGICAL_DELETED")
    assert(tableExists("transfer__edge"))

    // purge removes them
    spark.sql(
      "CALL sys.drop_graph_index(table => '`transfer`', index_type => 'adjacency', purge => true)")
    assert(!tableExists("transfer__edge"))
    assert(!tableExists("transfer__edge_rev"))
  }

  private def tableExists(tableName: String): Boolean = {
    try {
      paimonCatalog.getTable(Identifier.create(dbName0, tableName))
      true
    } catch {
      case _: org.apache.paimon.catalog.Catalog.TableNotExistException => false
    }
  }
}
