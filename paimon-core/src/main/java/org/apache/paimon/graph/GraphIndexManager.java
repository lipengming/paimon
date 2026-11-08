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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Create pipeline of graph indexes (design §5.6): expands the declarations into physical index
 * tables through the versioned planners, records build state in the per-database {@code
 * graph_index_status} table, and defines the batch backfill contract used by the engine jobs.
 *
 * <p>Create is idempotent (design §5.6 'create 重跑'): existing index tables are kept, status rows
 * are overwritten, and the backfill start snapshot stays the latest base table snapshot.
 */
public final class GraphIndexManager {

    public static final String INDEX_TYPE_NODE_MAP = "node_map";
    public static final String INDEX_TYPE_ADJACENCY = "adjacency";

    /** Backend label of edge adjacency indexes: they are always aggregation index tables. */
    public static final String BACKEND_MERGE_ENGINE = "merge-engine";

    private GraphIndexManager() {}

    /**
     * Creates the physical adjacency index tables of one edge declaration and records BUILDING
     * status rows carrying the backfill start snapshot (design §6.2). The engine backfill jobs read
     * the base table at exactly that snapshot, so the stream maintenance can resume from {@code
     * snapshot + 1} without double counting.
     */
    public static List<Identifier> createEdgeIndex(Catalog catalog, Identifier baseTable)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableAlreadyExistException {
        Table base = catalog.getTable(baseTable);
        TableSchema baseSchema = ((FileStoreTable) base).schema();
        GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan(baseTable.getTableName(), baseSchema);
        Long buildSnapshotId = latestSnapshotId(base);

        List<Identifier> created = new ArrayList<>();
        created.add(createIndexTable(catalog, baseTable, plan.edgeTableName(), plan.edge()));
        if (plan.reverse()) {
            created.add(
                    createIndexTable(catalog, baseTable, plan.edgeRevTableName(), plan.edgeRev()));
        }

        String aggregates = baseSchema.options().get(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key());
        long now = System.currentTimeMillis();
        upsertStatus(catalog, baseTable, plan.edgeTableName(), aggregates, buildSnapshotId, now);
        if (plan.reverse()) {
            upsertStatus(
                    catalog, baseTable, plan.edgeRevTableName(), aggregates, buildSnapshotId, now);
        }
        return created;
    }

    private static Identifier createIndexTable(
            Catalog catalog, Identifier baseTable, String indexTableName, TableSchema schema)
            throws Catalog.DatabaseNotExistException, Catalog.TableAlreadyExistException {
        Identifier indexTable = Identifier.create(baseTable.getDatabaseName(), indexTableName);
        catalog.createTable(indexTable, toSchema(schema), true);
        return indexTable;
    }

    private static Schema toSchema(TableSchema schema) {
        return new Schema(
                schema.fields(),
                schema.partitionKeys(),
                schema.primaryKeys(),
                schema.options(),
                schema.comment());
    }

    private static void upsertStatus(
            Catalog catalog,
            Identifier baseTable,
            String indexTableName,
            String aggregates,
            Long buildSnapshotId,
            long now) {
        GraphIndexStatusTable.upsert(
                catalog,
                baseTable.getDatabaseName(),
                new GraphIndexStatusTable.StatusRow(
                        baseTable.getDatabaseName() + "." + baseTable.getTableName(),
                        INDEX_TYPE_ADJACENCY,
                        indexTableName,
                        BACKEND_MERGE_ENGINE,
                        aggregates == null ? "" : aggregates,
                        GraphIndexStatusTable.STATUS_BUILDING,
                        buildSnapshotId,
                        null,
                        now));
    }

    /** The latest snapshot id of the table, null when the table has no snapshot yet. */
    static Long latestSnapshotId(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return null;
        }
        Snapshot snapshot = ((FileStoreTable) table).store().snapshotManager().latestSnapshot();
        return snapshot == null ? null : snapshot.id();
    }
}
