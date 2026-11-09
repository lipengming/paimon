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
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        mirrorStatus(
                catalog, baseTable, baseSchema.options(), plan.edgeTableName(), buildSnapshotId);
        if (plan.reverse()) {
            mirrorStatus(
                    catalog,
                    baseTable,
                    baseSchema.options(),
                    plan.edgeRevTableName(),
                    buildSnapshotId);
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

    /**
     * Mirrors one index entry's state onto the base table options so that the {@code
     * $graph_indexes} system table can expose it without a catalog reference; the maintenance
     * pipeline refreshes the watermark key per checkpoint (design §5.1/§5.5).
     */
    private static void mirrorStatus(
            Catalog catalog,
            Identifier baseTable,
            Map<String, String> currentOptions,
            String indexTableName,
            Long buildSnapshotId) {
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.setOption(
                        GraphIndexOptions.STATUS_KEY_PREFIX + indexTableName,
                        GraphIndexStatusTable.STATUS_BUILDING));
        String buildSnapshotKey = GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + indexTableName;
        if (buildSnapshotId != null) {
            changes.add(SchemaChange.setOption(buildSnapshotKey, String.valueOf(buildSnapshotId)));
        } else if (currentOptions.containsKey(buildSnapshotKey)) {
            changes.add(SchemaChange.removeOption(buildSnapshotKey));
        }
        String watermarkKey = GraphIndexOptions.WATERMARK_KEY_PREFIX + indexTableName;
        if (currentOptions.containsKey(watermarkKey)) {
            changes.add(SchemaChange.removeOption(watermarkKey));
        }
        try {
            catalog.alterTable(baseTable, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to mirror graph index status onto table '"
                            + baseTable.getDatabaseName()
                            + "."
                            + baseTable.getTableName()
                            + "'",
                    e);
        }
    }

    /** Lifecycle phase of a drop request (design §5.6). */
    public enum DropMode {
        LOGICAL,
        PURGE
    }

    private static final String AGGREGATION_MERGE_ENGINE = "aggregation";
    private static final String DEDUPLICATE_MERGE_ENGINE = "deduplicate";

    /**
     * Creates the physical representation of one graph index binding (design §5.6): the vertex
     * node-map expansion (pk-index column declarations or the standalone node-map table) or the
     * edge adjacency tables. When the spec carries declaration arguments and the table is not
     * declared yet, the declaration is written first, so the SQL procedures work on plain tables.
     *
     * <p>{@code dryRun} reports the planned operations without mutating the catalog or the tables.
     */
    public static List<GraphIndexOperationResult> createIndex(
            Catalog catalog, Identifier baseTable, GraphIndexSpec spec, boolean dryRun)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableAlreadyExistException {
        switch (spec.type()) {
            case NODE_MAP:
                return createNodeMapIndex(catalog, baseTable, spec.fields(), dryRun);
            case ADJACENCY:
                return createAdjacencyIndex(catalog, baseTable, spec, dryRun);
            default:
                throw new IllegalArgumentException("Unknown graph index type " + spec.type());
        }
    }

    private static List<GraphIndexOperationResult> createNodeMapIndex(
            Catalog catalog, Identifier baseTable, @Nullable String fields, boolean dryRun)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableAlreadyExistException {
        ensureVertexDeclaration(catalog, baseTable, fields, dryRun);
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema baseSchema = base.schema();
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(baseTable.getTableName(), baseSchema);
        String status =
                GraphIndexOptions.BACKEND_PK_INDEX.equals(plan.backend())
                        ? GraphIndexStatusTable.STATUS_ACTIVE
                        : GraphIndexStatusTable.STATUS_BUILDING;
        Long buildSnapshot =
                GraphIndexOptions.BACKEND_PK_INDEX.equals(plan.backend())
                        ? null
                        : latestSnapshotId(base);
        String indexTable =
                GraphIndexOptions.BACKEND_TABLE.equals(plan.backend())
                        ? baseTable.getTableName() + GraphVertexIndexPlanner.NODE_MAP_TABLE_SUFFIX
                        : "";

        if (GraphIndexOptions.BACKEND_TABLE.equals(plan.backend())) {
            if (!dryRun) {
                ensureNodeMapTable(catalog, baseTable, baseSchema, indexTable);
            }
            upsertVertexState(
                    catalog, baseTable, baseSchema, indexTable, status, buildSnapshot, dryRun);
            return Collections.singletonList(
                    new GraphIndexOperationResult(
                            INDEX_TYPE_NODE_MAP,
                            indexTable,
                            plan.backend(),
                            status,
                            dryRun,
                            dryRun
                                    ? "would create the standalone node-map index table"
                                    : "created the standalone node-map index table"));
        }

        List<SchemaChange> changes = new ArrayList<>();
        for (Map.Entry<String, String> option : plan.indexOptions().entrySet()) {
            changes.add(SchemaChange.setOption(option.getKey(), option.getValue()));
        }
        changes.add(
                SchemaChange.setOption(
                        GraphIndexOptions.STATUS_KEY_PREFIX + INDEX_TYPE_NODE_MAP, status));
        String buildSnapshotKey = GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + INDEX_TYPE_NODE_MAP;
        if (baseSchema.options().containsKey(buildSnapshotKey)) {
            changes.add(SchemaChange.removeOption(buildSnapshotKey));
        }
        String watermarkKey = GraphIndexOptions.WATERMARK_KEY_PREFIX + INDEX_TYPE_NODE_MAP;
        if (baseSchema.options().containsKey(watermarkKey)) {
            changes.add(SchemaChange.removeOption(watermarkKey));
        }
        if (!dryRun && !changes.isEmpty()) {
            alterTable(catalog, baseTable, changes);
        }
        if (!dryRun) {
            GraphIndexStatusTable.upsert(
                    catalog,
                    baseTable.getDatabaseName(),
                    new GraphIndexStatusTable.StatusRow(
                            baseTable.getDatabaseName() + "." + baseTable.getTableName(),
                            INDEX_TYPE_NODE_MAP,
                            "",
                            plan.backend(),
                            baseSchema
                                                    .options()
                                                    .get(
                                                            GraphIndexOptions
                                                                    .GRAPH_VERTEX_NODE_MAP_FIELDS
                                                                    .key())
                                            == null
                                    ? ""
                                    : baseSchema
                                            .options()
                                            .get(
                                                    GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS
                                                            .key()),
                            status,
                            null,
                            null,
                            System.currentTimeMillis()));
        }
        return Collections.singletonList(
                new GraphIndexOperationResult(
                        INDEX_TYPE_NODE_MAP,
                        "",
                        plan.backend(),
                        status,
                        dryRun,
                        dryRun
                                ? "would expand the node-map into primary-key index declarations: "
                                        + plan.indexOptions()
                                : "expanded the node-map into primary-key index declarations"));
    }

    private static List<GraphIndexOperationResult> createAdjacencyIndex(
            Catalog catalog, Identifier baseTable, GraphIndexSpec spec, boolean dryRun)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableAlreadyExistException {
        ensureEdgeDeclaration(catalog, baseTable, spec, dryRun);
        if (dryRun) {
            FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
            GraphEdgeIndexPlan plan =
                    GraphEdgeIndexPlanner.plan(baseTable.getTableName(), base.schema());
            List<GraphIndexOperationResult> results = new ArrayList<>();
            results.add(
                    new GraphIndexOperationResult(
                            INDEX_TYPE_ADJACENCY,
                            plan.edgeTableName(),
                            BACKEND_MERGE_ENGINE,
                            GraphIndexStatusTable.STATUS_BUILDING,
                            true,
                            "would create the forward adjacency index table"));
            if (plan.reverse()) {
                results.add(
                        new GraphIndexOperationResult(
                                INDEX_TYPE_ADJACENCY,
                                plan.edgeRevTableName(),
                                BACKEND_MERGE_ENGINE,
                                GraphIndexStatusTable.STATUS_BUILDING,
                                true,
                                "would create the reverse adjacency index table"));
            }
            return results;
        }
        List<Identifier> created = createEdgeIndex(catalog, baseTable);
        List<GraphIndexOperationResult> results = new ArrayList<>();
        for (Identifier identifier : created) {
            results.add(
                    new GraphIndexOperationResult(
                            INDEX_TYPE_ADJACENCY,
                            identifier.getTableName(),
                            BACKEND_MERGE_ENGINE,
                            GraphIndexStatusTable.STATUS_BUILDING,
                            false,
                            "created the adjacency index table"));
        }
        return results;
    }

    /**
     * Drops a graph index binding (design §5.6). {@link DropMode#LOGICAL} keeps every physical
     * artifact and only stops maintenance and consumption routing; {@link DropMode#PURGE} removes
     * the expanded declarations / index tables. Both support a dry run.
     */
    public static List<GraphIndexOperationResult> dropIndex(
            Catalog catalog,
            Identifier baseTable,
            GraphIndexSpec spec,
            DropMode mode,
            boolean dryRun)
            throws Catalog.TableNotExistException {
        switch (spec.type()) {
            case NODE_MAP:
                return dropNodeMapIndex(catalog, baseTable, mode, dryRun);
            case ADJACENCY:
                return dropAdjacencyIndex(catalog, baseTable, mode, dryRun);
            default:
                throw new IllegalArgumentException("Unknown graph index type " + spec.type());
        }
    }

    private static List<GraphIndexOperationResult> dropNodeMapIndex(
            Catalog catalog, Identifier baseTable, DropMode mode, boolean dryRun)
            throws Catalog.TableNotExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema baseSchema = base.schema();
        GraphVertexIndexDecl vertex =
                GraphIndexDecls.from(baseSchema)
                        .vertex()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Table '%s' is not declared as a graph"
                                                                + " vertex.",
                                                        baseTable.getTableName())));
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(baseTable.getTableName(), baseSchema);
        boolean purge = mode == DropMode.PURGE;
        String status =
                purge
                        ? GraphIndexStatusTable.STATUS_DELETED
                        : GraphIndexStatusTable.STATUS_LOGICAL_DELETED;
        String indexTable =
                GraphIndexOptions.BACKEND_TABLE.equals(plan.backend())
                        ? baseTable.getTableName() + GraphVertexIndexPlanner.NODE_MAP_TABLE_SUFFIX
                        : "";

        if (!dryRun) {
            if (purge) {
                if (GraphIndexOptions.BACKEND_TABLE.equals(plan.backend())) {
                    dropTableIfExists(catalog, baseTable.getDatabaseName(), indexTable);
                } else {
                    List<SchemaChange> changes = removeNodeMapColumns(vertex, baseSchema.options());
                    if (!changes.isEmpty()) {
                        alterTable(catalog, baseTable, changes);
                    }
                }
            }
            upsertVertexState(catalog, baseTable, base.schema(), indexTable, status, null, false);
            if (purge) {
                alterTable(
                        catalog,
                        baseTable,
                        Collections.singletonList(
                                SchemaChange.setOption(
                                        GraphIndexOptions.STATUS_KEY_PREFIX + INDEX_TYPE_NODE_MAP,
                                        GraphIndexStatusTable.STATUS_DELETED)));
                removeMirrorOptions(catalog, baseTable, base.schema(), INDEX_TYPE_NODE_MAP);
            } else {
                alterTable(
                        catalog,
                        baseTable,
                        Collections.singletonList(
                                SchemaChange.setOption(
                                        GraphIndexOptions.STATUS_KEY_PREFIX + INDEX_TYPE_NODE_MAP,
                                        GraphIndexStatusTable.STATUS_LOGICAL_DELETED)));
            }
        }
        return Collections.singletonList(
                new GraphIndexOperationResult(
                        INDEX_TYPE_NODE_MAP,
                        indexTable,
                        plan.backend(),
                        status,
                        dryRun,
                        dryRun
                                ? (purge
                                        ? "would purge the node-map index"
                                        : "would logically drop" + " the node-map index")
                                : (purge
                                        ? "purged the node-map index"
                                        : "logically dropped the node-map index")));
    }

    private static List<GraphIndexOperationResult> dropAdjacencyIndex(
            Catalog catalog, Identifier baseTable, DropMode mode, boolean dryRun)
            throws Catalog.TableNotExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema baseSchema = base.schema();
        GraphEdgeIndexDecl decl =
                GraphIndexDecls.from(baseSchema)
                        .edge()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Table '%s' is not declared as a graph"
                                                                + " edge.",
                                                        baseTable.getTableName())));
        GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan(baseTable.getTableName(), baseSchema);
        boolean purge = mode == DropMode.PURGE;
        String status =
                purge
                        ? GraphIndexStatusTable.STATUS_DELETED
                        : GraphIndexStatusTable.STATUS_LOGICAL_DELETED;

        List<String> indexTables = new ArrayList<>();
        indexTables.add(plan.edgeTableName());
        if (plan.reverse()) {
            indexTables.add(plan.edgeRevTableName());
        }
        List<GraphIndexOperationResult> results = new ArrayList<>();
        for (String indexTable : indexTables) {
            if (!dryRun) {
                if (purge) {
                    dropTableIfExists(catalog, baseTable.getDatabaseName(), indexTable);
                }
                GraphIndexStatusTable.upsert(
                        catalog,
                        baseTable.getDatabaseName(),
                        new GraphIndexStatusTable.StatusRow(
                                baseTable.getDatabaseName() + "." + baseTable.getTableName(),
                                INDEX_TYPE_ADJACENCY,
                                indexTable,
                                BACKEND_MERGE_ENGINE,
                                baseSchema
                                                        .options()
                                                        .get(
                                                                GraphIndexOptions
                                                                        .GRAPH_EDGE_AGGREGATES
                                                                        .key())
                                                == null
                                        ? ""
                                        : baseSchema
                                                .options()
                                                .get(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key()),
                                status,
                                null,
                                null,
                                System.currentTimeMillis()));
                if (purge) {
                    alterTable(
                            catalog,
                            baseTable,
                            Collections.singletonList(
                                    SchemaChange.setOption(
                                            GraphIndexOptions.STATUS_KEY_PREFIX + indexTable,
                                            GraphIndexStatusTable.STATUS_DELETED)));
                    removeMirrorOptions(catalog, baseTable, baseSchema, indexTable);
                } else {
                    alterTable(
                            catalog,
                            baseTable,
                            Collections.singletonList(
                                    SchemaChange.setOption(
                                            GraphIndexOptions.STATUS_KEY_PREFIX + indexTable,
                                            GraphIndexStatusTable.STATUS_LOGICAL_DELETED)));
                }
            }
            results.add(
                    new GraphIndexOperationResult(
                            INDEX_TYPE_ADJACENCY,
                            indexTable,
                            BACKEND_MERGE_ENGINE,
                            status,
                            dryRun,
                            dryRun
                                    ? (purge
                                            ? "would drop the adjacency index table"
                                            : "would logically drop the adjacency index")
                                    : (purge
                                            ? "dropped the adjacency index table"
                                            : "logically dropped the adjacency index")));
        }
        return results;
    }

    // --------------------------------------------------------------------------------------------
    // Declaration and table helpers
    // --------------------------------------------------------------------------------------------

    private static void ensureVertexDeclaration(
            Catalog catalog, Identifier baseTable, @Nullable String fields, boolean dryRun)
            throws Catalog.TableNotExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema schema = base.schema();
        Map<String, String> options = schema.options();
        List<SchemaChange> changes = new ArrayList<>();
        if (!Boolean.parseBoolean(options.get(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key()))) {
            changes.add(
                    SchemaChange.setOption(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true"));
        }
        if (options.get(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key()) == null) {
            if (schema.primaryKeys().size() != 1) {
                throw new IllegalArgumentException(
                        "graph.vertex.enabled requires a single-column primary key, but got "
                                + schema.primaryKeys()
                                + ".");
            }
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(),
                            schema.primaryKeys().get(0)));
        }
        if (fields != null
                && !fields.equals(
                        options.get(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key()))) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), fields));
        }
        if (!changes.isEmpty() && !dryRun) {
            alterTable(catalog, baseTable, changes);
        }
    }

    private static void ensureEdgeDeclaration(
            Catalog catalog, Identifier baseTable, GraphIndexSpec spec, boolean dryRun)
            throws Catalog.TableNotExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema schema = base.schema();
        Map<String, String> options = schema.options();
        List<SchemaChange> changes = new ArrayList<>();
        if (!Boolean.parseBoolean(options.get(GraphIndexOptions.GRAPH_EDGE_ENABLED.key()))) {
            changes.add(SchemaChange.setOption(GraphIndexOptions.GRAPH_EDGE_ENABLED.key(), "true"));
        }
        if (spec.srcField() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_SRC_FIELD.key(), spec.srcField()));
        }
        if (spec.dstField() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_DST_FIELD.key(), spec.dstField()));
        }
        if (spec.srcVertex() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_SRC_VERTEX.key(), spec.srcVertex()));
        }
        if (spec.dstVertex() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_DST_VERTEX.key(), spec.dstVertex()));
        }
        if (spec.aggregates() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key(), spec.aggregates()));
        }
        if (spec.reverse() != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.GRAPH_EDGE_REVERSE.key(),
                            String.valueOf(spec.reverse())));
        }
        if (!changes.isEmpty() && !dryRun) {
            alterTable(catalog, baseTable, changes);
        }
    }

    private static void ensureNodeMapTable(
            Catalog catalog, Identifier baseTable, TableSchema baseSchema, String indexTableName)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException {
        String idField = baseSchema.options().get(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key());
        DataType idType = null;
        for (DataField field : baseSchema.fields()) {
            if (field.name().equals(idField)) {
                idType = field.type();
            }
        }
        if (idType == null) {
            throw new IllegalArgumentException(
                    "The graph vertex id field '" + idField + "' does not exist.");
        }
        Map<String, String> options = new LinkedHashMap<>();
        options.put("bucket", "512");
        options.put("merge-engine", DEDUPLICATE_MERGE_ENGINE);
        options.put("changelog-producer", "none");
        options.put("file-index.bloom-filter.columns", "prop_value");
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "prop_name", DataTypes.STRING()),
                                new DataField(1, "prop_value", DataTypes.BYTES()),
                                new DataField(2, "node_id", idType)),
                        Collections.emptyList(),
                        Arrays.asList("prop_name", "prop_value", "node_id"),
                        options,
                        "Graph node-map index of table '"
                                + baseTable.getTableName()
                                + "' (design §4.3).");
        catalog.createTable(
                Identifier.create(baseTable.getDatabaseName(), indexTableName), schema, true);
    }

    private static void upsertVertexState(
            Catalog catalog,
            Identifier baseTable,
            TableSchema baseSchema,
            String indexTableName,
            String status,
            @Nullable Long buildSnapshot,
            boolean dryRun) {
        if (dryRun) {
            return;
        }
        String mirrorName = INDEX_TYPE_NODE_MAP;
        List<SchemaChange> changes = new ArrayList<>();
        changes.add(
                SchemaChange.setOption(GraphIndexOptions.STATUS_KEY_PREFIX + mirrorName, status));
        if (buildSnapshot != null) {
            changes.add(
                    SchemaChange.setOption(
                            GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + mirrorName,
                            String.valueOf(buildSnapshot)));
        }
        alterTable(catalog, baseTable, changes);
        GraphIndexStatusTable.upsert(
                catalog,
                baseTable.getDatabaseName(),
                new GraphIndexStatusTable.StatusRow(
                        baseTable.getDatabaseName() + "." + baseTable.getTableName(),
                        INDEX_TYPE_NODE_MAP,
                        indexTableName == null ? "" : indexTableName,
                        "",
                        baseSchema
                                                .options()
                                                .get(
                                                        GraphIndexOptions
                                                                .GRAPH_VERTEX_NODE_MAP_FIELDS
                                                                .key())
                                        == null
                                ? ""
                                : baseSchema
                                        .options()
                                        .get(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key()),
                        status,
                        buildSnapshot,
                        null,
                        System.currentTimeMillis()));
    }

    private static void removeMirrorOptions(
            Catalog catalog, Identifier baseTable, TableSchema schema, String indexTableName) {
        List<SchemaChange> changes = new ArrayList<>();
        for (String prefix :
                Arrays.asList(
                        GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX,
                        GraphIndexOptions.WATERMARK_KEY_PREFIX)) {
            String key = prefix + indexTableName;
            if (schema.options().containsKey(key)) {
                changes.add(SchemaChange.removeOption(key));
            }
        }
        if (!changes.isEmpty()) {
            alterTable(catalog, baseTable, changes);
        }
    }

    /** Removes only the node-map columns owned by the graph declaration. */
    private static List<SchemaChange> removeNodeMapColumns(
            GraphVertexIndexDecl vertex, Map<String, String> options) {
        String btreeKey = "global-index.btree.index-columns";
        String bitmapKey = "global-index.bitmap.index-columns";
        List<String> btree = splitColumns(options.get(btreeKey));
        List<String> bitmap = splitColumns(options.get(bitmapKey));
        for (GraphVertexIndexDecl.NodeMapField field : vertex.nodeMapFields()) {
            if ("bitmap".equals(field.family())) {
                bitmap.remove(field.field());
            } else {
                btree.remove(field.field());
            }
        }
        List<SchemaChange> changes = new ArrayList<>();
        addColumnOptionChange(changes, btreeKey, btree);
        addColumnOptionChange(changes, bitmapKey, bitmap);
        return changes;
    }

    private static void addColumnOptionChange(
            List<SchemaChange> changes, String key, List<String> columns) {
        if (columns.isEmpty()) {
            changes.add(SchemaChange.removeOption(key));
        } else {
            changes.add(SchemaChange.setOption(key, String.join(",", columns)));
        }
    }

    private static List<String> splitColumns(@Nullable String raw) {
        List<String> columns = new ArrayList<>();
        if (raw == null || raw.trim().isEmpty()) {
            return columns;
        }
        for (String token : raw.split(",")) {
            if (!token.trim().isEmpty()) {
                columns.add(token.trim());
            }
        }
        return columns;
    }

    private static void dropTableIfExists(Catalog catalog, String database, String tableName) {
        try {
            catalog.dropTable(Identifier.create(database, tableName), true);
        } catch (Catalog.TableNotExistException e) {
            // ignore: idempotent purge
        }
    }

    private static void alterTable(
            Catalog catalog, Identifier baseTable, List<SchemaChange> changes) {
        try {
            catalog.alterTable(baseTable, changes, false);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to update the graph index declaration of '"
                            + baseTable.getDatabaseName()
                            + "."
                            + baseTable.getTableName()
                            + "'.",
                    e);
        }
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
