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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Streaming maintenance micro-batch of the adjacency index tables (design §7.2/§7.3): consumes the
 * base table snapshots since the coverage watermark, folds each declared aggregate over the new
 * events with the official {@link org.apache.paimon.mergetree.compact.aggregate.FieldAggregator} of
 * that function, writes the per-pair partial results into {@code __edge} / {@code __edge_rev} (the
 * aggregation merge engine combines them with the existing totals on read), and advances the
 * coverage watermark to the consumed snapshot in both the {@code graph_index_status} table and the
 * base-table status mirror.
 *
 * <p>One {@link #maintain} call is one micro-batch: it is the engine-agnostic checkpoint unit that
 * a Flink job submits per checkpoint (design §7.2). The micro-batch is at-least-once: if a failure
 * happens between the index commit and the watermark update, the interval is consumed again and
 * sum-type aggregates would double count until the detail reconciler (design §5.6) detects the
 * drift. Engine jobs built on the same contract (Flink 2PC sink) upgrade this to exactly-once.
 */
public final class GraphEdgeIndexMaintainer {

    private GraphEdgeIndexMaintainer() {}

    /**
     * The snapshot up to which the adjacency index is covered: the coverage watermark if the
     * maintenance pipeline has already run, otherwise the backfill build snapshot (design §6.2).
     */
    @Nullable
    public static Long effectiveWatermark(TableSchema baseSchema, String indexTableName) {
        Map<String, String> options = baseSchema.options();
        Long watermark =
                parseLong(options.get(GraphIndexOptions.WATERMARK_KEY_PREFIX + indexTableName));
        if (watermark != null) {
            return watermark;
        }
        return parseLong(options.get(GraphIndexOptions.BUILD_SNAPSHOT_KEY_PREFIX + indexTableName));
    }

    /** Runs one maintenance micro-batch over the edge declaration of the base table. */
    public static MaintainResult maintain(Catalog catalog, Identifier baseTable)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException,
                    Catalog.TableAlreadyExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema baseSchema = base.schema();
        String baseName = baseTable.getTableName();
        GraphEdgeIndexDecl decl =
                GraphIndexDecls.from(baseSchema)
                        .edge()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Table '%s' is not declared as a graph"
                                                                + " edge.",
                                                        baseName)));

        String forwardTableName = GraphEdgeIndexPlanner.edgeTableName(baseName);
        checkArgument(
                !GraphIndexStatusTable.STATUS_LOGICAL_DELETED.equals(
                        baseSchema
                                .options()
                                .get(GraphIndexOptions.STATUS_KEY_PREFIX + forwardTableName)),
                "Graph adjacency index of '%s' is logically dropped; re-run"
                        + " create_graph_index to re-enable it.",
                baseName);
        Long from = effectiveWatermark(baseSchema, forwardTableName);
        checkArgument(
                from != null,
                "No build snapshot / coverage watermark recorded for '%s'; run the graph index"
                        + " create pipeline first.",
                baseName);

        Snapshot latest = base.store().snapshotManager().latestSnapshot();
        long to = latest == null ? 0L : latest.id();
        if (to <= from) {
            return MaintainResult.noop(from);
        }

        GraphEdgeIndexAggregator.AggregatedEdges aggregated =
                GraphEdgeIndexAggregator.incremental(base, baseSchema, decl, from, to);
        Map<String, GraphIndexStatusTable.StatusRow> previous = statusRows(catalog, baseTable);

        writePartialAggregates(
                loadIndexTable(catalog, baseTable, forwardTableName),
                baseName,
                decl,
                aggregated,
                false);

        String reverseTableName = GraphEdgeIndexPlanner.edgeRevTableName(baseName);
        if (decl.reverse()) {
            writePartialAggregates(
                    loadIndexTable(catalog, baseTable, reverseTableName),
                    baseName,
                    decl,
                    aggregated,
                    true);
        }

        advanceWatermark(
                catalog,
                baseTable,
                baseName,
                forwardTableName,
                previous.get(forwardTableName),
                from,
                to);
        if (decl.reverse()) {
            advanceWatermark(
                    catalog,
                    baseTable,
                    baseName,
                    reverseTableName,
                    previous.get(reverseTableName),
                    from,
                    to);
        }
        return new MaintainResult(false, from, to, aggregated.eventCount());
    }

    /**
     * Advances the coverage watermark of every adjacency index of the base table to {@code to},
     * after an engine job committed the partial aggregates of the interval {@code (from, to]}. The
     * build snapshot stays pinned to the create pipeline (design §6.2).
     *
     * <p>Callers must only invoke this after the index writes of the interval are durable. Doing so
     * before the writes would silently lose events; doing it twice for the same interval is
     * harmless for the watermark itself but double counts sum-type aggregates in the index.
     */
    public static void advanceWatermarks(
            Catalog catalog, Identifier baseTable, long fromExclusive, long toInclusive)
            throws Catalog.TableNotExistException {
        FileStoreTable base = (FileStoreTable) catalog.getTable(baseTable);
        TableSchema baseSchema = base.schema();
        String baseName = baseTable.getTableName();
        GraphEdgeIndexDecl decl =
                GraphIndexDecls.from(baseSchema)
                        .edge()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "Table '%s' is not declared as a graph"
                                                                + " edge.",
                                                        baseName)));
        Map<String, GraphIndexStatusTable.StatusRow> previous = statusRows(catalog, baseTable);
        String forwardTableName = GraphEdgeIndexPlanner.edgeTableName(baseName);
        advanceWatermark(
                catalog,
                baseTable,
                baseName,
                forwardTableName,
                previous.get(forwardTableName),
                fromExclusive,
                toInclusive);
        if (decl.reverse()) {
            String reverseTableName = GraphEdgeIndexPlanner.edgeRevTableName(baseName);
            advanceWatermark(
                    catalog,
                    baseTable,
                    baseName,
                    reverseTableName,
                    previous.get(reverseTableName),
                    fromExclusive,
                    toInclusive);
        }
    }

    @Nullable
    private static Long parseLong(@Nullable String value) {
        return value == null ? null : Long.valueOf(value);
    }

    private static FileStoreTable loadIndexTable(
            Catalog catalog, Identifier baseTable, String indexTableName)
            throws Catalog.TableNotExistException {
        return (FileStoreTable)
                catalog.getTable(Identifier.create(baseTable.getDatabaseName(), indexTableName));
    }

    /** Existing status rows of the base table, keyed by index table name. */
    private static Map<String, GraphIndexStatusTable.StatusRow> statusRows(
            Catalog catalog, Identifier baseTable) {
        Map<String, GraphIndexStatusTable.StatusRow> rows = new HashMap<>();
        String base = baseTable.getDatabaseName() + "." + baseTable.getTableName();
        for (GraphIndexStatusTable.StatusRow row :
                GraphIndexStatusTable.read(catalog, baseTable.getDatabaseName())) {
            if (row.baseTable().equals(base)) {
                rows.put(row.indexTable(), row);
            }
        }
        return rows;
    }

    // --------------------------------------------------------------------------------------------
    // Partial aggregate write
    // --------------------------------------------------------------------------------------------

    private static void writePartialAggregates(
            FileStoreTable indexTable,
            String baseName,
            GraphEdgeIndexDecl decl,
            GraphEdgeIndexAggregator.AggregatedEdges aggregated,
            boolean reverse) {
        List<InternalRow> rows = aggregated.rows(decl, reverse);
        if (rows.isEmpty()) {
            return;
        }
        try {
            BatchWriteBuilder builder = indexTable.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite()) {
                for (InternalRow row : rows) {
                    write.write(row);
                }
                try (BatchTableCommit commit = builder.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to write maintenance partial aggregates into the adjacency"
                                    + " index of '%s'.",
                            baseName),
                    e);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Watermark advance
    // --------------------------------------------------------------------------------------------

    private static void advanceWatermark(
            Catalog catalog,
            Identifier baseTable,
            String baseName,
            String indexTableName,
            @Nullable GraphIndexStatusTable.StatusRow previous,
            long from,
            long to) {
        Long buildSnapshotId =
                previous == null || previous.buildSnapshotId() == null
                        ? from
                        : previous.buildSnapshotId();
        String fields = previous == null ? "" : previous.fields();
        GraphIndexStatusTable.upsert(
                catalog,
                baseTable.getDatabaseName(),
                new GraphIndexStatusTable.StatusRow(
                        baseTable.getDatabaseName() + "." + baseName,
                        GraphIndexManager.INDEX_TYPE_ADJACENCY,
                        indexTableName,
                        GraphIndexManager.BACKEND_MERGE_ENGINE,
                        fields,
                        GraphIndexStatusTable.STATUS_ACTIVE,
                        buildSnapshotId,
                        to,
                        System.currentTimeMillis()));
        try {
            catalog.alterTable(
                    baseTable,
                    Collections.singletonList(
                            SchemaChange.setOption(
                                    GraphIndexOptions.STATUS_KEY_PREFIX + indexTableName,
                                    GraphIndexStatusTable.STATUS_ACTIVE)),
                    false);
            catalog.alterTable(
                    baseTable,
                    Collections.singletonList(
                            SchemaChange.setOption(
                                    GraphIndexOptions.WATERMARK_KEY_PREFIX + indexTableName,
                                    String.valueOf(to))),
                    false);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to advance the coverage watermark of '%s' to snapshot %d;"
                                    + " re-consuming the interval would double count sum-type"
                                    + " aggregates, reconcile before continuing (design §5.6).",
                            indexTableName, to),
                    e);
        }
    }

    /** Result of one maintenance micro-batch. */
    public static final class MaintainResult {

        private final boolean noop;
        private final long fromSnapshot;
        private final long toSnapshot;
        private final long eventsConsumed;

        private MaintainResult(
                boolean noop, long fromSnapshot, long toSnapshot, long eventsConsumed) {
            this.noop = noop;
            this.fromSnapshot = fromSnapshot;
            this.toSnapshot = toSnapshot;
            this.eventsConsumed = eventsConsumed;
        }

        private static MaintainResult noop(long watermark) {
            return new MaintainResult(true, watermark, watermark, 0L);
        }

        public boolean noop() {
            return noop;
        }

        /** The exclusive lower bound of the consumed snapshot interval. */
        public long fromSnapshot() {
            return fromSnapshot;
        }

        /** The inclusive upper bound (the new coverage watermark). */
        public long toSnapshot() {
            return toSnapshot;
        }

        public long eventsConsumed() {
            return eventsConsumed;
        }
    }
}
