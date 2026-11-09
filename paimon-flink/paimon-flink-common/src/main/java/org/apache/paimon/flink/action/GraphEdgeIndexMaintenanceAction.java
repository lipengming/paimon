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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.graph.GraphEdgeIndexBackfill;
import org.apache.paimon.graph.GraphEdgeIndexMaintainer;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Flink maintenance job of the graph adjacency indexes (design §7.2/§7.3): one run submits one
 * micro-batch that consumes the base table snapshots after the coverage watermark, pre-aggregates
 * the new events per {@code (src, dst)} pair with the aggregate functions of the declaration, sinks
 * the partial aggregates into {@code __edge} / {@code __edge_rev} through the aggregation merge
 * engine, and finally advances the coverage watermark (and its {@code $graph_indexes} mirror) to
 * the consumed snapshot.
 *
 * <p>The job is engine-distributed: the pre-aggregation is a Flink SQL {@code GROUP BY} over the
 * bounded incremental source, and both adjacency tables are written in one statement set (one
 * checkpointed Flink job). The watermark is advanced on the client only after the job completed,
 * which preserves the at-least-once contract of {@link GraphEdgeIndexMaintainer}: a failure before
 * the advance re-consumes the interval, a failure after the index commit but before the advance is
 * detected by the detail reconciler (design §5.6).
 *
 * <p>Scheduling the micro-batch is the responsibility of the deployment (cron / action scheduler),
 * one run per checkpoint interval. This mirrors how Paimon actions are executed and keeps the
 * exactly-once upgrade path open: a streaming variant only needs to replace the bounded source with
 * the unbounded incremental one and move the watermark advance into the sink committer.
 */
public class GraphEdgeIndexMaintenanceAction extends TableActionBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(GraphEdgeIndexMaintenanceAction.class);

    @Nullable private Long toSnapshot;

    public GraphEdgeIndexMaintenanceAction(
            String database, String tableName, Map<String, String> catalogConfig) {
        super(database, tableName, catalogConfig);
        checkArgument(
                table instanceof FileStoreTable,
                "Graph index maintenance requires a FileStoreTable, but table '%s' is %s.",
                identifier.getFullName(),
                table.getClass().getName());
    }

    /** Consumes up to the given snapshot instead of the latest one (manual catch-up / tests). */
    public GraphEdgeIndexMaintenanceAction withToSnapshot(@Nullable Long toSnapshot) {
        this.toSnapshot = toSnapshot;
        return this;
    }

    @Override
    public void run() throws Exception {
        FileStoreTable baseTable = (FileStoreTable) table;
        TableSchema baseSchema = baseTable.schema();
        String baseName = identifier.getTableName();
        String forwardTableName = GraphEdgeIndexPlanner.edgeTableName(baseName);
        Long from = GraphEdgeIndexMaintainer.effectiveWatermark(baseSchema, forwardTableName);
        checkArgument(
                from != null,
                "No build snapshot / coverage watermark recorded for '%s'; run the graph index"
                        + " create pipeline first.",
                identifier.getFullName());

        long to = toSnapshot != null ? toSnapshot : latestSnapshotId(baseTable);
        if (to <= from) {
            LOG.info(
                    "Graph index of '{}' is already covered: watermark={}, target snapshot={}.",
                    identifier.getFullName(),
                    from,
                    to);
            return;
        }

        List<String> sqls =
                GraphEdgeIndexBackfill.generateIncrementalSql(
                        identifier.getDatabaseName(), baseName, baseSchema, from, to);
        checkArgument(
                !sqls.isEmpty(),
                "No incremental maintenance statement was generated for '%s'.",
                identifier.getFullName());

        // One job per adjacency table: on this codebase Flink's source reuse across the two
        // INSERTs of a statement set loses the dynamic time-travel options of the scan, which
        // would silently read the full base table instead of the incremental interval.
        for (String sql : sqls) {
            batchTEnv.executeSql(sql).await();
        }

        GraphEdgeIndexMaintainer.advanceWatermarks(catalog, identifier, from, to);
        LOG.info(
                "Graph index of '{}' advanced from snapshot {} to {}.",
                identifier.getFullName(),
                from,
                to);
    }

    private static long latestSnapshotId(FileStoreTable table) {
        Snapshot latest = table.store().snapshotManager().latestSnapshot();
        return latest == null ? 0L : latest.id();
    }
}
