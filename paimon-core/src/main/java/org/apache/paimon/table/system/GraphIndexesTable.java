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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.graph.GraphEdgeIndexDecl;
import org.apache.paimon.graph.GraphEdgeIndexPlan;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.graph.GraphIndexDecls;
import org.apache.paimon.graph.GraphIndexManager;
import org.apache.paimon.graph.GraphIndexStatusTable;
import org.apache.paimon.graph.GraphVertexIndexPlan;
import org.apache.paimon.graph.GraphVertexIndexPlanner;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadOnceTableScan;
import org.apache.paimon.table.source.SingletonSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IteratorRecordReader;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * A {@link Table} showing the graph index bindings of one table (design §5.5). Read through the
 * catalog as the per-table system table named after the base table plus the {@code $graph_indexes}
 * suffix.
 *
 * <p>Each row is one index entry: the vertex node-map binding (one row, backend resolved by the
 * vertex planner) or the edge adjacency binding (one row per planned adjacency table). Declaration
 * columns come from the {@code graph.vertex.*} / {@code graph.edge.*} options; state columns come
 * from the base table's status mirror options that the create pipeline and maintenance jobs keep in
 * sync with the per-database {@code graph_index_status} table.
 */
public class GraphIndexesTable implements ReadonlyTable {

    private static final long serialVersionUID = 1L;

    public static final String GRAPH_INDEXES = "graph_indexes";

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "base_table", SerializationUtils.newStringType(false)),
                            new DataField(1, "index_type", SerializationUtils.newStringType(false)),
                            new DataField(2, "backend", SerializationUtils.newStringType(false)),
                            new DataField(3, "index_table", SerializationUtils.newStringType(true)),
                            new DataField(4, "fields", SerializationUtils.newStringType(true)),
                            new DataField(5, "direction", SerializationUtils.newStringType(true)),
                            new DataField(6, "src_vertex", SerializationUtils.newStringType(true)),
                            new DataField(7, "dst_vertex", SerializationUtils.newStringType(true)),
                            new DataField(8, "aggregates", SerializationUtils.newStringType(true)),
                            new DataField(9, "status", SerializationUtils.newStringType(false)),
                            new DataField(10, "coverage_watermark", DataTypes.BIGINT()),
                            new DataField(11, "lag_seconds", DataTypes.BIGINT()),
                            new DataField(
                                    12, "build_options", SerializationUtils.newStringType(true))));

    private final FileIO fileIO;
    private final Path location;
    private final FileStoreTable dataTable;

    public GraphIndexesTable(FileStoreTable dataTable) {
        this.fileIO = dataTable.fileIO();
        this.location = dataTable.location();
        this.dataTable = dataTable;
    }

    @Override
    public String name() {
        return location.getName() + SYSTEM_TABLE_SPLITTER + GRAPH_INDEXES;
    }

    @Override
    public RowType rowType() {
        return TABLE_TYPE;
    }

    @Override
    public List<String> primaryKeys() {
        return Collections.emptyList();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public InnerTableScan newScan() {
        return new GraphIndexesScan();
    }

    @Override
    public InnerTableRead newRead() {
        return new GraphIndexesRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new GraphIndexesTable(dataTable.copy(dynamicOptions));
    }

    private class GraphIndexesScan extends ReadOnceTableScan {

        @Override
        public InnerTableScan withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public Plan innerPlan() {
            return () -> Collections.singletonList(new GraphIndexesSplit(location));
        }
    }

    /** {@link Split} implementation for {@link GraphIndexesTable}. */
    private static class GraphIndexesSplit extends SingletonSplit {

        private static final long serialVersionUID = 1L;

        private final Path location;

        private GraphIndexesSplit(Path location) {
            this.location = location;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GraphIndexesSplit that = (GraphIndexesSplit) o;
            return Objects.equals(location, that.location);
        }

        @Override
        public int hashCode() {
            return Objects.hash(location);
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }

    /** {@link TableRead} implementation for {@link GraphIndexesTable}. */
    private class GraphIndexesRead implements InnerTableRead {

        private RowType readType;

        @Override
        public InnerTableRead withFilter(Predicate predicate) {
            return this;
        }

        @Override
        public InnerTableRead withReadType(RowType readType) {
            this.readType = readType;
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            if (!(split instanceof GraphIndexesSplit)) {
                throw new IllegalArgumentException("Unsupported split: " + split.getClass());
            }
            Iterator<InternalRow> rows = buildRows().iterator();
            if (readType != null) {
                rows =
                        Iterators.transform(
                                rows,
                                row ->
                                        ProjectedRow.from(readType, GraphIndexesTable.TABLE_TYPE)
                                                .replaceRow(row));
            }
            return new IteratorRecordReader<>(rows);
        }
    }

    private List<InternalRow> buildRows() {
        List<InternalRow> rows = new ArrayList<>();
        TableSchema schema = dataTable.schema();
        String baseTable = location.getName();
        Map<String, String> options = schema.options();
        GraphIndexDecls decls = GraphIndexDecls.from(schema);

        if (decls.vertex().isPresent()) {
            GraphVertexIndexPlan plan = GraphVertexIndexPlanner.plan(baseTable, schema);
            String indexTable =
                    GraphIndexOptions.BACKEND_TABLE.equals(plan.backend())
                            ? baseTable + GraphVertexIndexPlanner.NODE_MAP_TABLE_SUFFIX
                            : null;
            StringBuilder buildOptions = new StringBuilder();
            plan.indexOptions()
                    .forEach((k, v) -> buildOptions.append(k).append("=").append(v).append(","));
            if (plan.degraded()) {
                buildOptions
                        .append("unmet-requirements=")
                        .append(String.join(";", plan.unmetPkIndexRequirements()));
            }
            rows.add(
                    GenericRow.of(
                            BinaryString.fromString(baseTable),
                            BinaryString.fromString("node_map"),
                            BinaryString.fromString(plan.backend()),
                            indexTable == null ? null : BinaryString.fromString(indexTable),
                            mirrorString(
                                    options, GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key()),
                            null,
                            null,
                            null,
                            null,
                            BinaryString.fromString(mirrorStatus(options, "node_map")),
                            mirrorLong(
                                    options, GraphIndexOptions.WATERMARK_KEY_PREFIX + "node_map"),
                            lagSeconds(
                                    mirrorLong(
                                            options,
                                            GraphIndexOptions.WATERMARK_KEY_PREFIX + "node_map")),
                            buildOptions.length() == 0
                                    ? null
                                    : BinaryString.fromString(trimTrailingComma(buildOptions))));
        }

        if (decls.edge().isPresent()) {
            GraphEdgeIndexDecl edge = decls.edge().get();
            GraphEdgeIndexPlan plan = GraphEdgeIndexPlanner.plan(baseTable, schema);
            String fields = edge.srcField() + "," + edge.dstField();
            String aggregates = options.get(GraphIndexOptions.GRAPH_EDGE_AGGREGATES.key());
            // direction describes the binding, not the physical table: reverse adjacency adds the
            // 'in' direction on top of the mandatory 'out' one (design §3.2)
            String direction = edge.reverse() ? "out,in" : "out";
            addEdgeRow(
                    rows,
                    baseTable,
                    options,
                    plan.edgeTableName(),
                    plan.edge(),
                    fields,
                    direction,
                    edge,
                    aggregates);
            if (plan.reverse()) {
                addEdgeRow(
                        rows,
                        baseTable,
                        options,
                        plan.edgeRevTableName(),
                        plan.edgeRev(),
                        fields,
                        direction,
                        edge,
                        aggregates);
            }
        }
        return rows;
    }

    private void addEdgeRow(
            List<InternalRow> rows,
            String baseTable,
            Map<String, String> baseOptions,
            String indexTableName,
            TableSchema indexSchema,
            String fields,
            String direction,
            GraphEdgeIndexDecl edge,
            String aggregates) {
        String bucket = indexSchema.options().get(CoreOptions.BUCKET.key());
        String bucketKey = indexSchema.options().get(CoreOptions.BUCKET_KEY.key());
        String buildOptions = "bucket=" + bucket + ",bucket-key=" + bucketKey;
        Long watermark =
                mirrorLong(baseOptions, GraphIndexOptions.WATERMARK_KEY_PREFIX + indexTableName);
        rows.add(
                GenericRow.of(
                        BinaryString.fromString(baseTable),
                        BinaryString.fromString("adjacency"),
                        BinaryString.fromString(GraphIndexManager.BACKEND_MERGE_ENGINE),
                        BinaryString.fromString(indexTableName),
                        BinaryString.fromString(fields),
                        BinaryString.fromString(direction),
                        BinaryString.fromString(edge.srcVertex()),
                        BinaryString.fromString(edge.dstVertex()),
                        aggregates == null ? null : BinaryString.fromString(aggregates),
                        BinaryString.fromString(mirrorStatus(baseOptions, indexTableName)),
                        watermark,
                        lagSeconds(watermark),
                        BinaryString.fromString(buildOptions)));
    }

    private static String mirrorStatus(Map<String, String> options, String entryName) {
        String status = options.get(GraphIndexOptions.STATUS_KEY_PREFIX + entryName);
        return status == null ? GraphIndexStatusTable.STATUS_BUILDING : status;
    }

    private static BinaryString mirrorString(Map<String, String> options, String key) {
        String value = options.get(key);
        return value == null ? null : BinaryString.fromString(value);
    }

    private static Long mirrorLong(Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String trimTrailingComma(StringBuilder builder) {
        String value = builder.toString();
        return value.endsWith(",") ? value.substring(0, value.length() - 1) : value;
    }

    /**
     * Seconds between the base table's latest snapshot and the watermarked snapshot (design §5.5);
     * null when there is no watermark or the watermarked snapshot has expired.
     */
    private Long lagSeconds(Long watermarkId) {
        if (watermarkId == null) {
            return null;
        }
        SnapshotManager snapshotManager = dataTable.store().snapshotManager();
        Snapshot latest = snapshotManager.latestSnapshot();
        if (latest == null) {
            return null;
        }
        Snapshot watermarked;
        try {
            watermarked = snapshotManager.snapshot(watermarkId);
        } catch (Exception e) {
            return null;
        }
        if (watermarked == null) {
            return null;
        }
        return Math.max(0L, (latest.timeMillis() - watermarked.timeMillis()) / 1000L);
    }
}
