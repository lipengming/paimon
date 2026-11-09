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

package org.apache.paimon.graph.sdk;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.graph.GraphEdgeAggregate;
import org.apache.paimon.graph.GraphEdgeIndexAggregator;
import org.apache.paimon.graph.GraphEdgeIndexDecl;
import org.apache.paimon.graph.GraphEdgeIndexMaintainer;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.graph.GraphEdgePair;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.graph.sdk.GraphReads.fieldIndex;

/**
 * Adjacency scan over one edge binding (design §8.1 ②③): a primary-key prefix read on the forward
 * ({@code __edge}, prefix = src) or reverse ({@code __edge_rev}, prefix = dst) index table. The
 * prefix predicate locates one bucket via the bucket key and lets Paimon prune the key range, so
 * one-degree adjacency is a bounded merge read.
 *
 * <p>{@link SearchMode#FAST} reads the index snapshot only. {@link SearchMode#FULL} additionally
 * catches up the interval between the coverage watermark and the latest base table snapshot at
 * query time and merges the missed partial aggregates into the index rows (design §6.3). {@link
 * SearchMode#DETAIL} is the reconciliation mode: it is served by {@link
 * GraphStore#reconcile(String)}, not by a single adjacency scan.
 */
public class AdjQuery implements AdjIterator {

    private final FileStoreTable indexTable;
    private final RowType indexRowType;
    private final String edgeName;
    private final String prefixField;
    private final String dstField;
    private final SearchMode searchMode;

    @Nullable private final FileStoreTable baseTable;
    @Nullable private final TableSchema baseSchema;
    @Nullable private final GraphEdgeIndexDecl decl;
    private final boolean reverse;

    @Nullable private Long from;
    private final List<String> projectedColumns = new ArrayList<>();
    private int degreeCap = Integer.MAX_VALUE;

    private List<InternalRow> rows;
    private Map<String, Integer> positions;
    private Map<String, DataType> columnTypes;
    private int pos = -1;
    private InternalRow current;

    AdjQuery(
            FileStoreTable indexTable,
            String edgeName,
            String prefixField,
            String dstField,
            SearchMode searchMode) {
        this(indexTable, edgeName, prefixField, dstField, searchMode, null, null, null, false);
    }

    AdjQuery(
            FileStoreTable indexTable,
            String edgeName,
            String prefixField,
            String dstField,
            SearchMode searchMode,
            @Nullable FileStoreTable baseTable,
            @Nullable TableSchema baseSchema,
            @Nullable GraphEdgeIndexDecl decl,
            boolean reverse) {
        this.indexTable = indexTable;
        this.indexRowType = new RowType(indexTable.schema().fields());
        this.edgeName = edgeName;
        this.prefixField = prefixField;
        this.dstField = dstField;
        this.searchMode = searchMode;
        this.baseTable = baseTable;
        this.baseSchema = baseSchema;
        this.decl = decl;
        this.reverse = reverse;
    }

    @Override
    public AdjIterator from(long vertexId) {
        this.from = vertexId;
        return this;
    }

    @Override
    public AdjIterator project(String... columns) {
        projectedColumns.addAll(Arrays.asList(columns));
        return this;
    }

    @Override
    public AdjIterator degreeCap(int cap) {
        if (cap <= 0) {
            throw new IllegalArgumentException("degreeCap must be positive, but got " + cap);
        }
        this.degreeCap = cap;
        return this;
    }

    @Override
    public boolean next() {
        executeIfNeeded();
        if (pos + 1 < rows.size()) {
            pos++;
            current = rows.get(pos);
            return true;
        }
        return false;
    }

    @Override
    public long dst() {
        requireCurrent();
        return current.getLong(0);
    }

    @Override
    public long getLong(String column) {
        requireCurrent();
        int position = position(column);
        DataType type = columnTypes.get(column);
        switch (type.getTypeRoot()) {
            case BIGINT:
                return current.getLong(position);
            case INTEGER:
                return current.getInt(position);
            default:
                throw new IllegalArgumentException(
                        "Column '" + column + "' of adjacency rows is not a BIGINT/INT column.");
        }
    }

    @Nullable
    @Override
    public String getString(String column) {
        requireCurrent();
        int position = position(column);
        return current.isNullAt(position) ? null : current.getString(position).toString();
    }

    @Nullable
    @Override
    public BigDecimal getDecimal(String column) {
        requireCurrent();
        int position = position(column);
        if (current.isNullAt(position)) {
            return null;
        }
        DecimalType type = (DecimalType) columnTypes.get(column);
        return current.getDecimal(position, type.getPrecision(), type.getScale()).toBigDecimal();
    }

    @Override
    public void close() {
        // rows are fully materialized bounded lists; nothing to release
    }

    private void executeIfNeeded() {
        if (rows != null) {
            return;
        }
        if (searchMode == SearchMode.DETAIL) {
            throw new UnsupportedOperationException(
                    "SearchMode DETAIL is the reconciliation mode; call"
                            + " GraphStore.reconcile(\""
                            + edgeName
                            + "\") instead of a single adjacency scan.");
        }
        if (from == null) {
            throw new IllegalArgumentException(
                    "from(...) must be set before iterating an adjacency scan of '"
                            + edgeName
                            + "'.");
        }
        List<Integer> projection = projection();
        if (searchMode == SearchMode.FULL && baseTable != null && baseSchema != null) {
            executeFull(projection);
        } else {
            executeFast(projection);
        }
    }

    private List<Integer> projection() {
        List<Integer> projection = new ArrayList<>();
        projection.add(fieldIndex(indexRowType, dstField));
        for (String column : projectedColumns) {
            if (column.equals(prefixField) || column.equals(dstField)) {
                throw new IllegalArgumentException(
                        "Endpoint column '"
                                + column
                                + "' is implicit in adjacency scans; project payload columns only.");
            }
            projection.add(fieldIndex(indexRowType, column));
        }
        return projection;
    }

    private void executeFast(List<Integer> projection) {
        PredicateBuilder builder = new PredicateBuilder(indexRowType);
        Predicate filter = builder.equal(fieldIndex(indexRowType, prefixField), from);
        int[] projectionArray = projection.stream().mapToInt(Integer::intValue).toArray();
        rows = new ArrayList<>(GraphReads.readRows(indexTable, filter, projectionArray));
        checkDegreeCap(rows.size());
        bindPositions(projectionArray);
    }

    /**
     * FULL mode (design §6.3): reads the index snapshot and overlays the partial aggregates of the
     * interval after the coverage watermark, computed against the base table at query time.
     */
    private void executeFull(List<Integer> projection) {
        List<String> aggregateColumns = new ArrayList<>();
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            aggregateColumns.add(GraphEdgeIndexPlanner.aggregateColumn(aggregate));
        }

        // read every aggregate column of the index rows: the overlay needs them all even when the
        // caller only projects a subset
        List<Integer> fullProjection = new ArrayList<>();
        fullProjection.add(fieldIndex(indexRowType, dstField));
        for (String column : aggregateColumns) {
            fullProjection.add(fieldIndex(indexRowType, column));
        }
        PredicateBuilder builder = new PredicateBuilder(indexRowType);
        Predicate filter = builder.equal(fieldIndex(indexRowType, prefixField), from);
        int[] fullProjectionArray = fullProjection.stream().mapToInt(Integer::intValue).toArray();
        List<BinaryRow> indexRows = GraphReads.readRows(indexTable, filter, fullProjectionArray);

        Map<Long, Object[]> byDst = new LinkedHashMap<>();
        for (BinaryRow row : indexRows) {
            Object[] values = new Object[aggregateColumns.size()];
            for (int i = 0; i < aggregateColumns.size(); i++) {
                values[i] = rawValue(row, 1 + i, fieldType(indexRowType, aggregateColumns.get(i)));
            }
            byDst.put(row.getLong(0), values);
        }

        Long watermark = GraphEdgeIndexMaintainer.effectiveWatermark(baseSchema, indexTable.name());
        Snapshot latest = baseTable.store().snapshotManager().latestSnapshot();
        if (watermark != null && latest != null && latest.id() > watermark) {
            GraphEdgeIndexAggregator.AggregatedEdges gap =
                    GraphEdgeIndexAggregator.incremental(
                            baseTable, baseSchema, decl, watermark, latest.id());
            for (Map.Entry<GraphEdgePair, Object[]> entry : gap.values().entrySet()) {
                GraphEdgePair pair = reverse ? entry.getKey().reversed() : entry.getKey();
                if (pair.src() != from) {
                    continue;
                }
                Object[] stored = byDst.get(pair.dst());
                byDst.put(
                        pair.dst(),
                        stored == null
                                ? entry.getValue()
                                : GraphEdgeIndexAggregator.mergeValues(
                                        baseSchema, decl, stored, entry.getValue()));
            }
        }

        checkDegreeCap(byDst.size());
        List<InternalRow> materialized = new ArrayList<>(byDst.size());
        for (Map.Entry<Long, Object[]> entry : byDst.entrySet()) {
            GenericRow row = new GenericRow(projection.size());
            row.setField(0, entry.getKey());
            for (int i = 1; i < projection.size(); i++) {
                String column = projectedColumns.get(i - 1);
                int aggregatePosition = aggregateColumns.indexOf(column);
                DataType type = fieldType(indexRowType, column);
                row.setField(i, toStoredValue(entry.getValue()[aggregatePosition], type));
            }
            materialized.add(row);
        }
        rows = materialized;
        bindFullPositions();
    }

    private void bindFullPositions() {
        positions = new HashMap<>();
        columnTypes = new HashMap<>();
        positions.put(dstField, 0);
        columnTypes.put(
                dstField, indexRowType.getFields().get(fieldIndex(indexRowType, dstField)).type());
        for (int i = 0; i < projectedColumns.size(); i++) {
            String column = projectedColumns.get(i);
            positions.put(column, i + 1);
            columnTypes.put(column, fieldType(indexRowType, column));
        }
        pos = -1;
    }

    private void bindPositions(int[] projectionArray) {
        positions = new HashMap<>();
        columnTypes = new HashMap<>();
        positions.put(dstField, 0);
        columnTypes.put(dstField, indexRowType.getFields().get(projectionArray[0]).type());
        for (int i = 1; i < projectionArray.length; i++) {
            String column = projectedColumns.get(i - 1);
            positions.put(column, i);
            columnTypes.put(column, indexRowType.getFields().get(projectionArray[i]).type());
        }
        pos = -1;
    }

    private void checkDegreeCap(int matched) {
        if (matched > degreeCap) {
            throw new GraphCapExceededException(
                    String.format(
                            "Adjacency scan of '%s' from vertex %d hit degreeCap=%d but matched %d"
                                    + " edges; raise degreeCap or pre-filter the traversal.",
                            edgeName, from, degreeCap, matched));
        }
    }

    private static DataType fieldType(RowType rowType, String column) {
        for (int i = 0; i < rowType.getFields().size(); i++) {
            if (rowType.getFields().get(i).name().equals(column)) {
                return rowType.getFields().get(i).type();
            }
        }
        throw new IllegalArgumentException(
                "Column '" + column + "' does not exist in the adjacency schema.");
    }

    @Nullable
    private static Object rawValue(InternalRow row, int position, DataType type) {
        if (row.isNullAt(position)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BIGINT:
                return row.getLong(position);
            case INTEGER:
                return row.getInt(position);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return row.getDecimal(position, decimalType.getPrecision(), decimalType.getScale());
            case VARCHAR:
            case CHAR:
                return row.getString(position).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestamp(position, ((TimestampType) type).getPrecision());
            default:
                throw new IllegalArgumentException(
                        "Adjacency payload type " + type + " is not supported by the SDK.");
        }
    }

    @Nullable
    private static Object toStoredValue(@Nullable Object value, DataType type) {
        if (value == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case DECIMAL:
                if (value instanceof Decimal) {
                    return value;
                }
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        (BigDecimal) value, decimalType.getPrecision(), decimalType.getScale());
            case VARCHAR:
            case CHAR:
                return BinaryString.fromString(value.toString());
            default:
                return value;
        }
    }

    private int position(String column) {
        Integer position = positions.get(column);
        if (position == null) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' was not projected; call project(\""
                            + column
                            + "\") before reading it.");
        }
        return position;
    }

    private void requireCurrent() {
        if (current == null) {
            throw new IllegalStateException(
                    "next() must return true before adjacency row values are accessed.");
        }
    }
}
