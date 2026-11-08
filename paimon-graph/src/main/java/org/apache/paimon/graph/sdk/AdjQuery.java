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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.graph.sdk.GraphReads.fieldIndex;

/**
 * Adjacency scan over one edge binding (design §8.1 ②③): a primary-key prefix read on the forward
 * ({@code __edge}, prefix = src) or reverse ({@code __edge_rev}, prefix = dst) index table. The
 * prefix predicate locates one bucket via the bucket key and lets Paimon prune the key range, so
 * one-degree adjacency is a bounded merge read.
 */
public class AdjQuery implements AdjIterator {

    private final FileStoreTable indexTable;
    private final RowType indexRowType;
    private final String edgeName;
    private final String prefixField;
    private final String dstField;
    private final SearchMode searchMode;

    @Nullable private Long from;
    private final List<String> projectedColumns = new ArrayList<>();
    private int degreeCap = Integer.MAX_VALUE;

    private List<BinaryRow> rows;
    private Map<String, Integer> positions;
    private Map<String, DataType> columnTypes;
    private int pos = -1;
    private BinaryRow current;

    AdjQuery(
            FileStoreTable indexTable,
            String edgeName,
            String prefixField,
            String dstField,
            SearchMode searchMode) {
        this.indexTable = indexTable;
        this.indexRowType = new RowType(indexTable.schema().fields());
        this.edgeName = edgeName;
        this.prefixField = prefixField;
        this.dstField = dstField;
        this.searchMode = searchMode;
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
        org.apache.paimon.types.DecimalType type =
                (org.apache.paimon.types.DecimalType) columnTypes.get(column);
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
        if (searchMode != SearchMode.FAST) {
            throw new UnsupportedOperationException(
                    "SearchMode "
                            + searchMode
                            + " arrives with the full-mode reconciliation; use FAST for now.");
        }
        if (from == null) {
            throw new IllegalArgumentException(
                    "from(...) must be set before iterating an adjacency scan of '"
                            + edgeName
                            + "'.");
        }

        PredicateBuilder builder = new PredicateBuilder(indexRowType);
        Predicate filter = builder.equal(fieldIndex(indexRowType, prefixField), from);

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

        int[] projectionArray = projection.stream().mapToInt(Integer::intValue).toArray();
        rows = GraphReads.readRows(indexTable, filter, projectionArray);
        if (rows.size() > degreeCap) {
            throw new GraphCapExceededException(
                    String.format(
                            "Adjacency scan of '%s' from vertex %d hit degreeCap=%d but matched %d"
                                    + " edges; raise degreeCap or pre-filter the traversal.",
                            edgeName, from, degreeCap, rows.size()));
        }

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
