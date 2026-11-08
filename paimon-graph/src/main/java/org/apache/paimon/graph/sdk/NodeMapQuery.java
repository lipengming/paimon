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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.graph.sdk.GraphReads.fieldIndex;

/**
 * Attribute reverse lookup over one node-map field (design §8.1 ①): value to node-id set. The
 * predicate is pushed into the vertex table read; the pk-index backend accelerates it
 * automatically, other backends fall back to a plain filtered scan without affecting results.
 *
 * <p>Multi-attribute conjunction is composed in the SDK: intersect the returned bitmaps with {@link
 * RoaringNavigableMap64#and(RoaringNavigableMap64)}.
 */
public class NodeMapQuery {

    /** Hot-value guard (design §8.3): default result cap of 100k ids. */
    public static final int DEFAULT_RESULT_CAP = 100_000;

    private final FileStoreTable table;
    private final RowType rowType;
    private final String field;
    private final String idField;
    private final SearchMode searchMode;

    @Nullable private Object eqValue;
    @Nullable private Object lower;
    @Nullable private Object upper;
    private int resultCap = DEFAULT_RESULT_CAP;

    NodeMapQuery(FileStoreTable table, String field, String idField, SearchMode searchMode) {
        this.table = table;
        this.rowType = new RowType(table.schema().fields());
        this.field = field;
        this.idField = idField;
        this.searchMode = searchMode;
    }

    /** Equality predicate. */
    public NodeMapQuery eq(Object value) {
        this.eqValue = value;
        return this;
    }

    /** Inclusive range predicate. */
    public NodeMapQuery between(Object lower, Object upper) {
        this.lower = lower;
        this.upper = upper;
        return this;
    }

    /** Hot-value guard: throws {@link GraphCapExceededException} when more ids match. */
    public NodeMapQuery resultCap(int cap) {
        if (cap <= 0) {
            throw new IllegalArgumentException("resultCap must be positive, but got " + cap);
        }
        this.resultCap = cap;
        return this;
    }

    /** Scans the matching node ids into a bitmap for SDK-side conjunctions. */
    public RoaringNavigableMap64 scanToBitmap() {
        requireFast();
        PredicateBuilder builder = new PredicateBuilder(rowType);
        int fieldIndex = fieldIndex(rowType, field);
        DataType fieldType = rowType.getFields().get(fieldIndex).type();

        List<Predicate> predicates = new ArrayList<>();
        if (eqValue != null) {
            predicates.add(builder.equal(fieldIndex, toLiteral(fieldType, eqValue)));
        } else if (lower != null && upper != null) {
            predicates.add(builder.greaterOrEqual(fieldIndex, toLiteral(fieldType, lower)));
            predicates.add(builder.lessOrEqual(fieldIndex, toLiteral(fieldType, upper)));
        } else {
            throw new IllegalArgumentException(
                    "Set eq(...) or between(...) before scanToBitmap() on node-map field '"
                            + field
                            + "'.");
        }

        int idIndex = fieldIndex(rowType, idField);
        List<BinaryRow> rows =
                GraphReads.readRows(table, PredicateBuilder.and(predicates), new int[] {idIndex});
        if (rows.size() > resultCap) {
            throw new GraphCapExceededException(
                    String.format(
                            "Node-map scan of '%s' hit resultCap=%d but matched %d vertex ids;"
                                    + " narrow the predicate or raise resultCap.",
                            field, resultCap, rows.size()));
        }
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (BinaryRow row : rows) {
            bitmap.add(row.getLong(0));
        }
        return bitmap;
    }

    private static Object toLiteral(DataType type, Object value) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(String.valueOf(value));
            default:
                return value;
        }
    }

    private void requireFast() {
        if (searchMode != SearchMode.FAST) {
            throw new UnsupportedOperationException(
                    "SearchMode "
                            + searchMode
                            + " arrives with the full-mode reconciliation; use FAST for now.");
        }
    }
}
