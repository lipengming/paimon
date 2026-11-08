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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Shared table-read plumbing of the SDK: predicate + projection scans over public APIs. */
final class GraphReads {

    private GraphReads() {}

    /**
     * Reads the rows matching the filter with the given projection (field indices, null for full
     * rows). Rows are materialized as owned {@link BinaryRow} copies, because source iterators may
     * reuse row instances; result sizes are bounded by the SDK caps.
     *
     * <p>The split plan is built without the filter and the row-level predicate is evaluated by the
     * reader through {@link TableRead#executeFilter()}: on this codebase a filter-pruned plan may
     * drop matching rows (pruning consumes the predicate without re-evaluating it row by row),
     * while the unpruned plan plus reader-side filtering is exact. Index acceleration keeps
     * applying to the reader-side predicate.
     */
    static List<BinaryRow> readRows(
            FileStoreTable table, @Nullable Predicate filter, @Nullable int[] projection) {
        RowType full = new RowType(table.schema().fields());
        RowType readType =
                projection == null
                        ? full
                        : new RowType(
                                Arrays.stream(projection)
                                        .mapToObj(i -> full.getFields().get(i))
                                        .collect(Collectors.toList()));
        InternalRowSerializer serializer = new InternalRowSerializer(readType);

        ReadBuilder planBuilder = table.newReadBuilder();
        ReadBuilder readBuilder = table.newReadBuilder();
        if (filter != null) {
            readBuilder = readBuilder.withFilter(filter);
        }
        if (projection != null) {
            readBuilder = readBuilder.withProjection(projection);
        }

        List<BinaryRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().executeFilter().createReader(planBuilder.newScan().plan())) {
            // toBinaryRow reuses a single instance, so every row must be copied
            reader.forEachRemaining(row -> rows.add(serializer.toBinaryRow(row).copy()));
        } catch (IOException e) {
            throw new UncheckedIOException("Graph SDK read failed on table " + table.name(), e);
        }
        return rows;
    }

    /** The index of one field of the row type; fails with a readable error when absent. */
    static int fieldIndex(RowType rowType, String field) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(field)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Column '"
                        + field
                        + "' does not exist in schema ["
                        + fields.stream().map(DataField::name).collect(Collectors.joining(", "))
                        + "].");
    }

    static TableSchema schema(FileStoreTable table) {
        return table.schema();
    }
}
