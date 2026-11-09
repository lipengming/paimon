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
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Detail reconciliation of the adjacency indexes (design §5.6/§6.3): compares the full-history
 * aggregation of the base table at its latest snapshot with the current contents of {@code __edge}
 * and {@code __edge_rev}, reporting missing pairs, extra pairs and aggregate mismatches.
 *
 * <p>The comparison is by value: DECIMAL values are compared by {@link BigDecimal#compareTo}, other
 * aggregate values by equality. It is the quality gate behind the at-least-once maintenance
 * contract (a re-consumed interval or a failed index commit shows up as a mismatch here).
 */
public final class GraphEdgeIndexReconciler {

    private static final int MAX_SAMPLES = 10;

    private GraphEdgeIndexReconciler() {}

    /** Reconciles both adjacency index tables of one edge table. */
    public static List<IndexReconcileResult> reconcile(Catalog catalog, Identifier baseTable)
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
        Snapshot latest = base.store().snapshotManager().latestSnapshot();
        Map<GraphEdgePair, Object[]> baseAggregates =
                latest == null
                        ? new LinkedHashMap<>()
                        : GraphEdgeIndexAggregator.snapshot(base, baseSchema, decl, latest.id())
                                .values();

        List<IndexReconcileResult> results = new ArrayList<>();
        results.add(
                reconcileTable(
                        catalog,
                        baseTable,
                        decl,
                        plan.edge(),
                        plan.edgeTableName(),
                        baseAggregates,
                        false));
        if (plan.reverse()) {
            results.add(
                    reconcileTable(
                            catalog,
                            baseTable,
                            decl,
                            plan.edgeRev(),
                            plan.edgeRevTableName(),
                            baseAggregates,
                            true));
        }
        return results;
    }

    private static IndexReconcileResult reconcileTable(
            Catalog catalog,
            Identifier baseTable,
            GraphEdgeIndexDecl decl,
            TableSchema indexSchema,
            String indexTableName,
            Map<GraphEdgePair, Object[]> baseAggregates,
            boolean reverse)
            throws Catalog.TableNotExistException {
        FileStoreTable indexTable =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create(baseTable.getDatabaseName(), indexTableName));
        RowType indexRowType = new RowType(indexSchema.fields());
        List<String> aggregateColumns = new ArrayList<>();
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            aggregateColumns.add(GraphEdgeIndexPlanner.aggregateColumn(aggregate));
        }

        Map<GraphEdgePair, Object[]> indexAggregates = new LinkedHashMap<>();
        try (RecordReader<InternalRow> reader =
                indexTable
                        .newReadBuilder()
                        .newRead()
                        .createReader(indexTable.newReadBuilder().newScan().plan())) {
            // the reader reuses row instances: extract every value inside the callback
            reader.forEachRemaining(
                    row -> {
                        GraphEdgePair pair = new GraphEdgePair(row.getLong(0), row.getLong(1));
                        Object[] values = new Object[aggregateColumns.size()];
                        for (int i = 0; i < aggregateColumns.size(); i++) {
                            values[i] =
                                    readValue(
                                            row,
                                            2 + i,
                                            fieldType(indexRowType, aggregateColumns.get(i)));
                        }
                        indexAggregates.put(pair, values);
                    });
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read adjacency index table '" + indexTableName + "'.", e);
        }

        long missing = 0;
        long mismatched = 0;
        List<String> samples = new ArrayList<>();
        for (Map.Entry<GraphEdgePair, Object[]> entry : baseAggregates.entrySet()) {
            GraphEdgePair pair = reverse ? entry.getKey().reversed() : entry.getKey();
            Object[] expected = entry.getValue();
            Object[] actual = indexAggregates.get(pair);
            if (actual == null) {
                missing++;
                addSample(samples, "missing " + pair);
                continue;
            }
            if (!valuesEqual(expected, actual)) {
                mismatched++;
                addSample(
                        samples,
                        "mismatched "
                                + pair
                                + ": base="
                                + render(expected)
                                + " index="
                                + render(actual));
            }
        }

        long extra = 0;
        for (GraphEdgePair pair : indexAggregates.keySet()) {
            GraphEdgePair basePair = reverse ? pair.reversed() : pair;
            if (!baseAggregates.containsKey(basePair)) {
                extra++;
                addSample(samples, "extra " + pair);
            }
        }

        return new IndexReconcileResult(
                indexTableName,
                baseAggregates.size(),
                indexAggregates.size(),
                missing,
                extra,
                mismatched,
                samples);
    }

    private static DataType fieldType(RowType rowType, String column) {
        for (org.apache.paimon.types.DataField field : rowType.getFields()) {
            if (field.name().equals(column)) {
                return field.type();
            }
        }
        throw new IllegalArgumentException(
                "Column '" + column + "' is missing from the adjacency index schema.");
    }

    private static Object readValue(InternalRow row, int position, DataType type) {
        if (row.isNullAt(position)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BIGINT:
                return row.getLong(position);
            case INTEGER:
                return row.getInt(position);
            case DECIMAL:
                org.apache.paimon.types.DecimalType decimalType =
                        (org.apache.paimon.types.DecimalType) type;
                return row.getDecimal(position, decimalType.getPrecision(), decimalType.getScale());
            case VARCHAR:
            case CHAR:
                return row.getString(position).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestamp(
                        position, ((org.apache.paimon.types.TimestampType) type).getPrecision());
            default:
                return InternalRowUtils.get(row, position, type);
        }
    }

    private static boolean valuesEqual(Object[] expected, Object[] actual) {
        if (expected.length != actual.length) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            Object left = expected[i];
            Object right = actual[i];
            if (left == null || right == null) {
                if (left != right) {
                    return false;
                }
            } else if (decimalValue(left) != null && decimalValue(right) != null) {
                if (decimalValue(left).compareTo(decimalValue(right)) != 0) {
                    return false;
                }
            } else if (left instanceof Number
                    && right instanceof Number
                    && !(left instanceof BigDecimal)
                    && !(right instanceof BigDecimal)) {
                if (new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()))
                        != 0) {
                    return false;
                }
            } else if (!left.equals(right)) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static BigDecimal decimalValue(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof org.apache.paimon.data.Decimal) {
            return ((org.apache.paimon.data.Decimal) value).toBigDecimal();
        }
        return null;
    }

    private static String render(Object[] values) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(values[i]);
        }
        return builder.append("]").toString();
    }

    private static void addSample(List<String> samples, String sample) {
        if (samples.size() < MAX_SAMPLES) {
            samples.add(sample);
        }
    }

    /** Comparison result of one adjacency index table. */
    public static final class IndexReconcileResult {

        private final String indexTable;
        private final long basePairs;
        private final long indexPairs;
        private final long missingPairs;
        private final long extraPairs;
        private final long mismatchedPairs;
        private final List<String> samples;

        private IndexReconcileResult(
                String indexTable,
                long basePairs,
                long indexPairs,
                long missingPairs,
                long extraPairs,
                long mismatchedPairs,
                List<String> samples) {
            this.indexTable = indexTable;
            this.basePairs = basePairs;
            this.indexPairs = indexPairs;
            this.missingPairs = missingPairs;
            this.extraPairs = extraPairs;
            this.mismatchedPairs = mismatchedPairs;
            this.samples = samples;
        }

        public String indexTable() {
            return indexTable;
        }

        /** Number of distinct {@code (src, dst)} pairs of the base table. */
        public long basePairs() {
            return basePairs;
        }

        public long indexPairs() {
            return indexPairs;
        }

        public long missingPairs() {
            return missingPairs;
        }

        public long extraPairs() {
            return extraPairs;
        }

        public long mismatchedPairs() {
            return mismatchedPairs;
        }

        /** Up to ten human-readable samples of the differences. */
        public List<String> samples() {
            return samples;
        }

        public boolean consistent() {
            return missingPairs == 0 && extraPairs == 0 && mismatchedPairs == 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s{pairs=%d, indexPairs=%d, missing=%d, extra=%d, mismatched=%d, consistent=%s}",
                    indexTable,
                    basePairs,
                    indexPairs,
                    missingPairs,
                    extraPairs,
                    mismatchedPairs,
                    consistent());
        }
    }
}
