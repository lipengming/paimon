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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldSumAggFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Folds the edge events of the base table over an arbitrary snapshot range into per-pair partial
 * aggregates, using the official {@link FieldAggregator} of each declared function.
 *
 * <p>This is the shared aggregation engine behind three entry points of the graph index:
 *
 * <ul>
 *   <li>{@link #incremental} — the streaming maintenance micro-batch (design §7.2/§7.3), consuming
 *       the events of one snapshot interval;
 *   <li>{@link #snapshot} — the full-history scan used by the detail reconciliation (design §5.6)
 *       and by programmatic backfills;
 *   <li>the full read mode of the SDK (design §6.3), which merges the gap between the coverage
 *       watermark and the latest base table snapshot into the index rows at query time.
 * </ul>
 *
 * <p>Batch/stream equivalence: the wildcard {@code *:count} folds a constant 1 with a sum
 * aggregator, so one streaming batch and a full {@code COUNT(*)} backfill compose to the same total
 * under the sum aggregation of the merge engine.
 */
public final class GraphEdgeIndexAggregator {

    private GraphEdgeIndexAggregator() {}

    /**
     * Aggregates the events of the snapshot interval {@code (fromExclusive, toInclusive]}, the
     * streaming micro-batch contract of design §7.3.
     */
    public static AggregatedEdges incremental(
            FileStoreTable base,
            TableSchema baseSchema,
            GraphEdgeIndexDecl decl,
            long fromExclusive,
            long toInclusive) {
        checkArgument(
                toInclusive > fromExclusive,
                "The incremental interval must be non-empty, but got (%s, %s].",
                fromExclusive,
                toInclusive);
        Map<String, String> dynamic = new HashMap<>();
        dynamic.put(CoreOptions.INCREMENTAL_BETWEEN.key(), fromExclusive + "," + toInclusive);
        return aggregate(base.copy(dynamic), baseSchema, decl);
    }

    /** Aggregates the full history visible at one base table snapshot (batch backfill / detail). */
    public static AggregatedEdges snapshot(
            FileStoreTable base, TableSchema baseSchema, GraphEdgeIndexDecl decl, long snapshotId) {
        Map<String, String> dynamic = new HashMap<>();
        dynamic.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId));
        return aggregate(base.copy(dynamic), baseSchema, decl);
    }

    /**
     * Merges one partial aggregate array with another one using the declared functions, mirroring
     * the composition the aggregation merge engine applies on read. Used by the full read mode to
     * overlay the gap scan on the stored index rows.
     */
    public static Object[] mergeValues(
            TableSchema baseSchema, GraphEdgeIndexDecl decl, Object[] left, Object[] right) {
        List<FieldAggregator> aggregators = aggregators(baseSchema, decl);
        Object[] merged = new Object[aggregators.size()];
        for (int i = 0; i < aggregators.size(); i++) {
            merged[i] = aggregators.get(i).agg(left[i], right[i]);
        }
        return merged;
    }

    private static AggregatedEdges aggregate(
            FileStoreTable table, TableSchema baseSchema, GraphEdgeIndexDecl decl) {
        RowType rowType = new RowType(baseSchema.fields());
        int srcIndex = fieldIndex(rowType, decl.srcField());
        int dstIndex = fieldIndex(rowType, decl.dstField());
        DataType srcType = rowType.getFields().get(srcIndex).type();
        DataType dstType = rowType.getFields().get(dstIndex).type();
        checkEndpointType(decl.srcField(), srcType);
        checkEndpointType(decl.dstField(), dstType);

        CoreOptions coreOptions = new CoreOptions(baseSchema.options());
        List<ValueSource> sources = new ArrayList<>();
        List<FieldAggregator> aggregators = new ArrayList<>();
        List<Integer> projection = new ArrayList<>();
        projection.add(srcIndex);
        projection.add(dstIndex);
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            if (aggregate.isWildcard()) {
                sources.add(ValueSource.count());
                aggregators.add(
                        new FieldSumAggFactory()
                                .create(
                                        DataTypes.BIGINT(),
                                        coreOptions,
                                        GraphEdgeIndexPlanner.COUNT_COLUMN));
            } else {
                int valueIndex = fieldIndex(rowType, aggregate.field());
                int projectionPos;
                if (projection.contains(valueIndex)) {
                    projectionPos = projection.indexOf(valueIndex);
                } else {
                    projectionPos = projection.size();
                    projection.add(valueIndex);
                }
                DataType fieldType = rowType.getFields().get(valueIndex).type();
                sources.add(ValueSource.of(projectionPos, fieldType));
                aggregators.add(
                        FieldAggregatorFactory.create(
                                fieldType, aggregate.field(), aggregate.function(), coreOptions));
            }
        }

        Map<GraphEdgePair, Object[]> values = new LinkedHashMap<>();
        long[] eventCount = new long[] {0L};
        int[] projectionArray = projection.stream().mapToInt(Integer::intValue).toArray();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .withProjection(projectionArray)
                        .newRead()
                        .executeFilter()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(
                    row -> {
                        long src = endpoint(row, 0, srcType);
                        long dst = endpoint(row, 1, dstType);
                        eventCount[0]++;
                        GraphEdgePair pair = new GraphEdgePair(src, dst);
                        Object[] accumulator =
                                values.computeIfAbsent(pair, k -> new Object[aggregators.size()]);
                        for (int i = 0; i < aggregators.size(); i++) {
                            accumulator[i] =
                                    aggregators
                                            .get(i)
                                            .agg(accumulator[i], sources.get(i).extract(row));
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to read the edge events of table '%s'; if the requested"
                                    + " snapshots have expired, rebuild the graph index.",
                            table.name()),
                    e);
        }
        return new AggregatedEdges(values, eventCount[0]);
    }

    private static List<FieldAggregator> aggregators(
            TableSchema baseSchema, GraphEdgeIndexDecl decl) {
        RowType rowType = new RowType(baseSchema.fields());
        CoreOptions coreOptions = new CoreOptions(baseSchema.options());
        List<FieldAggregator> aggregators = new ArrayList<>();
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            if (aggregate.isWildcard()) {
                aggregators.add(
                        new FieldSumAggFactory()
                                .create(
                                        DataTypes.BIGINT(),
                                        coreOptions,
                                        GraphEdgeIndexPlanner.COUNT_COLUMN));
            } else {
                DataType fieldType =
                        rowType.getFields().get(fieldIndex(rowType, aggregate.field())).type();
                aggregators.add(
                        FieldAggregatorFactory.create(
                                fieldType, aggregate.field(), aggregate.function(), coreOptions));
            }
        }
        return aggregators;
    }

    private static int fieldIndex(RowType rowType, String field) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(field)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Column '" + field + "' does not exist in table schema.");
    }

    private static void checkEndpointType(String field, DataType type) {
        switch (type.getTypeRoot()) {
            case BIGINT:
            case INTEGER:
                return;
            default:
                throw new IllegalArgumentException(
                        "Endpoint field '"
                                + field
                                + "' of the edge declaration must be BIGINT or INT, but got "
                                + type);
        }
    }

    private static long endpoint(InternalRow row, int position, DataType type) {
        return type.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.INTEGER
                ? row.getInt(position)
                : row.getLong(position);
    }

    /** Where one aggregate's input value comes from in the projected base-table row. */
    private static final class ValueSource {

        private static final int COUNT = -1;

        private final int projectionPos;
        @Nullable private final DataType fieldType;

        private ValueSource(int projectionPos, @Nullable DataType fieldType) {
            this.projectionPos = projectionPos;
            this.fieldType = fieldType;
        }

        private static ValueSource count() {
            return new ValueSource(COUNT, null);
        }

        private static ValueSource of(int projectionPos, DataType fieldType) {
            return new ValueSource(projectionPos, fieldType);
        }

        private Object extract(InternalRow row) {
            if (projectionPos == COUNT) {
                return 1L;
            }
            return InternalRowUtils.get(row, projectionPos, fieldType);
        }
    }

    /** Per-pair partial aggregates over one snapshot range. */
    public static final class AggregatedEdges {

        private final Map<GraphEdgePair, Object[]> values;
        private final long eventCount;

        private AggregatedEdges(Map<GraphEdgePair, Object[]> values, long eventCount) {
            this.values = values;
            this.eventCount = eventCount;
        }

        /** Pair to aggregate array (in declaration order). */
        public Map<GraphEdgePair, Object[]> values() {
            return Collections.unmodifiableMap(values);
        }

        /** Number of base-table events folded into this result. */
        public long eventCount() {
            return eventCount;
        }

        public boolean isEmpty() {
            return values.isEmpty();
        }

        /**
         * The partial aggregates shaped like rows of the adjacency index table: the leading and
         * trailing endpoint columns first, then the aggregate columns in declaration order.
         *
         * @param reverse whether the reverse adjacency orientation (dst, src) is wanted
         */
        public List<InternalRow> rows(GraphEdgeIndexDecl decl, boolean reverse) {
            List<InternalRow> rows = new ArrayList<>(values.size());
            for (Map.Entry<GraphEdgePair, Object[]> entry : values.entrySet()) {
                GraphEdgePair pair = entry.getKey();
                long first = reverse ? pair.dst() : pair.src();
                long second = reverse ? pair.src() : pair.dst();
                GenericRow row = new GenericRow(2 + decl.aggregates().size());
                row.setField(0, first);
                row.setField(1, second);
                for (int i = 0; i < entry.getValue().length; i++) {
                    row.setField(2 + i, entry.getValue()[i]);
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
