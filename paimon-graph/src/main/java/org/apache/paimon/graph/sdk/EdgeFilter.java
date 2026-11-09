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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.math.BigDecimal;

/**
 * Predicate over the aggregate payload of an adjacency row, used to restrict the intermediate
 * vertices of a two-hop traversal (design §8.1 ⑤):
 *
 * <pre>{@code
 * store.twoHop("transfer", Direction.OUT, src,
 *         EdgeFilter.where("sum_amount").ge(1000),
 *         DegreeCap.of(1_000), ResultCap.of(10_000));
 * }</pre>
 *
 * <p>The column must be one of the aggregate columns of the adjacency index table. Values are
 * compared by {@link #test(AdjIterator)} on the projected payload; a NULL value never matches.
 */
@Public
public final class EdgeFilter {

    private final String column;
    private final Op op;
    private final Object value;

    private EdgeFilter(String column, Op op, Object value) {
        this.column = column;
        this.op = op;
        this.value = value;
    }

    /** Starts a filter on one adjacency payload column. */
    public static Column where(String column) {
        return new Column(column);
    }

    public String column() {
        return column;
    }

    /** Whether the current adjacency row passes this filter. */
    public boolean test(AdjIterator row) {
        if (value instanceof BigDecimal) {
            BigDecimal actual = row.getDecimal(column);
            return actual != null && compare(actual.compareTo((BigDecimal) value));
        }
        long actual = row.getLong(column);
        return compare(Long.compare(actual, (Long) value));
    }

    /** Whether the materialized payload of a multi-hop traversal passes this filter. */
    boolean testValues(EdgeValues values) {
        Object actual = values.raw(column);
        if (actual == null) {
            return false;
        }
        if (value instanceof BigDecimal) {
            if (!(actual instanceof BigDecimal)) {
                throw new IllegalArgumentException(
                        "Column '"
                                + column
                                + "' is not a decimal column, so it cannot be compared with a"
                                + " decimal EdgeFilter value.");
            }
            return compare(((BigDecimal) actual).compareTo((BigDecimal) value));
        }
        if (!(actual instanceof Number)) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' is not a numeric column, so it cannot be compared with a long"
                            + " EdgeFilter value.");
        }
        return compare(Long.compare(((Number) actual).longValue(), (Long) value));
    }

    private boolean compare(int comparison) {
        switch (op) {
            case EQ:
                return comparison == 0;
            case GE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException("Unknown edge filter operator " + op);
        }
    }

    /** The comparison operators of an {@link EdgeFilter}. */
    public enum Op {
        EQ,
        GE,
        GT,
        LE,
        LT
    }

    /** Column selector of {@link EdgeFilter}. */
    public static final class Column {

        private final String column;

        private Column(String column) {
            this.column = column;
        }

        public EdgeFilter eq(long value) {
            return new EdgeFilter(column, Op.EQ, value);
        }

        public EdgeFilter ge(long value) {
            return new EdgeFilter(column, Op.GE, value);
        }

        public EdgeFilter gt(long value) {
            return new EdgeFilter(column, Op.GT, value);
        }

        public EdgeFilter le(long value) {
            return new EdgeFilter(column, Op.LE, value);
        }

        public EdgeFilter lt(long value) {
            return new EdgeFilter(column, Op.LT, value);
        }

        public EdgeFilter eq(BigDecimal value) {
            return new EdgeFilter(column, Op.EQ, requireNonNull(value));
        }

        public EdgeFilter ge(BigDecimal value) {
            return new EdgeFilter(column, Op.GE, requireNonNull(value));
        }

        public EdgeFilter gt(BigDecimal value) {
            return new EdgeFilter(column, Op.GT, requireNonNull(value));
        }

        public EdgeFilter le(BigDecimal value) {
            return new EdgeFilter(column, Op.LE, requireNonNull(value));
        }

        public EdgeFilter lt(BigDecimal value) {
            return new EdgeFilter(column, Op.LT, requireNonNull(value));
        }

        private static BigDecimal requireNonNull(@Nullable BigDecimal value) {
            if (value == null) {
                throw new IllegalArgumentException("EdgeFilter decimal value must not be null.");
            }
            return value;
        }
    }
}
