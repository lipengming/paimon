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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Typed accessor of the aggregate payload of one adjacency row materialized by a multi-hop
 * traversal. The map keys are the expanded adjacency columns (for example {@code sum_amount},
 * {@code cnt}).
 */
@Public
public final class EdgeValues {

    private final Map<String, Object> values;

    EdgeValues(Map<String, Object> values) {
        this.values = new LinkedHashMap<>(values);
    }

    /** The adjacency column names, in declaration order. */
    public Iterable<String> columns() {
        return Collections.unmodifiableSet(values.keySet());
    }

    @Nullable
    public Object raw(String column) {
        if (!values.containsKey(column)) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' is not part of the adjacency payload; available columns are "
                            + values.keySet()
                            + ".");
        }
        return values.get(column);
    }

    public boolean isNull(String column) {
        return raw(column) == null;
    }

    public long getLong(String column) {
        Object value = raw(column);
        if (value == null) {
            throw new IllegalStateException(
                    "Column '" + column + "' of the adjacency row is null.");
        }
        return ((Number) value).longValue();
    }

    @Nullable
    public String getString(String column) {
        Object value = raw(column);
        return value == null ? null : value.toString();
    }

    @Nullable
    public BigDecimal getDecimal(String column) {
        Object value = raw(column);
        return value == null ? null : (BigDecimal) value;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
