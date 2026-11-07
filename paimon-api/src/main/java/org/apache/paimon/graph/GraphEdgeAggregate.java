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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** One aggregate entry declared by 'graph.edge.aggregates': field:function[:alias]. */
@Public
public class GraphEdgeAggregate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Wildcard field: applies to every event, currently only for {@link #COUNT}. */
    public static final String WILDCARD = "*";

    /** Graph-level count semantics, expanded into a sum-aggregated cnt column by the planner. */
    public static final String COUNT = "count";

    private final String field;
    private final String function;
    @Nullable private final String alias;

    public GraphEdgeAggregate(String field, String function, @Nullable String alias) {
        this.field = field;
        this.function = function;
        this.alias = alias;
    }

    public String field() {
        return field;
    }

    public String function() {
        return function;
    }

    @Nullable
    public String alias() {
        return alias;
    }

    public boolean isWildcard() {
        return WILDCARD.equals(field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphEdgeAggregate)) {
            return false;
        }
        GraphEdgeAggregate that = (GraphEdgeAggregate) o;
        return Objects.equals(field, that.field)
                && Objects.equals(function, that.function)
                && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, function, alias);
    }

    @Override
    public String toString() {
        return "GraphEdgeAggregate{field='"
                + field
                + "', function='"
                + function
                + (alias == null ? "'}" : "', alias='" + alias + "'}");
    }
}
