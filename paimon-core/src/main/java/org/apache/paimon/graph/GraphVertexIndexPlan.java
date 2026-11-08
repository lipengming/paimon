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

import org.apache.paimon.options.GraphIndexOptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Expansion result of {@link GraphVertexIndexPlanner}: the resolved vertex index backend and, for
 * the pk-index backend, the column-level index options to apply to the vertex table itself.
 *
 * <p>The resolved backend and the unmet pk-index requirements are the annotation surfaced later by
 * the '$graph_indexes' system table.
 */
public class GraphVertexIndexPlan {

    private final String baseTableName;
    private final String requestedBackend;
    private final String backend;
    private final List<String> unmetPkIndexRequirements;
    private final Map<String, String> indexOptions;

    public GraphVertexIndexPlan(
            String baseTableName,
            String requestedBackend,
            String backend,
            List<String> unmetPkIndexRequirements,
            Map<String, String> indexOptions) {
        this.baseTableName = baseTableName;
        this.requestedBackend = requestedBackend;
        this.backend = backend;
        this.unmetPkIndexRequirements = Collections.unmodifiableList(unmetPkIndexRequirements);
        this.indexOptions = Collections.unmodifiableMap(indexOptions);
    }

    public String baseTableName() {
        return baseTableName;
    }

    /** The backend requested by 'graph.index.backend' ('auto', 'pk-index' or 'table'). */
    public String requestedBackend() {
        return requestedBackend;
    }

    /**
     * The resolved backend: 'pk-index' (column-level secondary indexes on the vertex table) or
     * 'table' (standalone node-map index tables).
     */
    public String backend() {
        return backend;
    }

    /**
     * Whether 'auto' degraded to the 'table' backend because the pk-index prerequisites are not
     * met.
     */
    public boolean degraded() {
        return GraphIndexOptions.BACKEND_AUTO.equals(requestedBackend)
                && GraphIndexOptions.BACKEND_TABLE.equals(backend);
    }

    /**
     * The unmet primary-key index requirements that forced 'auto' to degrade (or that reject an
     * explicit 'pk-index' backend); empty when the prerequisites are met.
     */
    public List<String> unmetPkIndexRequirements() {
        return unmetPkIndexRequirements;
    }

    /**
     * The expanded column-level index options for the pk-index backend ({@code
     * global-index.btree.index-columns} / {@code global-index.bitmap.index-columns}, merged with
     * any existing declarations); empty for the table backend and for declarations without node-map
     * fields.
     */
    public Map<String, String> indexOptions() {
        return indexOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphVertexIndexPlan)) {
            return false;
        }
        GraphVertexIndexPlan that = (GraphVertexIndexPlan) o;
        return Objects.equals(baseTableName, that.baseTableName)
                && Objects.equals(requestedBackend, that.requestedBackend)
                && Objects.equals(backend, that.backend)
                && Objects.equals(unmetPkIndexRequirements, that.unmetPkIndexRequirements)
                && Objects.equals(indexOptions, that.indexOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                baseTableName, requestedBackend, backend, unmetPkIndexRequirements, indexOptions);
    }

    @Override
    public String toString() {
        return "GraphVertexIndexPlan{baseTableName='"
                + baseTableName
                + "', requestedBackend='"
                + requestedBackend
                + "', backend='"
                + backend
                + "', unmetPkIndexRequirements="
                + unmetPkIndexRequirements
                + ", indexOptions="
                + indexOptions
                + "}";
    }
}
