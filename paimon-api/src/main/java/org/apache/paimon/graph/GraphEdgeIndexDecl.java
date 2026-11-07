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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Parsed graph edge declaration from the 'graph.edge.*' table options.
 *
 * <p>The declaration is pure graph semantics: endpoints and their vertex types, direction coverage
 * and aggregate intents. Physical adjacency index tables (base table name plus the {@code __edge} /
 * {@code __edge_rev} suffix) are expanded by the versioned graph index planner.
 */
@Public
public class GraphEdgeIndexDecl implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String srcField;
    private final String dstField;
    private final String srcVertex;
    private final String dstVertex;
    private final boolean reverse;
    private final List<GraphEdgeAggregate> aggregates;

    public GraphEdgeIndexDecl(
            String srcField,
            String dstField,
            String srcVertex,
            String dstVertex,
            boolean reverse,
            List<GraphEdgeAggregate> aggregates) {
        this.srcField = srcField;
        this.dstField = dstField;
        this.srcVertex = srcVertex;
        this.dstVertex = dstVertex;
        this.reverse = reverse;
        this.aggregates = Collections.unmodifiableList(aggregates);
    }

    public String srcField() {
        return srcField;
    }

    public String dstField() {
        return dstField;
    }

    public String srcVertex() {
        return srcVertex;
    }

    public String dstVertex() {
        return dstVertex;
    }

    /** Whether a reverse adjacency index table (suffix {@code __edge_rev}) is declared. */
    public boolean reverse() {
        return reverse;
    }

    /** The aggregate entries carried by the adjacency index tables. */
    public List<GraphEdgeAggregate> aggregates() {
        return aggregates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphEdgeIndexDecl)) {
            return false;
        }
        GraphEdgeIndexDecl that = (GraphEdgeIndexDecl) o;
        return reverse == that.reverse
                && Objects.equals(srcField, that.srcField)
                && Objects.equals(dstField, that.dstField)
                && Objects.equals(srcVertex, that.srcVertex)
                && Objects.equals(dstVertex, that.dstVertex)
                && Objects.equals(aggregates, that.aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcField, dstField, srcVertex, dstVertex, reverse, aggregates);
    }

    @Override
    public String toString() {
        return "GraphEdgeIndexDecl{srcField='"
                + srcField
                + "', dstField='"
                + dstField
                + "', srcVertex='"
                + srcVertex
                + "', dstVertex='"
                + dstVertex
                + "', reverse="
                + reverse
                + ", aggregates="
                + aggregates
                + "}";
    }
}
