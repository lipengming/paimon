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

import org.apache.paimon.schema.TableSchema;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Expansion result of {@link GraphEdgeIndexPlanner}: the target {@link TableSchema} of the forward
 * adjacency index table (base table name plus the {@code __edge} suffix) and, when reverse
 * adjacency is declared, of the reverse one (suffix {@code __edge_rev}).
 *
 * <p>The schemas are expansion blueprints only; creating and backfilling the physical tables is the
 * job of the graph index create pipeline.
 */
public class GraphEdgeIndexPlan {

    private final String baseTableName;
    private final TableSchema edge;
    private final @Nullable TableSchema edgeRev;

    public GraphEdgeIndexPlan(
            String baseTableName, TableSchema edge, @Nullable TableSchema edgeRev) {
        this.baseTableName = baseTableName;
        this.edge = edge;
        this.edgeRev = edgeRev;
    }

    public String baseTableName() {
        return baseTableName;
    }

    public String edgeTableName() {
        return GraphEdgeIndexPlanner.edgeTableName(baseTableName);
    }

    /** Name of the reverse adjacency index table; only meaningful when {@link #reverse()}. */
    public String edgeRevTableName() {
        return GraphEdgeIndexPlanner.edgeRevTableName(baseTableName);
    }

    /** Whether a reverse adjacency index table is part of the plan. */
    public boolean reverse() {
        return edgeRev != null;
    }

    public TableSchema edge() {
        return edge;
    }

    /** The reverse adjacency index table schema, null when reverse adjacency is not declared. */
    @Nullable
    public TableSchema edgeRev() {
        return edgeRev;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphEdgeIndexPlan)) {
            return false;
        }
        GraphEdgeIndexPlan that = (GraphEdgeIndexPlan) o;
        return Objects.equals(baseTableName, that.baseTableName)
                && Objects.equals(edge, that.edge)
                && Objects.equals(edgeRev, that.edgeRev);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseTableName, edge, edgeRev);
    }

    @Override
    public String toString() {
        return "GraphEdgeIndexPlan{baseTableName='"
                + baseTableName
                + "', edge="
                + edge
                + ", edgeRev="
                + edgeRev
                + "}";
    }
}
