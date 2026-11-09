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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.util.Optional;

import static org.apache.paimon.graph.sdk.GraphReads.fieldIndex;

/**
 * Point lookup of vertex rows by node id (design §8.1 ④): a primary-key equality read on the base
 * vertex table.
 */
public class VertexQuerier {

    private final FileStoreTable table;
    private final RowType rowType;
    private final String vertexName;
    private final String idField;
    private final SearchMode searchMode;

    VertexQuerier(FileStoreTable table, String vertexName, String idField, SearchMode searchMode) {
        this.table = table;
        this.rowType = new RowType(table.schema().fields());
        this.vertexName = vertexName;
        this.idField = idField;
        this.searchMode = searchMode;
    }

    /** Looks up one vertex by its node id. */
    public Optional<Vertex> lookup(long id) {
        requireSupported();
        int idIndex = fieldIndex(rowType, idField);
        Predicate filter = new PredicateBuilder(rowType).equal(idIndex, id);
        java.util.List<BinaryRow> rows = GraphReads.readRows(table, filter, null);
        return rows.isEmpty()
                ? Optional.empty()
                : Optional.of(new Vertex(rows.get(0), rowType, idField));
    }

    /** Looks up one vertex by its global id; the vertex type must match this binding. */
    public Optional<Vertex> lookup(GraphVertexId id) {
        if (!id.vertexType().equals(vertexName)) {
            throw new IllegalArgumentException(
                    "GraphVertexId type mismatch: queried '"
                            + id.vertexType()
                            + "' but the vertex binding is '"
                            + vertexName
                            + "'.");
        }
        return lookup(id.id());
    }

    /**
     * The pk-index backend is snapshot-consistent, so FULL reads behave like FAST ones; DETAIL is
     * served by the reconciliation primitive instead of a single read.
     */
    private void requireSupported() {
        if (searchMode == SearchMode.DETAIL) {
            throw new UnsupportedOperationException(
                    "SearchMode DETAIL is the reconciliation mode; call GraphStore.reconcile(...)"
                            + " instead of a single read.");
        }
    }
}
