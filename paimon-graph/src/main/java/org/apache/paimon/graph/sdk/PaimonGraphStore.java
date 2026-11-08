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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.graph.GraphEdgeIndexDecl;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.graph.GraphIndexDecls;
import org.apache.paimon.graph.GraphVertexIndexDecl;
import org.apache.paimon.graph.GraphVertexIndexPlanner;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypeRoot;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * The default {@link GraphStore} implementation: binds vertex and edge tables of one catalog at
 * build time (fail fast on missing declarations or index tables) and serves the read primitives
 * over public APIs only.
 */
public final class PaimonGraphStore implements GraphStore {

    private final Map<String, VertexBinding> vertices;
    private final Map<String, EdgeBinding> edges;
    private final SearchMode searchMode;

    private PaimonGraphStore(
            Map<String, VertexBinding> vertices,
            Map<String, EdgeBinding> edges,
            SearchMode searchMode) {
        this.vertices = vertices;
        this.edges = edges;
        this.searchMode = searchMode;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public NodeMapQuery nodeMap(String vertex, String field) {
        VertexBinding binding = vertices.get(vertex);
        checkArgument(binding != null, "Vertex '%s' is not bound to this graph store.", vertex);
        checkArgument(
                binding.decl.nodeMapFields().stream()
                        .anyMatch(nodeMapField -> nodeMapField.field().equals(field)),
                "Field '%s' is not declared in 'graph.vertex.node-map.fields' of vertex '%s'.",
                field,
                vertex);
        checkArgument(
                !GraphIndexOptions.UPDATE_ACTION_FREEZE.equals(binding.decl.updateAction()),
                "Node-map of vertex '%s' is frozen ('graph.vertex.node-map.update-action' ="
                        + " 'freeze'); queries are not routed to it.",
                vertex);
        checkArgument(
                GraphIndexOptions.BACKEND_PK_INDEX.equals(binding.backend),
                "Node-map scan of vertex '%s' uses the table backend, whose '%s__node_map'"
                        + " creation and maintenance arrive with the node-map create pipeline;"
                        + " use the pk-index backend for now.",
                vertex,
                vertex);
        return new NodeMapQuery(binding.table, field, binding.decl.idField(), searchMode);
    }

    @Override
    public AdjQuery adj(String edge, Direction direction) {
        EdgeBinding binding = edges.get(edge);
        checkArgument(binding != null, "Edge '%s' is not bound to this graph store.", edge);
        if (direction == Direction.IN) {
            checkArgument(
                    binding.reverse != null,
                    "Edge '%s' does not declare reverse adjacency ('graph.edge.reverse' ="
                            + " 'false'); IN scans are unavailable.",
                    edge);
            return new AdjQuery(
                    binding.reverse,
                    edge,
                    binding.decl.dstField(),
                    binding.decl.srcField(),
                    searchMode);
        }
        return new AdjQuery(
                binding.forward,
                edge,
                binding.decl.srcField(),
                binding.decl.dstField(),
                searchMode);
    }

    @Override
    public VertexQuerier vertex(String vertex) {
        VertexBinding binding = vertices.get(vertex);
        checkArgument(binding != null, "Vertex '%s' is not bound to this graph store.", vertex);
        return new VertexQuerier(binding.table, vertex, binding.decl.idField(), searchMode);
    }

    @Override
    public void close() {
        // stateless facade: readers are closed by the iterators' try-with-resources
    }

    /** Builder of {@link PaimonGraphStore}. */
    public static final class Builder {

        @Nullable private Catalog catalog;
        @Nullable private String defaultDatabase;
        private SearchMode searchMode = SearchMode.FAST;
        private final Map<String, String> vertexNames = new LinkedHashMap<>();
        private final Map<String, String[]> edgeBindings = new LinkedHashMap<>();

        public Builder catalog(Catalog catalog) {
            this.catalog = catalog;
            return this;
        }

        /**
         * Database used to resolve table names without a database qualifier. Table names of the
         * form {@code db.table} are always resolved literally.
         */
        public Builder defaultDatabase(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
            return this;
        }

        public Builder vertex(String name) {
            vertexNames.put(name, name);
            return this;
        }

        public Builder edge(String table, String srcVertex, String dstVertex) {
            edgeBindings.put(table, new String[] {srcVertex, dstVertex});
            return this;
        }

        public Builder searchMode(SearchMode searchMode) {
            this.searchMode = searchMode;
            return this;
        }

        public GraphStore build() {
            checkArgument(catalog != null, "catalog(...) must be set.");
            Map<String, VertexBinding> vertices = new LinkedHashMap<>();
            for (String name : vertexNames.keySet()) {
                FileStoreTable table = loadVertexTable(name);
                GraphIndexDecls decls = GraphIndexDecls.from(table.schema());
                GraphVertexIndexDecl vertex =
                        decls.vertex()
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Table '"
                                                                + name
                                                                + "' is not declared as a graph"
                                                                + " vertex"
                                                                + " ('graph.vertex.enabled')."));
                String backend =
                        GraphVertexIndexPlanner.plan(
                                        identifier(name).getTableName(), table.schema())
                                .backend();
                vertices.put(name, new VertexBinding(table, vertex, backend));
            }

            Map<String, EdgeBinding> edges = new LinkedHashMap<>();
            for (Map.Entry<String, String[]> binding : edgeBindings.entrySet()) {
                String table = binding.getKey();
                String srcVertex = binding.getValue()[0];
                String dstVertex = binding.getValue()[1];
                FileStoreTable base = loadEdgeTable(table);
                GraphEdgeIndexDecl decl =
                        GraphIndexDecls.from(base.schema())
                                .edge()
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Table '"
                                                                + table
                                                                + "' is not declared as a graph"
                                                                + " edge"
                                                                + " ('graph.edge.enabled')."));
                checkArgument(
                        decl.srcVertex().equals(srcVertex) && decl.dstVertex().equals(dstVertex),
                        "Edge binding of '%s' declares vertex types (%s, %s), but the store"
                                + " binds (%s, %s).",
                        table,
                        decl.srcVertex(),
                        decl.dstVertex(),
                        srcVertex,
                        dstVertex);
                validateIntegerId(base.schema(), decl.srcField(), table);
                validateIntegerId(base.schema(), decl.dstField(), table);
                edges.put(
                        table,
                        new EdgeBinding(
                                base,
                                decl,
                                loadIndexTable(table, GraphEdgeIndexPlanner.edgeTableName(table)),
                                decl.reverse()
                                        ? loadIndexTable(
                                                table,
                                                GraphEdgeIndexPlanner.edgeRevTableName(table))
                                        : null));
            }
            return new PaimonGraphStore(vertices, edges, searchMode);
        }

        private FileStoreTable loadVertexTable(String name) {
            return loadTable(name, "vertex");
        }

        private FileStoreTable loadEdgeTable(String name) {
            return loadTable(name, "edge");
        }

        private FileStoreTable loadTable(String name, String kind) {
            Identifier identifier = identifier(name);
            try {
                return (FileStoreTable) catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new IllegalArgumentException(
                        "Bound " + kind + " table '" + name + "' does not exist in the catalog.",
                        e);
            }
        }

        private FileStoreTable loadIndexTable(String baseName, String indexTableName) {
            Identifier identifier = identifier(indexTableName);
            try {
                return (FileStoreTable) catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new IllegalArgumentException(
                        "Adjacency index table '"
                                + indexTableName
                                + "' of edge '"
                                + baseName
                                + "' does not exist; run the graph index create pipeline first.",
                        e);
            }
        }

        private Identifier identifier(String name) {
            int dot = name.indexOf('.');
            if (dot > 0) {
                return Identifier.create(name.substring(0, dot), name.substring(dot + 1));
            }
            checkArgument(
                    defaultDatabase != null,
                    "Table name '%s' has no database qualifier and defaultDatabase(...) is not"
                            + " set.",
                    name);
            return Identifier.create(defaultDatabase, name);
        }

        private void validateIntegerId(TableSchema schema, String field, String table) {
            DataTypeRoot root =
                    schema.fields().stream()
                            .filter(f -> f.name().equals(field))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Column '"
                                                            + field
                                                            + "' of edge '"
                                                            + table
                                                            + "' does not exist."))
                            .type()
                            .getTypeRoot();
            checkArgument(
                    root == DataTypeRoot.BIGINT,
                    "The traversal SDK v1 supports BIGINT node ids only, but the endpoint field"
                            + " '%s' of edge '%s' is %s.",
                    field,
                    table,
                    root);
        }
    }

    private static final class VertexBinding {
        private final FileStoreTable table;
        private final GraphVertexIndexDecl decl;
        private final String backend;

        private VertexBinding(FileStoreTable table, GraphVertexIndexDecl decl, String backend) {
            this.table = table;
            this.decl = decl;
            this.backend = backend;
        }
    }

    private static final class EdgeBinding {
        private final FileStoreTable base;
        private final GraphEdgeIndexDecl decl;
        private final FileStoreTable forward;
        @Nullable private final FileStoreTable reverse;

        private EdgeBinding(
                FileStoreTable base,
                GraphEdgeIndexDecl decl,
                FileStoreTable forward,
                @Nullable FileStoreTable reverse) {
            this.base = base;
            this.decl = decl;
            this.forward = forward;
            this.reverse = reverse;
        }
    }
}
