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
import org.apache.paimon.graph.GraphEdgeAggregate;
import org.apache.paimon.graph.GraphEdgeIndexDecl;
import org.apache.paimon.graph.GraphEdgeIndexPlanner;
import org.apache.paimon.graph.GraphEdgeIndexReconciler;
import org.apache.paimon.graph.GraphIndexDecls;
import org.apache.paimon.graph.GraphVertexIndexDecl;
import org.apache.paimon.graph.GraphVertexIndexPlanner;
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * The default {@link GraphStore} implementation: binds vertex and edge tables of one catalog at
 * build time (fail fast on missing declarations or index tables) and serves the read primitives
 * over public APIs only.
 */
public final class PaimonGraphStore implements GraphStore {

    private static final int MAX_HOPS = 5;

    private final Catalog catalog;
    private final Map<String, VertexBinding> vertices;
    private final Map<String, EdgeBinding> edges;
    private final SearchMode searchMode;

    private PaimonGraphStore(
            Catalog catalog,
            Map<String, VertexBinding> vertices,
            Map<String, EdgeBinding> edges,
            SearchMode searchMode) {
        this.catalog = catalog;
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
                    searchMode,
                    binding.base,
                    binding.base.schema(),
                    binding.decl,
                    true);
        }
        return new AdjQuery(
                binding.forward,
                edge,
                binding.decl.srcField(),
                binding.decl.dstField(),
                searchMode,
                binding.base,
                binding.base.schema(),
                binding.decl,
                false);
    }

    @Override
    public VertexQuerier vertex(String vertex) {
        VertexBinding binding = vertices.get(vertex);
        checkArgument(binding != null, "Vertex '%s' is not bound to this graph store.", vertex);
        return new VertexQuerier(binding.table, vertex, binding.decl.idField(), searchMode);
    }

    @Override
    public List<GraphEdgeIndexReconciler.IndexReconcileResult> reconcile(String edge) {
        EdgeBinding binding = edge(edge);
        try {
            return GraphEdgeIndexReconciler.reconcile(catalog, binding.identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new IllegalArgumentException(
                    "Base table of edge '" + edge + "' does not exist in the catalog.", e);
        }
    }

    @Override
    public List<Path2> twoHop(
            String edge,
            Direction direction,
            long src,
            @Nullable EdgeFilter filter,
            DegreeCap degreeCap,
            ResultCap resultCap) {
        EdgeBinding binding = edge(edge);
        List<AdjRow> firstHop = readAdjacencyRows(binding, edge, direction, src, Integer.MAX_VALUE);
        if (filter != null) {
            List<AdjRow> filtered = new ArrayList<>();
            for (AdjRow row : firstHop) {
                if (filter.testValues(row.values)) {
                    filtered.add(row);
                }
            }
            firstHop = filtered;
        }

        // rank the first hop by the filter column, or by the edge count when no filter is given,
        // then expand only the top-N intermediate vertices (design §8.3)
        String weight = filter != null ? filter.column() : numericWeightColumn(binding);
        if (weight != null) {
            final String weightColumn = weight;
            Collections.sort(firstHop, (left, right) -> comparePayload(right, left, weightColumn));
        }

        List<Path2> paths = new ArrayList<>();
        int topN = Math.min(degreeCap.value(), firstHop.size());
        for (int i = 0; i < topN; i++) {
            AdjRow mid = firstHop.get(i);
            List<AdjRow> secondHop =
                    readAdjacencyRows(binding, edge, direction, mid.dst, degreeCap.value());
            for (AdjRow row : secondHop) {
                paths.add(new Path2(src, mid.dst, row.dst, mid.values, row.values));
                if (paths.size() > resultCap.value()) {
                    throw new GraphCapExceededException(
                            String.format(
                                    "Two-hop traversal of '%s' from vertex %d exceeded resultCap=%d"
                                            + " paths; narrow the filter or raise resultCap.",
                                    edge, src, resultCap.value()));
                }
            }
        }
        return paths;
    }

    @Override
    public List<GraphPath> kHop(
            String edge,
            Direction direction,
            long src,
            int hops,
            DegreeCap degreeCap,
            ResultCap resultCap) {
        checkArgument(hops >= 1, "hops must be at least 1, but got %s.", hops);
        checkArgument(
                hops <= MAX_HOPS,
                "kHop supports at most %s hops, but got %s; deeper traversals belong on the batch"
                        + " engines (design §8.3).",
                MAX_HOPS,
                hops);
        EdgeBinding binding = edge(edge);

        RoaringNavigableMap64 visited = new RoaringNavigableMap64();
        visited.add(src);
        List<GraphPath> discovered = new ArrayList<>();
        List<GraphPath> frontier = Collections.singletonList(GraphPath.of(src));
        for (int depth = 0; depth < hops && !frontier.isEmpty(); depth++) {
            List<GraphPath> next = new ArrayList<>();
            for (GraphPath path : frontier) {
                for (long neighbour :
                        readDestinations(binding, direction, path.end(), degreeCap.value())) {
                    // the global visited set turns the BFS into a forest and bounds cycles
                    if (visited.contains(neighbour)) {
                        continue;
                    }
                    visited.add(neighbour);
                    GraphPath extended = path.append(neighbour);
                    next.add(extended);
                    discovered.add(extended);
                    if (discovered.size() > resultCap.value()) {
                        throw new GraphCapExceededException(
                                String.format(
                                        "K-hop traversal of '%s' from vertex %d exceeded"
                                                + " resultCap=%d paths; raise resultCap or lower"
                                                + " hops.",
                                        edge, src, resultCap.value()));
                    }
                }
            }
            frontier = next;
        }
        return discovered;
    }

    private EdgeBinding edge(String edge) {
        EdgeBinding binding = edges.get(edge);
        checkArgument(binding != null, "Edge '%s' is not bound to this graph store.", edge);
        return binding;
    }

    private List<AdjRow> readAdjacencyRows(
            EdgeBinding binding, String edge, Direction direction, long from, int degreeCap) {
        AdjQuery query = adjacencyQuery(binding, edge, direction);
        query.from(from).degreeCap(degreeCap);
        List<String> columns = aggregateColumns(binding.decl);
        for (String column : columns) {
            query.project(column);
        }
        RowType indexRowType = new RowType(indexTable(binding, direction).schema().fields());
        List<AdjRow> rows = new ArrayList<>();
        try (AdjQuery it = query) {
            while (it.next()) {
                Map<String, Object> values = new LinkedHashMap<>();
                for (String column : columns) {
                    values.put(column, payloadValue(it, column, indexRowType));
                }
                rows.add(new AdjRow(it.dst(), new EdgeValues(values)));
            }
        }
        return rows;
    }

    private List<Long> readDestinations(
            EdgeBinding binding, Direction direction, long from, int degreeCap) {
        AdjQuery query = adjacencyQuery(binding, null, direction);
        query.from(from).degreeCap(degreeCap);
        List<Long> destinations = new ArrayList<>();
        try (AdjQuery it = query) {
            while (it.next()) {
                destinations.add(it.dst());
            }
        }
        return destinations;
    }

    private AdjQuery adjacencyQuery(
            EdgeBinding binding, @Nullable String edge, Direction direction) {
        FileStoreTable indexTable = indexTable(binding, direction);
        String prefixField;
        String dstField;
        if (direction == Direction.IN) {
            checkArgument(
                    binding.reverse != null,
                    "Edge '%s' does not declare reverse adjacency ('graph.edge.reverse' ="
                            + " 'false'); IN traversals are unavailable.",
                    edge);
            prefixField = binding.decl.dstField();
            dstField = binding.decl.srcField();
        } else {
            prefixField = binding.decl.srcField();
            dstField = binding.decl.dstField();
        }
        return new AdjQuery(indexTable, edge, prefixField, dstField, searchMode);
    }

    private static FileStoreTable indexTable(EdgeBinding binding, Direction direction) {
        return direction == Direction.IN ? binding.reverse : binding.forward;
    }

    private static List<String> aggregateColumns(GraphEdgeIndexDecl decl) {
        List<String> columns = new ArrayList<>();
        for (GraphEdgeAggregate aggregate : decl.aggregates()) {
            columns.add(GraphEdgeIndexPlanner.aggregateColumn(aggregate));
        }
        return columns;
    }

    private static Object payloadValue(AdjQuery row, String column, RowType indexRowType) {
        DataType type = fieldType(indexRowType, column);
        switch (type.getTypeRoot()) {
            case BIGINT:
            case INTEGER:
                return row.getLong(column);
            case DECIMAL:
                return row.getDecimal(column);
            case VARCHAR:
            case CHAR:
                return row.getString(column);
            default:
                throw new IllegalArgumentException(
                        "Adjacency payload column '"
                                + column
                                + "' of type "
                                + type
                                + " is not readable by the traversal SDK (supported: BIGINT, INT,"
                                + " DECIMAL, STRING).");
        }
    }

    private static DataType fieldType(RowType rowType, String column) {
        for (org.apache.paimon.types.DataField field : rowType.getFields()) {
            if (field.name().equals(column)) {
                return field.type();
            }
        }
        throw new IllegalArgumentException(
                "Column '" + column + "' does not exist in the adjacency schema " + rowType + ".");
    }

    @Nullable
    private static String numericWeightColumn(EdgeBinding binding) {
        RowType rowType = new RowType(binding.forward.schema().fields());
        for (String column : aggregateColumns(binding.decl)) {
            DataTypeRoot root = fieldType(rowType, column).getTypeRoot();
            if (root == DataTypeRoot.BIGINT
                    || root == DataTypeRoot.INTEGER
                    || root == DataTypeRoot.DECIMAL) {
                return column;
            }
        }
        return null;
    }

    private static int comparePayload(AdjRow left, AdjRow right, String column) {
        Object leftValue = left.values.raw(column);
        Object rightValue = right.values.raw(column);
        if (leftValue == null || rightValue == null) {
            return leftValue == null ? (rightValue == null ? 0 : -1) : 1;
        }
        if (leftValue instanceof BigDecimal || rightValue instanceof BigDecimal) {
            return new BigDecimal(leftValue.toString())
                    .compareTo(new BigDecimal(rightValue.toString()));
        }
        return Long.compare(((Number) leftValue).longValue(), ((Number) rightValue).longValue());
    }

    /** One materialized adjacency row: destination id plus its aggregate payload. */
    private static final class AdjRow {

        private final long dst;
        private final EdgeValues values;

        private AdjRow(long dst, EdgeValues values) {
            this.dst = dst;
            this.values = values;
        }
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
                                catalog,
                                identifier(table),
                                base,
                                decl,
                                loadIndexTable(table, GraphEdgeIndexPlanner.edgeTableName(table)),
                                decl.reverse()
                                        ? loadIndexTable(
                                                table,
                                                GraphEdgeIndexPlanner.edgeRevTableName(table))
                                        : null));
            }
            return new PaimonGraphStore(catalog, vertices, edges, searchMode);
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
        private final Catalog catalog;
        private final Identifier identifier;
        private final FileStoreTable base;
        private final GraphEdgeIndexDecl decl;
        private final FileStoreTable forward;
        @Nullable private final FileStoreTable reverse;

        private EdgeBinding(
                Catalog catalog,
                Identifier identifier,
                FileStoreTable base,
                GraphEdgeIndexDecl decl,
                FileStoreTable forward,
                @Nullable FileStoreTable reverse) {
            this.catalog = catalog;
            this.identifier = identifier;
            this.base = base;
            this.decl = decl;
            this.forward = forward;
            this.reverse = reverse;
        }
    }
}
