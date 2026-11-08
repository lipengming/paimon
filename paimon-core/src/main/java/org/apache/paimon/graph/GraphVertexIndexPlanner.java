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
import org.apache.paimon.options.GraphIndexOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.graph.GraphVertexIndexDecl.FAMILY_BITMAP;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Versioned planner resolving the vertex index backend and expanding the node-map declaration into
 * physical column-level index options.
 *
 * <p>Backend resolution follows the design's pk-index row: 'auto' picks the pk-index backend when
 * the official primary-key index requirements are met (fixed or postpone bucket mode, deletion
 * vectors enabled with merge-on-read disabled, no pk-clustering override) and degrades to the
 * standalone table backend otherwise; an explicit 'pk-index' backend with unmet requirements fails
 * loudly instead of degrading.
 *
 * <p>For the pk-index backend there is no separate index table: each node-map field expands into
 * the official {@code global-index.<family>.index-columns} key on the vertex table itself
 * (btree/bitmap families), merged with any existing declarations. A node-map field must not be
 * indexed by another primary-key index family already.
 *
 * <p>Creating the physical indexes (applying the options plus the initial full compaction) and the
 * standalone '{base}__node_map' tables of the table backend is the create pipeline's job.
 */
public final class GraphVertexIndexPlanner {

    private GraphVertexIndexPlanner() {}

    /**
     * Resolves the vertex index backend of the base schema and expands the node-map declaration.
     * The declaration is expected to be schema-validated already ({@link
     * org.apache.paimon.schema.SchemaValidation}).
     */
    public static GraphVertexIndexPlan plan(String baseTableName, TableSchema baseSchema) {
        GraphVertexIndexDecl vertex = GraphIndexDecls.from(baseSchema).vertex().orElse(null);
        checkArgument(
                vertex != null,
                "Table '%s' is not declared as a graph vertex: option 'graph.vertex.enabled' is"
                        + " not set.",
                baseTableName);

        Options options = new Options(baseSchema.options());
        String requested = options.get(GraphIndexOptions.GRAPH_INDEX_BACKEND);
        List<String> unmet = unmetPkIndexRequirements(baseSchema, options);

        String backend;
        if (GraphIndexOptions.BACKEND_PK_INDEX.equals(requested)) {
            checkArgument(
                    unmet.isEmpty(),
                    "Option 'graph.index.backend' is 'pk-index', but table '%s' does not meet the"
                            + " primary-key index requirements: %s",
                    baseTableName,
                    String.join("; ", unmet));
            backend = GraphIndexOptions.BACKEND_PK_INDEX;
        } else if (GraphIndexOptions.BACKEND_AUTO.equals(requested)) {
            backend =
                    unmet.isEmpty()
                            ? GraphIndexOptions.BACKEND_PK_INDEX
                            : GraphIndexOptions.BACKEND_TABLE;
        } else {
            backend = GraphIndexOptions.BACKEND_TABLE;
        }

        Map<String, String> indexOptions =
                GraphIndexOptions.BACKEND_PK_INDEX.equals(backend)
                        ? expandIndexOptions(baseTableName, vertex, options)
                        : new LinkedHashMap<String, String>();
        return new GraphVertexIndexPlan(baseTableName, requested, backend, unmet, indexOptions);
    }

    /**
     * The unmet primary-key index requirements of the vertex table (official Requirements of the
     * btree/bitmap index families).
     */
    private static List<String> unmetPkIndexRequirements(TableSchema schema, Options options) {
        List<String> unmet = new ArrayList<>();
        int bucket = schema.numBuckets();
        if (bucket <= 0 && bucket != -2) {
            unmet.add(
                    String.format(
                            "Bucket mode must be fixed ('bucket' > 0) or postpone ('bucket' = -2)"
                                    + " for the pk-index backend, but got 'bucket' = %s.",
                            bucket));
        }
        if (!options.get(CoreOptions.DELETION_VECTORS_ENABLED)) {
            unmet.add("Option 'deletion-vectors.enabled' must be true for the pk-index backend.");
        }
        if (options.get(CoreOptions.DELETION_VECTORS_MERGE_ON_READ)) {
            unmet.add(
                    "Option 'deletion-vectors.merge-on-read' must be false for the pk-index"
                            + " backend.");
        }
        if (options.get(CoreOptions.PK_CLUSTERING_OVERRIDE)) {
            unmet.add("Option 'pk-clustering-override' must be false for the pk-index backend.");
        }
        return unmet;
    }

    /** Expands node-map fields into {@code global-index.<family>.index-columns} option values. */
    private static Map<String, String> expandIndexOptions(
            String baseTableName, GraphVertexIndexDecl vertex, Options options) {
        String btreeKey = CoreOptions.GLOBAL_INDEX_BTREE_INDEX_COLUMNS.key();
        String bitmapKey = CoreOptions.GLOBAL_INDEX_BITMAP_INDEX_COLUMNS.key();
        List<String> existingBtree =
                splitColumns(options.get(CoreOptions.GLOBAL_INDEX_BTREE_INDEX_COLUMNS), btreeKey);
        List<String> existingBitmap =
                splitColumns(options.get(CoreOptions.GLOBAL_INDEX_BITMAP_INDEX_COLUMNS), bitmapKey);

        List<String> btree = new ArrayList<>(existingBtree);
        List<String> bitmap = new ArrayList<>(existingBitmap);
        for (GraphVertexIndexDecl.NodeMapField nodeMapField : vertex.nodeMapFields()) {
            String declaredIn = null;
            if (existingBtree.contains(nodeMapField.field())) {
                declaredIn = btreeKey;
            } else if (existingBitmap.contains(nodeMapField.field())) {
                declaredIn = bitmapKey;
            }
            checkArgument(
                    declaredIn == null,
                    "Column '%s' in 'graph.vertex.node-map.fields' is already declared in option"
                            + " '%s' of table '%s'.",
                    nodeMapField.field(),
                    declaredIn,
                    baseTableName);
            (FAMILY_BITMAP.equals(nodeMapField.family()) ? bitmap : btree)
                    .add(nodeMapField.field());
        }

        Map<String, String> result = new LinkedHashMap<>();
        if (!btree.isEmpty()) {
            result.put(btreeKey, String.join(",", btree));
        }
        if (!bitmap.isEmpty()) {
            result.put(bitmapKey, String.join(",", bitmap));
        }
        return result;
    }

    private static List<String> splitColumns(String raw, String key) {
        List<String> columns = new ArrayList<>();
        if (raw == null) {
            return columns;
        }
        for (String token : raw.split(",", -1)) {
            String column = token.trim();
            checkArgument(
                    !column.isEmpty(),
                    "Option '%s' must not contain empty column names, but was '%s'.",
                    key,
                    raw);
            columns.add(column);
        }
        return columns;
    }
}
