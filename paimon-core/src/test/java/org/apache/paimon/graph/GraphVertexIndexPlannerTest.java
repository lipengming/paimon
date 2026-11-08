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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link GraphVertexIndexPlanner}: the prerequisite matrix (bucket mode / deletion vectors
 * / clustering override crossed with the three 'graph.index.backend' values) and the
 * 'global-index.*' expansion.
 */
class GraphVertexIndexPlannerTest {

    private static final String DEFAULT_NODE_MAP_FIELDS = "name,phone,city:bitmap";

    // --------------------------------------------------------------------------------------------
    // Prerequisite matrix x backend values
    // --------------------------------------------------------------------------------------------

    @Test
    void testExplicitPkIndexBackendMeetsPrerequisites() {
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person",
                        vertexSchema(DEFAULT_NODE_MAP_FIELDS, metPrerequisites("pk-index")));

        assertThat(plan.requestedBackend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(plan.degraded()).isFalse();
        assertThat(plan.unmetPkIndexRequirements()).isEmpty();
        assertThat(plan.indexOptions())
                .containsEntry("global-index.btree.index-columns", "name,phone")
                .containsEntry("global-index.bitmap.index-columns", "city")
                .hasSize(2);
    }

    @Test
    void testAutoResolvesPkIndexWhenPrerequisitesMet() {
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, metPrerequisites(null)));

        assertThat(plan.requestedBackend()).isEqualTo(GraphIndexOptions.BACKEND_AUTO);
        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(plan.degraded()).isFalse();
    }

    @Test
    void testPostponeBucketAccepted() {
        Map<String, String> extra = new HashMap<>();
        extra.put("bucket", "-2");
        extra.put("deletion-vectors.enabled", "true");
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra));

        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(plan.unmetPkIndexRequirements()).isEmpty();
    }

    @Test
    void testAutoDegradesToTableOnDynamicBucket() {
        Map<String, String> extra = new HashMap<>();
        extra.put("deletion-vectors.enabled", "true");
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra));

        assertThat(plan.requestedBackend()).isEqualTo(GraphIndexOptions.BACKEND_AUTO);
        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_TABLE);
        assertThat(plan.degraded()).isTrue();
        assertThat(plan.unmetPkIndexRequirements())
                .anySatisfy(
                        requirement ->
                                assertThat(requirement)
                                        .contains(
                                                "Bucket mode must be fixed ('bucket' > 0) or postpone"
                                                        + " ('bucket' = -2)"))
                .hasSize(1);
        assertThat(plan.indexOptions()).isEmpty();
    }

    @Test
    void testAutoDegradesToTableWhenDeletionVectorsDisabled() {
        Map<String, String> extra = new HashMap<>();
        extra.put("bucket", "8");
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra));

        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_TABLE);
        assertThat(plan.degraded()).isTrue();
        assertThat(plan.unmetPkIndexRequirements())
                .anySatisfy(
                        requirement ->
                                assertThat(requirement)
                                        .contains("'deletion-vectors.enabled' must be true"))
                .hasSize(1);
    }

    @Test
    void testExplicitPkIndexRejectedWhenPrerequisitesUnmet() {
        Map<String, String> extra = new HashMap<>();
        extra.put("bucket", "8");
        extra.put("graph.index.backend", "pk-index");
        extra.put("pk-clustering-override", "true");
        assertThatThrownBy(
                        () ->
                                GraphVertexIndexPlanner.plan(
                                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Option 'graph.index.backend' is 'pk-index', but table 'person' does not"
                                + " meet the primary-key index requirements: Option"
                                + " 'deletion-vectors.enabled' must be true for the pk-index"
                                + " backend.; Option 'pk-clustering-override' must be false for the"
                                + " pk-index backend.");
    }

    @Test
    void testExplicitTableBackendIgnoresPrerequisites() {
        Map<String, String> extra = new HashMap<>();
        extra.put("graph.index.backend", "table");
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra));

        assertThat(plan.requestedBackend()).isEqualTo(GraphIndexOptions.BACKEND_TABLE);
        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_TABLE);
        assertThat(plan.degraded()).isFalse();
        assertThat(plan.unmetPkIndexRequirements()).isNotEmpty();
        assertThat(plan.indexOptions()).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // global-index expansion
    // --------------------------------------------------------------------------------------------

    @Test
    void testMergeWithExistingDeclarations() {
        Map<String, String> extra = metPrerequisites(null);
        extra.put("global-index.btree.index-columns", "age");
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan(
                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra));

        assertThat(plan.indexOptions())
                .containsEntry("global-index.btree.index-columns", "age,name,phone")
                .containsEntry("global-index.bitmap.index-columns", "city")
                .hasSize(2);
    }

    @Test
    void testRejectNodeMapFieldAlreadyIndexed() {
        Map<String, String> extra = metPrerequisites(null);
        extra.put("global-index.bitmap.index-columns", "name");
        assertThatThrownBy(
                        () ->
                                GraphVertexIndexPlanner.plan(
                                        "person", vertexSchema(DEFAULT_NODE_MAP_FIELDS, extra)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Column 'name' in 'graph.vertex.node-map.fields' is already declared in"
                                + " option 'global-index.bitmap.index-columns' of table 'person'.");
    }

    @Test
    void testNoNodeMapFields() {
        GraphVertexIndexPlan plan =
                GraphVertexIndexPlanner.plan("person", vertexSchema(null, metPrerequisites(null)));

        assertThat(plan.backend()).isEqualTo(GraphIndexOptions.BACKEND_PK_INDEX);
        assertThat(plan.indexOptions()).isEmpty();
    }

    @Test
    void testPlanRequiresVertexDeclaration() {
        TableSchema schema =
                new TableSchema(
                        0L,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.BIGINT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        1,
                        emptyList(),
                        singletonList("id"),
                        new HashMap<String, String>(),
                        null);
        assertThatThrownBy(() -> GraphVertexIndexPlanner.plan("person", schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Table 'person' is not declared as a graph vertex: option"
                                + " 'graph.vertex.enabled' is not set.");
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    /** Bucket 8 + deletion vectors; the backend option is only set for a non-null argument. */
    private Map<String, String> metPrerequisites(String backend) {
        Map<String, String> extra = new HashMap<>();
        extra.put("bucket", "8");
        extra.put("deletion-vectors.enabled", "true");
        if (backend != null) {
            extra.put(GraphIndexOptions.GRAPH_INDEX_BACKEND.key(), backend);
        }
        return extra;
    }

    private TableSchema vertexSchema(String nodeMapFields, Map<String, String> extraOptions) {
        Map<String, String> options = new HashMap<>();
        options.put(GraphIndexOptions.GRAPH_VERTEX_ENABLED.key(), "true");
        options.put(GraphIndexOptions.GRAPH_VERTEX_ID_FIELD.key(), "id");
        if (nodeMapFields != null) {
            options.put(GraphIndexOptions.GRAPH_VERTEX_NODE_MAP_FIELDS.key(), nodeMapFields);
        }
        options.putAll(extraOptions);

        return new TableSchema(
                0L,
                Arrays.asList(
                        new DataField(0, "id", DataTypes.BIGINT()),
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "phone", DataTypes.STRING()),
                        new DataField(3, "city", DataTypes.STRING()),
                        new DataField(4, "age", DataTypes.INT())),
                4,
                emptyList(),
                singletonList("id"),
                options,
                null);
    }
}
