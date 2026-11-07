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
 * Parsed graph vertex declaration from the 'graph.vertex.*' table options.
 *
 * <p>The declaration is pure graph semantics: it says WHAT is bound (node id field, reverse index
 * attributes, update policy), not HOW it is physically realized. Physical expansion is done by the
 * versioned graph index planner.
 */
@Public
public class GraphVertexIndexDecl implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Node-map family for high-cardinality, low-churn attributes (range capable). */
    public static final String FAMILY_BTREE = "btree";

    /** Node-map family for enum-like, low-cardinality attributes. */
    public static final String FAMILY_BITMAP = "bitmap";

    private final String idField;
    private final List<NodeMapField> nodeMapFields;
    private final String encoding;
    private final String updateAction;

    public GraphVertexIndexDecl(
            String idField,
            List<NodeMapField> nodeMapFields,
            String encoding,
            String updateAction) {
        this.idField = idField;
        this.nodeMapFields = Collections.unmodifiableList(nodeMapFields);
        this.encoding = encoding;
        this.updateAction = updateAction;
    }

    /** The node id field, which must be the single-column primary key of the vertex table. */
    public String idField() {
        return idField;
    }

    /** The attributes to build node-map reverse indexes for. */
    public List<NodeMapField> nodeMapFields() {
        return nodeMapFields;
    }

    /** The property value encoding of node-map index entries. */
    public String encoding() {
        return encoding;
    }

    /** The update policy of node-map indexes: maintain or freeze. */
    public String updateAction() {
        return updateAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphVertexIndexDecl)) {
            return false;
        }
        GraphVertexIndexDecl that = (GraphVertexIndexDecl) o;
        return Objects.equals(idField, that.idField)
                && Objects.equals(nodeMapFields, that.nodeMapFields)
                && Objects.equals(encoding, that.encoding)
                && Objects.equals(updateAction, that.updateAction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idField, nodeMapFields, encoding, updateAction);
    }

    @Override
    public String toString() {
        return "GraphVertexIndexDecl{idField='"
                + idField
                + "', nodeMapFields="
                + nodeMapFields
                + ", encoding='"
                + encoding
                + "', updateAction='"
                + updateAction
                + "'}";
    }

    /** One node-map reverse index entry: an attribute plus its index family. */
    @Public
    public static class NodeMapField implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String field;
        private final String family;

        public NodeMapField(String field, String family) {
            this.field = field;
            this.family = family;
        }

        public String field() {
            return field;
        }

        public String family() {
            return family;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof NodeMapField)) {
                return false;
            }
            NodeMapField that = (NodeMapField) o;
            return Objects.equals(field, that.field) && Objects.equals(family, that.family);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, family);
        }

        @Override
        public String toString() {
            return field + ":" + family;
        }
    }
}
