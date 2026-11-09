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

import java.util.Objects;

/**
 * Input of the graph index management API (design §5.2/§5.3): either a vertex {@code node_map}
 * binding with optional field list, or an edge {@code adjacency} binding with the endpoint and
 * aggregate declaration.
 *
 * <p>The declaration is the source of truth. When a field/aggregate argument is given, the
 * management operation writes it into the {@code graph.*} table options first and then runs the
 * versioned expansion; when it is omitted, the existing declaration is used as is.
 */
@Public
public final class GraphIndexSpec {

    /** The graph index type. */
    public enum Type {
        NODE_MAP("node_map"),
        ADJACENCY("adjacency");

        private final String value;

        Type(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static Type fromString(String value) {
            for (Type type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(
                    "Unknown graph index type '"
                            + value
                            + "', expected 'node_map' or 'adjacency'.");
        }
    }

    private final Type type;
    @Nullable private final String fields;
    @Nullable private final String srcField;
    @Nullable private final String dstField;
    @Nullable private final String srcVertex;
    @Nullable private final String dstVertex;
    @Nullable private final String aggregates;
    @Nullable private final Boolean reverse;

    private GraphIndexSpec(
            Type type,
            @Nullable String fields,
            @Nullable String srcField,
            @Nullable String dstField,
            @Nullable String srcVertex,
            @Nullable String dstVertex,
            @Nullable String aggregates,
            @Nullable Boolean reverse) {
        this.type = type;
        this.fields = fields;
        this.srcField = srcField;
        this.dstField = dstField;
        this.srcVertex = srcVertex;
        this.dstVertex = dstVertex;
        this.aggregates = aggregates;
        this.reverse = reverse;
    }

    public static Builder vertex() {
        return new Builder(Type.NODE_MAP);
    }

    public static Builder edge() {
        return new Builder(Type.ADJACENCY);
    }

    public Type type() {
        return type;
    }

    @Nullable
    public String fields() {
        return fields;
    }

    @Nullable
    public String srcField() {
        return srcField;
    }

    @Nullable
    public String dstField() {
        return dstField;
    }

    @Nullable
    public String srcVertex() {
        return srcVertex;
    }

    @Nullable
    public String dstVertex() {
        return dstVertex;
    }

    @Nullable
    public String aggregates() {
        return aggregates;
    }

    @Nullable
    public Boolean reverse() {
        return reverse;
    }

    @Override
    public String toString() {
        return "GraphIndexSpec{type="
                + type.value()
                + ", fields="
                + fields
                + ", srcField="
                + srcField
                + ", dstField="
                + dstField
                + ", srcVertex="
                + srcVertex
                + ", dstVertex="
                + dstVertex
                + ", aggregates="
                + aggregates
                + ", reverse="
                + reverse
                + '}';
    }

    /** Builder of {@link GraphIndexSpec}. */
    public static final class Builder {

        private final Type type;
        @Nullable private String fields;
        @Nullable private String srcField;
        @Nullable private String dstField;
        @Nullable private String srcVertex;
        @Nullable private String dstVertex;
        @Nullable private String aggregates;
        @Nullable private Boolean reverse;

        private Builder(Type type) {
            this.type = type;
        }

        /** Node-map fields: {@code field} or {@code field:family}, comma separated. */
        public Builder fields(String fields) {
            this.fields = fields;
            return this;
        }

        public Builder srcField(String srcField) {
            this.srcField = srcField;
            return this;
        }

        public Builder dstField(String dstField) {
            this.dstField = dstField;
            return this;
        }

        public Builder srcVertex(String srcVertex) {
            this.srcVertex = srcVertex;
            return this;
        }

        public Builder dstVertex(String dstVertex) {
            this.dstVertex = dstVertex;
            return this;
        }

        /** Aggregate declaration: {@code field:function[:alias];...}. */
        public Builder aggregates(String aggregates) {
            this.aggregates = aggregates;
            return this;
        }

        public Builder reverse(boolean reverse) {
            this.reverse = reverse;
            return this;
        }

        public GraphIndexSpec build() {
            if (type == Type.ADJACENCY) {
                boolean anyEndpoint =
                        srcField != null
                                || dstField != null
                                || srcVertex != null
                                || dstVertex != null
                                || aggregates != null
                                || reverse != null;
                if (anyEndpoint) {
                    Objects.requireNonNull(srcField, "srcField must be set for an adjacency spec.");
                    Objects.requireNonNull(dstField, "dstField must be set for an adjacency spec.");
                }
            }
            return new GraphIndexSpec(
                    type, fields, srcField, dstField, srcVertex, dstVertex, aggregates, reverse);
        }
    }
}
