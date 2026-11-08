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

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.util.Objects;

/**
 * Global graph vertex identifier (design §4.7): the vertex type plus the vertex table's primary key
 * value. Storage layers keep raw ids; the composite form exists only at the SDK layer.
 */
@Public
public final class GraphVertexId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String vertexType;
    private final long id;

    public GraphVertexId(String vertexType, long id) {
        this.vertexType = vertexType;
        this.id = id;
    }

    public String vertexType() {
        return vertexType;
    }

    public long id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GraphVertexId)) {
            return false;
        }
        GraphVertexId that = (GraphVertexId) o;
        return id == that.id && Objects.equals(vertexType, that.vertexType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexType, id);
    }

    @Override
    public String toString() {
        return "(" + vertexType + ", " + id + ")";
    }
}
