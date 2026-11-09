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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One breadth-first path discovered by {@link GraphStore#kHop}: the vertex ids from the start
 * vertex to the reached vertex (design §8.1 ⑥). Cycles are broken by a global visited set, so the
 * returned paths form a forest rooted at the start vertex.
 */
@Public
public final class GraphPath {

    private final List<Long> vertices;

    GraphPath(List<Long> vertices) {
        this.vertices = Collections.unmodifiableList(new ArrayList<>(vertices));
    }

    static GraphPath of(long vertex) {
        return new GraphPath(Collections.singletonList(vertex));
    }

    GraphPath append(long vertex) {
        List<Long> extended = new ArrayList<>(vertices);
        extended.add(vertex);
        return new GraphPath(extended);
    }

    /** The vertex ids of the path, from the start vertex to the reached vertex. */
    public List<Long> vertices() {
        return vertices;
    }

    public long start() {
        return vertices.get(0);
    }

    public long end() {
        return vertices.get(vertices.size() - 1);
    }

    /** Number of edges of the path; one for a direct neighbour. */
    public int length() {
        return vertices.size() - 1;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < vertices.size(); i++) {
            if (i > 0) {
                builder.append(" -> ");
            }
            builder.append(vertices.get(i));
        }
        return builder.toString();
    }
}
