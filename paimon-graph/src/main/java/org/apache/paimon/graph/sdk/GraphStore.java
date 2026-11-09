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

import javax.annotation.Nullable;

import java.util.List;

/**
 * Facade of the graph traversal SDK (design §8.1): binds vertex and edge tables of one catalog and
 * exposes the read primitives over their graph indexes.
 *
 * <pre>{@code
 * GraphStore store = PaimonGraphStore.builder()
 *         .catalog(catalog)
 *         .defaultDatabase("mydb")
 *         .vertex("person")
 *         .edge("transfer", "person", "person")
 *         .searchMode(SearchMode.FAST)
 *         .build();
 * }</pre>
 */
@Public
public interface GraphStore extends AutoCloseable {

    /** Attribute reverse lookup over one node-map field of a vertex (design §8.1 ①). */
    NodeMapQuery nodeMap(String vertex, String field);

    /** Adjacency scan over one edge binding (design §8.1 ②③). */
    AdjQuery adj(String edge, Direction direction);

    /** Point lookup of vertex rows by node id (design §8.1 ④). */
    VertexQuerier vertex(String vertex);

    /**
     * Two-hop path lookup (design §8.1 ⑤): a semi-join where the first hop is filtered and ranked
     * by {@code filter} (when given) and only the top-{@code degreeCap} intermediate vertices are
     * expanded, so a super-vertex never fans out the whole second hop. Both hops use the same
     * direction.
     */
    List<Path2> twoHop(
            String edge,
            Direction direction,
            long src,
            @Nullable EdgeFilter filter,
            DegreeCap degreeCap,
            ResultCap resultCap);

    /**
     * Breadth-first K-hop traversal (design §8.1 ⑥): returns every path discovered up to {@code
     * hops} edges, with a global visited set to break cycles. Deeper traversals (more than three
     * hops) belong on the batch engines.
     */
    List<GraphPath> kHop(
            String edge,
            Direction direction,
            long src,
            int hops,
            DegreeCap degreeCap,
            ResultCap resultCap);

    @Override
    void close();
}
