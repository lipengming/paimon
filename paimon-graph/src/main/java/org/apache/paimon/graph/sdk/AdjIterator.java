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

/**
 * Streaming-style iterator over one vertex's adjacency rows (design §8.1 ②③). The fluent
 * configuration methods return the same iterator, so a query reads exactly as in the design doc:
 *
 * <pre>{@code
 * try (AdjIterator it = store.adj("transfer", Direction.OUT)
 *         .from(src)
 *         .project("sum_amount", "cnt")
 *         .degreeCap(100_000)) {
 *     while (it.next()) {
 *         long dst = it.dst();
 *         long cnt = it.getLong("cnt");
 *     }
 * }
 * }</pre>
 *
 * <p>The destination column is always available through {@link #dst()}; projected payload columns
 * are accessed by name. Exceeding {@link #degreeCap(int)} throws {@link GraphCapExceededException}
 * instead of truncating results.
 */
@Public
public interface AdjIterator extends AutoCloseable {

    /** Sets the source vertex id (OUT) or target vertex id (IN). Required before iteration. */
    AdjIterator from(long vertexId);

    /**
     * Projects additional payload columns (aggregate columns of the adjacency index table) by name.
     * The destination column is always included.
     */
    AdjIterator project(String... columns);

    /** Super-vertex guard: throws {@link GraphCapExceededException} when more edges match. */
    AdjIterator degreeCap(int cap);

    /** Advances to the next adjacency row; false when the scan is exhausted. */
    boolean next();

    /** The destination vertex id of the current row. */
    long dst();

    /** The long value of a projected BIGINT/INT column of the current row. */
    long getLong(String column);

    /** The string value of a projected STRING column of the current row. */
    @Nullable
    String getString(String column);

    /** The decimal value of a projected DECIMAL column of the current row. */
    @Nullable
    java.math.BigDecimal getDecimal(String column);

    @Override
    void close();
}
