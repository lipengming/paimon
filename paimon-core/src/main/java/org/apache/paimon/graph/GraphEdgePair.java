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

import java.util.Objects;

/**
 * Ordered pair of BIGINT node ids: the primary-key prefix of one adjacency row, {@code (src, dst)}
 * for the forward index table and {@code (dst, src)} for the reverse one.
 */
@Public
public final class GraphEdgePair {

    private final long src;
    private final long dst;

    public GraphEdgePair(long src, long dst) {
        this.src = src;
        this.dst = dst;
    }

    public long src() {
        return src;
    }

    public long dst() {
        return dst;
    }

    /** The same pair with the endpoints swapped (reverse adjacency). */
    public GraphEdgePair reversed() {
        return new GraphEdgePair(dst, src);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphEdgePair that = (GraphEdgePair) o;
        return src == that.src && dst == that.dst;
    }

    @Override
    public int hashCode() {
        return Objects.hash(src, dst);
    }

    @Override
    public String toString() {
        return "(" + src + ", " + dst + ")";
    }
}
