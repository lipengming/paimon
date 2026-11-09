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

/**
 * One two-hop path {@code src -> mid -> dst} together with the aggregate payload of both hops
 * (design §8.1 ⑤). The payload accessors of the second hop describe the final edge; the first hop
 * payload is the one the traversal was ranked and filtered on.
 */
@Public
public final class Path2 {

    private final long src;
    private final long mid;
    private final long dst;
    private final EdgeValues firstHop;
    private final EdgeValues secondHop;

    Path2(long src, long mid, long dst, EdgeValues firstHop, EdgeValues secondHop) {
        this.src = src;
        this.mid = mid;
        this.dst = dst;
        this.firstHop = firstHop;
        this.secondHop = secondHop;
    }

    public long src() {
        return src;
    }

    /** The intermediate vertex id of the path. */
    public long mid() {
        return mid;
    }

    public long dst() {
        return dst;
    }

    /** Aggregate payload of the {@code src -> mid} edge. */
    public EdgeValues firstHop() {
        return firstHop;
    }

    /** Aggregate payload of the {@code mid -> dst} edge. */
    public EdgeValues secondHop() {
        return secondHop;
    }

    @Override
    public String toString() {
        return src + " -> " + mid + " -> " + dst;
    }
}
