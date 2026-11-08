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
 * Thrown when a traversal exceeds one of the super-vertex guards (design §8.3): a {@code degreeCap}
 * on adjacency scans or a {@code resultCap} on node-map scans. Callers should narrow the query or
 * fall back to the batch path instead of silently truncating results.
 */
@Public
public class GraphCapExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public GraphCapExceededException(String message) {
        super(message);
    }
}
