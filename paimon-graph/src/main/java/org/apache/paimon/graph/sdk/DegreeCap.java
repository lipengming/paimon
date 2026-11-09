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

/** Super-vertex guard of a two-hop traversal: the maximum number of intermediate vertices. */
@Public
public final class DegreeCap {

    private final int value;

    private DegreeCap(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("DegreeCap must be positive, but got " + value);
        }
        this.value = value;
    }

    public static DegreeCap of(int value) {
        return new DegreeCap(value);
    }

    public int value() {
        return value;
    }

    @Override
    public String toString() {
        return "DegreeCap(" + value + ")";
    }
}
