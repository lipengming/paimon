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

/** Read mode of the graph index (design §6.3). */
@Public
public enum SearchMode {

    /**
     * Read only the index snapshot; data written after the coverage watermark is not returned. For
     * the pk-index vertex backend this is equivalent to snapshot-consistent reads.
     */
    FAST,

    /** Reconciles data written after the coverage watermark at query time (not available yet). */
    FULL,

    /** Reconciliation mode for data quality checks (not available yet). */
    DETAIL
}
