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

/**
 * One entry of the result of a graph index management operation (create / drop), formatted by the
 * SQL procedures and the Table API.
 */
@Public
public final class GraphIndexOperationResult {

    private final String indexType;
    private final String indexTable;
    private final String backend;
    private final String status;
    private final boolean dryRun;
    private final String message;

    public GraphIndexOperationResult(
            String indexType,
            String indexTable,
            String backend,
            String status,
            boolean dryRun,
            String message) {
        this.indexType = indexType;
        this.indexTable = indexTable;
        this.backend = backend;
        this.status = status;
        this.dryRun = dryRun;
        this.message = message;
    }

    public String indexType() {
        return indexType;
    }

    /**
     * The physical index table name, or an empty string when the index is part of the base table.
     */
    public String indexTable() {
        return indexTable;
    }

    public String backend() {
        return backend;
    }

    public String status() {
        return status;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return String.format(
                "%s[%s, backend=%s, status=%s, dryRun=%s] %s",
                indexType, indexTable, backend, status, dryRun, message);
    }
}
