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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.graph.GraphIndexManager;
import org.apache.paimon.graph.GraphIndexOperationResult;
import org.apache.paimon.graph.GraphIndexSpec;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.List;

/**
 * Drop graph index procedure (design §5.2/§5.6): logical by default, physical with {@code purge}.
 * Usage:
 *
 * <pre><code>
 *  CALL sys.drop_graph_index('tableId', 'node_map')
 *  CALL sys.drop_graph_index('tableId', 'adjacency', true, false)
 * </code></pre>
 */
public class DropGraphIndexProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "drop_graph_index";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING"))
            })
    public String[] call(ProcedureContext procedureContext, String tableId, String indexType)
            throws Exception {
        return drop(indexType, tableId, false, false);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "purge", type = @DataTypeHint("BOOLEAN"))
            })
    public String[] call(
            ProcedureContext procedureContext, String tableId, String indexType, Boolean purge)
            throws Exception {
        return drop(indexType, tableId, purge != null && purge, false);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "purge", type = @DataTypeHint("BOOLEAN")),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"))
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String indexType,
            Boolean purge,
            Boolean dryRun)
            throws Exception {
        return drop(indexType, tableId, purge != null && purge, dryRun != null && dryRun);
    }

    private String[] drop(String indexType, String tableId, boolean purge, boolean dryRun)
            throws Exception {
        GraphIndexSpec.Builder builder =
                GraphIndexSpec.Type.fromString(indexType) == GraphIndexSpec.Type.NODE_MAP
                        ? GraphIndexSpec.vertex()
                        : GraphIndexSpec.edge();
        List<GraphIndexOperationResult> results =
                GraphIndexManager.dropIndex(
                        catalog,
                        Identifier.fromString(tableId),
                        builder.build(),
                        purge
                                ? GraphIndexManager.DropMode.PURGE
                                : GraphIndexManager.DropMode.LOGICAL,
                        dryRun);
        return results.stream()
                .map(
                        result ->
                                String.format(
                                        "%s %s backend=%s status=%s%s: %s",
                                        result.indexType(),
                                        result.indexTable(),
                                        result.backend(),
                                        result.status(),
                                        result.dryRun() ? " (dry run)" : "",
                                        result.message()))
                .toArray(String[]::new);
    }
}
