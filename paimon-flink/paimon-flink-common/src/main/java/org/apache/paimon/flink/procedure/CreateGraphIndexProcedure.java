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
 * Create graph index procedure (design §5.2). Usage:
 *
 * <pre><code>
 *  CALL sys.create_graph_index('tableId', 'node_map', 'name,phone,city:bitmap')
 *  CALL sys.create_graph_index('tableId', 'adjacency', 'src_id', 'dst_id',
 *      'person', 'merchant', 'amount:sum;*:count', true)
 * </code></pre>
 */
public class CreateGraphIndexProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "create_graph_index";

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
        return create(
                GraphIndexSpec.Type.fromString(indexType),
                tableId,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "fields", type = @DataTypeHint("STRING"))
            })
    public String[] call(
            ProcedureContext procedureContext, String tableId, String indexType, String fields)
            throws Exception {
        return create(
                GraphIndexSpec.Type.fromString(indexType),
                tableId,
                fields,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "index_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "src_field", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dst_field", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "src_vertex", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dst_vertex", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "aggregates", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "reverse", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String indexType,
            String srcField,
            String dstField,
            String srcVertex,
            String dstVertex,
            String aggregates,
            Boolean reverse)
            throws Exception {
        return create(
                GraphIndexSpec.Type.fromString(indexType),
                tableId,
                null,
                srcField,
                dstField,
                srcVertex,
                dstVertex,
                aggregates,
                reverse);
    }

    private String[] create(
            GraphIndexSpec.Type type,
            String tableId,
            String fields,
            String srcField,
            String dstField,
            String srcVertex,
            String dstVertex,
            String aggregates,
            Boolean reverse)
            throws Exception {
        GraphIndexSpec.Builder builder =
                type == GraphIndexSpec.Type.NODE_MAP
                        ? GraphIndexSpec.vertex()
                        : GraphIndexSpec.edge();
        if (fields != null) {
            builder.fields(fields);
        }
        if (srcField != null) {
            builder.srcField(srcField);
        }
        if (dstField != null) {
            builder.dstField(dstField);
        }
        if (srcVertex != null) {
            builder.srcVertex(srcVertex);
        }
        if (dstVertex != null) {
            builder.dstVertex(dstVertex);
        }
        if (aggregates != null) {
            builder.aggregates(aggregates);
        }
        if (reverse != null) {
            builder.reverse(reverse);
        }
        List<GraphIndexOperationResult> results =
                GraphIndexManager.createIndex(
                        catalog, Identifier.fromString(tableId), builder.build(), false);
        return describe(results);
    }

    static String[] describe(List<GraphIndexOperationResult> results) {
        return results.stream()
                .map(
                        result ->
                                String.format(
                                        "%s %s backend=%s status=%s: %s",
                                        result.indexType(),
                                        result.indexTable(),
                                        result.backend(),
                                        result.status(),
                                        result.message()))
                .toArray(String[]::new);
    }
}
