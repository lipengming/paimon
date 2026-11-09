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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.graph.GraphIndexManager;
import org.apache.paimon.graph.GraphIndexOperationResult;
import org.apache.paimon.graph.GraphIndexSpec;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.spark.utils.CatalogUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.Locale;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Procedure to create a graph index (design §5.2):
 *
 * <pre><code>
 * CALL sys.create_graph_index(
 *   table =&gt; 'db.person', index_type =&gt; 'node_map', fields =&gt; 'name,phone,city:bitmap');
 *
 * CALL sys.create_graph_index(
 *   table =&gt; 'db.transfer', index_type =&gt; 'adjacency',
 *   src_field =&gt; 'src_id', dst_field =&gt; 'dst_id',
 *   src_vertex =&gt; 'person', dst_vertex =&gt; 'merchant',
 *   aggregates =&gt; 'amount:sum;tx_time:last_non_null_value;*:count', reverse =&gt; true);
 * </code></pre>
 */
public class CreateGraphIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("index_type", DataTypes.StringType),
                ProcedureParameter.optional("fields", DataTypes.StringType),
                ProcedureParameter.optional("src_field", DataTypes.StringType),
                ProcedureParameter.optional("dst_field", DataTypes.StringType),
                ProcedureParameter.optional("src_vertex", DataTypes.StringType),
                ProcedureParameter.optional("dst_vertex", DataTypes.StringType),
                ProcedureParameter.optional("aggregates", DataTypes.StringType),
                ProcedureParameter.optional("reverse", DataTypes.BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "index_type", DataTypes.StringType, false, Metadata.empty()),
                        new StructField(
                                "index_table", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("backend", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("status", DataTypes.StringType, true, Metadata.empty()),
                        new StructField("message", DataTypes.StringType, true, Metadata.empty())
                    });

    protected CreateGraphIndexProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public String description() {
        return "Create a graph index (node_map vertex binding or adjacency edge binding).";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier sparkIdentifier = toIdentifier(args.getString(0), PARAMETERS[0].name());
        checkArgument(tableCatalog() instanceof SparkBaseCatalog, "Paimon catalog expected.");
        SparkBaseCatalog sparkCatalog = (SparkBaseCatalog) tableCatalog();
        Catalog catalog = sparkCatalog.paimonCatalog();
        org.apache.paimon.catalog.Identifier paimonIdentifier =
                CatalogUtils.toIdentifier(sparkIdentifier, sparkCatalog.paimonCatalogName());

        GraphIndexSpec.Builder builder =
                GraphIndexSpec.Type.fromString(args.getString(1).toLowerCase(Locale.ROOT))
                                == GraphIndexSpec.Type.NODE_MAP
                        ? GraphIndexSpec.vertex()
                        : GraphIndexSpec.edge();
        if (!args.isNullAt(2) && !StringUtils.isNullOrWhitespaceOnly(args.getString(2))) {
            builder.fields(args.getString(2));
        }
        if (!args.isNullAt(3) && !StringUtils.isNullOrWhitespaceOnly(args.getString(3))) {
            builder.srcField(args.getString(3));
        }
        if (!args.isNullAt(4) && !StringUtils.isNullOrWhitespaceOnly(args.getString(4))) {
            builder.dstField(args.getString(4));
        }
        if (!args.isNullAt(5) && !StringUtils.isNullOrWhitespaceOnly(args.getString(5))) {
            builder.srcVertex(args.getString(5));
        }
        if (!args.isNullAt(6) && !StringUtils.isNullOrWhitespaceOnly(args.getString(6))) {
            builder.dstVertex(args.getString(6));
        }
        if (!args.isNullAt(7) && !StringUtils.isNullOrWhitespaceOnly(args.getString(7))) {
            builder.aggregates(args.getString(7));
        }
        if (!args.isNullAt(8)) {
            builder.reverse(args.getBoolean(8));
        }

        List<GraphIndexOperationResult> results;
        try {
            results =
                    GraphIndexManager.createIndex(
                            catalog, paimonIdentifier, builder.build(), false);
            refreshSparkCache(sparkIdentifier, loadSparkTable(sparkIdentifier));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create the graph index: " + e.getMessage(), e);
        }
        return toRows(results);
    }

    static InternalRow[] toRows(List<GraphIndexOperationResult> results) {
        InternalRow[] rows = new InternalRow[results.size()];
        for (int i = 0; i < results.size(); i++) {
            GraphIndexOperationResult result = results.get(i);
            rows[i] =
                    new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                            new Object[] {
                                UTF8String.fromString(result.indexType()),
                                UTF8String.fromString(result.indexTable()),
                                UTF8String.fromString(result.backend()),
                                UTF8String.fromString(result.status()),
                                UTF8String.fromString(result.message())
                            });
        }
        return rows;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link CreateGraphIndexProcedure}. */
    public static class Builder extends BaseProcedure.Builder<CreateGraphIndexProcedure> {

        @Override
        protected CreateGraphIndexProcedure doBuild() {
            return new CreateGraphIndexProcedure(tableCatalog());
        }
    }
}
