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
 * Procedure to drop a graph index (design §5.2/§5.6): the default is a logical drop (keep the data,
 * stop maintenance and routing), {@code purge =&gt; true} removes the physical artifacts, and
 * {@code dry_run =&gt; true} only reports the planned operations.
 *
 * <pre><code>
 * CALL sys.drop_graph_index(table =&gt; 'db.person', index_type =&gt; 'node_map');
 * CALL sys.drop_graph_index(
 *   table =&gt; 'db.transfer', index_type =&gt; 'adjacency', purge =&gt; true, dry_run =&gt; true);
 * </code></pre>
 */
public class DropGraphIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("index_type", DataTypes.StringType),
                ProcedureParameter.optional("fields", DataTypes.StringType),
                ProcedureParameter.optional("purge", DataTypes.BooleanType),
                ProcedureParameter.optional("dry_run", DataTypes.BooleanType)
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
                        new StructField("dry_run", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("message", DataTypes.StringType, true, Metadata.empty())
                    });

    protected DropGraphIndexProcedure(TableCatalog tableCatalog) {
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
        return "Drop a graph index, logically by default or physically with purge = true.";
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
        if (!args.isNullAt(2)) {
            builder.fields(args.getString(2));
        }
        boolean purge = !args.isNullAt(3) && args.getBoolean(3);
        boolean dryRun = !args.isNullAt(4) && args.getBoolean(4);

        List<GraphIndexOperationResult> results;
        try {
            results =
                    GraphIndexManager.dropIndex(
                            catalog,
                            paimonIdentifier,
                            builder.build(),
                            purge
                                    ? GraphIndexManager.DropMode.PURGE
                                    : GraphIndexManager.DropMode.LOGICAL,
                            dryRun);
            refreshSparkCache(sparkIdentifier, loadSparkTable(sparkIdentifier));
        } catch (Exception e) {
            throw new RuntimeException("Failed to drop the graph index: " + e.getMessage(), e);
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
                                result.dryRun(),
                                UTF8String.fromString(result.message())
                            });
        }
        return rows;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link DropGraphIndexProcedure}. */
    public static class Builder extends BaseProcedure.Builder<DropGraphIndexProcedure> {

        @Override
        protected DropGraphIndexProcedure doBuild() {
            return new DropGraphIndexProcedure(tableCatalog());
        }
    }
}
