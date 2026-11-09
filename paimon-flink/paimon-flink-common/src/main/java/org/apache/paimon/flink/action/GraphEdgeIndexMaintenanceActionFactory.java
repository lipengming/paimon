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

package org.apache.paimon.flink.action;

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link GraphEdgeIndexMaintenanceAction}. */
public class GraphEdgeIndexMaintenanceActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "graph_index_maintenance";

    private static final String TO_SNAPSHOT = "to_snapshot";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String database = params.getRequired(DATABASE);
        String table = params.getRequired(TABLE);
        Map<String, String> catalogConfig = catalogConfigMap(params);

        GraphEdgeIndexMaintenanceAction action =
                new GraphEdgeIndexMaintenanceAction(database, table, catalogConfig);
        if (params.has(TO_SNAPSHOT)) {
            action.withToSnapshot(Long.parseLong(params.get(TO_SNAPSHOT)));
        }
        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"graph_index_maintenance\" runs one maintenance micro-batch of the graph"
                        + " adjacency index of one edge table.");
        System.out.println();
        System.out.println("Syntax:");
        System.out.println(
                "  graph_index_maintenance --warehouse <warehouse_path> --database"
                        + " <database_name> --table <table_name> [--to_snapshot <snapshot_id>]");
    }
}
