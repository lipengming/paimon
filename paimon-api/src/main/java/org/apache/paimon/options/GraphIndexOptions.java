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

package org.apache.paimon.options;

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.paimon.options.ConfigOptions.key;

/** Options for graph index declarations on vertex and edge tables. */
@Public
public class GraphIndexOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String BACKEND_AUTO = "auto";
    public static final String BACKEND_PK_INDEX = "pk-index";
    public static final String BACKEND_TABLE = "table";

    public static final String SEARCH_MODE_FAST = "fast";
    public static final String SEARCH_MODE_FULL = "full";
    public static final String SEARCH_MODE_DETAIL = "detail";

    public static final String ENCODING_ORDERED_BINARY = "ordered-binary";

    public static final String UPDATE_ACTION_MAINTAIN = "maintain";
    public static final String UPDATE_ACTION_FREEZE = "freeze";

    // --------------------------------------------------------------------------------------------
    // Vertex declaration
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> GRAPH_VERTEX_ENABLED =
            key("graph.vertex.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to declare the table as a graph vertex table. A vertex table"
                                    + " must be a primary-key table with a single-column primary"
                                    + " key, which acts as the stable node id.");

    public static final ConfigOption<String> GRAPH_VERTEX_ID_FIELD =
            key("graph.vertex.id-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The node id field of the graph vertex. It must be the single-column"
                                    + " primary key of the table. Required when 'graph.vertex.enabled'"
                                    + " is true.");

    public static final ConfigOption<String> GRAPH_VERTEX_NODE_MAP_FIELDS =
            key("graph.vertex.node-map.fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated list of vertex attributes to build the node-map"
                                    + " reverse index for. Each entry is 'field' or 'field:family',"
                                    + " where family is 'btree' (default, for high-cardinality"
                                    + " attributes) or 'bitmap' (for enum-like low-cardinality"
                                    + " attributes). Only effective when 'graph.vertex.enabled' is"
                                    + " true. Example: 'name,phone,city:bitmap'.");

    public static final ConfigOption<String> GRAPH_VERTEX_NODE_MAP_ENCODING =
            key("graph.vertex.node-map.encoding")
                    .stringType()
                    .defaultValue(ENCODING_ORDERED_BINARY)
                    .withDescription(
                            "Encoding of the property value in node-map index entries. Currently"
                                    + " only 'ordered-binary' is supported, which preserves order"
                                    + " within buckets so that both equality and range scans work.");

    public static final ConfigOption<String> GRAPH_VERTEX_NODE_MAP_UPDATE_ACTION =
            key("graph.vertex.node-map.update-action")
                    .stringType()
                    .defaultValue(UPDATE_ACTION_MAINTAIN)
                    .withDescription(
                            "How the node-map index reacts to attribute updates: 'maintain' keeps"
                                    + " the index up to date, 'freeze' stops maintaining and"
                                    + " routing queries to the node-map index, which is equivalent"
                                    + " to logically dropping all node-map indexes of this table.");

    // --------------------------------------------------------------------------------------------
    // Edge declaration
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> GRAPH_EDGE_ENABLED =
            key("graph.edge.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to declare the table as a graph edge table. An edge table"
                                    + " should be an append-only event stream without primary keys;"
                                    + " adjacency indexes are derived from it.");

    public static final ConfigOption<String> GRAPH_EDGE_SRC_FIELD =
            key("graph.edge.src-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The source vertex id field of the edge. Required when"
                                    + " 'graph.edge.enabled' is true.");

    public static final ConfigOption<String> GRAPH_EDGE_DST_FIELD =
            key("graph.edge.dst-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The target vertex id field of the edge. Required when"
                                    + " 'graph.edge.enabled' is true.");

    public static final ConfigOption<String> GRAPH_EDGE_SRC_VERTEX =
            key("graph.edge.src-vertex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The vertex type (vertex table name) of the edge source, used for"
                                    + " composite vertex identification and cross-type edge"
                                    + " routing. Required when 'graph.edge.enabled' is true.");

    public static final ConfigOption<String> GRAPH_EDGE_DST_VERTEX =
            key("graph.edge.dst-vertex")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The vertex type (vertex table name) of the edge target, used for"
                                    + " composite vertex identification and cross-type edge"
                                    + " routing. Required when 'graph.edge.enabled' is true.");

    public static final ConfigOption<Boolean> GRAPH_EDGE_REVERSE =
            key("graph.edge.reverse")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to build the reverse adjacency index table ('<table>__edge_rev')"
                                    + " in addition to the forward one.");

    public static final ConfigOption<String> GRAPH_EDGE_AGGREGATES =
            key("graph.edge.aggregates")
                    .stringType()
                    .defaultValue("*:count")
                    .withDescription(
                            "Aggregate mapping for the adjacency index tables, format"
                                    + " 'field:function[:alias];...'. Functions must be legal Paimon"
                                    + " aggregate functions; the special entry '*:count' counts"
                                    + " events per (src, dst) pair and is the default. Example:"
                                    + " 'amount:sum;tx_time:last_non_null_value;*:count'.");

    // --------------------------------------------------------------------------------------------
    // Runtime options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> GRAPH_INDEX_ENABLED =
            key("graph.index.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether graph reads consume the graph index layer. Disabling keeps"
                                    + " all index data and declarations but degrades the tables back"
                                    + " to normal Paimon tables.");

    public static final ConfigOption<String> GRAPH_INDEX_BACKEND =
            key("graph.index.backend")
                    .stringType()
                    .defaultValue(BACKEND_AUTO)
                    .withDescription(
                            "Vertex index backend: 'auto' picks 'pk-index' when prerequisites are"
                                    + " met (fixed bucket, deletion vectors) and falls back to"
                                    + " 'table'; 'pk-index' stores the node-map as primary-key"
                                    + " secondary indexes on the vertex table itself; 'table' uses"
                                    + " standalone '<table>__node_map' index tables.");

    public static final ConfigOption<String> GRAPH_INDEX_SEARCH_MODE =
            key("graph.index.search-mode")
                    .stringType()
                    .defaultValue(SEARCH_MODE_FAST)
                    .withDescription(
                            "Read mode of the graph index: 'fast' reads only the index snapshot;"
                                    + " 'full' reconciles data written after the coverage watermark"
                                    + " at query time; 'detail' is the reconciliation mode for data"
                                    + " quality checks.");

    public static final ConfigOption<Duration> GRAPH_INDEX_WATERMARK_INTERVAL =
            key("graph.index.watermark-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "How often the coverage watermark of edge and table-backend indexes is"
                                    + " refreshed. Defaults to the checkpoint interval of the"
                                    + " maintenance job.");

    public static final ConfigOption<Integer> GRAPH_INDEX_BUILD_MAX_PARALLELISM =
            key("graph.index.build.max-parallelism")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Maximum parallelism of graph index backfill jobs.");

    public static final ConfigOption<Integer> GRAPH_INDEX_ALERT_LAG_SECONDS =
            key("graph.index.alert.lag-seconds")
                    .intType()
                    .defaultValue(300)
                    .withDescription(
                            "Alert threshold for the lag between the base table's latest snapshot"
                                    + " and the index coverage watermark.");

    // --------------------------------------------------------------------------------------------
    // Internal status mirror keys (not user configuration)
    //
    // The maintenance pipeline keeps the runtime truth in the per-database graph_index_status
    // table, and mirrors the per-index-entry state onto the base table's own options so that the
    // '$graph_indexes' system table can expose it without a catalog reference (design §5.1/§5.5).
    // The key suffix is the index table name, or 'node_map' for the pk-index vertex backend.
    // --------------------------------------------------------------------------------------------

    /** Prefix of the base-table option mirroring one index entry's build status. */
    public static final String STATUS_KEY_PREFIX = "graph.index.status.";

    /** Prefix of the base-table option mirroring one index entry's backfill start snapshot. */
    public static final String BUILD_SNAPSHOT_KEY_PREFIX = "graph.index.build-snapshot.";

    /** Prefix of the base-table option mirroring one index entry's coverage watermark. */
    public static final String WATERMARK_KEY_PREFIX = "graph.index.watermark.";
}
