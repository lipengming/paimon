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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The per-database graph index status table (design §5.1): a deduplicate primary-key table named
 * {@code graph_index_status}, keyed by (base_table, index_type, index_table). It carries build
 * state and the coverage watermark that the maintenance pipeline refreshes per checkpoint.
 *
 * <p>The {@code graph_index_status} table name is reserved for graph index bookkeeping within a
 * database.
 */
public final class GraphIndexStatusTable {

    public static final String TABLE_NAME = "graph_index_status";

    public static final String BASE_TABLE = "base_table";
    public static final String INDEX_TYPE = "index_type";
    public static final String INDEX_TABLE = "index_table";
    public static final String BACKEND = "backend";
    public static final String FIELDS = "fields";
    public static final String STATUS = "status";
    public static final String BUILD_SNAPSHOT_ID = "build_snapshot_id";
    public static final String COVERAGE_WATERMARK = "coverage_watermark";
    public static final String UPDATED_AT = "updated_at";

    public static final String STATUS_BUILDING = "BUILDING";
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_LOGICAL_DELETED = "LOGICAL_DELETED";

    private static final List<DataField> COLUMNS =
            Arrays.asList(
                    new DataField(0, BASE_TABLE, DataTypes.STRING()),
                    new DataField(1, INDEX_TYPE, DataTypes.STRING()),
                    new DataField(2, INDEX_TABLE, DataTypes.STRING()),
                    new DataField(3, BACKEND, DataTypes.STRING()),
                    new DataField(4, FIELDS, DataTypes.STRING()),
                    new DataField(5, STATUS, DataTypes.STRING()),
                    new DataField(6, BUILD_SNAPSHOT_ID, DataTypes.BIGINT()),
                    new DataField(7, COVERAGE_WATERMARK, DataTypes.BIGINT()),
                    new DataField(8, UPDATED_AT, DataTypes.BIGINT()));

    private GraphIndexStatusTable() {}

    /** Identifier of the status table of one database. */
    public static Identifier identifier(String database) {
        return Identifier.create(database, TABLE_NAME);
    }

    /** Schema of the status table: fixed-bucket deduplicate PK table. */
    public static Schema schema() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "none");
        return new Schema(
                new ArrayList<>(COLUMNS),
                Collections.emptyList(),
                Arrays.asList(BASE_TABLE, INDEX_TYPE, INDEX_TABLE),
                options,
                "Graph index build and maintenance status (design §5.1).");
    }

    /** Creates the status table if it does not exist yet. */
    public static void ensureExists(Catalog catalog, String database) {
        try {
            catalog.createTable(identifier(database), schema(), true);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to create the graph index status table of database '" + database + "'",
                    e);
        }
    }

    /** Upserts one status row: deduplicate keeps the latest write per primary key. */
    public static void upsert(Catalog catalog, String database, StatusRow row) {
        ensureExists(catalog, database);
        Table table;
        try {
            table = catalog.getTable(identifier(database));
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load the graph index status table of database '" + database + "'",
                    e);
        }
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            write.write(toRow(row));
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to update the graph index status of '"
                            + row.baseTable()
                            + "' ("
                            + row.indexTable()
                            + ")",
                    e);
        }
    }

    /** Reads all status rows of one database; an empty list when the status table is absent. */
    public static List<StatusRow> read(Catalog catalog, String database) {
        Table table;
        try {
            table = catalog.getTable(identifier(database));
        } catch (Catalog.TableNotExistException e) {
            return Collections.emptyList();
        }
        List<StatusRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .newRead()
                        .createReader(table.newReadBuilder().newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(fromRow(row)));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to read the graph index status table of database '" + database + "'",
                    e);
        }
        return rows;
    }

    private static InternalRow toRow(StatusRow row) {
        return GenericRow.of(
                BinaryString.fromString(row.baseTable()),
                BinaryString.fromString(row.indexType()),
                BinaryString.fromString(row.indexTable()),
                BinaryString.fromString(row.backend()),
                BinaryString.fromString(row.fields()),
                BinaryString.fromString(row.status()),
                row.buildSnapshotId(),
                row.coverageWatermark(),
                row.updatedAt());
    }

    private static StatusRow fromRow(InternalRow row) {
        return new StatusRow(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getString(2).toString(),
                row.getString(3).toString(),
                row.getString(4).toString(),
                row.getString(5).toString(),
                row.isNullAt(6) ? null : row.getLong(6),
                row.isNullAt(7) ? null : row.getLong(7),
                row.isNullAt(8) ? null : row.getLong(8));
    }

    /** One row of the status table. */
    public static class StatusRow {

        private final String baseTable;
        private final String indexType;
        private final String indexTable;
        private final String backend;
        private final String fields;
        private final String status;
        @Nullable private final Long buildSnapshotId;
        @Nullable private final Long coverageWatermark;
        private final long updatedAt;

        public StatusRow(
                String baseTable,
                String indexType,
                String indexTable,
                String backend,
                String fields,
                String status,
                @Nullable Long buildSnapshotId,
                @Nullable Long coverageWatermark,
                long updatedAt) {
            this.baseTable = baseTable;
            this.indexType = indexType;
            this.indexTable = indexTable;
            this.backend = backend;
            this.fields = fields;
            this.status = status;
            this.buildSnapshotId = buildSnapshotId;
            this.coverageWatermark = coverageWatermark;
            this.updatedAt = updatedAt;
        }

        public String baseTable() {
            return baseTable;
        }

        public String indexType() {
            return indexType;
        }

        public String indexTable() {
            return indexTable;
        }

        public String backend() {
            return backend;
        }

        public String fields() {
            return fields;
        }

        public String status() {
            return status;
        }

        @Nullable
        public Long buildSnapshotId() {
            return buildSnapshotId;
        }

        @Nullable
        public Long coverageWatermark() {
            return coverageWatermark;
        }

        public long updatedAt() {
            return updatedAt;
        }
    }
}
