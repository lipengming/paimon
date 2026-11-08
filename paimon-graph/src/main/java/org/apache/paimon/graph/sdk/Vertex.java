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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.math.BigDecimal;

import static org.apache.paimon.graph.sdk.GraphReads.fieldIndex;

/**
 * One vertex row of the base vertex table (design §8.1 ④): the node id plus all vertex attributes.
 */
@Public
public class Vertex {

    private final BinaryRow row;
    private final RowType rowType;
    private final String idField;

    Vertex(BinaryRow row, RowType rowType, String idField) {
        this.row = row;
        this.rowType = rowType;
        this.idField = idField;
    }

    /** The node id (the vertex table's primary key value). */
    public long id() {
        return getLong(idField);
    }

    /** The long value of a BIGINT/INT attribute. */
    public long getLong(String field) {
        int index = fieldIndex(rowType, field);
        switch (rowType.getFields().get(index).type().getTypeRoot()) {
            case BIGINT:
                return row.getLong(index);
            case INTEGER:
                return row.getInt(index);
            default:
                throw new IllegalArgumentException(
                        "Column '" + field + "' is not a BIGINT/INT attribute.");
        }
    }

    /** The string value of a STRING attribute, null when the attribute is null. */
    @Nullable
    public String getString(String field) {
        int index = fieldIndex(rowType, field);
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }

    /** The decimal value of a DECIMAL attribute, null when the attribute is null. */
    @Nullable
    public BigDecimal getDecimal(String field) {
        int index = fieldIndex(rowType, field);
        if (row.isNullAt(index)) {
            return null;
        }
        org.apache.paimon.types.DecimalType type =
                (org.apache.paimon.types.DecimalType) rowType.getFields().get(index).type();
        return row.getDecimal(index, type.getPrecision(), type.getScale()).toBigDecimal();
    }

    /** Whether the attribute is null. */
    public boolean isNull(String field) {
        return row.isNullAt(fieldIndex(rowType, field));
    }

    /** The raw row for advanced access. */
    public BinaryRow row() {
        return row;
    }
}
