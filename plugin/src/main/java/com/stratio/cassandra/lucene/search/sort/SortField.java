/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.search.sort;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.MsgPackMapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * A sorting for a field of a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortField {

    /** The default reverse option. */
    public static final boolean DEFAULT_REVERSE = false;

    /** The name of field to sortFields by. */
    public final String field;

    /** {@code true} if natural order should be reversed. */
    public final boolean reverse;

    public final Mapper mapper;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field   The name of field to sort by.
     * @param reverse {@code true} if natural order should be reversed.
     */
    public SortField(String field, Boolean reverse) {
        this(field, reverse, null);
    }

    /**
     * Returns a new {@link SortField}.
     *
     * @param field   The name of field to sort by.
     * @param reverse {@code true} if natural order should be reversed.
     * @param mapper The mapper for dynamic types.
     */
    public SortField(String field, Boolean reverse, Mapper mapper) {

        if (field == null || StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("Field name required");
        }

        this.field = field;
        this.reverse = reverse == null ? DEFAULT_REVERSE : reverse;
        this.mapper = mapper;
    }

    /**
     * Returns the name of field to sort by.
     *
     * @return The name of field to sort by.
     */
    public String getField() {
        return field;
    }

    /**
     * Returns {@code true} if natural order should be reversed.
     *
     * @return {@code true} if natural order should be reversed.
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema The {@link Schema} to be used.
     * @return the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     */
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        if (mapper != null){
            return mapper.sortField(reverse);
        }

        Mapper defaultMapper = schema.getMapper(field);

        if (defaultMapper instanceof MsgPackMapper){
            // TODO Hack for msgpack values, should be removed
            return new org.apache.lucene.search.SortField(field, org.apache.lucene.search.SortField.Type.STRING, reverse);
        }

        Log.info("Mapper is %s", defaultMapper);
        if (defaultMapper == null) {
            throw new IllegalArgumentException("No mapper found for sortFields field " + field);
        } else {
            return defaultMapper.sortField(reverse);
        }
    }

    /**
     * Returns a Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     *
     * @return A Java {@link Comparator} for {@link Columns} with the same logic as this {@link SortField}.
     */
    public Comparator<Columns> comparator(Schema schema) {
        final Mapper columnMapper = this.mapper == null
                ? schema.getMapper(field)
                : this.mapper;

        return new Comparator<Columns>() {
            public int compare(Columns o1, Columns o2) {

                if (o1 == null) {
                    return o2 == null ? 0 : 1;
                }
                if (o2 == null) {
                    return -1;
                }

                Log.info("Comparing by %s", field);

                Columns columns1 = o1.getColumnsByFullName(field);
                Columns columns2 = o2.getColumnsByFullName(field);
                if (columns1.size() > 1 || columns2.size() > 1) {
                    throw new RuntimeException("Sorting in multivalued columns is not supported");
                }
                Column<?> column1 = columns1.getFirst();
                Column<?> column2 = columns2.getFirst();

                if (column1 == null) {
                    Log.info("col1 is null");
                    return column2 == null ? 0 : 1;
                }
                if (column2 == null) {
                    Log.info("col2 is null");
                    return -1;
                }

                if (columnMapper != null) {
                    return reverse ? columnMapper.compare(column2, column1) : columnMapper.compare(column1, column2);
                }

                AbstractType<?> type = column1.getType();
                ByteBuffer value1 = column1.getDecomposedValue();
                ByteBuffer value2 = column2.getDecomposedValue();
                return reverse ? type.compare(value2, value1) : type.compare(value1, value2);
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("field", field).add("reverse", reverse).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortField sortField = (SortField) o;
        return reverse == sortField.reverse && field.equals(sortField.field);
    }
}
