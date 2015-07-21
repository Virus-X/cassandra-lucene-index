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
package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;

/**
 * A {@link Condition} implementation that matches a field within an range of values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeCondition extends SingleFieldCondition {

    /** The default include lower option. */
    public static final boolean DEFAULT_INCLUDE_LOWER = false;

    /** The default include upper option. */
    public static final boolean DEFAULT_INCLUDE_UPPER = false;

    /** The name of the field to be matched. */
    public final String field;

    /** The lower accepted value. Maybe null meaning no lower limit. */
    public final Object lower;

    /** The upper accepted value. Maybe null meaning no upper limit. */
    public final Object upper;

    /** If the lower value must be included if not null. */
    public final boolean includeLower;

    /** If the upper value must be included if not null. */
    public final boolean includeUpper;

    /**
     * Constructs a query selecting all fields greater/equal than {@code lowerValue} but less/equal than {@code
     * upperValue}.
     *
     * If an endpoint is null, it is said to be "open". Either or both endpoints may be open. Open endpoints may not be
     * exclusive (you can't select all but the first or last term without explicitly specifying the term to exclude.)
     *
     * @param boost        The boost for this query clause. Documents matching this clause will (in addition to the
     *                     normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                     #DEFAULT_BOOST} is used as default.
     * @param field        The name of the field to be matched.
     * @param lowerValue   The lower accepted value. Maybe {@code null} meaning no lower limit.
     * @param upperValue   The upper accepted value. Maybe {@code null} meaning no upper limit.
     * @param includeLower if {@code true}, the {@code lowerValue} is included in the range.
     * @param includeUpper if {@code true}, the {@code upperValue} is included in the range.
     * @param mapper       Mapper for dynamic types
     */
    public RangeCondition(Float boost,
                          String field,
                          Object lowerValue,
                          Object upperValue,
                          Boolean includeLower,
                          Boolean includeUpper,
                          SingleColumnMapper<?> mapper) {
        super(boost, field, mapper);
        this.field = field;
        this.lower = lowerValue;
        this.upper = upperValue;
        this.includeLower = includeLower == null ? DEFAULT_INCLUDE_LOWER : includeLower;
        this.includeUpper = includeUpper == null ? DEFAULT_INCLUDE_UPPER : includeUpper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query query(Schema schema) {
        SingleColumnMapper<?> columnMapper = getMapper(schema, field);
        Class<?> clazz = columnMapper.baseClass();
        Query query;
        if (clazz == String.class) {
            String lower = (String) columnMapper.base(field, this.lower);
            String upper = (String) columnMapper.base(field, this.upper);
            query = TermRangeQuery.newStringRange(field, lower, upper, includeLower, includeUpper);
        } else if (clazz == Integer.class) {
            Integer lower = (Integer) columnMapper.base(field, this.lower);
            Integer upper = (Integer) columnMapper.base(field, this.upper);
            query = NumericRangeQuery.newIntRange(field, lower, upper, includeLower, includeUpper);
        } else if (clazz == Long.class) {
            Long lower = (Long) columnMapper.base(field, this.lower);
            Long upper = (Long) columnMapper.base(field, this.upper);
            query = NumericRangeQuery.newLongRange(field, lower, upper, includeLower, includeUpper);
        } else if (clazz == Float.class) {
            Float lower = (Float) columnMapper.base(field, this.lower);
            Float upper = (Float) columnMapper.base(field, this.upper);
            query = NumericRangeQuery.newFloatRange(field, lower, upper, includeLower, includeUpper);
        } else if (clazz == Double.class) {
            Double lower = (Double) columnMapper.base(field, this.lower);
            Double upper = (Double) columnMapper.base(field, this.upper);
            query = NumericRangeQuery.newDoubleRange(field, lower, upper, includeLower, includeUpper);
        } else {
            String message = String.format("Range queries are not supported by %s mapper", clazz.getSimpleName());
            throw new UnsupportedOperationException(message);
        }
        query.setBoost(boost);
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("boost", boost)
                      .add("field", field)
                      .add("lower", lower)
                      .add("upper", upper)
                      .add("includeLower", includeLower)
                      .add("includeUpper", includeUpper)
                      .toString();
    }
}