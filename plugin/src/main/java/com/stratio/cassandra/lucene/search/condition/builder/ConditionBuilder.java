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
package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import com.stratio.cassandra.lucene.search.condition.Condition;
import com.stratio.cassandra.lucene.util.Builder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Class for building new {@link Condition}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = BooleanConditionBuilder.class, name = "boolean"),
               @JsonSubTypes.Type(value = ContainsConditionBuilder.class, name = "contains"),
               @JsonSubTypes.Type(value = FuzzyConditionBuilder.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = LuceneConditionBuilder.class, name = "lucene"),
               @JsonSubTypes.Type(value = MatchConditionBuilder.class, name = "match"),
               @JsonSubTypes.Type(value = MatchAllConditionBuilder.class, name = "match_all"),
               @JsonSubTypes.Type(value = RangeConditionBuilder.class, name = "range"),
               @JsonSubTypes.Type(value = PhraseConditionBuilder.class, name = "phrase"),
               @JsonSubTypes.Type(value = PrefixConditionBuilder.class, name = "prefix"),
               @JsonSubTypes.Type(value = RegexpConditionBuilder.class, name = "regexp"),
               @JsonSubTypes.Type(value = WildcardConditionBuilder.class, name = "wildcard"),
               @JsonSubTypes.Type(value = GeoDistanceConditionBuilder.class, name = "geo_distance"),
               @JsonSubTypes.Type(value = GeoBBoxConditionBuilder.class, name = "geo_bbox"),
               @JsonSubTypes.Type(value = DateRangeConditionBuilder.class, name = "date_range"),
               @JsonSubTypes.Type(value = BitemporalConditionBuilder.class, name = "bitemporal")})
public abstract class ConditionBuilder<T extends Condition, K extends ConditionBuilder<T, K>> implements Builder<T> {

    /** The boost for the {@link Condition} to be built. */
    @JsonProperty("boost")
    Float boost;

    @JsonProperty("mapper")
    MapperBuilder<?> mapper;

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     * Condition#DEFAULT_BOOST}
     *
     * @param boost The boost for the {@link Condition} to be built.
     * @return This builder with the specified boost.
     */
    @SuppressWarnings("unchecked")
    public K boost(float boost) {
        this.boost = boost;
        return (K) this;
    }

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     * Condition#DEFAULT_BOOST}
     *
     * @param boost The boost for the {@link Condition} to be built.
     * @return This builder with the specified boost.
     */
    @SuppressWarnings("unchecked")
    public K boost(Number boost) {
        this.boost = boost == null ? null : boost.floatValue();
        return (K) this;
    }

    public K mapper(MapperBuilder<?> mapper){
        this.mapper = mapper;
        return (K) this;
    }

    /**
     * Returns the {@link Condition} represented by this builder.
     *
     * @return The {@link Condition} represented by this builder.
     */
    @Override
    public abstract T build();

    protected SingleColumnMapper<?> buildMapper(String field){
        if (mapper != null){
            return  (SingleColumnMapper<?>)mapper.build(field);
        }

        return null;
    }
}
