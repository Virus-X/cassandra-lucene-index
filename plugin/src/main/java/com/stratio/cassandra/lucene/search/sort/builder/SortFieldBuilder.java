/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.schema.mapping.Mapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder;
import com.stratio.cassandra.lucene.search.sort.SortField;
import com.stratio.cassandra.lucene.util.Builder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link Builder} for building a new {@link SortField}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SortFieldBuilder implements Builder<SortField> {

    /** The name of the field to be used for sort. */
    @JsonProperty("field")
    final String field;

    /** If natural order should be reversed. */
    @JsonProperty("reverse")
    boolean reverse;

    @JsonProperty("mapper")
    MapperBuilder<?> mapper;

    /**
     * Creates a new {@link SortFieldBuilder} for the specified field and reverse option.
     *
     * @param field The name of the field to be used for sort.
     */
    @JsonCreator
    public SortFieldBuilder(@JsonProperty("field") String field) {
        this.field = field;
        this.reverse = SortField.DEFAULT_REVERSE;
        this.mapper = null;
    }

    /**
     * Returns this {@link SortFieldBuilder} with the specified reverse option.
     *
     * @param reverse {@code true} if natural order should be reversed.
     * @return This.
     */
    public SortFieldBuilder reverse(boolean reverse) {
        this.reverse = reverse;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public SortField build() {
        return new SortField(field, reverse, buildMapper(field));
    }

    private Mapper buildMapper(String field){
        if (mapper != null){
            return  mapper.build(field);
        }

        return null;
    }
}
