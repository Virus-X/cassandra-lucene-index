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
package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.search.condition.Condition;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.Sort;
import com.stratio.cassandra.lucene.search.sort.builder.SortBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * Class for building a new {@link Search}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchBuilder {

    /** The {@link Condition} for querying, maybe {@code null} meaning no querying. */
    @JsonProperty("query")
    ConditionBuilder queryBuilder;

    /** The {@link Condition} for filtering, maybe {@code null} meaning no filtering. */
    @JsonProperty("filter")
    ConditionBuilder filterBuilder;

    /** The {@link Sort} for the query, maybe {@code null} meaning no filtering. */
    @JsonProperty("sort")
    SortBuilder sortBuilder;

    @JsonProperty("skip")
    int skip;

    /**
     * Returns this builder with the specified querying condition.
     *
     * @param queryBuilder The querying condition to be set.
     * @return This builder with the specified querying condition.
     */
    public SearchBuilder query(ConditionBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    /**
     * Returns this builder with the specified filtering condition.
     *
     * @param filterBuilder The filtering condition to be set.
     * @return This builder with the specified filtering condition.
     */
    public SearchBuilder filter(ConditionBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        return this;
    }

    /**
     * Returns this builder with the specified sorting.
     *
     * @param sortBuilder The sorting fields to be set.
     * @return This builder with the specified sorting.
     */
    public SearchBuilder sort(SortBuilder sortBuilder) {
        this.sortBuilder = sortBuilder;
        return this;
    }

    /**
     * Returns this builder with the specified sorting.
     *
     * @param sortFieldBuilders The sorting fields to be set.
     * @return This builder with the specified sorting.
     */
    public SearchBuilder sort(SortFieldBuilder... sortFieldBuilders) {
        this.sortBuilder = new SortBuilder(sortFieldBuilders);
        return this;
    }

    /**
     * Returns the {@link Search} represented by this builder.
     *
     * @return The {@link Search} represented by this builder.
     */
    public Search build() {
        Condition query = queryBuilder == null ? null : queryBuilder.build();
        Condition filter = filterBuilder == null ? null : filterBuilder.build();
        Sort sort = sortBuilder == null ? null : sortBuilder.build();
        return new Search(query, filter, sort, skip);
    }

    /**
     * Returns the JSON representation of this object.
     *
     * @return the JSON representation of this object.
     */
    public String toJson() {
        build(); // Validate
        try {
            return JsonSerializer.toString(this);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unformateable JSON search: %s", e.getMessage()), e);
        }
    }

    /**
     * Returns the {@link SearchBuilder} represented by the specified JSON {@code String}.
     *
     * @param json A JSON {@code String} representing a {@link SearchBuilder}.
     * @return The {@link SearchBuilder} represented by the specified JSON {@code String}.
     */
    public static SearchBuilder fromJson(String json) {
        try {
            return JsonSerializer.fromString(json, SearchBuilder.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format("Unformateable JSON search: %s", e.getMessage()), e);
        }
    }

}
