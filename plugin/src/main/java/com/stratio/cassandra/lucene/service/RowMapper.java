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
package com.stratio.cassandra.lucene.service;

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.column.Columns;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;

import java.nio.ByteBuffer;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;

/**
 * Class for several {@link Row} mappings between Cassandra and Lucene.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class RowMapper {

    final CFMetaData metadata; // The indexed table metadata
    final ColumnDefinition columnDefinition; // The indexed column definition
    final Schema schema; // The indexing schema

    final TokenMapper tokenMapper; // A token mapper for the indexed table
    final PartitionKeyMapper partitionKeyMapper; // A partition key mapper for the indexed table
    final RegularCellsMapper regularCellsMapper; // A regular cell mapper for the indexed table

    /**
     * Builds a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     */
    RowMapper(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        this.metadata = metadata;
        this.columnDefinition = columnDefinition;
        this.schema = schema;
        this.tokenMapper = TokenMapper.instance();
        this.partitionKeyMapper = PartitionKeyMapper.instance(metadata);
        this.regularCellsMapper = RegularCellsMapper.instance(metadata);
    }

    /**
     * Returns a new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     *
     * @param metadata         The indexed column family metadata.
     * @param columnDefinition The indexed column definition.
     * @param schema           The mapping {@link Schema}.
     * @return A new {@link RowMapper} for the specified column family metadata, indexed column definition and {@link
     * Schema}.
     */
    public static RowMapper build(CFMetaData metadata, ColumnDefinition columnDefinition, Schema schema) {
        if (metadata.clusteringColumns().size() > 0) {
            return new RowMapperWide(metadata, columnDefinition, schema);
        } else {
            return new RowMapperSkinny(metadata, columnDefinition, schema);
        }
    }

    /**
     * Returns the {@link Columns} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The columns contained in the specified columns.
     */
    public abstract Columns columns(Row row);

    /**
     * Returns the {@link Document} representing the specified {@link Row}.
     *
     * @param row A {@link Row}.
     * @return The {@link Document} representing the specified {@link Row}.
     */
    public abstract Document document(Row row);

    /**
     * Returns the decorated partition key representing the specified raw partition key.
     *
     * @param key A partition key.
     * @return The decorated partition key representing the specified raw partition key.
     */
    public final DecoratedKey partitionKey(ByteBuffer key) {
        return partitionKeyMapper.partitionKey(key);
    }

    /**
     * Returns the decorated partition key contained in the specified {@link Document}.
     *
     * @param document A {@link Document}.
     * @return The decorated partition key contained in the specified {@link Document}.
     */
    public final DecoratedKey partitionKey(Document document) {
        return partitionKeyMapper.partitionKey(document);
    }

    /**
     * Returns the Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     *
     * @param partitionKey A decorated partition key.
     * @return The Lucene {@link Term} to get the {@link Document}s containing the specified decorated partition key.
     */
    public Term term(DecoratedKey partitionKey) {
        return partitionKeyMapper.term(partitionKey);
    }

    /**
     * Returns the Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     *
     * @param dataRange A {@link DataRange}.
     * @return The Lucene {@link Query} to get the {@link Document}s satisfying the specified {@link DataRange}.
     */
    public abstract Query query(DataRange dataRange);

    /**
     * Returns the Lucene {@link Sort} to get {@link Document}s in the same order that is used in Cassandra.
     *
     * @return The Lucene {@link Sort} to get {@link Document}s in the same order that is used in Cassandra.
     */
    public abstract Sort sort();

    /**
     * Returns a {@link CellName} for the indexed column in the specified column family.
     *
     * @param columnFamily A column family.
     * @return A {@link CellName} for the indexed column in the specified column family.
     */
    public abstract CellName makeCellName(ColumnFamily columnFamily);

    /**
     * Returns a {@link RowComparator} using the same order that is used in Cassandra.
     *
     * @return A {@link RowComparator} using the same order that is used in Cassandra.
     */
    public abstract RowComparator naturalComparator();

    /**
     * Returns the {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     *
     * @param document A {@link Document}.
     * @param scoreDoc A {@link ScoreDoc}.
     * @return The {@link SearchResult} defined by the specified {@link Document} and {@link ScoreDoc}.
     */
    public abstract SearchResult searchResult(Document document, ScoreDoc scoreDoc);

    /**
     * Returns the specified row position as a Lucene {@link Query}.
     *
     * @param rowPosition The row position to be converted.
     * @return The specified row position as a Lucene {@link Query}.
     */
    public Query query(RowPosition rowPosition) {
        if (rowPosition instanceof DecoratedKey) {
            return partitionKeyMapper.query((DecoratedKey) rowPosition);
        } else {
            return tokenMapper.query(rowPosition.getToken());
        }
    }

    public Query query(RowPosition lower, RowPosition upper, boolean includeLower, boolean includeUpper) {
        if (lower instanceof DecoratedKey && upper instanceof DecoratedKey) {
            return partitionKeyMapper.query((DecoratedKey) lower, (DecoratedKey) upper, includeLower, includeUpper);
        } else if (lower instanceof DecoratedKey) {
            BooleanQuery query = new BooleanQuery();
            query.add(partitionKeyMapper.query((DecoratedKey) lower, null, includeLower, includeUpper), FILTER);
            query.add(tokenMapper.query(null, upper.getToken(), includeLower, includeUpper), FILTER);
            return query;
        } else if (upper instanceof DecoratedKey) {
            BooleanQuery query = new BooleanQuery();
            query.add(tokenMapper.query(lower.getToken(), null, includeLower, includeUpper), FILTER);
            query.add(partitionKeyMapper.query(null, (DecoratedKey) upper, includeLower, includeUpper), FILTER);
            return query;
        } else {
            return tokenMapper.query(lower.getToken(), upper.getToken(), includeLower, includeUpper);
        }
    }

}
