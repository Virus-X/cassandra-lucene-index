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

import com.google.common.collect.Lists;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * {@link RowService} that manages wide rows.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RowServiceWide extends RowService {

    /** The names of the Lucene fields to be loaded. */
    private static final Set<String> FIELDS_TO_LOAD;

    static {
        FIELDS_TO_LOAD = new HashSet<>();
        FIELDS_TO_LOAD.add(PartitionKeyMapper.FIELD_NAME);
        FIELDS_TO_LOAD.add(ClusteringKeyMapper.FIELD_NAME);
    }

    /** The used row mapper. */
    private final RowMapperWide rowMapper;

    /**
     * Returns a new {@code RowServiceWide} for manage wide rows.
     *
     * @param baseCfs          The base column family store.
     * @param columnDefinition The indexed column definition.
     * @throws IOException If there are I/O errors.
     */
    public RowServiceWide(ColumnFamilyStore baseCfs, ColumnDefinition columnDefinition) throws IOException {
        super(baseCfs, columnDefinition);
        this.rowMapper = (RowMapperWide) super.rowMapper;
        luceneIndex.init(rowMapper.sort());
    }

    /**
     * {@inheritDoc}
     *
     * These fields are the partition and clustering keys.
     */
    @Override
    public Set<String> fieldsToLoad() {
        return FIELDS_TO_LOAD;
    }

    /** {@inheritDoc} */
    @Override
    public void doIndex(ByteBuffer key, ColumnFamily columnFamily, long timestamp) throws IOException {
        DeletionInfo deletionInfo = columnFamily.deletionInfo();
        DecoratedKey partitionKey = rowMapper.partitionKey(key);

        try{
        if (columnFamily.iterator().hasNext()) {
            List<CellName> clusteringKeys = rowMapper.clusteringKeys(columnFamily);
            Map<CellName, Row> rows = rows(partitionKey, clusteringKeys, timestamp);
            for (Map.Entry<CellName, Row> entry : rows.entrySet()) {
                CellName clusteringKey = entry.getKey();
                Row row = entry.getValue();
                Document document = rowMapper.document(row);
                Term term = rowMapper.term(partitionKey, clusteringKey);
                luceneIndex.upsert(term, document); // Store document
            }
        } else if (deletionInfo != null) {
            Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    RangeTombstone rangeTombstone = iterator.next();
                    Query query = rowMapper.query(partitionKey, rangeTombstone);
                    luceneIndex.delete(query);
                }
            } else {
                Term term = rowMapper.term(partitionKey);
                luceneIndex.delete(term);
            }
        }}catch (Exception ex){
            Log.error("FAILED TO PERFORM INDEXING: %s consider recreating your index", ex.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override
    public void doDelete(DecoratedKey partitionKey) throws IOException {
        Term term = rowMapper.term(partitionKey);
        luceneIndex.delete(term);
    }

    /**
     * {@inheritDoc}
     *
     * The {@link Row} is a logical one.
     */
    @Override
    protected List<ScoredRow> scoredRows(List<SearchResult> searchResults, long timestamp, boolean usesRelevance) {

        Map<String, Row> rowsByScore = new HashMap<>(searchResults.size());

        // Group key queries by partition keys
        Map<String, ScoreDoc> scoresByClusteringKey = new HashMap<>(searchResults.size());
        Map<DecoratedKey, List<CellName>> keys = new HashMap<>();
        for (SearchResult searchResult : searchResults) {
            DecoratedKey partitionKey = searchResult.getPartitionKey();
            CellName clusteringKey = searchResult.getClusteringKey();
            ScoreDoc scoreDoc = searchResult.getScoreDoc();
            String rowHash = rowMapper.hash(partitionKey, clusteringKey);
            scoresByClusteringKey.put(rowHash, scoreDoc);
            List<CellName> clusteringKeys = keys.get(partitionKey);
            if (clusteringKeys == null) {
                clusteringKeys = new ArrayList<>();
                keys.put(partitionKey, clusteringKeys);
            }
            clusteringKeys.add(clusteringKey);
        }

        for (Map.Entry<DecoratedKey, List<CellName>> entry : keys.entrySet()) {
            DecoratedKey partitionKey = entry.getKey();
            for (List<CellName> clusteringKeys : Lists.partition(entry.getValue(), 1000)) {
                Map<CellName, Row> partitionRows = rows(partitionKey, clusteringKeys, timestamp);
                for (Map.Entry<CellName, Row> entry1 : partitionRows.entrySet()) {
                    CellName clusteringKey = entry1.getKey();
                    Row row = entry1.getValue();
                    String rowHash = rowMapper.hash(partitionKey, clusteringKey);
                    ScoreDoc scoreDoc = scoresByClusteringKey.get(rowHash);
                    if (usesRelevance) {
                        Float score = scoreDoc.score;
                        Row scoredRow = addScoreColumn(row, timestamp, score);
                        rowsByScore.put(scoreDoc.toString(), scoredRow);
                    } else {
                        rowsByScore.put(scoreDoc.toString(), row);
                    }
                }
            }
        }

        List<ScoredRow> scoredRows = new ArrayList<>(searchResults.size());
        for (SearchResult searchResult : searchResults) {
            ScoreDoc scoreDoc = searchResult.getScoreDoc();
            Row row = rowsByScore.get(scoreDoc.toString());
            if (row != null) {
                scoredRows.add(new ScoredRow(row, scoreDoc));
            }
        }

        return scoredRows;
    }

    /**
     * Returns the CQL3 {@link Row} identified by the specified key pair, using the specified time stamp to ignore
     * deleted columns. The {@link Row} is retrieved from the storage engine, so it involves IO operations.
     *
     * @param partitionKey   The partition key.
     * @param clusteringKeys The clustering keys.
     * @param timestamp      The time stamp to ignore deleted columns.
     * @return The CQL3 {@link Row} identified by the specified key pair.
     */
    private Map<CellName, Row> rows(DecoratedKey partitionKey, List<CellName> clusteringKeys, long timestamp) {
        ColumnSlice[] slices = rowMapper.columnSlices(clusteringKeys);

        if (baseCfs.metadata.hasStaticColumns()) {
            LinkedList<ColumnSlice> l = new LinkedList<>(Arrays.asList(slices));
            l.addFirst(baseCfs.metadata.comparator.staticPrefix().slice());
            slices = new ColumnSlice[l.size()];
            slices = l.toArray(slices);
        }

        SliceQueryFilter dataFilter = new SliceQueryFilter(slices,
                                                           false,
                                                           Integer.MAX_VALUE,
                                                           baseCfs.metadata.clusteringColumns().size());
        QueryFilter queryFilter = new QueryFilter(partitionKey, baseCfs.name, dataFilter, timestamp);

        ColumnFamily queryColumnFamily = baseCfs.getColumnFamily(queryFilter);

        // Avoid null
        if (queryColumnFamily == null) {
            return Collections.emptyMap();
        }

        // Remove deleted/expired columns
        ColumnFamily cleanQueryColumnFamily = cleanExpired(queryColumnFamily, timestamp);

        // Split CQL3 row column families
        Map<CellName, ColumnFamily> columnFamilies = rowMapper.splitRows(cleanQueryColumnFamily);

        // Build and return rows
        Map<CellName, Row> rows = new HashMap<>(columnFamilies.size());
        for (Map.Entry<CellName, ColumnFamily> entry : columnFamilies.entrySet()) {
            Row row = new Row(partitionKey, entry.getValue());
            rows.put(entry.getKey(), row);
        }
        return rows;
    }

}
