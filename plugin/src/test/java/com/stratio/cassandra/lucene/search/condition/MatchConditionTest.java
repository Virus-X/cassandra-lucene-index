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

import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.BlobMapper;
import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import com.stratio.cassandra.lucene.schema.mapping.FloatMapper;
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import com.stratio.cassandra.lucene.schema.mapping.TextMapper;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class MatchConditionTest extends AbstractConditionTest {

    @Test
    public void testBuild() {
        MatchCondition condition = new MatchCondition(0.5f, "field", "value", null);
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals("value", condition.value);
    }

    @Test
    public void testBuildDefaults() {
        MatchCondition condition = new MatchCondition(null, "field", "value", null);
        assertEquals(Condition.DEFAULT_BOOST, condition.boost, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildNullValue() {
        new MatchCondition(null, "field", null, null);
    }

    @Test
    public void testBuildBlankValue() {
        MatchCondition condition = new MatchCondition(0.5f, "field", " ", null);
        assertEquals(0.5f, condition.boost, 0);
        assertEquals("field", condition.field);
        assertEquals(" ", condition.value);
    }

    @Test
    public void testString() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new StringMapper("field", null, null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "value", null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("value", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringStopwords() {

        Schema schema = mockSchema("name", new TextMapper("name", null, null, "english"), "english");

        MatchCondition matchCondition = new MatchCondition(0.5f, "name", "the", null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(BooleanQuery.class, query.getClass());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInteger() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new IntegerMapper("field", null, null, null));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42, null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLong() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new LongMapper("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42L, null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloat() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new FloatMapper("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42.42F, null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDouble() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new DoubleMapper("field", null, null, 1f));

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", 42.42D, null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testBlob() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new BlobMapper("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "0Fa1", null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("0fa1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new InetMapper("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "192.168.0.01", null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("192.168.0.1", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(new InetMapper("field", null, null));
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "2001:DB8:2de::0e13", null);
        Query query = matchCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermQuery.class, query.getClass());
        assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermQuery) query).getTerm().bytes().utf8ToString());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedMapper() {

        SingleColumnMapper<UUID> mapper = new SingleColumnMapper<UUID>("field", null, null, UUIDType.instance) {
            @Override
            public Field indexedField(String name, UUID value) {
                return null;
            }

            @Override
            public Field sortedField(String name, UUID value, boolean isCollection) {
                return null;
            }

            @Override
            public Class<UUID> baseClass() {
                return UUID.class;
            }

            @Override
            public UUID base(String field, Object value) {
                return null;
            }

            @Override
            public org.apache.lucene.search.SortField sortField(boolean reverse) {
                return null;
            }
        };

        Schema schema = mock(Schema.class);
        when(schema.getMapper("field")).thenReturn(mapper);
        when(schema.getAnalyzer()).thenReturn(new KeywordAnalyzer());

        MatchCondition matchCondition = new MatchCondition(0.5f, "field", "2001:DB8:2de::0e13", null);
        matchCondition.query(schema);
    }

    @Test
    public void testToString() {
        MatchCondition condition = new MatchCondition(0.5f, "name", "2001:DB8:2de::0e13", null);
        assertEquals("MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13}", condition.toString());
    }

}
