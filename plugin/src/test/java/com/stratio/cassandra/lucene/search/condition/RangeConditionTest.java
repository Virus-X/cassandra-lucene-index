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
import com.stratio.cassandra.lucene.schema.mapping.DoubleMapper;
import com.stratio.cassandra.lucene.schema.mapping.FloatMapper;
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.LongMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.junit.Test;

import static com.stratio.cassandra.lucene.search.SearchBuilders.range;
import static org.junit.Assert.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class RangeConditionTest extends AbstractConditionTest {

    @Test
    public void testStringClose() {

        Schema schema = mockSchema("name", new StringMapper("name", null, null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("beta", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testStringOpen() {

        Schema schema = mockSchema("name", new StringMapper("name", null, null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("alpha", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals(null, ((TermRangeQuery) query).getUpperTerm());
        assertNull(((TermRangeQuery) query).getUpperTerm());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(false, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerClose() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testIntegerOpen() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongClose() {

        Schema schema = mockSchema("name", new LongMapper("name", true, true, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43L, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testLongOpen() {

        Schema schema = mockSchema("name", new LongMapper("name", true, true, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42L, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatClose() {

        Schema schema = mockSchema("name", new FloatMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42F, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43.42f, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testFloatOpen() {

        Schema schema = mockSchema("name", new FloatMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42f, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleClose() {

        Schema schema = mockSchema("name", new DoubleMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(43.42D, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testDoubleOpen() {

        Schema schema = mockSchema("name", new DoubleMapper("name", null, null, 1f));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(NumericRangeQuery.class, query.getClass());
        assertEquals("name", ((NumericRangeQuery<?>) query).getField());
        assertEquals(42.42D, ((NumericRangeQuery<?>) query).getMin());
        assertEquals(null, ((NumericRangeQuery<?>) query).getMax());
        assertEquals(true, ((NumericRangeQuery<?>) query).includesMin());
        assertEquals(false, ((NumericRangeQuery<?>) query).includesMax());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV4() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RangeCondition rangeCondition = new RangeCondition(0.5f, "name", "192.168.0.01", "192.168.0.045", true, true, null);
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("192.168.0.1", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("192.168.0.45", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        RangeCondition rangeCondition = range("name").boost(0.5f)
                                                     .lower("2001:DB8:2de::e13")
                                                     .upper("2001:DB8:02de::e23")
                                                     .includeLower(true)
                                                     .includeUpper(true)
                                                     .build();
        Query query = rangeCondition.query(schema);

        assertNotNull(query);
        assertEquals(TermRangeQuery.class, query.getClass());
        assertEquals("name", ((TermRangeQuery) query).getField());
        assertEquals("2001:db8:2de:0:0:0:0:e13", ((TermRangeQuery) query).getLowerTerm().utf8ToString());
        assertEquals("2001:db8:2de:0:0:0:0:e23", ((TermRangeQuery) query).getUpperTerm().utf8ToString());
        assertEquals(true, ((TermRangeQuery) query).includesLower());
        assertEquals(true, ((TermRangeQuery) query).includesUpper());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        RangeCondition condition = range("name").boost(0.5f)
                                                .lower("2001:DB8:2de::e13")
                                                .upper("2001:DB8:02de::e23")
                                                .includeLower(true)
                                                .includeUpper(true)
                                                .build();
        assertEquals("RangeCondition{boost=0.5, field=name, lower=2001:DB8:2de::e13, " +
                     "upper=2001:DB8:02de::e23, includeLower=true, includeUpper=true}", condition.toString());
    }

}
