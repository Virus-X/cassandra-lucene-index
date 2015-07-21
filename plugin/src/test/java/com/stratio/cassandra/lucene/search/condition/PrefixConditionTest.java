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
import com.stratio.cassandra.lucene.schema.mapping.InetMapper;
import com.stratio.cassandra.lucene.schema.mapping.IntegerMapper;
import com.stratio.cassandra.lucene.schema.mapping.StringMapper;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class PrefixConditionTest extends AbstractConditionTest {

    @Test
    public void testString() {

        Schema schema = mockSchema("name", new StringMapper("name", true, true, null));

        PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", "tr", null);
        Query query = prefixCondition.query(schema);

        assertNotNull(query);
        assertEquals(PrefixQuery.class, query.getClass());
        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals("tr", luceneQuery.getPrefix().text());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInteger() {

        Schema schema = mockSchema("name", new IntegerMapper("name", null, null, 1f));

        PrefixCondition prefixCondition = new PrefixCondition(0.5f, "name", "2*", null);
        prefixCondition.query(schema);
    }

    @Test
    public void testInetV4() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "192.168.", null);
        Query query = wildcardCondition.query(schema);

        assertNotNull(query);
        assertEquals(PrefixQuery.class, query.getClass());
        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals("192.168.", luceneQuery.getPrefix().text());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testInetV6() {

        Schema schema = mockSchema("name", new InetMapper("name", null, null));

        PrefixCondition wildcardCondition = new PrefixCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e", null);
        Query query = wildcardCondition.query(schema);

        assertNotNull(query);
        assertEquals(PrefixQuery.class, query.getClass());
        PrefixQuery luceneQuery = (PrefixQuery) query;
        assertEquals("name", luceneQuery.getField());
        assertEquals("2001:db8:2de:0:0:0:0:e", luceneQuery.getPrefix().text());
        assertEquals(0.5f, query.getBoost(), 0);
    }

    @Test
    public void testToString() {
        PrefixCondition condition = new PrefixCondition(0.5f, "name", "tr", null);
        assertEquals("PrefixCondition{field=name, value=tr}", condition.toString());
    }

}
