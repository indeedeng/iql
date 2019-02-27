/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indeed.iql.language;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author vladimir
 */

public class TestStatementParser {

    @Test
    public void testShow() {
        assertTrue(StatementParser.parseIQLToStatement("show tables") instanceof ShowStatement);
        assertTrue(StatementParser.parseIQLToStatement("show datasets") instanceof ShowStatement);
        assertTrue(StatementParser.parseIQLToStatement(" SHOW    TABLES ") instanceof ShowStatement);
    }

    @Test
    public void testDescribe() {
        String dataset = "testndx";

        IQLStatement query = StatementParser.parseIQLToStatement("describe " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);
        assertNull(((DescribeStatement) query).field);

        query = StatementParser.parseIQLToStatement("DESC " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);

        String field = "myfield";
        query = StatementParser.parseIQLToStatement("describe " + dataset + "." + field);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);
        assertEquals(field, ((DescribeStatement) query).field);
    }

    @Test
    public void testExplain() {
        String query = "FROM dataset 2d 1d select count()";
        IQLStatement statement = StatementParser.parseIQLToStatement("explain " + query);
        assertTrue(statement instanceof ExplainStatement);
        assertEquals(query, ((ExplainStatement) statement).selectQuery);
    }
}
