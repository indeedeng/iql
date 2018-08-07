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

import com.google.common.collect.Lists;
import com.indeed.iql1.sql.ast.BinaryExpression;
import com.indeed.iql1.sql.ast.Expression;
import com.indeed.iql1.sql.ast.NameExpression;
import com.indeed.iql1.sql.ast.Op;
import com.indeed.iql.language.DescribeStatement;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql.language.IQLStatement;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql.language.ShowStatement;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

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
        assertEquals(null, ((DescribeStatement) query).field);

        query = StatementParser.parseIQLToStatement("DESC " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);

        query = StatementParser.parseIQLToStatement("explain " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);

        String field = "myfield";
        query = StatementParser.parseIQLToStatement("describe " + dataset + "." + field);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);
        assertEquals(field, ((DescribeStatement) query).field);
    }
}
