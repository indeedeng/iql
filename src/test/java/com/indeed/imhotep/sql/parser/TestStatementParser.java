/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.sql.parser;

import com.google.common.collect.Lists;
import com.indeed.imhotep.sql.ast.BinaryExpression;
import com.indeed.imhotep.sql.ast.Expression;
import com.indeed.imhotep.sql.ast.NameExpression;
import com.indeed.imhotep.sql.ast.Op;
import com.indeed.imhotep.sql.ast2.*;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author vladimir
 */

public class TestStatementParser {

    @Test
    public void testShow() {
        assertTrue(StatementParser.parse("show tables") instanceof ShowStatement);
        assertTrue(StatementParser.parse("show datasets") instanceof ShowStatement);
        assertTrue(StatementParser.parse(" SHOW    TABLES ") instanceof ShowStatement);
    }

    @Test
    public void testDescribe() {
        String dataset = "testndx";

        IQLStatement query = StatementParser.parse("describe " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);
        assertEquals(null, ((DescribeStatement) query).field);

        query = StatementParser.parse("DESC " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);

        query = StatementParser.parse("explain " + dataset);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);

        String field = "myfield";
        query = StatementParser.parse("describe " + dataset + "." + field);
        assertTrue(query instanceof DescribeStatement);
        assertEquals(dataset, ((DescribeStatement) query).dataset);
        assertEquals(field, ((DescribeStatement) query).field);
    }

    @Test
    public void testBasicSelectQuery() {
        String testQuery = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc";
        SelectStatement expected = new SelectStatement(
                Lists.newArrayList((Expression) new NameExpression("sjc")),
                new FromClause("jobsearch", new DateTime(2012,1,1,0,0), new DateTime(2012,1,2,0,0)),
                new BinaryExpression(new NameExpression("rcv"), Op.EQ, new NameExpression("jsv")),
                Lists.newArrayList((Expression)new NameExpression("grp")));

        assertEquals(expected, StatementParser.parse(testQuery));
    }
}
