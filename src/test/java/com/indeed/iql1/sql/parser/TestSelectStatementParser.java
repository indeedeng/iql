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
 package com.indeed.iql1.sql.parser;

import com.google.common.collect.Lists;
import com.indeed.iql1.sql.ast.BinaryExpression;
import com.indeed.iql1.sql.ast.Expression;
import com.indeed.iql1.sql.ast.NameExpression;
import com.indeed.iql1.sql.ast.Op;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql1.sql.parser.SelectStatementParser;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author vladimir
 */

public class TestSelectStatementParser {
    @Test
    public void testBasicSelectQuery() {
        String testQuery = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc";
        IQL1SelectStatement expected = new IQL1SelectStatement(
                Lists.newArrayList((Expression) new NameExpression("sjc")),
                new FromClause("jobsearch", new DateTime(2012,1,1,0,0), new DateTime(2012,1,2,0,0)),
                new BinaryExpression(new NameExpression("rcv"), Op.EQ, new NameExpression("jsv")),
                Lists.newArrayList((Expression)new NameExpression("grp")));

        assertEquals(expected, SelectStatementParser.parseSelectStatement(testQuery, null));
    }

    @Test
    public void testLimitParser() {
        String testQuery = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc";
        IQL1SelectStatement expected = SelectStatementParser.parseSelectStatement(testQuery, null);
        assertEquals(expected.limit, Integer.MAX_VALUE - 1);

        String testQueryLimitZero = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc limit 0";
        IQL1SelectStatement expectedLimitZero = SelectStatementParser.parseSelectStatement(testQueryLimitZero, null);
        assertEquals(expectedLimitZero.limit, Integer.MAX_VALUE - 1);

        String testQueryLimitOverflow = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc limit 9999999999 ";
        IQL1SelectStatement expectedLimitOverflow = SelectStatementParser.parseSelectStatement(testQueryLimitOverflow, null);
        assertEquals(expectedLimitOverflow.limit, Integer.MAX_VALUE - 1);

        String testQueryLimitNegative = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc limit -400";
        IQL1SelectStatement expectedLimitNegative = SelectStatementParser.parseSelectStatement(testQueryLimitNegative, null);
        assertEquals(expectedLimitNegative.limit, Integer.MAX_VALUE - 1);

        String testQueryLimitNormal = "from jobsearch '2012-01-01' '2012-01-02' where rcv=jsv group by grp select sjc limit 5000";
        IQL1SelectStatement expectedLimitNormal = SelectStatementParser.parseSelectStatement(testQueryLimitNormal, null);
        assertEquals(expectedLimitNormal.limit, 5000);

    }
}
