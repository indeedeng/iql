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
import com.indeed.iql1.sql.ast.BracketsExpression;
import com.indeed.iql1.sql.ast.Expression;
import com.indeed.iql1.sql.ast.Op;
import com.indeed.iql1.sql.ast.UnaryExpression;
import com.indeed.iql1.sql.ast2.GroupByClause;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
/**
 * @author vladimir
 */

public class TestParser {

    @Test
    public void testGroupByClause() {
        String testQuery = "country[top 5 by sjc / sji]";
        GroupByClause expected = new GroupByClause(Lists.newArrayList((Expression) BracketsExpression.of("country", "top 5 by sjc / sji")));
        GroupByClause result = SelectStatementParser.parseGroupByClause(testQuery);
        assertEquals(expected, result);
    }

    @Test
    public void testWhereClauseNegation() {
        BinaryExpression e = (BinaryExpression) SelectStatementParser.parseWhereClause("rcv!=jsv -rcv=ctk").getExpression();
        assertEquals(Op.AND, e.operator);
        BinaryExpression condition1 = (BinaryExpression) e.left;
        assertEquals(Op.NOT_EQ, condition1.operator);
        assertEquals("rcv", getStr(condition1.left));
        assertEquals("jsv", getStr(condition1.right));

        UnaryExpression condition2negation = (UnaryExpression) e.right;
        assertEquals(Op.NEG, condition2negation.operator);
        BinaryExpression condition2 = (BinaryExpression) condition2negation.operand;
        assertEquals(Op.EQ, condition2.operator);
        assertEquals("rcv", getStr(condition2.left));
        assertEquals("ctk", getStr(condition2.right));

    }

    private static final Expression.Matcher<String> GET_STR = new Expression.Matcher<String>() {
        protected String numberExpression(final String value) {
            return value;
        }

        protected String stringExpression(final String value) {
            return value;
        }

        protected String nameExpression(final String value) {
            return value;
        }
    };

    private static String getStr(Expression expression) {
        return expression.match(GET_STR);
    }

}
