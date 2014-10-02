package com.indeed.imhotep.sql.parser;

import com.google.common.collect.Lists;
import com.indeed.imhotep.sql.ast.*;
import com.indeed.imhotep.sql.ast2.*;
import org.junit.Test;

import static org.junit.Assert.*;
/**
 * @author vladimir
 */

public class TestParser {

    @Test
    public void testGroupByClause() {
        String testQuery = "country[top 5 by sjc / sji]";
        GroupByClause expected = new GroupByClause(Lists.newArrayList((Expression) new BracketsExpression("country", "top 5 by sjc / sji")));
        GroupByClause result = StatementParser.parseGroupByClause(testQuery);
        assertEquals(expected, result);
    }

    @Test
    public void testWhereClauseNegation() {
        BinaryExpression e = (BinaryExpression) StatementParser.parseWhereClause("rcv!=jsv -rcv=ctk").getExpression();
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
