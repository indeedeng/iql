package com.indeed.imhotep.sql.parser;


import com.indeed.imhotep.sql.ast2.QueryParts;

import static org.junit.Assert.*;

/**
 * @author vladimir
 */

public class TestQuerySplitter {
    @org.junit.Test
    public void testBasicSplit() {
        doTest(new QueryParts("fr", "wh", "grp", "sel"));
    }

    @org.junit.Test
    public void testQuotesSplit() {
        doTest(new QueryParts("fr", "wh = \" test from where group by select \" and " +
                "wh2 = ' test from where group by select ' and wh3 = 'te's\"t\"3", "grp", "sel"));
    }


    @org.junit.Test
    public void testSplitAmbiguousTermsFrom() {
        // keyword in dataset part of FROM
        doLINQTest(new QueryParts("where", "", "", "sel"));
        doLINQTest(new QueryParts("group by", "", "", "sel"));
        doLINQTest(new QueryParts("select", "", "", "sel"));

        doSQLTest(new QueryParts("where", "", "", "sel"));
        doSQLTest(new QueryParts("group by", "", "", "sel"));

        // keyword in date part of FROM
        doLINQTest(new QueryParts("jobsearch from", "", "", "sel"));
        doSQLTest(new QueryParts("jobsearch select", "", "", "sel"));
    }

    @org.junit.Test
    public void testSplitAmbiguousTermsWhere() {
        doTest(new QueryParts("frm", "a=b group = 1", "", "sel"));
        doTest(new QueryParts("frm", "a=b from = 1", "", "sel"));
        doTest(new QueryParts("frm", "a=b select = 1", "", "sel"));
        doTest(new QueryParts("frm", "a=b select : 1", "", "sel"));

        // potential TODO handle ambiguous select/group when '=' is not a separate word
//        doTest(new QueryParts("frm", "a=b and select =1", "", "sel"));
    }

    @org.junit.Test
    public void testSplitAmbiguousTermsSelect() {
        doTest(new QueryParts("frm", "", "", "a, select , where , group"));

        // 'from' inside 'select' only works in LINQ style
        doLINQTest(new QueryParts("frm", "", "", "a, from"));
    }

    @org.junit.Test
    public void testSplitAmbiguousTermsGroupBy() {
        doTest(new QueryParts("frm", "", "a, from , where , group", "sel"));
        // 'select' inside 'group by' only works in SQL style
        doSQLTest(new QueryParts("frm", "", "a, select", "sel"));
    }

    private static void doTest(QueryParts t) {
        doSQLTest(t);
        doLINQTest(t);
    }

    private static void doLINQTest(QueryParts t) {
        assertEquals(t, QuerySplitter.splitQuery(t.toString(true)));
    }

    private static void doSQLTest(QueryParts t) {
        assertEquals(t, QuerySplitter.splitQuery(t.toString(false)));
    }
}
