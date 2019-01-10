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


import com.indeed.iql1.sql.ast2.QueryParts;
import com.indeed.iql1.sql.parser.QuerySplitter;

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
