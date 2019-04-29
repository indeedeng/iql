package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1LegacyMode;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testOriginalIQL1;

public class StringEscapeTest {
    @Test
    public void testTermIs() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today where term='x\\\\y' select value");
        testIQL1LegacyMode(value(2), "from escape yesterday today where term='x\\\\y' select value");
        testIQL2(value(2), "from escape yesterday today where term='x\\\\y' select value");

        // double quotes
        testAll(value(2), "from escape yesterday today where term=\"x\\\\y\" select value");
    }

    @Test
    public void testTermIsnt() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today where term!='x\\\\y' select value");
        testIQL1LegacyMode(value(4), "from escape yesterday today where term!='x\\\\y' select value");
        testIQL2(value(4), "from escape yesterday today where term!='x\\\\y' select value");

        // double quotes
        testAll(value(4), "from escape yesterday today where term!=\"x\\\\y\" select value");
    }

    @Test
    public void testTermListInWhere() throws Exception {
        // single quotes
        testOriginalIQL1(value(5), "from escape yesterday today where term in ('x\\y', 'x\\\\y') select value");
        testIQL1LegacyMode(value(3), "from escape yesterday today where term in ('x\\y', 'x\\\\y') select value");
        testIQL2(value(3), "from escape yesterday today where term in ('x\\y', 'x\\\\y') select value");

        // double quotes
        testAll(value(3), "from escape yesterday today where term in (\"x\\y\", \"x\\\\y\") select value");
    }

    @Test
    public void testTermListInGroupBy() throws Exception {
        final List<List<String>> singleQuotes = ImmutableList.of(
                ImmutableList.of("x\\\\y", "3"),
                ImmutableList.of("x\\y", "2"));
        final List<List<String>> doubleQuotes = ImmutableList.of(
                ImmutableList.of("x\\y", "2"),
                ImmutableList.of("xy", "1"));
        // there are some issues when decoding saved CSV
        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options().setSkipCsv(true);

        // single quotes
        testOriginalIQL1(singleQuotes, "from escape yesterday today group by term in ('x\\y', 'x\\\\y') select value", options);
        testIQL1LegacyMode(doubleQuotes, "from escape yesterday today group by term in ('x\\y', 'x\\\\y') select value", options);
        testIQL2(doubleQuotes, "from escape yesterday today group by term in ('x\\y', 'x\\\\y') select value", options);

        // double quotes
        testAll(doubleQuotes, "from escape yesterday today group by term in (\"x\\y\", \"x\\\\y\") select value", options);
    }

    @Test
    public void testRegex() throws Exception {
        // single quotes
        testOriginalIQL1(value(2), "from escape yesterday today where term=~'x\\\\y' select value");
        testIQL1LegacyMode(value(1), "from escape yesterday today where term=~'x\\\\y' select value");
        testIQL2(value(1), "from escape yesterday today where term=~'x\\\\y' select value");

        // double quotes
        testAll(value(1), "from escape yesterday today where term=~\"x\\\\y\" select value");
    }

    @Test
    public void testNotRegex() throws Exception {
        // single quotes
        testOriginalIQL1(value(4), "from escape yesterday today where term!=~'x\\\\y' select value");
        testIQL1LegacyMode(value(5), "from escape yesterday today where term!=~'x\\\\y' select value");
        testIQL2(value(5), "from escape yesterday today where term!=~'x\\\\y' select value");

        // double quotes
        testAll(value(5), "from escape yesterday today where term!=~\"x\\\\y\" select value");
    }

    @Test
    public void testSample() throws Exception {
        // single quotes
        testOriginalIQL1(value(4), "from organic yesterday today where sample(tk, 1, 2, '\\x\\\\y') select count()");
        testIQL1LegacyMode(value(145), "from organic yesterday today where sample(tk, 1, 2, '\\x\\\\y') select count()");
        testIQL2(value(145), "from organic yesterday today where sample(tk, 1, 2, '\\x\\\\y') select count()");

        // double quotes
        testAll(value(145), "from organic yesterday today where sample(tk, 1, 2, \"\\x\\\\y\") select count()");
    }

    @Test
    public void testLuceneFilter() throws Exception {
        // single quotes
        testOriginalIQL1(value(2), "from escape yesterday today where lucene('term:x\\\\y') select value");
        testIQL1LegacyMode(value(1), "from escape yesterday today where lucene('term:x\\\\y') select value");
        testIQL2(value(1), "from escape yesterday today where lucene('term:x\\\\y') select value");

        // double quotes
        testAll(value(1), "from escape yesterday today where lucene(\"term:x\\\\y\") select value");
    }

    @Test
    public void testLuceneMetric() throws Exception {
        // single quotes
        testOriginalIQL1(value(2), "from escape yesterday today select lucene('term:x\\\\y')*(value)");
        testIQL1LegacyMode(value(1), "from escape yesterday today select lucene('term:x\\\\y')*(value)");
        testIQL2(value(1), "from escape yesterday today select [lucene('term:x\\\\y')*(value)]");

        // double quotes
        testIQL1(value(1), "from escape yesterday today select lucene(\"term:x\\\\y\")*(value)");
        testIQL2(value(1), "from escape yesterday today select [lucene(\"term:x\\\\y\")*(value)]");
    }

    @Test
    public void testHasStr() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today select (value) * hasstr(term, 'x\\\\y')");
        testIQL1LegacyMode(value(2), "from escape yesterday today select value * hasstr(term, 'x\\\\y')");
        testIQL2(value(2), "from escape yesterday today select [value * hasstr(term, 'x\\\\y')]");

        // double quotes
        testIQL1(value(2), "from escape yesterday today select (value) * hasstr(term, \"x\\\\y\")");
        testIQL2(value(2), "from escape yesterday today select [value * hasstr(term, \"x\\\\y\")]");
    }

    @Test
    public void testHasTerm() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today select (value) * hasstr('term:x\\\\y')");
        testIQL1LegacyMode(value(2), "from escape yesterday today select value * hasstr('term:x\\\\y')");

        // double quotes
        testIQL1(value(2), "from escape yesterday today select (value) * hasstr(\"term:x\\\\y\")");
    }

    @Test
    public void testStringEqualsMetric() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today select (value) * (term='x\\\\y')");
        testIQL1LegacyMode(value(2), "from escape yesterday today select value * (term='x\\\\y')");
        testIQL2(value(2), "from escape yesterday today select [value * (term='x\\\\y')]");

        // double quotes
        testIQL1(value(2), "from escape yesterday today select (value) * (term=\"x\\\\y\")");
        testIQL2(value(2), "from escape yesterday today select [value * (term=\"x\\\\y\")]");
    }

    @Test
    public void testStringNotEqualsMetric() throws Exception {
        // single quotes
        testOriginalIQL1(value(3), "from escape yesterday today select (value) * (term != 'x\\\\y')");
        testIQL1LegacyMode(value(4), "from escape yesterday today select value * (term != 'x\\\\y')");
        testIQL2(value(4), "from escape yesterday today select [value * (term != 'x\\\\y')]");

        // double quotes
        testIQL1(value(4), "from escape yesterday today select (value) * (term != \"x\\\\y\")");
        testIQL2(value(4), "from escape yesterday today select [value * (term != \"x\\\\y\")]");
    }

    private static List<List<String>> value(final int val) {
        return ImmutableList.of(ImmutableList.of("", Integer.toString(val)));
    }
}
