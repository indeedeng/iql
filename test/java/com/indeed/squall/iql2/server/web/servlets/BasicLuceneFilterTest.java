package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author zheli
 */

public class BasicLuceneFilterTest extends BasicTest {
    final Dataset dataset = OrganicDataset.create();

    @Test
    public void testBasicLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where lucene(\"oji:3\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where lucene(\"OJI:3\") select count()", false);

        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:a\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tk:b\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:c\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "141")), "from organic yesterday today where lucene(\"TK:d\") select count()", false);
    }

    @Test
    public void testBooleanLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"oji:3 OR OJI:4\") select count()", false);

        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "6")), "from organic yesterday today where lucene(\"TK:a OR TK:b\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "0")), "from organic yesterday today where lucene(\"TK:a AND TK:b\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "10")), "from organic yesterday today where lucene(\"TK:a OR TK:b OR TK:c\") select count()", false);
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:a\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tK:b\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"Tk:c\") select count()", false);
        testAll(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "143")), "from organic yesterday today where lucene(\"tk:d OR Tk:b\") select count()", false);
    }
}
