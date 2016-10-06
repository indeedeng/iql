package com.indeed.squall.iql2.server.web.servlets;

import java.util.List;
import com.google.common.collect.ImmutableList;

import org.junit.Test;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;

/**
 * @author zheli
 */

public class BasicLuceneFilter {
    @Test
    public void testBasicLuceneFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:a\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tk:b\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"TK:c\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "141")), "from organic yesterday today where lucene(\"TK:d\") select count()");
    }

    @Test
    public void testBolleanLuceneFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "6")), "from organic yesterday today where lucene(\"TK:a OR TK:b\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "0")), "from organic yesterday today where lucene(\"TK:a AND TK:b\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "10")), "from organic yesterday today where lucene(\"TK:a OR TK:b OR TK:c\") select count()");
    }

    @Test
    public void testCaseInsensitiveLuceneFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"tk:a\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where lucene(\"tK:b\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where lucene(\"Tk:c\") select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "143")), "from organic yesterday today where lucene(\"tk:d OR Tk:b\") select count()");
    }
}
