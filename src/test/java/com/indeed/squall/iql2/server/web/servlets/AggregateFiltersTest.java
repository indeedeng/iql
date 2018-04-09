package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.JobsearchDataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateFiltersTest {
    @Test
    public void testMetricIs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() = 4 select count()");
    }

    @Test
    public void testMetricIsRegex() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("uk", "2"));
        expected.add(ImmutableList.of("us", "3"));
        QueryServletTestUtils.testIQL2(JobsearchDataset.create(), expected, "from jobsearch yesterday today group by country having term() =~ \"u.*\" select count()");
    }

    @Test
    public void testMetricIsnt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() != 4 select count()");
    }

    @Test
    public void testMetricGte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() >= 4 select count()");
    }

    @Test
    public void testMetricGt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() > 4 select count()");
    }

    @Test
    public void testMetricLt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 select count()");
    }

    @Test
    public void testMetricLte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 2 select count()");
    }

    @Test
    public void testAnd() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 4 AND count() >= 4 select count()");
    }

    @Test
    public void testOr() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 OR count() > 4 select count()");
    }

    @Test
    public void testAlways() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having true select count()");
    }

    @Test
    public void testNever() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having false select count()");
    }

    @Test
    public void testRegex() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "84"));
        expected.add(ImmutableList.of("2", "1"));
        expected.add(ImmutableList.of("3", "60"));
        expected.add(ImmutableList.of("5", "1"));
        expected.add(ImmutableList.of("10", "2"));
        expected.add(ImmutableList.of("15", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by ojc having tk=~\".*\" select count()", true);
    }

    @Test
    public void testSample() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "142"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today where sample(oji, 1, 2, \"SomeRandomSalt\") select count()", true);
    }

    @Test
    public void testSampleDocId() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "63"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today where sample(DOCID(), 1, 2, \"SomeRandomSalt\") select count()", true);
    }
}
