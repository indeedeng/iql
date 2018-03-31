package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.MultipleDataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class BasicFilterTest extends BasicTest {
    @Test
    public void testBasicFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"a\" select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where tk=\"b\" select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"c\" select count()");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "141")), "from organic yesterday today where tk=\"d\" select count()");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "141")), "from organic yesterday today where tk:d select count()");
    }

    @Test
    public void testMultipleDatasetFilters() throws Exception {
        testIQL2(MultipleDataset.create(), ImmutableList.of(ImmutableList.of("", "2")), "from dataset1 yesterday today, dataset2 where intField1=1 select count()");
        testIQL2(MultipleDataset.create(), ImmutableList.of(ImmutableList.of("", "22")), "from dataset1 1month today, dataset2 where strField1='1' select count()");
        testIQL2(MultipleDataset.create(), ImmutableList.of(ImmutableList.of("", "3")), "from dataset1 1w today, dataset2 where dataset1.intField2=1 select dataset1.count()");
    }

    @Test
    public void testNegateFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "149")), "from organic yesterday today where NOT(tk=\"b\") select count()");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "147")), "from organic yesterday today where tk!=\"a\" select count()");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "147")), "from organic yesterday today where -tk:c select count()");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "10")), "from organic yesterday today where -tk=~'d' select count()");
    }

}
