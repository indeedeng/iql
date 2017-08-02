package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.FieldEqualDataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class FieldEqualFilterTest extends BasicTest {
    final Dataset dataset = FieldEqualDataset.create();
    @Test
    public void testEqualFieldFilter() throws Exception {
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "3")), "from organic yesterday today where i1=i2");
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where s1=s2");
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where i1=i2 and s1=s2");

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "2"));
        expected.add(ImmutableList.of("2", "1"));
        testIQL2(dataset, expected, "from organic yesterday today where i1=i2 group by i1", true);

    }

    @Test
    public void testMultiDatasetEqualFieldFilter() throws Exception {
        try {
            testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "fail")), "from organic yesterday today as o1, organic yesterday today as o2 where o1.i1 = o2.i2");
            Assert.fail("field on different dataset should throw exception");
        } catch (Exception e) {
        }
    }

    @Test
    public void testNotEqualFieldFilter() throws Exception {
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where i1!=i2");
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where s1!=s2");
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today where i1!=i2 and s1!=s2");
    }
}
