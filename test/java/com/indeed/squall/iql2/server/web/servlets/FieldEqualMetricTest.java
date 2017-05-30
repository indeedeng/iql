package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.FieldEqualDataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class FieldEqualMetricTest extends BasicTest {

    @Test
    public void testEqualFieldMetric() throws Exception {
        final Dataset dataset = FieldEqualDataset.create();
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select s1=s2", true);
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "3", "4")), "from organic yesterday today select i1=i2, count()", true);
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2", "3", "4")), "from organic yesterday today select s1=s2, i1=i2, count()", true);

        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1"));
        expected.add(ImmutableList.of("b", "1"));
        testIQL2(dataset, expected, "from organic yesterday today group by s1 select s1=s2", true);
    }

    @Test
    public void testMultiDatasetEqualFieldMetric() throws Exception {
        try {
            testIQL2(FieldEqualDataset.create(), ImmutableList.<List<String>>of(ImmutableList.of("", "fail")), "from organic yesterday today as o1, organic yesterday today as o2 select o1.i1=o2.i2", true);
            Assert.fail("field on different dataset should throw exception");
        } catch (Exception e) {
        }
    }

    @Test
    public void testNotEqualFieldMetric() throws Exception {
        final Dataset dataset = FieldEqualDataset.create();
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "1")), "from organic yesterday today select i1!=i2", true);
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today select s1!=s2", true);
        testIQL2(dataset, ImmutableList.<List<String>>of(ImmutableList.of("", "2", "1")), "from organic yesterday today select s1!=s2, i1!=i2", true);
    }
}
