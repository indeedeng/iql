package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.ims.client.ImsClientInterface;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

/**
 *
 */
public class DimensionTest extends BasicTest {
    private ImsClientInterface imsClient;

    @Before
    public void setUp() throws Exception {
        imsClient = new DimensionUtils.ImsClient();
    }

    @Test
    public void testSelect() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "0", "5", "200", "205", "10", "205", "1", "20", "70", "2", "3", "3"));
        testAll(DimensionUtils.createDataset(), expected,
                "from dimension yesterday today SELECT " +
                        "empty, same, calc, combined, aliasi1, aliasCombined, i1divi2, i1+aliasi1, floatf1, aliasi1=0,  plus!=5, distinct(aliasi1)",
                imsClient);
        assertFailQuery("from dimension yesterday today SELECT i1divi2=1");
        assertFailQuery("from dimension yesterday today SELECT distinct(i1divi2)");
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "59")), "from dimension yesterday today SELECT [i1*plus]", imsClient);
        testIQL1(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "59")), "from dimension yesterday today SELECT i1*plus", imsClient);
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "200")), "from dimension yesterday today SELECT i1*plus", imsClient);
    }

    @Test
    public void testSelectMultiple() throws Exception {
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "200", "100", "300", "1", "2", "2", "2")),
                "from dimension 2015-01-01 2015-01-02 as d1, dimension 2015-01-02 2015-01-03 as d2 " +
                        "SELECT d1.calc, d2.calc, calc, d1.i1divi2,  [(d1.aliasi1+d1.i2)=5], [if d1.plus=5 then 1 else 0], [d1.aliasi1 > d1.i2]",
                imsClient);

        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "10", "4", "14", "204")),
                "from dimension yesterday today as d1, dimension2 as d2 SELECT d1.i2, d2.i2, i2, calc",
                imsClient);
        assertIQL2FailQuery("from dimension yesterday today, dimension2 SELECT plus");
    }

    @Test
    public void testAggregateDimension() throws Exception {
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "11", "100")), "from dimension yesterday today SELECT i1+i1divi2, aliasi1*10", imsClient);
        assertIQL1FailQuery("from dimension yesterday today SELECT i1+i1divi2");
    }

    @Test
    public void testGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("4", "1", "1"));
        expected.add(ImmutableList.of("3", "2", "1"));
        expected.add(ImmutableList.of("3", "5", "1"));
        testIQL2(DimensionUtils.createDataset(), expected, "from dimension yesterday today GROUP BY i1[HAVING plus > 3], i2 HAVING counts > 0 SELECT counts", imsClient);
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("1", "1")), "from dimension yesterday today GROUP BY i2 HAVING i1divi2 > 1", imsClient);
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("0", "2"), ImmutableList.of("2", "2")), "from dimension yesterday today, dimension2 GROUP BY i2 HAVING counts > 1", imsClient);
        assertFailQuery("from dimension yesterday today GROUP BY calc");
        assertFailQuery("from dimension yesterday today GROUP BY i1divi2 in (1, 2)");
        assertIQL2FailQuery("from dimension yesterday today GROUP BY i1divi2 > 1");

    }

    @Test
    public void testFilter() throws Exception {
        testAll(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE aliasi1=0 SELECT counts", imsClient);
        testAll(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today WHERE aliasi1=0 AND i2 = 0 SELECT counts", imsClient);
        testAll(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE plus=5 SELECT counts", imsClient);
        testAll(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "3")), "from dimension yesterday today WHERE plus!=5 SELECT counts", imsClient);
        testAll(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE BETWEEN(plus, 0, 5) SELECT counts", imsClient);
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today WHERE i1=plus SELECT counts", imsClient);
        assertFailQuery("from dimension yesterday today WHERE i1divi2=1");
    }

    @Test
    public void testFilterMultiple() throws Exception {
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today as d1, dimension2 as d2 WHERE i2 = 4 AND i1 = 4", imsClient);
        testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today as d1, dimension2 as d2 WHERE calc = 0", imsClient);
        assertIQL2FailQuery("from dimension yesterday today, dimension2 WHERE plus = 0");
    }

    private void assertIQL1FailQuery(final String query) throws Exception {
        try {
            testIQL1(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of()), query, imsClient);
            Assert.fail(query + " is expected to fail");
        } catch (IllegalArgumentException ex) {}

    }

    private void assertIQL2FailQuery(final String query) throws Exception {
        try {
            testIQL2(DimensionUtils.createDataset(), ImmutableList.of(ImmutableList.of()), query, imsClient);
            Assert.fail(query + " is expected to fail");
        } catch (IllegalArgumentException ex) {}

    }

    private void assertFailQuery(final String query) throws Exception {
        assertIQL1FailQuery(query);
        assertIQL2FailQuery(query);
    }
}
