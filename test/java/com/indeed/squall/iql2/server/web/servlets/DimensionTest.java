package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Assert;
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
    private ImsClientInterface imsClient = new DimensionUtils.ImsClient();
    private final Dataset dataset = DimensionUtils.createDataset();
    private final QueryServletTestUtils.Options options = QueryServletTestUtils.Options.create().setSkipTestDimension(true).setImsClient(imsClient);

    @Test
    public void testDefaultDimension() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "25300", "20", "5"));
        testAll(dataset, expected, "from dimension yesterday today SELECT timeofday, dayofweek, counts", options);
    }

    @Test
    public void testSelect() throws Exception {
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "0", "5", "200", "10", "10", "1", "20", "70", "2", "3", "3")),
                "from dimension yesterday today SELECT " +
                        "empty, same, calc, i3, aliasi1, i1divi2, i1+aliasi1, floatf1, aliasi1=0,  plus!=5, distinct(aliasi1)",
                options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "59")), "from dimension yesterday today SELECT [i1*plus]", options);
        testIQL1(dataset, ImmutableList.of(ImmutableList.of("", "59")), "from dimension yesterday today SELECT i1*plus", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "200")), "from dimension yesterday today SELECT i1*plus", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "4", "1")), "from dimension yesterday today SELECT plus!=i1, plus=calc", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today SELECT LUCENE('i1:0')", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "1", "2.67", "0.89", "13")), "from dimension yesterday today SELECT " +
                "DISTINCT(i1 HAVING i1divi2 > 2), PRINTF('%.2f', SUM_OVER(s1, i1divi2)), PRINTF('%.2f', AVG_OVER(s1, i1divi2)), AVG_OVER(s1 HAVING plus > 5, plus)", options);

        assertFailQuery("from dimension yesterday today SELECT i1divi2=1", "");
        assertFailQuery("from dimension yesterday today SELECT field_min(plus)", "non alias dimension in FTGS is not supported");
        assertFailQuery("from dimension yesterday today SELECT distinct(i1divi2)", "non alias dimension in FTGS is not supported");
        assertIQL2FailQuery("from dimension yesterday today SELECT LUCENE('aliasi1:0')", "dimension is not supported in lucene");
    }

    @Test
    public void testSelectMultiple() throws Exception {
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "200", "100", "300", "1", "2", "2", "2", "2")),
                "from dimension 2015-01-01 2015-01-02 as d1, dimension 2015-01-02 2015-01-03 as d2 " +
                        "SELECT d1.calc, d2.calc, calc, d1.i1divi2, d1.calc = 50, [(d1.aliasi1+d1.i2)=5], [if d1.plus=5 then 1 else 0], [d1.aliasi1 > d1.i2]",
                options);
        assertIQL2FailQuery("from dimension yesterday today as d1, dimension2 as d2 SELECT d1.i1 = d2.calc", "field equality for different datasets is not supported");
        assertIQL2FailQuery("from dimension yesterday today, dimension2 SELECT plus", "metric plus is not in dimension2");
    }

    @Test
    public void testAggregateDimension() throws Exception {
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "11", "100")), "from dimension yesterday today SELECT i1+i1divi2, aliasi1*10", options);
        assertIQL1FailQuery("from dimension yesterday today SELECT i1+i1divi2", "compound dimension is not supported in IQL1");
    }

    @Test
    public void testDimensionAliasFieldRewrite() throws Exception {
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "10")), "from dimension yesterday today SELECT si1", options);
    }

    @Test
    public void testGroupBy() throws Exception {
        {
            final List<List<String>> expected = new ArrayList<>();
            expected.add(ImmutableList.of("4", "1", "1"));
            expected.add(ImmutableList.of("3", "2", "1"));
            expected.add(ImmutableList.of("3", "5", "1"));
            testIQL2(dataset, expected, "from dimension yesterday today GROUP BY aliasi1[HAVING plus > 3], i2 HAVING counts > 0 SELECT counts", options);
        }
        {
            final List<List<String>> expected = new ArrayList<>();
            expected.add(ImmutableList.of("[0, 2)", "1"));
            expected.add(ImmutableList.of("[2, 4)", "1"));
            expected.add(ImmutableList.of("[4, 6)", "0"));
            expected.add(ImmutableList.of("[-∞, 0)", "0"));
            expected.add(ImmutableList.of("[6, ∞)", "3"));
            testIQL2(dataset, expected, "from dimension yesterday today GROUP BY bucket(plus, 0, 5, 2)", options);
        }
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("1", "1")), "from dimension yesterday today GROUP BY i2 HAVING i1divi2 > 1", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("0", "2"), ImmutableList.of("2", "2")), "from dimension yesterday today, dimension2 GROUP BY i2 HAVING count() > 1", options);

        assertFailQuery("from dimension yesterday today GROUP BY calc", "group by non alias metric is not supported");
        assertFailQuery("from dimension yesterday today GROUP BY i1divi2 in (1, 2)", "group by in aggregate metric is not supported");
        assertIQL2FailQuery("from dimension yesterday today GROUP BY i1divi2 > 1", "group by aggregate metric is not supported");

    }

    @Test
    public void testFilter() throws Exception {
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE aliasi1=0 SELECT counts", options);
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today WHERE aliasi1=0 AND i2 = 0 SELECT counts", options);
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE plus=5 SELECT counts", options);
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "3")), "from dimension yesterday today WHERE plus!=5 SELECT counts", options);
        testAll(dataset, ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today WHERE BETWEEN(plus, 0, 5) SELECT counts", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today WHERE i1=plus SELECT counts", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today WHERE plus=calc SELECT counts", options);
        assertFailQuery("from dimension yesterday today WHERE i1divi2=1", "equality for aggregate metric is not supported");
    }

    @Test
    public void testFilterMultiple() throws Exception {
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "1")), "from dimension yesterday today as d1, dimension2 as d2 WHERE i2 = 4 AND i1 = 4", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "2")), "from dimension yesterday today as d1, dimension2 as d2 WHERE calc = 0", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "3")), "from dimension yesterday today as d1, dimension2 as d2 WHERE d1.plus = 0", options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "1")), "from dimension 2015-01-01 2015-01-02 as d1, dimension 2015-01-02 2015-01-03 as d2 WHERE plus = 0", options);
        assertIQL2FailQuery("from dimension yesterday today, dimension2 WHERE plus = 0", "dimension [plus] is not in dimension2");
    }

    @Test
    public void testDatasetAlias() throws Exception {
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "5")),
                "from dimension yesterday today ALIASING(i2 as aliasi1) where i2 > 2 SELECT aliasi1",
                options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "5")),
                "from dimension yesterday today ALIASING(aliasi1 as a1) where i1 > 3 SELECT a1+i2",
                options);
        testIQL2(dataset, ImmutableList.of(ImmutableList.of("", "10", "10", "210")),
                "from dimension yesterday today ALIASING(i2 as aliasi1, aliasi2 as aliasi3) SELECT aliasi1, aliasi3, calc+aliasi3",
                options);
        assertIQL2FailQuery("from dimension yesterday today ALIASING(plus as p) SELECT p", "non-alias metrics cannot be used in alias");
    }

    private void assertIQL1FailQuery(final String query, final String reason) throws Exception {
        try {
            testIQL1(dataset, ImmutableList.of(ImmutableList.of()), query, options);
            Assert.fail(reason);
        } catch (IllegalArgumentException ex) {
        }

    }

    private void assertIQL2FailQuery(final String query, final String reason) throws Exception {
        try {
            testIQL2(dataset, ImmutableList.of(ImmutableList.of()), query, options);
            Assert.fail(reason);
        } catch (IllegalArgumentException ex) {
        }

    }

    private void assertFailQuery(final String query, final String reason) throws Exception {
        assertIQL1FailQuery(query, reason);
        assertIQL2FailQuery(query, reason);
    }
}
