package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AggregateMetricsTest extends BasicTest {
    @Test
    public void testLog() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", String.valueOf(Math.log(100)), String.valueOf(Math.log(151))));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select log(100), log(count())");
    }

    @Test
    public void testAbs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100.5", "100.5", "151"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select abs(100.5), abs(-100.5), abs(count())");
    }

    @Test
    public void testModulus() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "0", "0", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select 3 % 2, 2 % 2, 100 % 2, count() % 2");
    }

    @Test
    public void testPower() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "9", "27", String.valueOf(Math.pow(2, 0.5)), String.valueOf(151 * 151), String.valueOf(Math.pow(151, 0.5))));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select 3 ^ 2, 3 ^ 3, 2 ^ 0.5, count() ^ 2, count() ^ 0.5");
    }

    @Test
    public void testIfThenElse() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100", "0"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select if count() > 0 then 100 else 0, if count() <= 0 then 100 else 0");
    }

    @Test
    public void testNamed() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "156", String.valueOf(151 * 151)));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select count() as c, c + 5, c * c");
    }

    @Test
    public void testParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "151", "4"));
        expected.add(ImmutableList.of("b", "151", "2"));
        expected.add(ImmutableList.of("c", "151", "4"));
        expected.add(ImmutableList.of("d", "151", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected,
                "from organic yesterday today group by tk select parent(count()), count()", true);
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), QueryServletTestUtils.addConstantColumn(1, "[1, 2)", expected),
                "from organic yesterday today group by tk, (true) having count() > 0 select parent(parent(count())), count()", true);
    }

    // TODO: LAG(0, X) should work like X.
    @Test
    public void testLag() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "60", "10"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "60", "60"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "1", "60"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "1", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by time(1h) select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testIterateLag() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "0", "0"));
        expected.add(ImmutableList.of("b", "2", "4", "0"));
        expected.add(ImmutableList.of("c", "4", "2", "4"));
        expected.add(ImmutableList.of("d", "141", "4", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testWindow() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "10", "10"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "70", "70"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "120", "130"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "61", "131"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "2", "132"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "2", "123"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "2", "64"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "2", "5"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "2", "5"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by time(1h) select count(), window(2, count()), window(5, count())");
    }

    @Test
    public void testRunning() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "10"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "70"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "130"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1", "131"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1", "132"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1", "133"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1", "134"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1", "135"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1", "136"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1", "137"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1", "138"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1", "139"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1", "140"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1", "141"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1", "142"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1", "143"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1", "144"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1", "145"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1", "146"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1", "147"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1", "148"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1", "149"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1", "150"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "151"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by time(1h) select count(), running(count())");
    }

    // TODO: Make a real test for percentile calculations
    @Test
    public void testPercentile1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "1", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select percentile(allbit, 0.00001), percentile(allbit, 50), percentile(allbit, 100)", true);
    }

    @Test
    public void sumAcross() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "4", "302"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today select sum_over(tk, count()), sum_over(tk, 1), sum_over(tk, [2])");
    }

    @Test
    public void testAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "0.3"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic 2015-01-01 00:00 2015-01-01 01:00 SELECT AVG(oji), AVG(DISTINCT(tk))");
    }

    @Test
    public void testMultiAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "10", "0.3", "25.43", "25.43"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected,
                "from organic 2015-01-01 00:00 2015-01-01 01:00 as o1, organic 2015-01-01 01:00 2015-01-01 02:00 as o2 " +
                        "SELECT AVG(o1.oji), AVG(o2.oji), AVG(DISTINCT(o1.tk)), PRINTF('%.2f', AVG(oji)), PRINTF('%.2f', AVG(o1.oji+o2.oji))");
    }
}
