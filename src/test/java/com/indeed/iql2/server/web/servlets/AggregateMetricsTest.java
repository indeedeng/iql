/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateMetricsTest extends BasicTest {
    @Test
    public void testLog() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        //log(100), log(151)
        expected.add(ImmutableList.of("", String.valueOf(4.6051702), String.valueOf(5.0172798)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select log(100), log(count())");
    }

    @Test
    public void testAbs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100.5", "100.5", "151"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select abs(100.5), abs(-100.5), abs(count())");
    }

    @Test
    public void testModulus() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "0", "0", "1"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select 3 % 2, 2 % 2, 100 % 2, count() % 2");
    }

    @Test
    public void testPower() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "9", "27", String.valueOf(1.4142136), String.valueOf(151 * 151), String.valueOf(12.2882057)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select 3 ^ 2, 3 ^ 3, 2 ^ 0.5, count() ^ 2, count() ^ 0.5");
    }

    @Test
    public void testIfThenElse() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "100", "0"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select if count() > 0 then 100 else 0, if count() <= 0 then 100 else 0");
    }

    @Test
    public void testIfThenElseBothBranches() throws Exception {
        // Testing that both branches of 'if' statement are processed during execution
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "0", "0"));
        expected.add(ImmutableList.of("b", "2", "4", "4"));
        expected.add(ImmutableList.of("c", "4", "2", "2"));
        expected.add(ImmutableList.of("d", "141", "4", "4"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk select count(), lag(1, count()), if (tk=\"a\"+tk=\"c\") > 0 then lag(1, count()) else lag(1, count())");
    }

    @Test
    public void testNamed() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "156", String.valueOf(151 * 151)));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select count() as c, c + 5, c * c");
    }

    @Test
    public void testParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "151", "4"));
        expected.add(ImmutableList.of("b", "151", "2"));
        expected.add(ImmutableList.of("c", "151", "4"));
        expected.add(ImmutableList.of("d", "151", "141"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk select parent(count()), count()", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, QueryServletTestUtils.addConstantColumn(1, "1", expected),
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
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testIterateLag() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "0", "0"));
        expected.add(ImmutableList.of("b", "2", "4", "0"));
        expected.add(ImmutableList.of("c", "4", "2", "4"));
        expected.add(ImmutableList.of("d", "141", "4", "2"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by tk select count(), lag(1, count()), lag(2, count())");
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic yesterday today group by tk in (\"a\", \"b\", \"c\", \"d\") select count(), lag(1, count()), lag(2, count())");
    }

    @Test
    public void testIterateLagInt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("3", "1", "0"));
        expected.add(ImmutableList.of("5", "1", "499"));
        expected.add(ImmutableList.of("7", "1", "699"));
        expected.add(ImmutableList.of("100", "1", "9999"));
        expected.add(ImmutableList.of("1000", "1", "99999"));
        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                expected,
                "from organic yesterday today group by oji in (3, 5, 7, 100, 1000) select COUNT(), if (lag(1,count()) > 0) then oji * 100 * count() - lag(1, count()) else 0",
                true);
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
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), window(2, count()), window(5, count())");
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
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), running(count())");
    }

    // TODO: Make a real test for percentile calculations
    @Test
    public void testPercentile1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "1", "1", "1"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select percentile(allbit, 0.00001), percentile(allbit, 50), percentile(allbit, 100)", true);
    }

    @Test
    public void testMedian() throws Exception {
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","1")), "from organic yesterday today select median(allbit)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","10")), "from organic 2015-01-01 00:00:00  2015-01-01 01:00:00 select median(oji)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","13")), "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 select median(oji)", true);
        QueryServletTestUtils.testIQL2(AllData.DATASET, ImmutableList.of(ImmutableList.of("","1")), "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 select median(ojc)", true);
    }

    @Test
    public void sumAcross() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "4", "302"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic yesterday today select sum_over(tk, count()), sum_over(tk, 1), sum_over(tk, [2])");
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "306", "2653", "151")),
                "from organic yesterday today select sum_over(oji, ojc), sum_over(ojc, oji), sum_over(tk, count())", true);
    }

    @Test
    public void multiDatasetSumAcross() throws Exception {
        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "306")),
                "from organic yesterday today, distinct select sum_over(organic.oji, organic.ojc)",
                true
        );

        QueryServletTestUtils.testIQL2(
                AllData.DATASET,
                ImmutableList.of(ImmutableList.of("", "1180")),
                "from organic yesterday today, distinct select sum_over(organic.tk HAVING count() < 60, organic.oji)",
                true
        );
    }

    @Test
    public void testRegroupIntoParent() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "151", "151", "151"));
        // SUM_OVER and RUNNING in one query cause RegroupIntoParent command to happen.
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "FROM organic yesterday today SELECT count(), SUM_OVER(time(1d), COUNT()), RUNNING(COUNT())");
    }

    @Test
    public void testAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "0.3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 2015-01-01 00:00 2015-01-01 01:00 SELECT AVG(oji), AVG(DISTINCT(tk))");
    }

    @Test
    public void testMultiAVG() throws  Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "118", "10", "0.3", "25.43", "25.43"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected,
                "from organic 2015-01-01 00:00 2015-01-01 01:00 as o1, organic 2015-01-01 01:00 2015-01-01 02:00 as o2 " +
                        "SELECT AVG(o1.oji), AVG(o2.oji), AVG(DISTINCT(o1.tk)), PRINTF('%.2f', AVG(oji)), PRINTF('%.2f', AVG(o1.oji+o2.oji))");
    }

    @Test
    public void testBootstrap() throws Exception {
        testBootstrapMetric("\"min\"", "0.0204");
        testBootstrapMetric("\"max\"", "0.1772");
        //testBootstrapMetric("all", "")); // TODO: write separate test for "all"
        testBootstrapMetric("\"numTerms\"", "4.0000");
        testBootstrapMetric("\"skippedTerms\"", "0.0000");
        testBootstrapMetric("\"mean\"", "0.1213");
        testBootstrapMetric("\"variance\"", "0.0030");

        // double value as metric means persentile
        testBootstrapMetric("0.25", "0.1193");
        testBootstrapMetric("0.50", "0.1213");
        testBootstrapMetric("0.75", "0.1233");
    }

    private void testBootstrapMetric(final String metric, final String expectedValue) throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", expectedValue));
        QueryServletTestUtils.testIQL2(AllData.DATASET,
                expected,
                "from organic yesterday today select PRINTF('%.4f', BOOTSTRAP(tk, ojc / oji, 100, \"seed\", " + metric + "))");
    }



}
