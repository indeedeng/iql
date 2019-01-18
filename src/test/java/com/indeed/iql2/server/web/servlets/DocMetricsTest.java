package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DocMetricsTest extends BasicTest {

    @Test
    public void testMetrics() throws Exception {
        QueryServletTestUtils.testOriginalIQL1(
                ImmutableList.of(ImmutableList.of("", "499500", "324572", "999000")),
                "from big yesterday today where field < 1000 select cached(field), mulshr(10, field, field), shldiv(10, field, 512)", true);

        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "499500", "250000", "40315004", "5366", "1", "0", "1000")),
                "from big yesterday today where field < 1000 select field, abs(field-500), " +
                        "exp(field, 128), log(field+1), " +
                        "hasint(\"field:100\"), hasstrfield(field), hasintfield(field)", true);
    }

    @Test
    public void testCached() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "499500"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, cached(field)");
    }

    @Test
    public void testMulShr() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "324572"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, mulshr(10, field, field)");
    }

    @Test
    public void testShlDiv() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "999000"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, shldiv(10, field, 512)");
    }

    @Test
    public void testPercentile() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "599", "2999", "5399"));
        QueryServletTestUtils.testAll(expected, "from big yesterday today select percentile(field, 10), percentile(field, 50), percentile(field, 90)", true);
    }

    @Test
    public void testDivideByConstant() throws Exception {
        // In this test we check that in IQL1 and Legacy mode metric/constant
        // is treated as sum (metric per doc) / constant
        // and not as sum (metric per doc/constant).
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "500.5", "500500"));
        QueryServletTestUtils.testAll(expected, "from big yesterday today where field <= 1000 select field/1000, field");
        QueryServletTestUtils.testAll(expected, "from big yesterday today where field <= 1000 select field/1/1000, field");
    }

    @Test
    public void testStdev() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "289.0"));
        QueryServletTestUtils.testIQL2(expected, "from big yesterday today where field <= 1000 select stdev(field) rounding 1");
    }

    @Test
    public void testDiffPdiff() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "2347.00", "766.99"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today select diff(ojc, oji), pdiff(ojc, oji) rounding 2");
    }

    @Test
    public void testRatioDiff() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "8.555"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today select ratiodiff(ojc, oji, oji, ojc) rounding 3");
    }

    @Test
    public void testSingleScore() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "2.386"));
        expected.add(ImmutableList.of("b", "4.312"));
        expected.add(ImmutableList.of("c", "97.801"));
        expected.add(ImmutableList.of("d", "0.000"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk select singlescore(oji, ojc) rounding 3");
    }

    @Test
    public void testRatioScore() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "0.070"));
        expected.add(ImmutableList.of("b", "0.131"));
        expected.add(ImmutableList.of("c", "3.035"));
        expected.add(ImmutableList.of("d", "0.000"));
        QueryServletTestUtils.testIQL2(expected, "from organic yesterday today group by tk select ratioscore(oji, ojc, ojc, oji) rounding 3");
    }

    @Test
    public void testRMSError() throws Exception {
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "2347")),
                "from organic yesterday today select rmserror(oji, ojc, count(), tk, 0, 100, 10)");
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "7.670")),
                "from organic yesterday today select rmserror(oji, if ojc = 0 then 1 else ojc, count(), tk, 0, 100, 10, true) rounding 3");
    }
}
