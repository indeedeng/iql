package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.*;

public class MetricInequalityTest extends BasicTest {

    public static final Dataset DATASET = AllData.DATASET;

    @Test
    public void greaterThan() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "15"));
        testAll(expected, "from organic 1d 0d where oji > 10", Options.create());
        testIQL1(DATASET, expected, "from organic 1d 0d select oji > 10", Options.create());
        testIQL2(expected, "from organic 1d 0d select [oji > 10]", Options.create());
    }

    @Test
    public void greaterThanOrEqual() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "144"));
        testAll(expected, "from organic 1d 0d where oji >= 10", Options.create());
        testIQL1(DATASET, expected, "from organic 1d 0d select oji >= 10", Options.create());
        testIQL2(expected, "from organic 1d 0d select [oji >= 10]", Options.create());
    }

    @Test
    public void equal() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "129"));
        testAll(expected, "from organic 1d 0d where oji = 10", Options.create());
        testAll(expected, "from organic 1d 0d select oji = 10", Options.create());
    }

    @Test
    public void lessThanOrEqual() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "136"));
        testAll(expected, "from organic 1d 0d where oji <= 10", Options.create());
        testIQL1(DATASET, expected, "from organic 1d 0d select oji <= 10", Options.create());
        testIQL2(expected, "from organic 1d 0d select [oji <= 10]", Options.create());
    }

    @Test
    public void lessThan() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "7"));
        testAll(expected, "from organic 1d 0d where oji < 10", Options.create());
        testIQL1(DATASET, expected, "from organic 1d 0d select oji < 10", Options.create());
        testIQL2(expected, "from organic 1d 0d select [oji < 10]", Options.create());
    }

    @Test
    public void metricInequalityOr() throws Exception {
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "15"));
        testIQL2(expected, "from organic 1d 0d where oji > 10 OR oji > 15", Options.create());
        testIQL2(expected, "from organic 1d 0d select [m(oji > 10 OR oji > 15)]", Options.create());
    }
}
