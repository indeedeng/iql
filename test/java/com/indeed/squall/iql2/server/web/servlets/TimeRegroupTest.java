package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.withoutLastColumn;

public class TimeRegroupTest extends BasicTest {
    @Test
    public void testTimeRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1180", "45", "3"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "600", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "600", "180", "1"));
        for (int i = 3; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", String.valueOf(i), "1", "1"));
        }
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "23", "1", "1"));

        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by time(1h) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(OrganicDataset.create(), withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
    }

    @Test
    public void testTimeRegroupMultipleDatasource() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "0"));
        for (int i = 3; i < 12; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "0"));
        }
        for (int i = 12; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "1"));
        }

        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "0", "1"));

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRegroupMultipleDatasourceHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "0"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "0"));
        }
        for (int i = 6; i < 18; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "0"));
        }
        for (int i = 18; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "1"));
        }

        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "0", "1"));

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeEqualRange() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 12; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeEqualRangeHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeNonEqualRangeHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 6, 7), "1", "0"));


        testIQL2(OrganicDataset.create(), expected, "from organic 24h 17h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeMultipleEqualRange() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1", "1"));
        for (int i = 3; i < 8; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1", "1"));
        }

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 16h as o1, organic 16h 8h as o2, organic 8h today as o3 group by time(1h relative) select o1.count(), o2.count(), o3.count()");
    }

    @Test
    public void testTimeRelativeAfterRangeLarger() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 11; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 11, 12), "0", "1"));
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 12, 13), "0", "1"));

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 13h as o1, organic 13h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativePrevRangeLarger() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));

        for (int i = 3; i < 11; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 11, 12), "1", "0"));
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 12, 13), "1", "0"));

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 11h as o1, organic 11h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

}
