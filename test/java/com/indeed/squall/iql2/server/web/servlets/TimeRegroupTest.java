package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
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
        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by time(24b) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(OrganicDataset.create(), withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
        testAll(OrganicDataset.create(), withoutLastColumn(expected), "from organic yesterday today group by time(24b) select count(), oji, ojc");
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
        testIQL2(OrganicDataset.create(), expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(24b) select o1.count(), o2.count()");
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h) select o1.count(), o2.count()", true);
        testIQL2(OrganicDataset.create(), expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(24b) select o1.count(), o2.count()", true);
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h relative) select o1.count(), o2.count()", true);
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()", true);
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


        testIQL2(OrganicDataset.create(), expected, "from organic 24h 17h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()", true);
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 16h as o1, organic 16h 8h as o2, organic 8h today as o3 group by time(1h relative) select o1.count(), o2.count(), o3.count()", true);
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 13h as o1, organic 13h today as o2 group by time(1h relative) select o1.count(), o2.count()", true);
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

        testIQL2(OrganicDataset.create(), expected, "from organic 24h 11h as o1, organic 11h today as o2 group by time(1h relative) select o1.count(), o2.count()", true);
    }

    @Test
    public void testMonthRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("January 2015", "10", "10"));
        expected.add(ImmutableList.of("February 2015", "100", "200"));
        expected.add(ImmutableList.of("March 2015", "1", "3"));
        testIQL2(multiMonthDataset(), expected, "from dataset 2015-01-01 2015-04-01 group by time(1M) select count(), month", true);
    }

    @Test
    public void testMonthRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "January 2015", "10", "10"));
        expected.add(ImmutableList.of("1", "February 2015", "0", "0"));
        expected.add(ImmutableList.of("1", "March 2015", "0", "0"));
        expected.add(ImmutableList.of("2", "January 2015", "0", "0"));
        expected.add(ImmutableList.of("2", "February 2015", "100", "200"));
        expected.add(ImmutableList.of("2", "March 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "January 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "February 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "March 2015", "1", "3"));
        testIQL2(multiMonthDataset(), expected, "from dataset 2015-01-01 2015-04-01 group by month, time(1M) select count(), month");
    }

    private static Dataset multiMonthDataset() {
        final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withZone(TIME_ZONE);
        // 10 documents in January 2015 with month = 1
        // 100 documents in February 2015 with month = 2
        // 1 document in March 2015 with month = 3
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 10; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("month", 1);
                doc.addIntTerm("unixtime", DateTime.parse("2015-01-01T00:00:00", formatter).getMillis() / 1000);
                doc.addIntTerm("fakeField", 0);
                flamdex.addDocument(doc);
            }
            shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 100; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("month", 2);
                doc.addIntTerm("unixtime", DateTime.parse("2015-02-01T00:00:00", formatter).getMillis() / 1000);
                doc.addIntTerm("fakeField", 0);
                flamdex.addDocument(doc);
            }
            shards.add(new Dataset.DatasetShard("dataset", "index20150201", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 1; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("month", 3);
                doc.addIntTerm("unixtime", DateTime.parse("2015-03-01T00:00:00", formatter).getMillis() / 1000);
                doc.addIntTerm("fakeField", 0);
                flamdex.addDocument(doc);
            }
            shards.add(new Dataset.DatasetShard("dataset", "index20150301", flamdex));
        }

        return new Dataset(shards);
    }

    @Test
    public void testGroupByDayOfWeek() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Monday", "18", "146"));
        expected.add(ImmutableList.of("Tuesday", "107", "691"));
        expected.add(ImmutableList.of("Wednesday", "6", "49"));
        expected.add(ImmutableList.of("Thursday", "51", "401"));
        expected.add(ImmutableList.of("Friday", "8", "37"));
        expected.add(ImmutableList.of("Saturday", "0", "0"));
        expected.add(ImmutableList.of("Sunday", "16", "169"));
        testIQL2(dayOfWeekDataset(), expected, "from dataset 2015-01-01 2015-01-15 group by dayofweek select count(), day", true);
    }

    @Test
    public void testGroupByDayOfWeek2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Monday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Tuesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Wednesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Thursday", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Friday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Saturday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Sunday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Monday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Tuesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Wednesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Thursday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Friday", "5", "10"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Saturday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Sunday", "0", "0"));
        testIQL2(dayOfWeekDataset(), expected, "from dataset 2015-01-01 2015-01-03 group by time(1d), dayofweek select count(), day", true);
    }

    private static Dataset dayOfWeekDataset() {
        // 2015-01-01 THU 1
        // 2015-01-02 FRI 5
        // 2015-01-03 SAT 0
        // 2015-01-04 SUN 1
        // 2015-01-05 MON 10
        // 2015-01-06 TUE 100
        // 2015-01-07 WED 5
        // 2015-01-08 THU 50
        // 2015-01-09 FRI 3
        // 2015-01-10 SAT 0
        // 2015-01-11 SUN 15
        // 2015-01-12 MON 8
        // 2015-01-13 TUE 7
        // 2015-01-14 WED 1
        final int[] counts = new int[]{1, 5, 0, 1, 10, 100, 5, 50, 3, 0, 15, 8, 7, 1};
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] == 0) {
                continue;
            }
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int j = 0; j < counts[i]; j++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("day", i + 1);
                doc.addIntTerm("unixtime", new DateTime(2015, 1, i + 1, 0, 0, TIME_ZONE).getMillis() / 1000);
                doc.addIntTerm("fakeField", 0);
                flamdex.addDocument(doc);
            }
            shards.add(new Dataset.DatasetShard("dataset", String.format("index201501%02d", i + 1), flamdex));
        }
        return new Dataset(shards);
    }
}
