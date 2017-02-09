package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class DatasetGroupByTest {
    private static final String DATASET = "dataset";
    public static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyyMMdd");
    public static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forOffsetHours(-6);

    private static Shard makeShard(LocalDate day, int count) {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final FlamdexDocument document = new FlamdexDocument.Builder().addIntTerm("label", 1).addIntTerm("fakeField", 0).addIntTerm("unixtime", day.toDateTimeAtStartOfDay(DATE_TIME_ZONE).getMillis() / 1000).build();
        for (int i = 0; i < count / 2; i++) {
            flamdex.addDocument(document);
        }
        final FlamdexDocument document2 = new FlamdexDocument.Builder().addIntTerm("label", 2).addIntTerm("fakeField", 0).addIntTerm("unixtime", day.toDateTimeAtStartOfDay(DATE_TIME_ZONE).getMillis() / 1000).build();
        for (int i = 0; i < count / 2; i++) {
            flamdex.addDocument(document2);
        }
        return new Shard(DATASET, "index" + FORMATTER.print(day), flamdex);
    }

    /**
     * 2015-01-01: 100
     * 2015-01-02: 200
     * 2015-01-03: 300
     * 2015-01-04: 400
     * 2015-01-05: 500
     * 2015-01-06: 600
     * 2015-01-07: 700
     * 2015-01-08: 1000
     * 2015-01-09: 2000
     * 2015-01-10: 3000
     * 2015-01-11: 4000
     * 2015-01-12: 5000
     * 2015-01-13: 6000
     * 2015-01-14: 7000
     */
    private List<Shard> createDataset() {
        final List<Shard> result = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            result.add(makeShard(new LocalDate(2015, 1, 1 + i), 100 * (i + 1)));
            result.add(makeShard(new LocalDate(2015, 1, 8 + i), 1000 * (i + 1)));
            result.add(makeShard(new LocalDate(2015, 1, 15 + i), 10000 * (i + 1)));
        }
        return result;
    }

    @Test
    public void basic() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("dataset", "100"));
        expected.add(ImmutableList.of("ds2", "200"));
        expected.add(ImmutableList.of("ds3", "300"));
        expected.add(ImmutableList.of("abc", "400"));
        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-02, dataset 2015-01-02 2015-01-03 as ds2, dataset 2015-01-03 2015-01-04 as ds3, dataset 2015-01-04 2015-01-05 as abc " +
                        "group by DATASET() select COUNT()"
        );
    }

    @Test
    public void supercool() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "d1", "100", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "d2", "1000", "900"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "d3", "10000", "9000"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "d1", "200", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "d2", "2000", "1800"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "d3", "20000", "18000"));
        expected.add(ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "d1", "300", "0"));
        expected.add(ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "d2", "3000", "2700"));
        expected.add(ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "d3", "30000", "27000"));
        expected.add(ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "d1", "400", "0"));
        expected.add(ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "d2", "4000", "3600"));
        expected.add(ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "d3", "40000", "36000"));
        expected.add(ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "d1", "500", "0"));
        expected.add(ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "d2", "5000", "4500"));
        expected.add(ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "d3", "50000", "45000"));
        expected.add(ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "d1", "600", "0"));
        expected.add(ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "d2", "6000", "5400"));
        expected.add(ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "d3", "60000", "54000"));
        expected.add(ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "d1", "700", "0"));
        expected.add(ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "d2", "7000", "6300"));
        expected.add(ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "d3", "70000", "63000"));
        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-08 as d1, dataset 2015-01-08 2015-01-15 as d2, dataset 2015-01-15 2015-01-22 as d3 " +
                "group by time(1d relative), DATASET() select COUNT(), if (lag(1,count()) > 0) then count() - lag(1, count()) else 0"
        );
    }

    @Test
    public void groupByDatasetParentTest() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("ds1", "3"));
        expected.add(ImmutableList.of("ds2", "1.5"));

        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-02 as ds1, dataset 2015-01-02 2015-01-03 as ds2 group by DATASET() " +
                        "select PARENT(COUNT()) / COUNT()"
        );
    }

    @Test
    public void groupByDatasetParentMultiple1Test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "ds1", "6", "3"));
        expected.add(ImmutableList.of("1", "ds2", "3", "1.5"));
        expected.add(ImmutableList.of("2", "ds1", "6", "3"));
        expected.add(ImmutableList.of("2", "ds2", "3", "1.5"));

        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-02 as ds1, dataset 2015-01-02 2015-01-03 as ds2 group by label, DATASET() " +
                        "select PARENT(PARENT(COUNT())) / COUNT(), PARENT(COUNT()) / COUNT()"
        );
    }

    @Test
    public void groupByDatasetParentMultiple2Test() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("ds1", "1", "6", "2"));
        expected.add(ImmutableList.of("ds2", "1", "3", "2"));
        expected.add(ImmutableList.of("ds1", "2", "6", "2"));
        expected.add(ImmutableList.of("ds2", "2", "3", "2"));

        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-02 as ds1, dataset 2015-01-02 2015-01-03 as ds2 group by DATASET(), label " +
                        "select PARENT(PARENT(COUNT())) / COUNT(), PARENT(COUNT()) / COUNT()"
        );
    }

    @Test
    public void groupByDatasetParentDistinctTest() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("ds1", "2", "2"));
        expected.add(ImmutableList.of("ds2", "2", "2"));

        QueryServletTestUtils.testIQL2(createDataset(), expected,
                "from dataset 2015-01-01 2015-01-02 as ds1, dataset 2015-01-02 2015-01-03 as ds2 " +
                        "group by DATASET() select DISTINCT(label), PARENT(DISTINCT(label))"
        );
    }

}
