package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DistinctTest extends BasicTest {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        // A is present on the 2nd through the 5th inclusive
        for (int i = 1; i < 5; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, i + 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis() / 1000);
            doc.addStringTerm("tk", "a");
            flamdex.addDocument(doc);
        }

        // B, C are present on the 3rd through the 7th inclusive
        for (int i = 2; i < 7; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, i + 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis() / 1000);
            doc.addStringTerm("tk", "b");
            doc.addStringTerm("tk", "c");
            flamdex.addDocument(doc);
        }

        // D is present on the 3rd through the 6th inclusive
        for (int i = 2; i < 6; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, i + 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis() / 1000);
            doc.addStringTerm("tk", "d");
            flamdex.addDocument(doc);
        }

        shards.add(new Dataset.DatasetShard("distinct", "index20150101.00-20150130.00", flamdex));
        return new Dataset(shards);
    }

    @Test
    public void basicDistinct() throws Exception {
        testAll(createDataset(), ImmutableList.of(ImmutableList.of("", "4")), "from distinct yesterday 2015-01-10 select distinct(tk)");
    }

    @Test
    public void timeDistinct() throws Exception {
        testAll(createDataset(), ImmutableList.of(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "3"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "2"),
                ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "0"),
                ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0")
        ), "from distinct yesterday 2015-01-10 group by time(1d) select distinct(tk)");
    }

    @Test
    public void timeDistinctWindow() throws Exception {
        testIQL2(createDataset(), ImmutableList.of(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "4"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "3"),
                ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "2"),
                ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0")
        ), "from distinct yesterday 2015-01-10 group by time(1d) select distinct_window(2, tk)");
    }

    @Test
    public void timeDistinctWindow2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"));
        for (int i = 3; i < 30; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-%02d 00:00:00, 2015-01-%02d 00:00:00)", i, i + 1), "4"));
        }
        testIQL2(createDataset(), expected, "from distinct yesterday 2015-01-30 group by time(1d) select distinct_window(30, tk)");
    }
}
