package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql2.server.web.servlets.BasicTest;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class TimeRegroupDatasets {
    static Dataset multiMonthDataset() {
        final DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withZone(BasicTest.TIME_ZONE);
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
            shards.add(new Dataset.DatasetShard("multiMonth", "index20150101", flamdex));
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
            shards.add(new Dataset.DatasetShard("multiMonth", "index20150201", flamdex));
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
            shards.add(new Dataset.DatasetShard("multiMonth", "index20150301", flamdex));
        }

        return new Dataset(shards);
    }

    static Dataset dayOfWeekDataset() {
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
                doc.addIntTerm("unixtime", new DateTime(2015, 1, i + 1, 0, 0, BasicTest.TIME_ZONE).getMillis() / 1000);
                doc.addIntTerm("fakeField", 0);
                flamdex.addDocument(doc);
            }
            shards.add(new Dataset.DatasetShard("dayOfWeek", String.format("index201501%02d", i + 1), flamdex));
        }
        return new Dataset(shards);
    }
}
