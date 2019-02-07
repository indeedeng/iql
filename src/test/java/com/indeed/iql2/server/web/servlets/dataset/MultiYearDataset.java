package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

public class MultiYearDataset {

    public static Dataset create() {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);

        final List<Dataset.DatasetShard> result = new ArrayList<>();

        // 2015-01-01 00:00:00 - 2017-12-01 01:00:00
        // One document per month for 5 years
        // total count = 12*5
        // total intfield1 = 12*5;
        // total intfield2 = 12*5;
        // distinct(stringfield1) = { "a" }
        // count(stringfield="a") = 12*5
        final DateTime start = new DateTime(2015, 1, 1, 0, 0, 0, timeZone);
        for (int i = 0; i < 12*5; i++)
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            final DateTime documentDate = start.plusMonths(i);
            flamdex.addDocument(makeDocument(documentDate, i, i, "a"));
            result.add(new Dataset.DatasetShard("multiyeardataset", "index"+documentDate.toString("YYYMMdd"), flamdex));
        }

        return new Dataset(result);
    }

    private static FlamdexDocument makeDocument(final DateTime timestamp, final int intfield1, final int intfield2, final String stringfield1) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("intfield1", intfield1);
        doc.addIntTerm("intfield2", intfield2);
        doc.addStringTerm("stringfield1", stringfield1);
        return doc;
    }


}
