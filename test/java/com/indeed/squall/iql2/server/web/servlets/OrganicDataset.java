package com.indeed.squall.iql2.server.web.servlets;

import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

class OrganicDataset {
    // Overall:
    // count = 151
    // oji = 2653
    // ojc = 306
    // distinct(ctk) = { "a", "b", "c", "d" }, || = 4
    public static Dataset create() {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);

        final List<Dataset.DatasetShard> result = new ArrayList<>();

        // 2015-01-01 00:00:00 - 2015-01-01 01:00:00
        // Random smattering of documents, including one 1ms before the shard ends.
        // total count = 10
        // total oji = 1180
        // total ojc = 45
        // distinct(ctk) = { "a", "b", "c" }, || = 3
        // count(ctk="a") = 4
        // count(ctk="b") = 2
        // count(ctk="c") = 4
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 0, timeZone), 10, 0, "a"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30, timeZone), 10, 1, "a"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 1, 15, timeZone), 10, 5, "a"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 10, 0, timeZone), 10, 2, "b"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 15, 0, timeZone), 10, 1, "a"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 20, 0, timeZone), 100, 15, "b"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 25, 0, timeZone), 1000, 1, "c"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 30, 30, timeZone), 10, 10, "c"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 45, 30, timeZone), 10, 10, "c"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 59, 59, 999, timeZone), 10, 0, "c"));
            result.add(new Dataset.DatasetShard("organic", "index20150101.00", flamdex));
        }

        // 2015-01-01 01:00:00 - 2015-01-01 02:00:00
        // 1 document per minute with oji=10, ojc=1
        // total count = 60
        // total oji = 10 * 60 = 600
        // total ojc = 1 * 60 = 60
        // distinct(ctk) = { "d" }, || = 1
        // count(ctk="d") = 60
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 60; i++) {
                flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 1, i, 0, timeZone), 10, 1, "d"));
            }
            result.add(new Dataset.DatasetShard("organic", "index20150101.01", flamdex));
        }

        // 2015-01-01 02:00:00 - 03:00:00
        // 1 document per minute with oji=10, ojc=3
        // total count = 60
        // total oji = 10 * 60 = 600
        // total ojc = 3 * 60 = 180
        // distinct(ctk) = { "d" }, || = 1
        // count(ctk="d") = 60
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 60; i++) {
                flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, i, 0, timeZone), 10, 3, "d"));
            }
            result.add(new Dataset.DatasetShard("organic", "index20150101.02", flamdex));
        }

        // 1 document per hour from 2015-01-01 03:00:00 to 2015-01-02 00:00:00
        // oji = the hour
        // ojc = 1
        // count(ctk="d") = 1
        // total count = 21
        // total oji = sum [3 .. 23] = 273
        // total ojc = 21
        // distinct(ctk) = { "d" }, || = 1
        // total count(ctk="d") = 21
        for (int h = 3; h < 24; h++) {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, h, 0, 0, timeZone), h, 1, "d"));
            result.add(new Dataset.DatasetShard("organic", String.format("index20150101.%02d", h), flamdex));
        }
        return new Dataset(result);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int oji, int ojc, String tk) {
        if (!timestamp.getZone().equals(DateTimeZone.forOffsetHours(-6))) {
            throw new IllegalArgumentException("Bad timestamp timezone: " + timestamp.getZone());
        }
        if (ojc > oji) {
            throw new IllegalArgumentException("Can't have more clicks than impressions");
        }

        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("oji", oji);
        doc.addIntTerm("ojc", ojc);
        doc.addIntTerm("allbit", 1);
        doc.addStringTerm("tk", tk);

        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);

        return doc;
    }
}
