package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

/**
 * @author jwolfe
 */
public class DistinctDataset {
    public static Dataset createDataset() {
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
}
