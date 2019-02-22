package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class RegroupEmptyFieldDataset {
    // fields [time, s1, s2, i1, i2]
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 12), 0, 1, "", "a"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 13), 0, 2, "", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 15), 1, 2, "a", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30), 2, 0, "b", ""));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 30), 2, 1, "b", "a"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 40), 2, 2, "b", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 45), 2, 2, "b", "b"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 48), 3, 3, "c", "c"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 48), 3, 3, "c", "c"));
        flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 58), 4, 3, "d", "c"));
        shards.add(new Dataset.DatasetShard("regroupEmptyField", "index20150101.00", flamdex));
        return new Dataset(shards);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int i1, int i2, String s1, String s2) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        if (i1 > 0) {
            doc.addIntTerm("i1", i1);
        }
        if (i2 > 0) {
            doc.addIntTerm("i2", i2);
        }
        if (!s1.isEmpty()) {
            doc.addStringTerm("s1", s1);
        }
        if (!s2.isEmpty()) {
            doc.addStringTerm("s2", s2);
        }
        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);
        return doc;
    }
}
