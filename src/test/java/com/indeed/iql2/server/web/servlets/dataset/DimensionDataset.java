package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class DimensionDataset {
    static Dataset createDataset() {
        final DateTimeZone timeZone = Constants.DEFAULT_IQL_TIME_ZONE;
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 12, timeZone), 0, 0, "", "0.1"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 0, 13, timeZone), 0, 2, "", "0.2"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 1, 0, 15, timeZone), 3, 2, "a", "0.3"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, 0, 30, timeZone), 3, 5, "b", "0.4"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 4, 0, 30, timeZone), 4, 1, "b", "1.0"));
            shards.add(new Dataset.DatasetShard("DIMension", "index20150101.00", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 2, 2, 0, 30, timeZone), 4, 6, "b", "0.1"));
            shards.add(new Dataset.DatasetShard("DIMension", "index20150102.00", flamdex));
        }

        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 0, 2, 0, 30, timeZone), 0, 0, "a", "0.1"));
            flamdex.addDocument(makeDocument(new DateTime(2015, 1, 1, 2, 2, 0, 30, timeZone), 4, -1, "b", "0.1"));
            shards.add(new Dataset.DatasetShard("dimension2", "index20150101.00", flamdex));
        }

        return new Dataset(shards);
    }

    private static FlamdexDocument makeDocument(DateTime timestamp, int i1, int i2, String s1, String f1) {
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", timestamp.getMillis() / 1000);
        doc.addIntTerm("i1", i1);
        if (i2 != -1) {
            doc.addIntTerm("i2", i2);
        }
        doc.addIntTerm("i3", i1);
        doc.addStringTerm("s1", s1);
        doc.addStringTerm("si1", "");
        doc.addIntTerm("si1", i1);
        doc.addStringTerm("floatf1", f1);
        doc.addIntTerm("empty", 0);
        doc.addIntTerm("same", 1);
        // TODO: This is a work-around for MemoryFlamdex not handling missing fields.
        doc.addIntTerm("fakeField", 0);
        return doc;
    }
}
