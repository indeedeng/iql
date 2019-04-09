package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class GroupByHavingDataset {
    static Dataset createDataset() {
        final DateTimeZone timeZone = DateTimeZone.forOffsetHours(-6);
        final List<Dataset.DatasetShard> result = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, 0, timeZone).getMillis() / 1000);
            doc.addIntTerm("label", 1);
            doc.addIntTerm("val", 2);
            flamdex.addDocument(doc);
        }
        {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 5, 0, 0, timeZone).getMillis() / 1000);
            doc.addIntTerm("label", 0);
            doc.addIntTerm("val", 2);
            flamdex.addDocument(doc);
        }
        {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 13, 0, 0, timeZone).getMillis() / 1000);
            doc.addIntTerm("label", 0);
            doc.addIntTerm("val", 1);
            flamdex.addDocument(doc);
        }
        {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 18, 0, 0, timeZone).getMillis() / 1000);
            doc.addIntTerm("label", 0);
            doc.addIntTerm("val", 0);
            flamdex.addDocument(doc);
        }
        result.add(new Dataset.DatasetShard("test", "index20150101", flamdex));
        return new Dataset(result);
    }
}
