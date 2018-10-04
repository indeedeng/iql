package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql2.server.web.servlets.BasicTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jwolfe
 */
public class ValidationDataset {
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);

    public static List<Dataset.DatasetShard> trivialOrganic() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, BasicTest.TIME_ZONE).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 1);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Dataset.DatasetShard("trivialOrganic", "index20150101", flamdex)
        );
    }

    public static List<Dataset.DatasetShard> trivialSponsored() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, BasicTest.TIME_ZONE).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 0);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Dataset.DatasetShard("trivialSponsored", "index20150101", flamdex)
        );
    }

    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        shards.addAll(trivialOrganic());
        shards.addAll(trivialSponsored());
        return new Dataset(shards);
    }
}
