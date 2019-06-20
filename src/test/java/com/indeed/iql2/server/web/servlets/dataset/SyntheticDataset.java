package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

public class SyntheticDataset {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE).getMillis() / 1000);

        doc.addIntTerm("A", 1);
        doc.addIntTerm("B", 1);
        doc.addIntTerm("C", 1);
        doc.addIntTerm("D", 1);
        doc.addIntTerm("E", 1);
        doc.addIntTerm("X", 1);
        doc.addIntTerm("Y", 1);
        doc.addIntTerm("Z", 1);

        doc.addIntTerm("oji", 1);
        doc.addIntTerm("ojc", 1);
        doc.addStringTerm("tk", "");

        flamdex.addDocument(doc);
        shards.add(new Dataset.DatasetShard("synthetic", "index20150101", flamdex));

        return new Dataset(shards);
    }
}
