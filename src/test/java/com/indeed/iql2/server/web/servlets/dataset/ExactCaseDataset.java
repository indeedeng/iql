package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class ExactCaseDataset {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();

        // First dataset with same case-insensitive name as other dataset
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE).getMillis() / 1000);

            // Same case-insensitive name, same type, different values
            doc.addIntTerm("i1", 1);
            doc.addIntTerm("I1", 100);

            // Same case-insensitive name, different type
            doc.addIntTerm("f1", 100);
            doc.addStringTerm("F1", "One Hundred");

            flamdex.addDocument(doc);
            // IMPORTANT: uppercase leading E
            shards.add(new Dataset.DatasetShard("ExactCase", "index20150101", flamdex));
        }

        // Second dataset with same case-insensitive name
        {
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE).getMillis() / 1000);

            // Same case-insensitive name, same type, different values
            doc.addStringTerm("s1", "s1 term");
            doc.addStringTerm("S1", "S1 TERM");

            flamdex.addDocument(doc);
            // IMPORTANT: lowercase leading E
            shards.add(new Dataset.DatasetShard("exactCase", "index20150101", flamdex));
        }

        return new Dataset(shards);
    }
}
