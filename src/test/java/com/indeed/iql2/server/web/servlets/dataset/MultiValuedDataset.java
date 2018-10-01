package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class MultiValuedDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 100; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("id", i);
            doc.addIntTerm("intField", 1);
            doc.addStringTerm("strField", "1");
            if (i % 2 == 0) {
                doc.addIntTerm("intField", 2);
                doc.addStringTerm("strField", "2");
            }
            if (i % 3 == 0) {
                doc.addIntTerm("intField", 3);
                doc.addStringTerm("strField", "3");
            }
            flamdex.addDocument(doc);
        }

        shards.add(new Dataset.DatasetShard("termCount", "index20150101", flamdex));

        return new Dataset(shards);
    }
}
