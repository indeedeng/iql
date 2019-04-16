package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jwolfe
 */
public class MultiValueDataset {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        for (int i = 0; i < 100; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("f", 1);
            doc.addStringTerm("sf", "1");
            if (i % 2 == 0) {
                doc.addIntTerm("f", 2);
                doc.addStringTerm("sf", "2");
            }
            if (i % 3 == 0) {
                doc.addIntTerm("f", 3);
                doc.addStringTerm("sf", "3");
            }
            if ((i & 1) == 1) {
                doc.addStringTerm("grp", "a");
            }
            if ((i & 2) == 2) {
                doc.addStringTerm("grp", "b");
            }
            if ((i & 4) == 4) {
                doc.addStringTerm("grp", "c");
            }
            if ((i & 8) == 8) {
                doc.addStringTerm("grp", "d");
            }
            doc.addIntTerm("i", i % 2);
            flamdex.addDocument(doc);
        }
        shards.add(new Dataset.DatasetShard("multiValue", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
