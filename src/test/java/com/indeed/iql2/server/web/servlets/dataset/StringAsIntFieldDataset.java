package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jwolfe
 */
public class StringAsIntFieldDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 100; i++) {
            flamdex.addDocument(
                    new FlamdexDocument.Builder()
                            .addIntTerm("id", i)
                            .addStringTerm("page", ((i % 2) == 0) ? "0" : "1")
                            .addStringTerm("vp", ((i % 2) == 0) ? "0" : "1")
                            .build()
            );
        }

        shards.add(new Dataset.DatasetShard("stringAsInt1", "index20150101", flamdex));
        // jobsearch and mobsearch are special cased.
        shards.add(new Dataset.DatasetShard("mobsearch", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
