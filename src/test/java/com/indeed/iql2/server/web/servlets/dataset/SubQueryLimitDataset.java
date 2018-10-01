package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jwolfe
 */
public class SubQueryLimitDataset {
    public static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 105; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("f", i);
            flamdex.addDocument(doc);
        }

        shards.add(new Dataset.DatasetShard("subQueryLimit", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
