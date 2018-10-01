package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jwolfe
 */
public class ExtractDataset {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("field1", "a 2 5");
        doc1.addStringTerm("field2", "7");
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("field1", "a 3 2");
        doc2.addStringTerm("field2", "5");
        doc2.addStringTerm("field3", "10");
        flamdex.addDocument(doc2);
        shards.add(new Dataset.DatasetShard("extract", "index20150101.00", flamdex));
        return new Dataset(shards);
    }
}
