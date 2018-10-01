package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;

/**
 * @author jwolfe
 */
public class FloatScaleDataset {
    public static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        final FlamdexDocument doc1 = new FlamdexDocument();
        doc1.addStringTerm("field1", "100.0");
        doc1.addStringTerm("field2", "100.000000");
        flamdex.addDocument(doc1);

        final FlamdexDocument doc2 = new FlamdexDocument();
        doc2.addStringTerm("field1", "0.001");
        doc2.addStringTerm("field2", "1000000");
        flamdex.addDocument(doc2);
        shards.add(new Dataset.DatasetShard("floatscaletest", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
