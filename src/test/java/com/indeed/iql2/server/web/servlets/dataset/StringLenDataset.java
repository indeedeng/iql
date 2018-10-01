package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class StringLenDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 100; i++) {
            flamdex.addDocument(
                    new FlamdexDocument.Builder()
                            .addIntTerm("id", i)
                            .addIntTerm("groupId", i % 2)
                            .addStringTerm("strField", i % 2 == 0 ? "Hi" : "Hello")
                            .build()
            );
        }

        shards.add(new Dataset.DatasetShard("stringLen", "index20150101", flamdex));

        return new Dataset(shards);
    }
}
