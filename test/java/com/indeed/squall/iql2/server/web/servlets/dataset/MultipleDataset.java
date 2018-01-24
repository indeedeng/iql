package com.indeed.squall.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

public class MultipleDataset {
    public static Dataset create() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        {
            for (int i = 1; i <= 31; i++) {
                final Dataset.DatasetFlamdex flamdex1 = new Dataset.DatasetFlamdex();
                final FlamdexDocument doc1 = new FlamdexDocument();
                doc1.addIntTerm("intField1", 1);
                doc1.addIntTerm("intField2", i % 2);
                doc1.addStringTerm("strField1", Integer.toString(i % 2));
                flamdex1.addDocument(doc1);

                final Dataset.DatasetFlamdex flamdex2 = new Dataset.DatasetFlamdex();
                final FlamdexDocument doc2 = new FlamdexDocument();
                doc2.addIntTerm("intField1", 1);
                doc2.addIntTerm("intField3", i % 3);
                doc2.addStringTerm("strField1", Integer.toString(i % 4));
                flamdex2.addDocument(doc2);
                if (i <= 30) {
                    shards.add(new Dataset.DatasetShard("dataset1", String.format("index201412%02d", i), flamdex1));
                    shards.add(new Dataset.DatasetShard("dataset2", String.format("index201412%02d", i), flamdex2));
                } else {
                    shards.add(new Dataset.DatasetShard("dataset1", "index20150101", flamdex1));
                    shards.add(new Dataset.DatasetShard("dataset2", "index20150101", flamdex2));
                }

            }
        }
        return new Dataset(shards);
    }
}
