package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql2.server.web.servlets.BasicTest;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */
public class QuantilesDataset {
    public static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        shards.addAll(createShards("quantiles_mandatory", FieldType.MANDATORY));
        shards.addAll(createShards("quantiles_optional", FieldType.OPTIONAL));
        shards.addAll(createShards("quantiles_multi_value", FieldType.MULTI_VALUE));
        return new Dataset(shards);
    }

    private static List<Dataset.DatasetShard> createShards(final String datasetName, final FieldType fieldType) {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (int i = 0; i < 100; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            switch (fieldType) {
                case MANDATORY:
                    doc.addIntTerm("f", i);
                    break;
                case OPTIONAL:
                    if ((i % 2) == 0) {
                        doc.addIntTerm("f", i);
                    }
                    break;
                case MULTI_VALUE:
                    // re-distribute [0, 100)
                    if ((i % 5) == 0) {
                        doc.addIntTerm("f", i);
                        doc.addIntTerm("f", i+1);
                    } else if((i % 5) == 2) {
                        doc.addIntTerm("f", i);
                        doc.addIntTerm("f", i+1);
                        doc.addIntTerm("f", i+2);
                    }
                    break;
            }
            doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, BasicTest.TIME_ZONE).getMillis() / 1000);
            doc.addIntTerm("fakeField", 0);
            flamdex.addDocument(doc);
        }

        shards.add(new Dataset.DatasetShard(datasetName, "index20150101", flamdex));

        return shards;
    }

    public enum FieldType {
        MANDATORY,
        OPTIONAL,
        MULTI_VALUE;
    }
}
