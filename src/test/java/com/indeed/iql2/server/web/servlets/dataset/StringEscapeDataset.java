package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.List;

public class StringEscapeDataset {
    private StringEscapeDataset() {
    }

    static Dataset create() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        flamdex.addDocument(createDoc("xy", 1));
        flamdex.addDocument(createDoc("x\\y", 2));
        flamdex.addDocument(createDoc("x\\\\y", 3));

        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        shards.add(new Dataset.DatasetShard("escape", "index20150101", flamdex));

        return new Dataset(shards);
    }

    private static FlamdexDocument createDoc(final String term, final long value) {
        return new FlamdexDocument.Builder()
                .addStringTerm("term", term)
                .addIntTerm("value", value)
                .build();
    }
}