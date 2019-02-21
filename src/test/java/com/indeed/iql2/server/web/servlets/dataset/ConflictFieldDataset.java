package com.indeed.iql2.server.web.servlets.dataset;

import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConflictFieldDataset {
    static Dataset create() {
        final List<Dataset.DatasetShard> result = new ArrayList<>();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument intDocument = new FlamdexDocument();
        intDocument.addIntTerms("fieldName", new long[] {1L, 2L, 3L});
        flamdex.addDocument(intDocument);
        final FlamdexDocument strDocument = new FlamdexDocument();
        strDocument.addStringTerms("fieldName", Arrays.asList("A", "B", "C"));
        flamdex.addDocument(strDocument);
        result.add(new Dataset.DatasetShard("conflict", "index20150101", flamdex));

        return new Dataset(result);
    }
}
