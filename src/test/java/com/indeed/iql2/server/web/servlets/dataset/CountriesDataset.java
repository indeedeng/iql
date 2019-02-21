package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.List;
import java.util.Random;

public class CountriesDataset {
    private static final ImmutableList<String> SOME_COUNTRIES =ImmutableList.of(
            "ae", "aq", "ar", "at", "au", "be", "bh", "br", "ca", "ch", "cl",
            "cn", "co", "cr", "cz", "de", "dk", "ec", "eg", "es", "fi", "fr",
            "gb", "gr", "hk", "hu", "id", "ie", "il", "in", "it", "jp", "kr",
            "kw", "lu", "ma", "mx", "my", "ng", "nl", "no", "nz", "om", "pa",
            "pe", "ph", "pk", "pl", "pt", "qa", "ro", "ru", "sa", "se", "sg",
            "th", "tr", "tw", "ua", "us", "uy", "ve", "vn", "za"
    );

    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        final Random rng = new Random(1234L);

        for (final String country : SOME_COUNTRIES) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addStringTerm("country", country);
            final int r = rng.nextInt(10);
            doc.addIntTerm("random", r);
            flamdex.addDocument(doc);
        }

        shards.add(new Dataset.DatasetShard("countries", "index20150101.00", flamdex));
        return new Dataset(shards);
    }
}
