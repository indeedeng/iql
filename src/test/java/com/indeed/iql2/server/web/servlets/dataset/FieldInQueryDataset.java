package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author jwolfe
 */
public class FieldInQueryDataset {
    public static List<Dataset> createAll() {
        final List<Dataset> result = new ArrayList<>();
        result.add(create("other1", "thefield", Collections.emptyList(), Arrays.asList("a", "b")));
        result.add(create("other2", "thefield", Collections.emptyList(), Arrays.asList("a", "b")));
        result.add(create("other3", "thefield", Arrays.asList(1, 10), Collections.emptyList()));
        result.add(create("other4", "thefield", Arrays.asList(1, 10), Collections.emptyList()));
        return result;
    }

    public static Dataset create(final String dataset, final String field, final List<Integer> intTerms, final List<String> stringTerms) {
        final Dataset organicDataset = OrganicDataset.create();

        final List<Dataset.DatasetShard> shards = Lists.newArrayList();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (final String term : stringTerms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>emptyMap(), Collections.singletonMap(field, Collections.singletonList(term))));
        }
        for (final Integer term : intTerms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>singletonMap(field, new LongArrayList(new long[]{term})), Collections.emptyMap()));
        }
        shards.add(new Dataset.DatasetShard(dataset, "index20150101", flamdex));
        shards.addAll(organicDataset.shards);
        return new Dataset(shards);
    }
}
