package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.iql2.server.web.servlets.dataset.Dataset.DatasetShard;
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
    static Dataset createAll() {
        final List<DatasetShard> shards = new ArrayList<>();
        shards.addAll(create("other1", "thefield", Collections.emptyList(), Arrays.asList("a", "b")));
        shards.addAll(create("other3", "thefield", Arrays.asList(1, 10), Collections.emptyList()));

        {
            final int count = 10000;
            final List<Integer> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(i);
            }
            shards.addAll(create("manyValues", "thefield", values, Collections.emptyList()));
        }
        return new Dataset(shards);
    }

    private static List<DatasetShard> create(final String dataset, final String field, final List<Integer> intTerms, final List<String> stringTerms) {
        final List<DatasetShard> shards = Lists.newArrayList();

        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        for (final String term : stringTerms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>emptyMap(), Collections.singletonMap(field, Collections.singletonList(term))));
        }
        for (final Integer term : intTerms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>singletonMap(field, new LongArrayList(new long[]{term})), Collections.emptyMap()));
        }
        shards.add(new DatasetShard(dataset, "index20150101", flamdex));
        return shards;
    }
}
