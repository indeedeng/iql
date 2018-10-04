package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jwolfe
 */
public class GroupBySelectDataset {
    static Dataset createDataset() {
        final List<Dataset.DatasetShard> dataset = new ArrayList<>();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        flamdex.addDocument(new FlamdexDocument(
                Collections.emptyMap(),
                ImmutableMap.of(
                    "select",
                    ImmutableList.of("a")
                )
        ));
        flamdex.addDocument(new FlamdexDocument(
                Collections.emptyMap(),
                ImmutableMap.of(
                    "select",
                    ImmutableList.of("b")
                )
        ));
        dataset.add(new Dataset.DatasetShard("groupBySelect", "index20150101", flamdex));
        return new Dataset(dataset);
    }
}
