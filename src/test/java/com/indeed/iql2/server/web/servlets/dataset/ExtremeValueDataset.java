package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtremeValueDataset {
    static Dataset create() {
        // Values are 0, Int.(MIN|MAX)_VALUE, Long.(MIN|MAX)_VALUE and +-1 of them.
        // Create 10 documents per value.
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final Set<Long> extremeValues = Stream
                .of(
                        0L,
                        (long) Integer.MIN_VALUE,
                        (long) Integer.MAX_VALUE,
                        Long.MAX_VALUE,
                        Long.MIN_VALUE
                )
                .flatMap(x -> Stream.of(x - 1, x, x + 1))
                .collect(Collectors.toSet());
        for (final long extremeValue : extremeValues) {
            for (int i = 0; i < 10; ++i) {
                final FlamdexDocument doc = new FlamdexDocument.Builder()
                        .addIntTerm("field", extremeValue)
                        .addIntTerm("replicationId", i)
                        .addIntTerm("primary", (i == 0) ? 1 : 0)
                        .build();
                flamdex.addDocument(doc);
            }
        }
        return new Dataset(ImmutableList.of(
                new Dataset.DatasetShard("extremevalue", "index20150101.00-20150102.00", flamdex)
        ));
    }
}
