package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MultiValueRegroupTest extends BasicTest {
    @Test
    public void testTopKLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from dataset yesterday today group by f[2] select count()", true);
    }

    @Test
    public void testFieldInLast() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "100"));
        expected.add(ImmutableList.of("2", "50"));
        QueryServletTestUtils.testAll(createDataset(), expected, "from dataset yesterday today group by f in (1,2) select count()", true);
    }

    @Test
    public void testGroupByMultiValueInIQL1() throws Exception {
        QueryServletTestUtils.testIQL1(createDataset(),
                ImmutableList.of(ImmutableList.of("1", "50")),
                "from dataset yesterday today where f in (1,2) i=1 GROUP BY f", true);
        QueryServletTestUtils.testIQL1(createDataset(),
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50")
                ),
                "from dataset yesterday today where f in (1,2) GROUP BY i, f", true);
        QueryServletTestUtils.testIQL1(createDataset(),
                ImmutableList.of(),
                "from dataset yesterday today where f in (1) f in (2) i = 1 GROUP BY f", true);
    }

    @Test
    public void testGroupByMultiValueInIQL2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "50"));
        expected.add(ImmutableList.of("3", "17"));
        QueryServletTestUtils.testIQL2(createDataset(), expected, "from dataset yesterday today where f in (1,2) i=1 GROUP BY f", true);

        QueryServletTestUtils.testIQL2(createDataset(),
                ImmutableList.of(
                        ImmutableList.of("0", "1", "50"),
                        ImmutableList.of("1", "1", "50"),
                        ImmutableList.of("0", "2", "50"),
                        ImmutableList.of("0", "3", "17"),
                        ImmutableList.of("1", "3", "17")
                ),
                "from dataset yesterday today where f in (1,2) GROUP BY i, f", true);
    }

    static Dataset createDataset() {
        final List<Dataset.DatasetShard> shards = Lists.newArrayList();
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();

        for (int i = 0; i < 100; i++) {
            final FlamdexDocument doc = new FlamdexDocument();
            doc.addIntTerm("f", 1);
            if (i % 2 == 0) {
                doc.addIntTerm("f", 2);
            }
            if (i % 3 == 0) {
                doc.addIntTerm("f", 3);
            }
            doc.addIntTerm("i", i % 2);
            flamdex.addDocument(doc);
        }
        shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));
        return new Dataset(shards);
    }
}
