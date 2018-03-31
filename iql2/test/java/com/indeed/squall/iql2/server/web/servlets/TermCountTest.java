package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TermCountTest extends BasicTest {
    @Test
    public void testIntTermCount() throws Exception {
        final Dataset dataset = MultiValuedDataset.create();

        final List<List<String>> expected1 = new ArrayList<>();
        expected1.add(ImmutableList.of("", "33"));
        QueryServletTestUtils.testIQL2(dataset, expected1, "from dataset yesterday today where inttermcount(intField) = 1", true);

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "50"));
        QueryServletTestUtils.testIQL2(dataset, expected2, "from dataset yesterday today where inttermcount(intField) = 2", true);

        final List<List<String>> expected3 = new ArrayList<>();
        expected3.add(ImmutableList.of("", "17"));
        QueryServletTestUtils.testIQL2(dataset, expected3, "from dataset yesterday today where inttermcount(intField) = 3", true);
    }

    @Test
    public void testStrTermCount() throws Exception {
        final Dataset dataset = MultiValuedDataset.create();

        final List<List<String>> expected1 = new ArrayList<>();
        expected1.add(ImmutableList.of("", "33"));
        QueryServletTestUtils.testIQL2(dataset, expected1, "from dataset yesterday today where strtermcount(strField) = 1", true);

        final List<List<String>> expected2 = new ArrayList<>();
        expected2.add(ImmutableList.of("", "50"));
        QueryServletTestUtils.testIQL2(dataset, expected2, "from dataset yesterday today where strtermcount(strField) = 2", true);

        final List<List<String>> expected3 = new ArrayList<>();
        expected3.add(ImmutableList.of("", "17"));
        QueryServletTestUtils.testIQL2(dataset, expected3, "from dataset yesterday today where strtermcount(strField) = 3", true);
    }

    private static class MultiValuedDataset {
        public static Dataset create() {
            final List<Dataset.DatasetShard> shards = new ArrayList<>();
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 100; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("id", i);
                doc.addIntTerm("intField", 1);
                doc.addStringTerm("strField", "1");
                if (i % 2 == 0) {
                    doc.addIntTerm("intField", 2);
                    doc.addStringTerm("strField", "2");
                }
                if (i % 3 == 0) {
                    doc.addIntTerm("intField", 3);
                    doc.addStringTerm("strField", "3");
                }
                flamdex.addDocument(doc);
            }

            shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));

            return new Dataset(shards);
        }
    }
}
