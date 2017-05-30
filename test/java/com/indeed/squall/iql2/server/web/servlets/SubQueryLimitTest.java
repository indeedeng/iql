package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.Options;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubQueryLimitTest extends BasicTest {
    final Dataset dataset = SubQueryDataset.create();
    @Test
    public void testQueryNoLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from dataset yesterday today where f in (from same group by f) select count()", true);
    }

    @Test
    public void testQueryLimitNotTriggered() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSkipTestDimension(true).setSubQueryTermLimit(200L));
        QueryServletTestUtils.testIQL2(dataset, expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSkipTestDimension(true).setSubQueryTermLimit(105L));
    }

    @Test
    public void testQueryLimitTriggered() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        try {
            QueryServletTestUtils.testIQL2(dataset, expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSkipTestDimension(true).setSubQueryTermLimit(104L));
            Assert.fail();
        } catch (Exception e) {
        }
        try {
            QueryServletTestUtils.testIQL2(dataset, expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSkipTestDimension(true).setSubQueryTermLimit(1L));
            Assert.fail();
        } catch (Exception e) {
        }
    }

    private static class SubQueryDataset {
        public static Dataset create() {
            final List<Dataset.DatasetShard> shards = Lists.newArrayList();
            final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
            for (int i = 0; i < 105; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("f", i);
                flamdex.addDocument(doc);
            }

            shards.add(new Dataset.DatasetShard("dataset", "index20150101", flamdex));
            return new Dataset(shards);
        }
    }
}
