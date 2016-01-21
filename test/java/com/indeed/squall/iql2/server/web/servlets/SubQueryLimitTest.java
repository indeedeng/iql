package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.Options;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubQueryLimitTest {
    @Test
    public void testQueryNoLimit() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(SubQueryDataset.create(), expected, "from dataset yesterday today where f in (from same group by f) select count()");
    }

    @Test
    public void testQueryLimitNotTriggered() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        QueryServletTestUtils.testIQL2(SubQueryDataset.create(), expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSubQueryTermLimit(200L));
        QueryServletTestUtils.testIQL2(SubQueryDataset.create(), expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSubQueryTermLimit(105L));
    }

    @Test
    public void testQueryLimitTriggered() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "105"));
        try {
            QueryServletTestUtils.testIQL2(SubQueryDataset.create(), expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSubQueryTermLimit(104L));
            Assert.fail();
        } catch (Exception e) {
        }
        try {
            QueryServletTestUtils.testIQL2(SubQueryDataset.create(), expected, "from dataset yesterday today where f in (from same group by f) select count()", Options.create().setSubQueryTermLimit(1L));
            Assert.fail();
        } catch (Exception e) {
        }
    }

    private static class SubQueryDataset {
        public static List<Shard> create() {
            final List<Shard> result = new ArrayList<>();
            final MemoryFlamdex flamdex = new MemoryFlamdex();
            for (int i = 0; i < 105; i++) {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("f", i);
                flamdex.addDocument(doc);
            }

            result.add(new Shard("dataset", "index20150101", flamdex));

            return result;
        }
    }
}
