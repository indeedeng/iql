package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.Options;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StringLenTest extends BasicTest {

    private void doTest(String query, List<List<String>> expected) throws Exception {
        QueryServletTestUtils.testAll(MultiValuedDataset.create(), expected, query, Options.create(), LanguageVersion.IQL2);
    }

    @Test
    public void testStringLen() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "350")); //"Hi".length() * 50 + "Hello".length() * 50
        doTest("from dataset yesterday today select len(strField)", expected);
    }

    @Test
    public void testStringLenGroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "100"));//"Hi".length() * 50
        expected.add(ImmutableList.of("1", "250"));//"Hello".length() * 50
        doTest("from dataset yesterday today group by groupId select len(strField)", expected);
    }

    private static class MultiValuedDataset {
        public static List<Shard> create() {
            final List<Shard> result = new ArrayList<>();
            final MemoryFlamdex flamdex = new MemoryFlamdex();
            for (int i = 0; i < 100; i++) {
                flamdex.addDocument(
                        new FlamdexDocument.Builder()
                                .addIntTerm("id", i)
                                .addIntTerm("groupId", i % 2)
                                .addStringTerm("strField", i % 2 == 0 ? "Hi" : "Hello")
                                .build()
                );
            }

            result.add(new Shard("dataset", "index20150101", flamdex));

            return result;
        }
    }
}
