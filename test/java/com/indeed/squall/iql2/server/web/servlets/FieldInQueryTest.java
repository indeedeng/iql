package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FieldInQueryTest {
    private static List<Shard> shardWithStringTerms(String dataset, String field, List<String> terms) {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        for (final String term : terms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>emptyMap(), Collections.singletonMap(field, Collections.singletonList(term))));
        }
        return ImmutableList.of(new Shard(dataset, "index20150101", flamdex));
    }

    @Test
    public void fieldInQueryStrings() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4", "1"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        final List<Shard> allShards = new ArrayList<>();
        allShards.addAll(OrganicDataset.create());
        allShards.addAll(shardWithStringTerms("other", "thefield", Arrays.asList("a", "b")));
        QueryServletTestUtils.testIQL2(allShards, expected, "from organic yesterday today where tk in (from other 1d 0d group by thefield) group by tk with default select count(), distinct(tk)");
    }

    @Test
    public void fieldNotInQueryStrings() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "4", "1"));
        expected.add(ImmutableList.of("d", "141", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        final List<Shard> allShards = new ArrayList<>();
        allShards.addAll(OrganicDataset.create());
        allShards.addAll(shardWithStringTerms("other", "thefield", Arrays.asList("a", "b")));
        QueryServletTestUtils.testIQL2(allShards, expected, "from organic yesterday today where tk not in (from other 1d 0d group by thefield) group by tk with default select count(), distinct(tk)");
    }

    private static List<Shard> shardWithIntTerms(String dataset, String field, List<Integer> terms) {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        for (final Integer term : terms) {
            flamdex.addDocument(new FlamdexDocument(Collections.<String, LongList>singletonMap(field, new LongArrayList(new long[]{term})), Collections.<String, List<String>>emptyMap()));
        }
        return ImmutableList.of(new Shard(dataset, "index20150101", flamdex));
    }

    @Test
    public void fieldInQueryInts() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "3"));
        expected.add(ImmutableList.of("10", "2", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        final List<Shard> allShards = new ArrayList<>();
        allShards.addAll(OrganicDataset.create());
        allShards.addAll(shardWithIntTerms("other", "thefield", Arrays.asList(1, 10)));
        QueryServletTestUtils.testIQL2(allShards, expected, "from organic yesterday today where ojc in (from other 1d 0d group by thefield) group by ojc with default select count(), distinct(tk)");
    }

    @Test
    public void fieldNotInQueryInts() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "2"));
        expected.add(ImmutableList.of("2", "1", "1"));
        expected.add(ImmutableList.of("3", "60", "1"));
        expected.add(ImmutableList.of("5", "1", "1"));
        expected.add(ImmutableList.of("15", "1", "1"));
        expected.add(ImmutableList.of("DEFAULT", "0", "0"));
        final List<Shard> allShards = new ArrayList<>();
        allShards.addAll(OrganicDataset.create());
        allShards.addAll(shardWithIntTerms("other", "thefield", Arrays.asList(1, 10)));
        QueryServletTestUtils.testIQL2(allShards, expected, "from organic yesterday today where ojc not in (from other 1d 0d group by thefield) group by ojc with default select count(), distinct(tk)");
    }
}
