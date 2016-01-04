package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ValidationTests {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    private List<Shard> trivialOrganic() {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 1);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Shard("organic", "index20150101", flamdex)
        );
    }

    private List<Shard> trivialSponsored() {
        final MemoryFlamdex flamdex = new MemoryFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 0);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Shard("sponsored", "index20150101", flamdex)
        );
    }

    @Test
    public void testBasicValidationPassing() throws Exception {
        final List<Shard> shards = new ArrayList<>();
        shards.addAll(trivialOrganic());
        shards.addAll(trivialSponsored());
        final String query =
                "FROM organic 2015-01-01 2015-01-02, sponsored " +
                "SELECT COUNT(), organic.COUNT(), sponsored.COUNT(), clicked, organic.clicked, sponsored.clicked, [organic.clicked], [sponsored.clicked]";
        final List<List<String>> expected = ImmutableList.of(Arrays.asList("", "2", "1", "1", "2", "1", "1", "1", "1"));

        Assert.assertEquals(
                expected,
                QueryServletTestUtils.runQuery(shards, query, QueryServletTestUtils.LanguageVersion.IQL2, false)
        );
        Assert.assertEquals(
                expected,
                QueryServletTestUtils.runQuery(shards, query, QueryServletTestUtils.LanguageVersion.IQL2, true)
        );
    }

    @Test
    public void testBasicValidationRejecting() throws Exception {
        final List<Shard> shards = new ArrayList<>();
        shards.addAll(trivialOrganic());
        shards.addAll(trivialSponsored());
        final String query =
                "FROM organic 2015-01-01 2015-01-02, sponsored " +
                "SELECT [organic.clicked + sponsored.clicked]";
        try {
            QueryServletTestUtils.runQuery(shards, query, QueryServletTestUtils.LanguageVersion.IQL2, false);
            Assert.fail();
        } catch (Exception ignored) {
        }
    }
}
