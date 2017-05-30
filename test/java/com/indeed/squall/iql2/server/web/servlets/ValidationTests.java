package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.Options;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ValidationTests extends BasicTest {
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);

    private List<Dataset.DatasetShard> trivialOrganic() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, TIME_ZONE).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 1);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Dataset.DatasetShard("organic", "index20150101", flamdex)
        );
    }

    private List<Dataset.DatasetShard> trivialSponsored() {
        final Dataset.DatasetFlamdex flamdex = new Dataset.DatasetFlamdex();
        final FlamdexDocument doc = new FlamdexDocument();
        doc.addIntTerm("unixtime", new DateTime(2015, 1, 1, 0, 0, TIME_ZONE).getMillis() / 1000);
        doc.addIntTerm("clicked", 1);
        doc.addIntTerm("isOrganic", 0);
        flamdex.addDocument(doc);
        return Collections.singletonList(
            new Dataset.DatasetShard("sponsored", "index20150101", flamdex)
        );
    }

    @Test
    public void testBasicValidationPassing() throws Exception {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        shards.addAll(trivialOrganic());
        shards.addAll(trivialSponsored());
        final String basic =
                "FROM organic 2015-01-01 2015-01-02, sponsored " +
                "SELECT COUNT(), organic.COUNT(), sponsored.COUNT(), clicked, organic.clicked, sponsored.clicked, [organic.clicked], [sponsored.clicked]";
        final String aliases =
                "FROM organic 2015-01-01 2015-01-02 as o, sponsored as s " +
                        "SELECT COUNT(), o.COUNT(), s.COUNT(), clicked, o.clicked, s.clicked, [o.clicked], [s.clicked]";
        final String sameDataset =
                "FROM organic 2015-01-01 2015-01-02 as o1, organic as o2 " +
                        "SELECT COUNT(), o1.COUNT(), o2.COUNT(), clicked, o1.clicked, o2.clicked, [o1.clicked], [o2.clicked]";

        final List<List<String>> expected = ImmutableList.of(Arrays.asList("", "2", "1", "1", "2", "1", "1", "1", "1"));

        for (final String query : Arrays.asList(basic, aliases, sameDataset)) {
            QueryServletTestUtils.testIQL2(new Dataset(shards), expected, query, true);
        }
    }

    @Test
    public void testBasicValidationRejecting() throws Exception {
        final List<Dataset.DatasetShard> shards = new ArrayList<>();
        shards.addAll(trivialOrganic());
        shards.addAll(trivialSponsored());
        final String query =
                "FROM organic 2015-01-01 2015-01-02, sponsored " +
                "SELECT [organic.clicked + sponsored.clicked]";
        try {
            QueryServletTestUtils.runQuery(new Dataset(shards).getShards(), query, QueryServletTestUtils.LanguageVersion.IQL2, false, Options.create());
            Assert.fail();
        } catch (Exception ignored) {
        }
    }
}
