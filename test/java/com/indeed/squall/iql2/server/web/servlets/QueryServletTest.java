package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.TestImhotepClient;
import com.indeed.squall.iql2.server.dimensions.DimensionsLoader;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.squall.iql2.server.web.topterms.TopTermsCache;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryServletTest {
    public static class Shard {
        public final String dataset;
        public final String shardId;
        public final MemoryFlamdex flamdex;

        public Shard(String dataset, String shardId, MemoryFlamdex flamdex) {
            this.dataset = dataset;
            this.shardId = shardId;
            this.flamdex = flamdex;
        }
    }

    private static QueryServlet create(List<Shard> shards) {
        final Long imhotepLocalTempFileSizeLimit = -1L;
        final Long imhotepDaemonTempFileSizeLimit = -1L;
        final ImhotepClient imhotepClient = new TestImhotepClient(shards);
        final ExecutionManager executionManager = new ExecutionManager();

        try {
            final Field maxQueriesPerUser = ExecutionManager.class.getDeclaredField("maxQueriesPerUser");
            maxQueriesPerUser.setAccessible(true);
            maxQueriesPerUser.setInt(executionManager, 1);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return new QueryServlet(
                imhotepClient,
                new NoOpQueryCache(),
                executionManager,
                new DimensionsLoader("", null),
                new KeywordAnalyzerWhitelistLoader("", null, imhotepClient),
                new AccessControl(Collections.<String>emptySet()),
                new TopTermsCache(imhotepClient, "", true),
                imhotepLocalTempFileSizeLimit,
                imhotepDaemonTempFileSizeLimit,
                new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis())
        );
    }

    private enum LanguageVersion {
        IQL1, IQL2
    }

    public static List<List<String>> runQuery(List<Shard> shards, String query, LanguageVersion version, boolean stream) throws Exception {
        final QueryServlet queryServlet = create(shards);
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", stream ? "text/event-stream" : "");
        request.addParameter("username", "fakeUsername");
        request.addParameter("client", "test");
        switch (version) {
            case IQL1:
                request.addParameter("v", "1");
                break;
            case IQL2:
                request.addParameter("v", "2");
                break;
        }
        final MockHttpServletResponse response = new MockHttpServletResponse();
        queryServlet.query(request, response, query);

        final List<List<String>> output = new ArrayList<>();
        if (stream) {
            boolean readingData = false;
            boolean readingError = false;
            final List<String> errorLines = new ArrayList<>();
            for (final String line : Splitter.on('\n').split(response.getContentAsString())) {
                if (line.startsWith("event: resultstream")) {
                    readingData = true;
                } else if (line.startsWith("event: servererror")) {
                    readingError = true;
                } else if (line.startsWith("event: ")) {
                    readingData = false;
                    if (readingError) {
                        throw new IllegalArgumentException("Error encountered when running query: " + Joiner.on('\n').join(errorLines));
                    }
                } else if (readingData && line.startsWith("data: ")) {
                    output.add(Lists.newArrayList(Splitter.on('\t').split(line.substring("data: ".length()))));
                } else if (readingError && line.startsWith("data: ")) {
                    errorLines.add(line.substring("data: ".length()));
                }
            }

            if (readingError) {
                throw new IllegalArgumentException("Error encountered when running query: " + Joiner.on('\n').join(errorLines));
            }
        } else {
            if (response.getStatus() == 200) {
                for (final String line : Splitter.on('\n').split(response.getContentAsString())) {
                    if (!line.isEmpty()) {
                        output.add(Lists.newArrayList(Splitter.on('\t').split(line)));
                    }
                }
            } else {
                throw new IllegalArgumentException("Error encountered when running query: " + response.getContentAsString());
            }
        }
        return output;
    }

    private static void testAll(List<List<String>> expected, String query) throws Exception {
        Assert.assertEquals(expected, runQuery(OrganicDataset.create(), query, LanguageVersion.IQL1, false));
        Assert.assertEquals(expected, runQuery(OrganicDataset.create(), query, LanguageVersion.IQL1, true));
        Assert.assertEquals(expected, runQuery(OrganicDataset.create(), query, LanguageVersion.IQL2, false));
        Assert.assertEquals(expected, runQuery(OrganicDataset.create(), query, LanguageVersion.IQL2, true));
    }

    private static List<List<String>> withoutLastColumn(List<List<String>> input) {
        final List<List<String>> output = new ArrayList<>();
        for (final List<String> row : input) {
            if (row.isEmpty()) {
                output.add(row);
            } else {
                output.add(row.subList(0, row.size() - 1));
            }
        }
        return output;
    }

    @Test
    public void testUngrouped() throws Exception {
        final List<List<String>> expected = ImmutableList.<List<String>>of(ImmutableList.of("", "151", "2653", "306", "4"));
        testAll(expected, "from organic yesterday today select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(withoutLastColumn(expected), "from organic yesterday today select count(), oji, ojc");
    }

    @Test
    public void testTimeRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1180", "45", "3"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "600", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "600", "180", "1"));
        for (int i = 3; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", String.valueOf(i), "1", "1"));
        }
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "23", "1", "1"));

        testAll(expected, "from organic yesterday today group by time(1h) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
    }

    @Test
    public void testBasicFilters() throws Exception {
        testAll(ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"a\" select count()");
        testAll(ImmutableList.<List<String>>of(ImmutableList.of("", "2")), "from organic yesterday today where tk=\"b\" select count()");
        testAll(ImmutableList.<List<String>>of(ImmutableList.of("", "4")), "from organic yesterday today where tk=\"c\" select count()");
        testAll(ImmutableList.<List<String>>of(ImmutableList.of("", "141")), "from organic yesterday today where tk=\"d\" select count()");
    }
}