package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.TestImhotepClient;
import com.indeed.squall.iql2.server.dimensions.DimensionsLoader;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.squall.iql2.server.web.servlets.query.QueryServlet;
import com.indeed.squall.iql2.server.web.topterms.TopTermsCache;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryServletTestUtils extends BasicTest {

    public static QueryServlet create(List<Shard> shards, Options options) {
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
                options.queryCache,
                executionManager,
                new DimensionsLoader("", null),
                new KeywordAnalyzerWhitelistLoader("", null, imhotepClient),
                new AccessControl(Collections.<String>emptySet()),
                new TopTermsCache(imhotepClient, "", true),
                imhotepLocalTempFileSizeLimit,
                imhotepDaemonTempFileSizeLimit,
                new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis()),
                options.subQueryTermLimit
        );
    }

    enum LanguageVersion {
        IQL1, IQL2
    }

    static List<List<String>> runQuery(List<Shard> shards, String query, LanguageVersion version, boolean stream, Options options) throws Exception {
        final QueryServlet queryServlet = create(shards, options);
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

    static class Options {
        public Long subQueryTermLimit = -1L;
        public QueryCache queryCache = new NoOpQueryCache();

        Options() {
        }

        public static Options create() {
            return new Options();
        }

        public Long getSubQueryTermLimit() {
            return subQueryTermLimit;
        }

        public Options setSubQueryTermLimit(Long subQueryTermLimit) {
            this.subQueryTermLimit = subQueryTermLimit;
            return this;
        }

        public QueryCache getQueryCache() {
            return queryCache;
        }

        public Options setQueryCache(QueryCache queryCache) {
            this.queryCache = queryCache;
            return this;
        }
    }

    static void testIQL1(List<Shard> shards, List<List<String>> expected, String query) throws Exception {
        testIQL1(shards, expected, query, Options.create());
    }

    private static void testIQL1(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, true, options));

    }

    static void testIQL2(List<Shard> shards, List<List<String>> expected, String query) throws Exception {
        testIQL2(shards, expected, query, Options.create());
    }

    static void testIQL2(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true, options));
    }

    static void testAll(List<Shard> shards, List<List<String>> expected, String query) throws Exception {
        testAll(shards, expected, query, Options.create());
    }

    static void testAll(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, true, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true, options));
    }

    static List<List<String>> withoutLastColumn(List<List<String>> input) {
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

    static List<List<String>> addConstantColumn(int index, String value, List<List<String>> oldData) {
        final List<List<String>> output = new ArrayList<>();
        for (final List<String> line : oldData) {
            final List<String> newLine = new ArrayList<>(line.size() + 1);
            newLine.addAll(line);
            newLine.add(index, value);
            output.add(newLine);
        }
        return output;
    }
}