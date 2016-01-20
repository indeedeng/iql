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
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
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

public class QueryServletTestUtils {

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

    enum LanguageVersion {
        IQL1, IQL2
    }

    static List<List<String>> runQuery(List<Shard> shards, String query, LanguageVersion version, boolean stream) throws Exception {
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

    static void testIQL2(List<Shard> shards, List<List<String>> expected, String query) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true));
    }

    static void testAll(List<Shard> shards, List<List<String>> expected, String query) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, false));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, true));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true));
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