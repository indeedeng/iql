package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.TestImhotepClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import com.indeed.squall.iql2.server.web.model.IQLDB;
import com.indeed.squall.iql2.server.web.model.Limits;
import com.indeed.squall.iql2.server.web.model.RunningQueriesManager;
import com.indeed.squall.iql2.server.web.model.SelectQuery;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.Shard;
import com.indeed.squall.iql2.server.web.servlets.query.QueryServlet;
import com.indeed.squall.iql2.server.web.topterms.TopTermsCache;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryServletTestUtils extends BasicTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static IQLDB iqldb = Mockito.mock(IQLDB.class);

    public static QueryServlet create(List<Shard> shards, Options options) {
        final Long imhotepLocalTempFileSizeLimit = -1L;
        final Long imhotepDaemonTempFileSizeLimit = -1L;
        final ImhotepClient imhotepClient = new TestImhotepClient(shards);

        final MetadataCache metadataCache = new MetadataCache(options.imsClient, imhotepClient);
        metadataCache.updateMetadata();

        return new QueryServlet(
                imhotepClient,
                options.queryCache,
                new RunningQueriesManager(iqldb) {
                    @Override
                    public void register(SelectQuery selectQuery) {
                        super.register(selectQuery);
                        selectQuery.onStarted(DateTime.now());
                    }
                },
                metadataCache,
                new AccessControl(Collections.<String>emptySet(), Collections.<String>emptySet(),
                        null, new Limits(50, 50_000, 1000, 1000, 8, 8)),
                new TopTermsCache(imhotepClient, "", true),
                imhotepLocalTempFileSizeLimit,
                imhotepDaemonTempFileSizeLimit,
                options.wallClock,
                options.subQueryTermLimit
        );
    }

    @SuppressWarnings("WeakerAccess")
    public enum LanguageVersion {
        IQL1, IQL2
    }

    static List<List<String>> runQuery(List<Shard> shards, String query, LanguageVersion version, boolean stream, Options options) throws Exception {
        return run(shards, query, version, stream, options).data;
    }


    static JsonNode getQueryHeader(List<Shard> shards, String query, LanguageVersion version, Options options) throws Exception {
        return run(shards, query, version, true, options).header;
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryResult run(List<Shard> shards, String query, LanguageVersion version, boolean stream, Options options) throws Exception {
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
        JsonNode header = null;
        if (stream) {
            boolean readingData = false;
            boolean readingError = false;
            boolean readingHeader = false;
            final List<String> errorLines = new ArrayList<>();
            for (final String line : Splitter.on('\n').split(response.getContentAsString())) {
                if (line.startsWith("event: resultstream")) {
                    readingData = true;
                } else if (line.startsWith("event: servererror")) {
                    readingError = true;
                } else if (line.startsWith("event: header")) {
                    readingHeader = true;
                    readingData = false;
                } else if (line.startsWith("event: ")) {
                    readingData = false;
                    if (readingError) {
                        throw new IllegalArgumentException("Error encountered when running query: " + Joiner.on('\n').join(errorLines));
                    }
                } else if (readingData && line.startsWith("data: ")) {
                    output.add(Lists.newArrayList(Splitter.on('\t').split(line.substring("data: ".length()))));
                } else if (readingError && line.startsWith("data: ")) {
                    errorLines.add(line.substring("data: ".length()));
                } else if (readingHeader && line.startsWith("data: ")) {
                    header = OBJECT_MAPPER.readTree(line.substring("data: ".length()));
                    readingHeader = false;
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
        return new QueryResult(header, output);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Options {
        private Long subQueryTermLimit = -1L;
        private QueryCache queryCache = new NoOpQueryCache();
        private ImsClientInterface imsClient;
        private boolean skipTestDimension = false;
        private WallClock wallClock = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());

        Options() {
        }

        public static Options create() {
            return new Options();
        }

        public static Options create(boolean skipTestDimension) {
            final Options options = create();
            options.skipTestDimension = skipTestDimension;
            return options;
        }

        public Options setImsClient(ImsClientInterface imsClient) {
            this.imsClient = imsClient;
            return this;
        }

        public Options setSkipTestDimension(boolean skipTestDimension) {
            this.skipTestDimension = skipTestDimension;
            return this;
        }

        public Options setSubQueryTermLimit(Long subQueryTermLimit) {
            this.subQueryTermLimit = subQueryTermLimit;
            return this;
        }

        public Options setQueryCache(QueryCache queryCache) {
            this.queryCache = queryCache;
            return this;
        }

        public Options setWallClock(final WallClock wallClock) {
            this.wallClock = wallClock;
            return this;
        }
    }

    static void testWarning(List<Shard> shards, List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        final JsonNode header = getQueryHeader(shards, query, version, Options.create());
        if (expectedWarnings.isEmpty()) {
            Assert.assertNull(header.get("IQL-Warning"));
        } else {
            Assert.assertArrayEquals(expectedWarnings.toArray(new String[expectedWarnings.size()]), header.get("IQL-Warning").textValue().split("\n"));
        }
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query) throws Exception {
        testWarning(dataset.getShards(), expectedWarnings, query, LanguageVersion.IQL1);
        testWarning(dataset.getShards(), expectedWarnings, query, LanguageVersion.IQL2);
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        testWarning(dataset.getShards(), expectedWarnings, query, version);
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL1(dataset, expected, query, false);
    }


    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, boolean testDimension) throws Exception {
        testIQL1(dataset, expected, query, Options.create(testDimension));
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(dataset.getShards(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL1(dataset.getDimensionShards(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL1(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, true, options));
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL2(dataset, expected, query, false);
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL2(dataset.getShards(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL2(dataset.getDimensionShards(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL2(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false, options));
        Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true, options));
    }

    static void runIQL2(List<Shard> shards, String query) throws Exception {
        runIQL2(shards, query, Options.create());
    }

    static void runIQL2(List<Shard> shards, String query, Options options) throws Exception {
        runQuery(shards, query, LanguageVersion.IQL2, false, options);
        runQuery(shards, query, LanguageVersion.IQL2, true, options);
    }

    static void runIQL1(List<Shard> shards, String query) throws Exception {
        runIQL1(shards, query, Options.create());
    }


    static void runIQL1(List<Shard> shards, String query, Options options) throws Exception {
        runQuery(shards, query, LanguageVersion.IQL1, false, options);
        runQuery(shards, query, LanguageVersion.IQL1, true, options);
    }

    static void runAll(List<Shard> shards, String query) throws Exception {
        runIQL1(shards, query);
        runIQL2(shards, query);
    }

    static void testAll(List<Shard> shards, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(shards, expected, query, options);
        testIQL2(shards, expected, query, options);
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testAll(dataset, expected, query, false);
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testAll(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testAll(dataset.getShards(), expected, query, options);
        if (!options.skipTestDimension) {
            testAll(dataset.getDimensionShards(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
        }
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

    @SuppressWarnings("WeakerAccess")
    public static class QueryResult {
        public JsonNode header;
        public List<List<String>> data;

        public QueryResult(final JsonNode header, final List<List<String>> data) {
            this.header = header;
            this.data = data;
        }
    }
}