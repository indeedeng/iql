/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.AccessControl;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql.web.IQLDB;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql.web.RunningQueriesManager;
import com.indeed.iql.web.TopTermsCache;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QueryServletTestUtils extends BasicTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static IQLDB iqldb;

    private static ExecutorService executorService = new ThreadPoolExecutor(
                3, 20, 30,TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000),
                new NamedThreadFactory("IQL-Worker")
        );

    // This is list of not-production-ready features which are available only with "... OPTIONS['xxx']"
    // Add here features you want to test.
    // Each tested query will run with each option from this list.
    // Be sure not to delete empty string (no options) from the list to test main execution path.
    private static final String[] OPTIONS_TO_TEST =
            {
                    "", // no options
                    "OPTIONS [\"" + QueryOptions.Experimental.USE_MULTI_FTGS + "\"]", // multi FTGS
                    "OPTIONS [\"" + QueryOptions.Experimental.USE_AGGREGATE_DISTINCT + "\"]", // aggregate distinct
            };

    public static QueryServlet create(ImhotepClient client, Options options) {
        final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(options.imsClient, client, "", new FieldFrequencyCache(null), true);
        metadataCache.updateDatasets();
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb);

        return new QueryServlet(
                client,
                metadataCache,
                metadataCache,
                new TopTermsCache(client, "", true, false),
                options.queryCache,
                runningQueriesManager,
                executorService,
                new AccessControl(Collections.<String>emptySet(), Collections.<String>emptySet(),
                        null, new Limits(50, options.subQueryTermLimit.intValue(), 1000, 1000, 2, 8)),
                MetricStatsEmitter.NULL_EMITTER,
				new FieldFrequencyCache(null),
                options.wallClock);
    }

    @SuppressWarnings("WeakerAccess")
    public enum LanguageVersion {
        IQL1, IQL2
    }

    static List<List<String>> runQuery(ImhotepClient client, String query, LanguageVersion version, boolean stream, Options options, String optionsToTest) throws Exception {
        final String queryWithOptions = optionsToTest.isEmpty() ? query : (query + " " + optionsToTest);
        return run(client, queryWithOptions, version, stream, options).data;
    }

    private static JsonNode getQueryHeader(final ImhotepClient client, String query, LanguageVersion version, Options options) throws Exception {
        return run(client, query, version, true, options).header;
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryResult run(ImhotepClient client, String query, LanguageVersion version, boolean stream, Options options) throws Exception {
        final QueryServlet queryServlet = create(client, options);
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", stream ? "text/event-stream" : "");
        request.addParameter("username", "fakeUsername");
        request.addParameter("client", "test");
        switch (version) {
            case IQL1:
                request.addParameter("v", "1");
                request.addParameter("legacymode", "1");
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

    static void testWarning(ImhotepClient client, List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        final JsonNode header = getQueryHeader(client, query, version, Options.create());
        if (expectedWarnings.isEmpty()) {
            try {
                Assert.assertNull(header.get("IQL-Warning"));
            } catch (Error e) {
                System.out.println("oh no");
            }
        } else {
            Assert.assertArrayEquals(expectedWarnings.toArray(new String[expectedWarnings.size()]), header.get("IQL-Warning").textValue().split("\n"));
        }
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query) throws Exception {
        testWarning(dataset.getNormalClient(), expectedWarnings, query, LanguageVersion.IQL1);
        testWarning(dataset.getNormalClient(), expectedWarnings, query, LanguageVersion.IQL2);
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        testWarning(dataset.getNormalClient(), expectedWarnings, query, version);
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL1(dataset, expected, query, false);
    }


    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, boolean testDimension) throws Exception {
        testIQL1(dataset, expected, query, Options.create(testDimension));
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL1(dataset.getDimensionsClient(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL1(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL1, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL1, true, options, queryOptions));
        }
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL2(dataset, expected, query, false);
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query) throws Exception {
        testIQL2(client, expected, query, Options.create());
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL2(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL2(dataset.getDimensionsClient(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL2, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL2, true, options, queryOptions));
        }
    }

    static void runIQL2(ImhotepClient client, String query) throws Exception {
        runIQL2(client, query, Options.create());
    }

    static void runIQL2(ImhotepClient client, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            runQuery(client, query, LanguageVersion.IQL2, false, options, queryOptions);
            runQuery(client, query, LanguageVersion.IQL2, true, options, queryOptions);
        }
    }

    private static void runIQL1(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query, Options.create());
    }


    private static void runIQL1(ImhotepClient client, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            runQuery(client, query, LanguageVersion.IQL1, false, options, queryOptions);
            runQuery(client, query, LanguageVersion.IQL1, true, options, queryOptions);
        }
    }

    static void runAll(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query);
        runIQL2(client, query);
    }

    static void testAll(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(client, expected, query, options);
        testIQL2(client, expected, query, options);
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testAll(dataset, expected, query, false);
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testAll(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testAll(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testAll(dataset.getDimensionsClient(), expected, query, options.setImsClient(dataset.getDimensionImsClient()));
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