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

package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.TestImhotepClient;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.imhotep.web.AccessControl;
import com.indeed.imhotep.web.FieldFrequencyCache;
import com.indeed.imhotep.web.IQLDB;
import com.indeed.imhotep.web.Limits;
import com.indeed.imhotep.web.RunningQueriesManager;
import com.indeed.imhotep.web.TopTermsCache;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.Shard;
import com.indeed.squall.iql2.server.web.servlets.query.QueryServlet;
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

public class QueryServletTestUtils extends BasicTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static IQLDB iqldb;

    // This is list of not-production-ready features which are available only with "... OPTIONS['xxx']"
    // Add here features you want to test.
    // Each tested query will run with each option from this list.
    // Be sure not to delete empty string (no options) from list.
    private static final List<String> OPTIONS_TO_TEST = Lists.newArrayList("");

    public static QueryServlet create(List<Shard> shards, Options options) {
        final ImhotepClient imhotepClient = new TestImhotepClient(shards);

        final MetadataCache metadataCache = new MetadataCache(options.imsClient, imhotepClient, new FieldFrequencyCache(null));
        metadataCache.updateMetadata();
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb);

        return new QueryServlet(
                imhotepClient,
                options.queryCache,
                runningQueriesManager,
                metadataCache,
                new AccessControl(Collections.<String>emptySet(), Collections.<String>emptySet(),
                        null, new Limits(50, options.subQueryTermLimit.intValue(), 1000, 1000, 2, 8)),
                new TopTermsCache(imhotepClient, "", true, false),
                MetricStatsEmitter.NULL_EMITTER,
                options.wallClock,
				new FieldFrequencyCache(null));
    }

    @SuppressWarnings("WeakerAccess")
    public enum LanguageVersion {
        IQL1, IQL2
    }

    static List<List<String>> runQuery(List<Shard> shards, String query, LanguageVersion version, boolean stream, Options options, String optionToTest) throws Exception {
        final String queryWithOptions = optionToTest.isEmpty() ? query : (query + " OPTIONS['" + optionToTest + "']");
        return run(shards, queryWithOptions, version, stream, options).data;
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
        for (final String queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL1, true, options, queryOptions));
        }
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
        for (final String queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(shards, query, LanguageVersion.IQL2, true, options, queryOptions));
        }
    }

    static void runIQL2(List<Shard> shards, String query) throws Exception {
        runIQL2(shards, query, Options.create());
    }

    static void runIQL2(List<Shard> shards, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            runQuery(shards, query, LanguageVersion.IQL2, false, options, queryOptions);
            runQuery(shards, query, LanguageVersion.IQL2, true, options, queryOptions);
        }
    }

    static void runIQL1(List<Shard> shards, String query) throws Exception {
        runIQL1(shards, query, Options.create());
    }


    static void runIQL1(List<Shard> shards, String query, Options options) throws Exception {
        for (final String queryOptions : OPTIONS_TO_TEST) {
            runQuery(shards, query, LanguageVersion.IQL1, false, options, queryOptions);
            runQuery(shards, query, LanguageVersion.IQL1, true, options, queryOptions);
        }
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