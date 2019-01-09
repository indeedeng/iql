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
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import com.indeed.iql.web.config.IQLEnv;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    // Be sure not to delete empty set (no options) from the list to test main execution path.
    private static final List<Set<String>> OPTIONS_TO_TEST =
            ImmutableList.of(
                    Collections.emptySet(), // no options
                    ImmutableSet.of(QueryOptions.Experimental.USE_MULTI_FTGS), // multi FTGS
                    ImmutableSet.of(QueryOptions.Experimental.USE_AGGREGATE_DISTINCT) // aggregate distinct
            );

    public static QueryServlet create(ImhotepClient client, Options options, final IQL2Options defaultOptions) {
        final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(options.imsClient, client, "", new FieldFrequencyCache(null), true);
        metadataCache.updateDatasets();
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(iqldb, Integer.MAX_VALUE);

        return new QueryServlet(
                options.tmpDir,
                client,
                metadataCache,
                metadataCache,
                new TopTermsCache(client, "", true, false),
                options.queryCache,
                runningQueriesManager,
                executorService,
                new AccessControl(Collections.<String>emptySet(), Collections.<String>emptySet(),
                        null, new Limits(50, options.subQueryTermLimit.intValue(), 1000, 1000, 2, 8)),
                options.maxCacheQuerySizeLimitBytes,
                MetricStatsEmitter.NULL_EMITTER,
				new FieldFrequencyCache(null),
                options.wallClock,
                defaultOptions,
                IQLEnv.DEVELOPER
        );
    }

    @SuppressWarnings("WeakerAccess")
    public enum LanguageVersion {
        ORIGINAL_IQL1, // original IQL1
        IQL1_LEGACY_MODE, // legacy mode in IQL2
        IQL2
    }

    static List<List<String>> runQuery(ImhotepClient client, String query, LanguageVersion version, boolean stream, Options options, Set<String> extraQueryOptions) throws Exception {
        final IQL2Options defaultOptions = new IQL2Options();
        defaultOptions.addOptions(extraQueryOptions);
        return run(client, query, version, stream, options, defaultOptions).data;
    }

    private static JsonNode getQueryHeader(final ImhotepClient client, String query, LanguageVersion version, Options options) throws Exception {
        return run(client, query, version, true, options, new IQL2Options()).header;
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryResult run(ImhotepClient client, String query, LanguageVersion version, boolean stream, Options options, final IQL2Options defaultOptions) throws Exception {
        final QueryServlet queryServlet = create(client, options, defaultOptions);
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Accept", stream ? "text/event-stream" : "");
        request.addParameter("username", "fakeUsername");
        request.addParameter("client", "test");
        switch (version) {
            case ORIGINAL_IQL1:
                request.addParameter("v", "1");
                break;
            case IQL1_LEGACY_MODE:
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
        @Nullable
        private File tmpDir = null; // null == system default temporary directory
        private Long subQueryTermLimit = 1_000_000L;
        private QueryCache queryCache = new NoOpQueryCache();
        private ImsClientInterface imsClient;
        private boolean skipTestDimension = false;
        private WallClock wallClock = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
        @Nullable
        private Long maxCacheQuerySizeLimitBytes;

        Options() {
        }

        public static Options create() {
            return new Options();
        }

        public Options copy() {
            final Options copy = new Options();
            copy.tmpDir = tmpDir;
            copy.subQueryTermLimit = subQueryTermLimit;
            copy.queryCache = queryCache;
            copy.imsClient = imsClient;
            copy.skipTestDimension = skipTestDimension;
            copy.wallClock = wallClock;
            return copy;
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

        public Options setTmpDir(@Nullable final File tmpDir) {
            this.tmpDir = tmpDir;
            return this;
        }

        public Options setMaxCacheQuerySizeLimitBytes(@Nullable final Long maxCacheQuerySizeLimitBytes) {
            this.maxCacheQuerySizeLimitBytes = maxCacheQuerySizeLimitBytes;
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
            expectedWarnings = expectedWarnings.stream().map(s -> "[\"" + s + "\"]").collect(Collectors.toList());
            Assert.assertArrayEquals(expectedWarnings.toArray(new String[expectedWarnings.size()]), header.get("IQL-Warning").textValue().split("\n"));
        }
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query) throws Exception {
        testWarning(dataset.getNormalClient(), expectedWarnings, query, LanguageVersion.ORIGINAL_IQL1);
        testWarning(dataset.getNormalClient(), expectedWarnings, query, LanguageVersion.IQL1_LEGACY_MODE);
        testWarning(dataset.getNormalClient(), expectedWarnings, query, LanguageVersion.IQL2);
    }

    static void testWarning(Dataset dataset, List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        testWarning(dataset.getNormalClient(), expectedWarnings, query, version);
    }

    // test only original IQL1
    static void testOriginalIQL1(final Dataset dataset, final List<List<String>> expected, final String query) throws Exception {
        testOriginalIQL1(dataset, expected, query, false);
    }

    static void testOriginalIQL1(final Dataset dataset, final List<List<String>> expected, final String query, final boolean skipTestDimension) throws Exception {
        testOriginalIQL1(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testOriginalIQL1(final Dataset dataset, final List<List<String>> expected, final String query, final Options options) throws Exception {
        testOriginalIQL1(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testOriginalIQL1(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testOriginalIQL1(final ImhotepClient client, final List<List<String>> expected, final String query, final Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.ORIGINAL_IQL1, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.ORIGINAL_IQL1, true, options, queryOptions));
        }
    }

    // test only legacy mode in IQL2
    static void testIQL1LegacyMode(final Dataset dataset, final List<List<String>> expected, final String query) throws Exception {
        testIQL1LegacyMode(dataset, expected, query, false);
    }

    static void testIQL1LegacyMode(final Dataset dataset, final List<List<String>> expected, final String query, final boolean skipTestDimension) throws Exception {
        testIQL1LegacyMode(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL1LegacyMode(final Dataset dataset, final List<List<String>> expected, final String query, final Options options) throws Exception {
        testIQL1LegacyMode(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL1LegacyMode(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL1LegacyMode(final ImhotepClient client, final List<List<String>> expected, final String query, final Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, true, options, queryOptions));
        }
    }

    // test both original IQL1 and legacy mode.
    static void testIQL1(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL1(dataset, expected, query, false);
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL1(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL1(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL1(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL1(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        testOriginalIQL1(client, expected, query, options);
        testIQL1LegacyMode(client, expected, query, options);
    }

    // test legacy mode and IQL2.
    // testIQL2AndLegacy call means that there are some differences between legacy mode and original Iql1.
    // Each call must have explaining comment about diffs.
    static void testIQL2AndLegacy(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL2AndLegacy(dataset, expected, query, false);
    }

    static void testIQL2AndLegacy(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2AndLegacy(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL2AndLegacy(final Dataset dataset, final List<List<String>> expected, final String query, final Options options) throws Exception {
        testIQL1LegacyMode(dataset, expected, query, options);
        testIQL2(dataset, expected, query, options);
    }

    // test only IQL2
    static void testIQL2(List<List<String>> expected, String query) throws Exception {
        testIQL2(AllData.DATASET, expected, query);
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testIQL2(dataset, expected, query, false);
    }

    static void testIQL2(List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2(AllData.DATASET, expected, query, skipTestDimension);
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query) throws Exception {
        testIQL2(client, expected, query, Options.create());
    }

    static void testIQL2(List<List<String>> expected, String query, Options options) throws Exception {
        testIQL2(AllData.DATASET, expected, query, options);
    }

    static void testIQL2(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL2(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL2(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL2, false, options, queryOptions));
            Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL2, true, options, queryOptions));
        }
    }

    static void runIQL2(ImhotepClient client, String query) throws Exception {
        runIQL2(client, query, Options.create());
    }

    static void runIQL2(ImhotepClient client, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            runQuery(client, query, LanguageVersion.IQL2, false, options, queryOptions);
            runQuery(client, query, LanguageVersion.IQL2, true, options, queryOptions);
        }
    }

    private static void runIQL1(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query, Options.create());
    }


    private static void runIQL1(ImhotepClient client, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            runQuery(client, query, LanguageVersion.ORIGINAL_IQL1, false, options, queryOptions);
            runQuery(client, query, LanguageVersion.ORIGINAL_IQL1, true, options, queryOptions);
            runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, false, options, queryOptions);
            runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, true, options, queryOptions);
        }
    }

    static void runAll(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query);
        runIQL2(client, query);
    }

    // test all 3 language versions
    static void testAll(Dataset dataset, List<List<String>> expected, String query) throws Exception {
        testAll(dataset, expected, query, false);
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testAll(dataset, expected, query, Options.create(skipTestDimension));
    }

    static void testAll(Dataset dataset, List<List<String>> expected, String query, Options options) throws Exception {
        testAll(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testAll(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testAll(List<List<String>> expected, String query, Options options) throws Exception {
        testAll(AllData.DATASET, expected, query, options);
    }

    static void testAll(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        testOriginalIQL1(client, expected, query, options);
        testIQL1LegacyMode(client, expected, query, options);
        testIQL2(client, expected, query, options);
    }

    static void expectException(Dataset dataset, String query, LanguageVersion version, Predicate<String> exceptionMessagePredicate) {
        final ImhotepClient client = dataset.getNormalClient();
        try {
            runQuery(client, query, version, true, Options.create(), Collections.emptySet());
            Assert.fail("No exception returned in expectException");
        } catch (Exception e) {
            Assert.assertTrue(exceptionMessagePredicate.apply(e.getMessage()));
        }
    }

    static void expectExceptionAll(Dataset dataset, String query, Predicate<String> exceptionMessagePredicate) {
        for (LanguageVersion languageVersion: LanguageVersion.values()) {
            expectException(dataset, query, languageVersion, exceptionMessagePredicate);
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