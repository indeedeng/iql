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

import au.com.bytecode.opencsv.CSVReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.iql.Constants;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.AccessControl;
import com.indeed.iql.web.FieldFrequencyCache;
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
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import javax.annotation.Nullable;
import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.ResultFormat.CSV;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.ResultFormat.EVENT_STREAM;

public class QueryServletTestUtils extends BasicTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final ExecutorService executorService = new ThreadPoolExecutor(
                3, 20, 30,TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000),
                new NamedThreadFactory("IQL-Worker")
        );

    public static final boolean FAST_TEST = "1".equals(System.getenv("FAST_TEST"));

    // This is list of not-production-ready features which are available only with "... OPTIONS['xxx']"
    // Add here features you want to test.
    // Each tested query will run with each option from this list.
    // Be sure not to delete empty set (no options) from the list to test main execution path.
    private static final List<Set<String>> OPTIONS_TO_TEST;

    static {
        if (FAST_TEST) {
            OPTIONS_TO_TEST = ImmutableList.of(ImmutableSet.of());
        } else {
            OPTIONS_TO_TEST = ImmutableList.of(
                    ImmutableSet.of(),
                    ImmutableSet.of(QueryOptions.PARANOID),
                    ImmutableSet.of(QueryOptions.Experimental.PWHERE),
                    ImmutableSet.of(QueryOptions.Experimental.FTGS_POOLED_CONNECTION)
            );
        }
    }

    public static QueryServlet create(ImhotepClient client, Options options, final IQL2Options defaultOptions) {
        final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(options.imsClient, client, "", new FieldFrequencyCache(null));
        metadataCache.updateDatasets();
        final RunningQueriesManager runningQueriesManager = new RunningQueriesManager(null, Integer.MAX_VALUE);

        return new QueryServlet(
                options.tmpDir,
                client,
                metadataCache,
                new TopTermsCache(client, "", true, false),
                options.queryCache,
                runningQueriesManager,
                executorService,
                new AccessControl(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                        null, new Limits((byte)0, 50, options.subQueryTermLimit.intValue(), 1000, 1000, 2, 8),
                        Collections.emptySet(), Collections.emptySet()),
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
        IQL1_LEGACY_MODE, // legacy mode in IQL2
        IQL2;

        public void addRequestParameters(final MockHttpServletRequest request) {
            switch (this) {
                case IQL1_LEGACY_MODE:
                    request.addParameter("v", "1");
                    break;
                case IQL2:
                    request.addParameter("v", "2");
                    break;
            }
        }
    }

    public enum ResultFormat {
        TSV,
        CSV,
        EVENT_STREAM
    }

    static List<List<String>> runQuery(String query, LanguageVersion version, ResultFormat resultFormat, Options options, Set<String> extraQueryOptions) throws Exception {
        return runQuery(AllData.DATASET.getNormalClient(), query, version, resultFormat, options, extraQueryOptions);
    }

    static List<List<String>> runQuery(ImhotepClient client, String query, LanguageVersion version, ResultFormat resultFormat, Options options, Set<String> extraQueryOptions) throws Exception {
        final IQL2Options defaultOptions = new IQL2Options();
        defaultOptions.addOptions(extraQueryOptions);
        return run(client, query, version, resultFormat, options, defaultOptions).data;
    }

    static JsonNode getQueryHeader(
            final ImhotepClient client,
            final String query,
            final LanguageVersion version,
            final Options options,
            final ResultFormat resultFormat
    ) throws Exception {
        return run(client, query, version, resultFormat, options, new IQL2Options()).header;
    }

    static JsonNode getQueryHeader(final String query, final LanguageVersion version) throws Exception {
        return getQueryHeader(AllData.DATASET.getNormalClient(), query, version, Options.create(), EVENT_STREAM);
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryResult run(
            final ImhotepClient client,
            final String query,
            final LanguageVersion version,
            final ResultFormat resultFormat,
            final Options options,
            final IQL2Options defaultOptions
    ) throws Exception {
        final QueryServlet queryServlet = create(client, options, defaultOptions);
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final boolean stream = resultFormat.equals(EVENT_STREAM);
        request.addHeader("Accept", stream ? "text/event-stream" : "");
        request.addParameter("username", "fakeUsername");
        request.addParameter("client", "test");

        final boolean csv = resultFormat.equals(CSV);

        if (csv) {
            request.addParameter("csv", "1");
        }
        if (options.headOnly) {
            request.addParameter("head", "1");
        }
        if (options.getVersion) {
            request.addParameter("getversion", "1");
        }
        version.addRequestParameters(request);
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
                if (csv) {
                    try (final CSVReader csvReader = new CSVReader(new StringReader(response.getContentAsString()))) {
                        csvReader.readAll().forEach(line -> output.add(Arrays.asList(line)));
                    }
                } else {
                    for (final String line : Splitter.on('\n').split(response.getContentAsString())) {
                        if (!line.isEmpty()) {
                            output.add(Lists.newArrayList(Splitter.on('\t').split(line)));
                        }
                    }
                }
                final Map<String, String> headers = new HashMap<>();
                for (final String name : response.getHeaderNames()) {
                    headers.put(name, response.getHeader(name));
                }
                header = OBJECT_MAPPER.valueToTree(headers);
            } else {
                throw new IllegalArgumentException("Error encountered when running query: " + response.getContentAsString());
            }
        }
        return new QueryResult(header, output);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Options {
        private Dataset dataset = AllData.DATASET;
        @Nullable
        private File tmpDir = null; // null == system default temporary directory
        private Long subQueryTermLimit = 1_000_000L;
        private QueryCache queryCache = CollisionCheckingQueryCache.INSTANCE;
        private ImsClientInterface imsClient;
        private boolean skipTestDimension = FAST_TEST;
        private WallClock wallClock = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE).getMillis());
        @Nullable
        private Long maxCacheQuerySizeLimitBytes;
        private boolean skipCsv;
        private boolean onlyCsv;
        private boolean headOnly;
        private boolean getVersion;

        Options() {
        }

        public static Options create() {
            return new Options();
        }

        public Options copy() {
            final Options copy = new Options();
            copy.dataset = dataset;
            copy.tmpDir = tmpDir;
            copy.subQueryTermLimit = subQueryTermLimit;
            copy.queryCache = queryCache;
            copy.imsClient = imsClient;
            copy.skipTestDimension = skipTestDimension;
            copy.wallClock = wallClock;
            copy.maxCacheQuerySizeLimitBytes = maxCacheQuerySizeLimitBytes;
            copy.skipCsv = skipCsv;
            copy.onlyCsv = onlyCsv;
            return copy;
        }

        public static Options create(boolean skipTestDimension) {
            final Options options = create();
            options.skipTestDimension = skipTestDimension;
            return options;
        }

        public Options setDataset(final Dataset dataset) {
            this.dataset = dataset;
            return this;
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

        public Options setSkipCsv(final boolean skipCsv) {
            this.skipCsv = skipCsv;
            return this;
        }

        public Options setOnlyCsv(final boolean onlyCsv) {
            this.onlyCsv = onlyCsv;
            return this;
        }

        public void setHeadOnly(final boolean headOnly) {
            this.headOnly = headOnly;
        }

        public void setGetVersion(final boolean getVersion) {
            this.getVersion = getVersion;
        }
    }

    static void testWarning(List<String> expectedWarnings, String query, LanguageVersion version) throws Exception {
        testWarning(expectedWarnings, query, version, Options.create());
    }

    static void testWarning(List<String> expectedWarnings, String query, LanguageVersion version, final Options options) throws Exception {
        expectedWarnings = expectedWarnings.stream().map(s -> "[\"" + s + "\"]").collect(Collectors.toList());
        Assert.assertEquals(expectedWarnings, getWarnings(query, version, options));
    }

    static List<String> getWarnings(final String query, final LanguageVersion version, final Options options) throws Exception {
        final ImhotepClient client = options.dataset.getNormalClient();
        final JsonNode header = getQueryHeader(client, query, version, options, EVENT_STREAM);
        if (header.get("IQL-Warning") == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(header.get("IQL-Warning").textValue().split("\n"));
        }
    }

    static void testWarning(List<String> expectedWarnings, String query) throws Exception {
        testWarning(expectedWarnings, query, Options.create());
    }

    static void testWarning(List<String> expectedWarnings, String query, final Options options) throws Exception {
        testWarning(expectedWarnings, query, LanguageVersion.IQL1_LEGACY_MODE, options);
        testWarning(expectedWarnings, query, LanguageVersion.IQL2, options);
    }

    // test legacy mode.
    static void testIQL1(List<List<String>> expected, String query) throws Exception {
        testIQL1(expected, query, false);
    }

    static void testIQL1(List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL1(expected, query, Options.create(skipTestDimension));
    }

    static void testIQL1(List<List<String>> expected, String query, Options options) throws Exception {
        final Dataset dataset = options.dataset;
        testIQL1(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL1(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL1(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            for (final ResultFormat resultFormat : ResultFormat.values()) {
                if (!shouldRun(options, resultFormat)) {
                    continue;
                }
                Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, resultFormat, options, queryOptions));
            }
        }
    }

    // test only IQL2
    static void testIQL2(List<List<String>> expected, String query) throws Exception {
        testIQL2(expected, query, false);
    }

    static void testIQL2(List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testIQL2(expected, query, Options.create(skipTestDimension));
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query) throws Exception {
        testIQL2(client, expected, query, Options.create());
    }

    static void testIQL2(List<List<String>> expected, String query, Options options) throws Exception {
        final Dataset dataset = options.dataset;
        testIQL2(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testIQL2(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testIQL2(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            for (final ResultFormat resultFormat : ResultFormat.values()) {
                if (!shouldRun(options, resultFormat)) {
                    continue;
                }
                Assert.assertEquals(expected, runQuery(client, query, LanguageVersion.IQL2, resultFormat, options, queryOptions));
            }
        }
    }

    static void runIQL2(String query) throws Exception {
        runIQL2(Options.create().dataset.getNormalClient(), query);
    }

    static void runIQL2(ImhotepClient client, String query) throws Exception {
        runIQL2(client, query, Options.create());
    }

    static void runIQL2(ImhotepClient client, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            for (final ResultFormat resultFormat : ResultFormat.values()) {
                if (!shouldRun(options, resultFormat)) {
                    continue;
                }
                runQuery(client, query, LanguageVersion.IQL2, resultFormat, options, queryOptions);
            }
        }
    }

    private static void runIQL1(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query, Options.create());
    }


    private static void runIQL1(ImhotepClient client, String query, Options options) throws Exception {
        for (final Set<String> queryOptions : OPTIONS_TO_TEST) {
            for (final ResultFormat resultFormat : ResultFormat.values()) {
                if (!shouldRun(options, resultFormat)) {
                    continue;
                }
                runQuery(client, query, LanguageVersion.IQL1_LEGACY_MODE, resultFormat, options, queryOptions);
            }
        }
    }

    private static boolean shouldRun(final Options options, final ResultFormat resultFormat) {
        if (CSV.equals(resultFormat) && !options.onlyCsv && FAST_TEST) {
            return false;
        }
        if (EVENT_STREAM.equals(resultFormat) && FAST_TEST) {
            return false;
        }
        if (options.skipCsv && CSV.equals(resultFormat)) {
            return false;
        }
        if (options.onlyCsv && !CSV.equals(resultFormat)) {
            return false;
        }
        return true;
    }

    static void runAll(ImhotepClient client, String query) throws Exception {
        runIQL1(client, query);
        runIQL2(client, query);
    }

    // test both language versions
    static void testAll(List<List<String>> expected, String query) throws Exception {
        testAll(expected, query, false);
    }

    static void testAll(List<List<String>> expected, String query, boolean skipTestDimension) throws Exception {
        testAll(expected, query, Options.create(skipTestDimension));
    }

    static void testAll(List<List<String>> expected, String query, Options options) throws Exception {
        final Dataset dataset = options.dataset;
        testAll(dataset.getNormalClient(), expected, query, options);
        if (!options.skipTestDimension) {
            testAll(dataset.getDimensionsClient(), expected, query, options.copy().setImsClient(dataset.getDimensionImsClient()));
        }
    }

    static void testAll(ImhotepClient client, List<List<String>> expected, String query, Options options) throws Exception {
        testIQL1(client, expected, query, options);
        testIQL2(client, expected, query, options);
    }

    static void expectException(String query, LanguageVersion version, Predicate<String> exceptionMessagePredicate) {
        final Options options = Options.create();
        expectException(query, version, options, exceptionMessagePredicate);
    }

    static <T extends Throwable> void expectException(final String query, final LanguageVersion version, final Matcher<Throwable> exceptionMatcher) {
        final Options options = Options.create();
        expectException(query, version, options, exceptionMatcher);
    }

    static void expectException(
            final String query,
            final LanguageVersion version,
            final Options options,
            final Matcher<Throwable> exceptionMatcher
    ) {
        final ImhotepClient client = options.dataset.getNormalClient();
        try {
            runQuery(client, query, version, EVENT_STREAM, options, Collections.emptySet());
            Assert.fail("No exception returned in expectException");
        } catch (final Exception e) {
            Assert.assertThat(e, exceptionMatcher);
        }
    }

    static void expectException(
            final String query,
            final LanguageVersion version,
            final Options options,
            final Predicate<String> exceptionMessagePredicate) {
        final ImhotepClient client = options.dataset.getNormalClient();
        try {
            runQuery(client, query, version, EVENT_STREAM, options, Collections.emptySet());
            Assert.fail("No exception returned in expectException");
        } catch (final Exception e) {
            Assert.assertTrue("Thrown exception message \"" + e.getMessage() + "\" failed to match predicate", exceptionMessagePredicate.test(e.getMessage()));
        }
    }

    static void expectExceptionAll(final String query, final Options options, final Predicate<String> exceptionMessagePredicate) {
        for (final LanguageVersion languageVersion: LanguageVersion.values()) {
            expectException(query, languageVersion, options, exceptionMessagePredicate);
        }
    }

    static void expectExceptionAll(final String query, final Predicate<String> exceptionMessagePredicate) {
        final Options options = Options.create();
        expectExceptionAll(query, options, exceptionMessagePredicate);
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

    private static class QueryResult {
        final JsonNode header;
        final List<List<String>> data;

        QueryResult(final JsonNode header, final List<List<String>> data) {
            this.header = header;
            this.data = data;
        }
    }
}