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

package com.indeed.iql2.language.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.DefaultWallClock;
import com.indeed.util.core.time.StoppedClock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class QueriesTest {
    private static final Set<String> NO_OPTIONS = Collections.emptySet();

    @Test
    public void testSplitQuery() {
        final StoppedClock clock = new StoppedClock(DateTime.parse("2015-01-02").withZoneRetainFields(DateTimeZone.forOffsetHours(-6)).getMillis());
        final DatasetsMetadata datasetsMetadata = AllData.DATASET.getDatasetsMetadata();
        {
            final String query = "FROM organic 1d 0d";
            Assert.assertEquals(
                    new SplitQuery("organic 1d 0d", "", "", "", "", ImmutableList.of("", "count()"),
                            ImmutableList.of(), ImmutableList.of(), "organic",
                            "2015-01-01T00:00:00.000-06:00", "1d", "2015-01-02T00:00:00.000-06:00", "0d",
                            extractDatasetsHelper(query, false)),
                    Queries.parseSplitQuery(query, true, NO_OPTIONS, clock, datasetsMetadata));
        }
        {
            String query = "FROM organic(ojc < 10 tk='a') 1d 0d WHERE ojc > 10 AND country='us' GROUP BY country[TOP BY ojc HAVING oji > 0], tk SELECT count(), oji";
            Assert.assertEquals(
                    new SplitQuery("organic(ojc < 10 tk='a') 1d 0d",
                            "ojc > 10 AND country='us'", "country[TOP BY ojc HAVING oji > 0], tk", "count(), oji", "",
                            extractHeadersHelper(query, false),
                            ImmutableList.of("country[TOP BY ojc HAVING oji > 0]", "tk"), ImmutableList.of("count()", "oji"), "organic",
                            "2015-01-01T00:00:00.000-06:00", "1d", "2015-01-02T00:00:00.000-06:00", "0d",
                            extractDatasetsHelper(query, false)),
                    Queries.parseSplitQuery(query, false, NO_OPTIONS, clock, datasetsMetadata));
        }
        {
            String query = "FROM organic 1d 0d WHERE ojc > 10 AND country='us' GROUP BY country[TOP BY ojc HAVING oji > 0] AS myalias, tk SELECT count(), oji";
            Assert.assertEquals(
                    new SplitQuery("organic 1d 0d",
                            "ojc > 10 AND country='us'", "country[TOP BY ojc HAVING oji > 0] AS myalias, tk", "count(), oji", "",
                            Lists.newArrayList("myalias", "tk", "count()", "oji"),
                            ImmutableList.of("country[TOP BY ojc HAVING oji > 0] AS myalias", "tk"), ImmutableList.of("count()", "oji"), "organic",
                            "2015-01-01T00:00:00.000-06:00", "1d", "2015-01-02T00:00:00.000-06:00", "0d",
                            extractDatasetsHelper(query, false)),
                    Queries.parseSplitQuery(query, false, NO_OPTIONS, clock, datasetsMetadata));
        }
        {
            final String query = "FROM organic 1d 0d /* mid */, sponsored /* after */ " +
                    "WHERE /* before */ ojc > 10 /* mid */ OR country='us' /* after */ " +
                    "GROUP BY /* before */ country /* mid */, tk /* after */ " +
                    "SELECT /* before */ count() /* num */, oji /* impression */";
            Assert.assertEquals(
                    new SplitQuery("organic 1d 0d /* mid */, sponsored /* after */",
                            "/* before */ ojc > 10 /* mid */ OR country='us' /* after */",
                            "/* before */ country /* mid */, tk /* after */",
                            "/* before */ count() /* num */, oji /* impression */", "",
                            ImmutableList.of("country", "tk", "count()", "oji"),
                            ImmutableList.of("country", "tk"), ImmutableList.of("count()", "oji"),
                            "", "", "", "", "",
                            extractDatasetsHelper(query, false)),
                    Queries.parseSplitQuery(query, false, NO_OPTIONS, clock, datasetsMetadata));
        }
    }

    @Test
    public void extractHeaders() throws Exception {
        final boolean useLegacy = false;
        Assert.assertEquals(ImmutableList.of("", "count()"),
                extractHeadersHelper("FROM organic 1d 0d", useLegacy));
        Assert.assertEquals(ImmutableList.of("oji", "count()"),
                extractHeadersHelper("FROM organic 1d 0d GROUP BY oji", useLegacy));
        Assert.assertEquals(ImmutableList.of("oji", "total_count", "o1.count()", "add"),
                extractHeadersHelper("FROM organic 1d 0d as o1, organic as o2 GROUP BY oji " +
                        "SELECT count() as total_count, o1.count(), o1.oji+o2.ojc as add", useLegacy));
        Assert.assertEquals(ImmutableList.of("myalias", "ojc", "total_count"),
                extractHeadersHelper("FROM organic 1d 0d GROUP BY oji AS myalias, ojc " +
                        "SELECT count() as total_count", useLegacy));
        Assert.assertEquals(ImmutableList.of("myalias", "ojc", "total_count"),
                extractHeadersHelper("FROM organic 1d 0d as o1, organic as o2 GROUP BY oji HAVING (oji > ojc) AS myalias, ojc " +
                        "SELECT count() as total_count", useLegacy));
    }

    private List<String> extractHeadersHelper(final String q, final boolean useLegacy) {
        final Query query = Queries.parseQuery(q, useLegacy, AllData.DATASET.getDatasetsMetadata(), NO_OPTIONS, new DefaultWallClock()).query;
        return Queries.extractHeaders(query);
    }

    @Test
    public void extractDatasets() {
        Assert.assertEquals(
                ImmutableList.of(new SplitQuery.Dataset("jobsearch", "", "1d", "0d", "j1", "")),
                extractDatasetsHelper("FROM jobsearch 1d 0d as j1", true));
        Assert.assertEquals(
                ImmutableList.of(new SplitQuery.Dataset("jobsearch", "oji=1 tk='1'", "1d", "0d", "", "ALIASING(oji as o, ojc as c)")),
                extractDatasetsHelper("FROM jobsearch(oji=1 tk='1') 1d 0d ALIASING(oji as o, ojc as c)", false));
        Assert.assertEquals(
                ImmutableList.of(
                        new SplitQuery.Dataset("jobsearch", "oji=1", "1d", "0d", "j1", ""),
                        new SplitQuery.Dataset("jobsearch", "", "2d", "1d", "j2", ""),
                        new SplitQuery.Dataset("mobsearch", "", "1d", "0d", "", "ALIASING(ojc as c)")
                ),
                extractDatasetsHelper("FROM /* dumb */ jobsearch(oji=1) 1d 0d as j1, jobsearch 2d 1d as j2 /* dumb */, mobsearch ALIASING(ojc as c) /* dumb */ GROUP BY oji", false)
        );
    }

    private List<SplitQuery.Dataset> extractDatasetsHelper(final String q, final boolean useLegacy) {
        final JQLParser.QueryContext queryContext = Queries.parseQueryContext(q, useLegacy);
        return Queries.extractDatasets(queryContext.fromContents(), queryContext.start.getInputStream());
    }
}