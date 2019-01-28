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

package com.indeed.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.GroupBys;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldResolver;
import com.indeed.iql2.language.query.fieldresolution.FieldResolverTest;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Consumer;

public class GroupByTest {
    public static final Consumer<String> WARN = s -> System.out.println("PARSE WARNING: " + s);
    private static final WallClock CLOCK = new StoppedClock(new DateTime(2015, 2, 1, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
    public static final FieldResolver FIELD_RESOLVER = FieldResolverTest.fromQuery("from organic 2d 1d");
    private static final Query.Context CONTEXT = new Query.Context(
            Collections.emptyList(),
            AllData.DATASET.getDatasetsMetadata(),
            null,
            WARN,
            CLOCK,
            new TracingTreeTimer(),
            FIELD_RESOLVER.universalScope(),
            new NullShardResolver()
    );

    public static final Function<JQLParser, GroupByEntry> PARSE_IQL1_GROUP_BY = new Function<JQLParser, GroupByEntry>() {
        @Override
        public GroupByEntry apply(JQLParser input) {
            return GroupBys.parseGroupByEntry(input.groupByEntry(true), CONTEXT);
        }
    };

    public static final Function<JQLParser, GroupByEntry> PARSE_IQL2_GROUP_BY = new Function<JQLParser, GroupByEntry>() {
        @Override
        public GroupByEntry apply(JQLParser input) {
            return GroupBys.parseGroupByEntry(input.groupByEntry(false), CONTEXT);
        }
    };

    @Test
    public void groupByMetric() throws Exception {
        final GroupByEntry bucketOji1to10by1 = new GroupByEntry(new GroupBy.GroupByMetric(new DocMetric.Field(FieldSet.of("organic", "oji")), 1, 10, 1, false, false), Optional.<AggregateFilter>absent(), Optional.<String>absent());
        Assert.assertEquals(bucketOji1to10by1, Queries.runParser("bucket(oji, 1, 10, 1)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(bucketOji1to10by1, Queries.runParser("bucket(oji, 1, 10, 1)", PARSE_IQL2_GROUP_BY));

        final GroupByEntry bucketOji1to10by2NoGutter = new GroupByEntry(new GroupBy.GroupByMetric(new DocMetric.Field(FieldSet.of("organic", "oji")), 1, 10, 2, true, false), Optional.<AggregateFilter>absent(), Optional.<String>absent());
        Assert.assertEquals(bucketOji1to10by2NoGutter, Queries.runParser("BUCKETS(oji, 1, 10, 2, true)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(bucketOji1to10by2NoGutter, Queries.runParser("BUCKETS(oji, 1, 10, 2, true)", PARSE_IQL2_GROUP_BY));

        final GroupByEntry withNegatives = new GroupByEntry(new GroupBy.GroupByMetric(new DocMetric.Field(FieldSet.of("organic", "oji")), -10, 10, 1, false, false), Optional.<AggregateFilter>absent(), Optional.<String>absent());
        Assert.assertEquals(withNegatives, Queries.runParser("BUCKETS(oji, -10, 10, 1)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(withNegatives, Queries.runParser("BUCKETS(oji, -10, 10, 1)", PARSE_IQL2_GROUP_BY));

        final GroupByEntry withDefault = new GroupByEntry(new GroupBy.GroupByMetric(new DocMetric.Field(FieldSet.of("organic", "oji")), -10, 10, 1, true, true), Optional.<AggregateFilter>absent(), Optional.<String>absent());
        Assert.assertEquals(withDefault, Queries.runParser("BUCKET(oji, -10, 10, 1) WITH DEFAULT", PARSE_IQL2_GROUP_BY));
    }
}
