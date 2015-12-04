package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.GroupBys;
import com.indeed.squall.iql2.language.query.Queries;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class GroupByTest {
    public static final Consumer<String> WARN = new Consumer<String>() {
        @Override
        public void accept(String s) {
            System.out.println("PARSE WARNING: " + s);
        }
    };

    public static final Function<JQLParser, GroupByMaybeHaving> PARSE_IQL1_GROUP_BY = new Function<JQLParser, GroupByMaybeHaving>() {
        @Override
        public GroupByMaybeHaving apply(JQLParser input) {
            return GroupBys.parseGroupByMaybeHaving(input.groupByElementWithHaving(true), Collections.<String, Set<String>>emptyMap(), Collections.<String, Set<String>>emptyMap(), WARN);
        }
    };

    public static final Function<JQLParser, GroupByMaybeHaving> PARSE_IQL2_GROUP_BY = new Function<JQLParser, GroupByMaybeHaving>() {
        @Override
        public GroupByMaybeHaving apply(JQLParser input) {
            return GroupBys.parseGroupByMaybeHaving(input.groupByElementWithHaving(false), Collections.<String, Set<String>>emptyMap(), Collections.<String, Set<String>>emptyMap(), WARN);
        }
    };

    @Test
    public void groupByMetric() throws Exception {
        final GroupByMaybeHaving bucketOji1to10by1 = new GroupByMaybeHaving(new GroupBy.GroupByMetric(new DocMetric.Field("OJI"), 1, 10, 1, false), Optional.<AggregateFilter>absent());
        Assert.assertEquals(bucketOji1to10by1, Queries.runParser("bucket(oji, 1, 10, 1)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(bucketOji1to10by1, Queries.runParser("bucket(oji, 1, 10, 1)", PARSE_IQL2_GROUP_BY));
        Assert.assertEquals(bucketOji1to10by1, Queries.runParser("oji from 1 to 10 by 1)", PARSE_IQL2_GROUP_BY));

        final GroupByMaybeHaving bucketOji1to10by2NoGutter = new GroupByMaybeHaving(new GroupBy.GroupByMetric(new DocMetric.Field("OJI"), 1, 10, 2, true), Optional.<AggregateFilter>absent());
        Assert.assertEquals(bucketOji1to10by2NoGutter, Queries.runParser("BUCKETS(oji, 1, 10, 2, true)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(bucketOji1to10by2NoGutter, Queries.runParser("BUCKETS(oji, 1, 10, 2, true)", PARSE_IQL2_GROUP_BY));

        final GroupByMaybeHaving withNegatives = new GroupByMaybeHaving(new GroupBy.GroupByMetric(new DocMetric.Field("OJI"), -10, 10, 1, false), Optional.<AggregateFilter>absent());
        Assert.assertEquals(withNegatives, Queries.runParser("BUCKETS(oji, -10, 10, 1)", PARSE_IQL1_GROUP_BY));
        Assert.assertEquals(withNegatives, Queries.runParser("BUCKETS(oji, -10, 10, 1)", PARSE_IQL2_GROUP_BY));
        Assert.assertEquals(withNegatives, Queries.runParser("oji FROM -10 TO 10 BY 1)", PARSE_IQL2_GROUP_BY));
    }
}
