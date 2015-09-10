package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.execution.ExecutionStep;

import java.util.Set;

public interface GroupBy {
    
    GroupBy transform(
            Function<GroupBy, GroupBy> groupBy, 
            Function<AggregateMetric, AggregateMetric> f, 
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    );

    GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f);

    ExecutionStep executionStep(Set<String> scope);

    class GroupByMetric implements GroupBy {
        private final DocMetric metric;
        private final long min;
        private final long max;
        private final long interval;
        private final boolean excludeGutters;

        public GroupByMetric(DocMetric metric, long min, long max, long interval, boolean excludeGutters) {
            this.metric = metric;
            this.min = min;
            this.max = max;
            this.interval = interval;
            this.excludeGutters = excludeGutters;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByMetric(metric.transform(g, i), min, max, interval, excludeGutters));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeMetric(metric, min, max, interval, scope, excludeGutters);
        }

        @Override
        public String toString() {
            return "GroupByMetric{" +
                    "metric=" + metric +
                    ", min=" + min +
                    ", max=" + max +
                    ", interval=" + interval +
                    ", excludeGutters=" + excludeGutters +
                    '}';
        }
    }

    class GroupByTime implements GroupBy {
        private final long periodMillis;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTime(long periodMillis, Optional<String> field, Optional<String> format) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeTimePeriod(periodMillis, field, format);
        }

        @Override
        public String toString() {
            return "GroupByTime{" +
                    "periodMillis=" + periodMillis +
                    ", field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByTimeBuckets implements GroupBy {
        private final int numBuckets;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<String> field, Optional<String> format) {
            this.numBuckets = numBuckets;
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeTimeBuckets(numBuckets, field, format);
        }

        @Override
        public String toString() {
            return "GroupByTimeBuckets{" +
                    "numBuckets=" + numBuckets +
                    ", field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByMonth implements GroupBy {
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeMonthOfYear();
        }

        @Override
        public String toString() {
            return "GroupByMonth{" +
                    "field=" + field +
                    ", format=" + format +
                    '}';
        }
    }

    class GroupByField implements GroupBy {
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Long> limit;
        public final AggregateMetric metric;
        public final boolean withDefault;
        public final boolean forceNonStreaming;

        public GroupByField(String field, Optional<AggregateFilter> filter, Optional<Long> limit, Optional<AggregateMetric> metric, boolean withDefault, boolean forceNonStreaming) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.forceNonStreaming = forceNonStreaming;
            this.metric = metric.or(new AggregateMetric.DocStats(new DocMetric.Count()));
            this.withDefault = withDefault;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().transform(f, g, h, i, groupBy));
            } else {
                filter = Optional.absent();
            }
            final AggregateMetric metric = this.metric.transform(f, g, h, i, groupBy);
            return groupBy.apply(new GroupByField(field, filter, limit, Optional.of(metric), withDefault, forceNonStreaming));
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.absent();
            }
            final AggregateMetric metric = f.apply(this.metric);
            return new GroupByField(field, filter, limit, Optional.of(metric), withDefault, forceNonStreaming);
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeAndRegroup(field, filter, limit, metric, withDefault, forceNonStreaming);
        }

        @Override
        public String toString() {
            return "GroupByField{" +
                    "field='" + field + '\'' +
                    ", filter=" + filter +
                    ", limit=" + limit +
                    ", metric=" + metric +
                    ", withDefault=" + withDefault +
                    '}';
        }
    }

    class GroupByDayOfWeek implements GroupBy {
        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeDayOfWeek();
        }

        @Override
        public String toString() {
            return "GroupByDayOfWeek{}";
        }
    }

    class GroupBySessionName implements GroupBy {
        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodeSessionNames();
        }

        @Override
        public String toString() {
            return "GroupBySessionName{}";
        }
    }

    class GroupByQuantiles implements GroupBy {
        private final String field;
        private final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }

        @Override
        public GroupBy transform(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }

        @Override
        public GroupBy traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public ExecutionStep executionStep(Set<String> scope) {
            return new ExecutionStep.ExplodePerDocPercentile(field, numBuckets);
        }

        @Override
        public String toString() {
            return "GroupByQuantiles{" +
                    "field='" + field + '\'' +
                    ", numBuckets=" + numBuckets +
                    '}';
        }
    }
}
