package com.indeed.jql.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;

public interface GroupBy {
    
    GroupBy traverse(
            Function<GroupBy, GroupBy> groupBy, 
            Function<AggregateMetric, AggregateMetric> f, 
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    );

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
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(new GroupByMetric(metric.traverse(g, i), min, max, interval, excludeGutters));
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
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
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
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
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
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }
    }

    class GroupByField implements GroupBy {
        private final String field;
        private final Optional<AggregateFilter> filter;
        private final Optional<Long> limit;
        private final Optional<AggregateMetric> metric;
        private final boolean withDefault;

        public GroupByField(String field, Optional<AggregateFilter> filter, Optional<Long> limit, Optional<AggregateMetric> metric, boolean withDefault) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.metric = metric;
            this.withDefault = withDefault;
        }

        @Override
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse(f,g,h,i));
            } else {
                filter = Optional.absent();
            }
            final Optional<AggregateMetric> metric;
            if (this.metric.isPresent()) {
                metric = Optional.of(this.metric.get().traverse(f,g,h,i));
            } else {
                metric = Optional.absent();
            }
            return groupBy.apply(new GroupByField(field, filter, limit, metric, withDefault));
        }
    }

    class GroupByDayOfWeek implements GroupBy {
        @Override
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }
    }

    class GroupBySessionName implements GroupBy {
        @Override
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
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
        public GroupBy traverse(Function<GroupBy, GroupBy> groupBy, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return groupBy.apply(this);
        }
    }
}
