package com.indeed.jql.language.query;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocMetric;

public interface GroupBy {

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
    }

    class GroupByMonth implements GroupBy {
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
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
    }

    class GroupByDayOfWeek implements GroupBy {}
    class GroupBySessionName implements GroupBy {}

    class GroupByQuantiles implements GroupBy {
        private final String field;
        private final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }
    }
}
