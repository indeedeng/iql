package com.indeed.jql.language.query;

import com.google.common.base.Optional;
import com.indeed.jql.language.DocMetric;

public interface GroupBy<F,M> {

    class GroupByMetric<F,M> implements GroupBy<F,M> {
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

    class GroupByTime<F,M> implements GroupBy<F,M> {
        private final long periodMillis;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTime(long periodMillis, Optional<String> field, Optional<String> format) {
            this.periodMillis = periodMillis;
            this.field = field;
            this.format = format;
        }
    }

    class GroupByTimeBuckets<F,M> implements GroupBy<F,M> {
        private final int numBuckets;
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByTimeBuckets(int numBuckets, Optional<String> field, Optional<String> format) {
            this.numBuckets = numBuckets;
            this.field = field;
            this.format = format;
        }
    }

    class GroupByMonth<F,M> implements GroupBy<F,M> {
        private final Optional<String> field;
        private final Optional<String> format;

        public GroupByMonth(Optional<String> field, Optional<String> format) {
            this.field = field;
            this.format = format;
        }
    }

    class GroupByField<F,M> implements GroupBy<F,M> {
        private final String field;
        private final Optional<F> filter;
        private final Optional<Long> limit;
        private final Optional<M> metric;
        private final boolean withDefault;

        public GroupByField(String field, Optional<F> filter, Optional<Long> limit, Optional<M> metric, boolean withDefault) {
            this.field = field;
            this.filter = filter;
            this.limit = limit;
            this.metric = metric;
            this.withDefault = withDefault;
        }
    }

    class GroupByDayOfWeek<F,M> implements GroupBy<F,M> {}
    class GroupBySessionName<F,M> implements GroupBy<F,M> {}

    class GroupByQuantiles<F,M> implements GroupBy<F,M> {
        private final String field;
        private final int numBuckets;

        public GroupByQuantiles(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }
    }
}
