package com.indeed.jql.language.precomputed;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.DocMetric;

import java.util.Objects;

public interface Precomputed {
    class PrecomputedDistinct implements Precomputed {
        private final String field;
        private final Optional<AggregateFilter> filter;
        private final Optional<Integer> windowSize;

        public PrecomputedDistinct(String field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedDistinct that = (PrecomputedDistinct) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(windowSize, that.windowSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, filter, windowSize);
        }
    }

    class PrecomputedPercentile implements Precomputed {
        private final String field;
        private final double percentile;

        public PrecomputedPercentile(String field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedPercentile that = (PrecomputedPercentile) o;
            return Objects.equals(percentile, that.percentile) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, percentile);
        }
    }

    class PrecomputedRawStats implements Precomputed {
        private final DocMetric docMetric;

        public PrecomputedRawStats(DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedRawStats that = (PrecomputedRawStats) o;
            return Objects.equals(docMetric, that.docMetric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docMetric);
        }
    }
}
