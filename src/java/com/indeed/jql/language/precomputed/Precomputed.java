package com.indeed.jql.language.precomputed;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.commands.Command;
import com.indeed.jql.language.commands.GetGroupDistincts;
import com.indeed.jql.language.commands.GetGroupPercentiles;
import com.indeed.jql.language.commands.GetGroupStats;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public interface Precomputed {
    Precomputation commands(Set<String> scope);

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
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupDistincts(scope, field, filter, windowSize.or(1)));
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

        @Override
        public String toString() {
            return "PrecomputedDistinct{" +
                    "field='" + field + '\'' +
                    ", filter=" + filter +
                    ", windowSize=" + windowSize +
                    '}';
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
        public Precomputation commands(Set<String> scope) {
            return Precomputation.noContext(new GetGroupPercentiles(scope, field, new double[]{percentile}));
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

        @Override
        public String toString() {
            return "PrecomputedPercentile{" +
                    "field='" + field + '\'' +
                    ", percentile=" + percentile +
                    '}';
        }
    }

    class PrecomputedRawStats implements Precomputed {
        private final DocMetric docMetric;

        public PrecomputedRawStats(DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public Precomputation commands(Set<String> scope) {
            AggregateMetric metric = null;
            for (final String dataset : scope) {
                final AggregateMetric aMetric = new AggregateMetric.DocStatsPushes(dataset, docMetric.getPushes(dataset));
                if (metric == null) {
                    metric = aMetric;
                } else {
                    metric = new AggregateMetric.Add(aMetric, metric);
                }
            }
            return Precomputation.noContext(new GetGroupStats(Collections.singletonList(metric), false));
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

        @Override
        public String toString() {
            return "PrecomputedRawStats{" +
                    "docMetric=" + docMetric +
                    '}';
        }
    }

    class Precomputation {
        public final List<Command> beforeCommands;
        public final Command computationCommand;
        public final List<Command> afterCommands;

        public Precomputation(List<Command> beforeCommands, Command computationCommand, List<Command> afterCommands) {
            this.beforeCommands = beforeCommands;
            this.computationCommand = computationCommand;
            this.afterCommands = afterCommands;
        }

        public static Precomputation noContext(Command command) {
            return new Precomputation(Collections.<Command>emptyList(), command, Collections.<Command>emptyList());
        }
    }
}
